//  Copyright (c) 2017-2018 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package imports

import (
	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/utils"
	"math"
	"sync"
)

const maxBatchesPerFile = 5000
const commitInterval = 100

type upsertBatchBundle struct {
	batch       *common.UpsertBatch
	kafkaOffset int64
}

// kafkaPartitionReader is kafka partition level consumer, also implementation of RedoLogManager
type kafkaPartitionReader struct {
	sync.RWMutex

	Topic     string `json:"topic"`
	TableName string `json:"table"`
	Shard     int    `json:"shard"`

	// MaxEventTime per virtual redolog file
	MaxEventTimePerFile map[int64]uint32 `json:"maxEventTimePerFile"`
	// FirstKafkaOffset per virtual redolog file
	FirstKafkaOffsetPerFile map[int64]int64 `json:"firstKafkaOffsetPerFile"`
	SizePerFile             map[int64]int   `json:"sizePerFile"`
	TotalRedologSize        int             `json:"totalRedologSize"`

	consumer          sarama.Consumer
	partitionConsumer sarama.PartitionConsumer

	done    chan struct{}
	batches chan upsertBatchBundle

	commitFunc     func(offset int64) error
	checkPointFunc func(offset int64) error

	count int
}

// NewKafkaRedologManager creates kafka redolog manager
func NewKafkaPartitionReader(namespace, table string, shard int, consumer sarama.Consumer, commitFunc func(offset int64) error, checkPointFunc func(offset int64) error) *kafkaPartitionReader {
	topic := utils.GetTopicFromTable(namespace, table)
	return &kafkaPartitionReader{
		TableName:               table,
		Shard:                   shard,
		Topic:                   topic,
		consumer:                consumer,
		commitFunc:              commitFunc,
		checkPointFunc:          checkPointFunc,
		MaxEventTimePerFile:     make(map[int64]uint32),
		FirstKafkaOffsetPerFile: make(map[int64]int64),
		SizePerFile:             make(map[int64]int),
		done:                    make(chan struct{}),
		batches:                 make(chan upsertBatchBundle, 1),
	}
}

// AppendToRedoLog to record upsertbatch info as redolog
func (k *kafkaPartitionReader) AppendToRedoLog(upsertBatch *common.UpsertBatch, kafkaOffset int64) (int64, uint32) {
	fileID, fileOffset := k.getFileOffset(kafkaOffset)
	size := len(upsertBatch.GetBuffer())

	k.Lock()
	defer k.Unlock()
	k.count++
	if k.count%commitInterval == 0 {
		k.commitFunc(kafkaOffset)
	}
	if currentFirstOffset, ok := k.FirstKafkaOffsetPerFile[fileID]; !ok || currentFirstOffset > kafkaOffset {
		k.FirstKafkaOffsetPerFile[fileID] = kafkaOffset
	}

	if _, exist := k.SizePerFile[fileID]; !exist {
		k.SizePerFile[fileID] = 0
	} else {
		k.SizePerFile[fileID] += size
	}
	k.TotalRedologSize += size
	return fileID, fileOffset
}

// Commit offset only, redolog will be in file system
func (k* kafkaPartitionReader) Commit(kafkaOffset int64) {
	k.Lock()
	defer k.Unlock()
	k.count++
	if k.count%commitInterval == 0 {
		k.commitFunc(kafkaOffset)
	}
}

func (k *kafkaPartitionReader) UpdateMaxEventTime(eventTime uint32, fileID int64) {
	k.Lock()
	defer k.Unlock()
	if _, ok := k.MaxEventTimePerFile[fileID]; ok && eventTime <= k.MaxEventTimePerFile[fileID] {
		return
	}
	k.MaxEventTimePerFile[fileID] = eventTime
}

func (k *kafkaPartitionReader) getFileOffset(kafkaOffset int64) (int64, uint32) {
	return kafkaOffset / maxBatchesPerFile, uint32(kafkaOffset % maxBatchesPerFile)
}

func (k *kafkaPartitionReader) CheckpointRedolog(eventTimeCutoff uint32, fileIDCheckpointed int64, batchOffset uint32) error {
	k.RLock()
	var firstUnpurgeableFileID int64 = math.MaxInt64
	var firstKafkaOffset int64 = math.MaxInt64
	for fileID, maxEventTime := range k.MaxEventTimePerFile {
		if maxEventTime >= eventTimeCutoff ||
			fileID > fileIDCheckpointed ||
			(fileID == fileIDCheckpointed && batchOffset != maxBatchesPerFile-1) {
			// file not purgeable
			if fileID < firstUnpurgeableFileID {
				firstUnpurgeableFileID = fileID
				// fileID existing in MaxEventTimePerFile should always have entry in FirstKafkaOffsetPerFile
				firstKafkaOffset = k.FirstKafkaOffsetPerFile[fileID]
			}
		}
	}
	k.RUnlock()

	if firstUnpurgeableFileID < math.MaxInt64 {
		err := k.checkPointFunc(firstKafkaOffset)
		if err != nil {
			return err
		}

		k.Lock()
		for fileID := range k.MaxEventTimePerFile {
			if fileID < firstUnpurgeableFileID {
				delete(k.MaxEventTimePerFile, fileID)
				delete(k.FirstKafkaOffsetPerFile, fileID)
				k.TotalRedologSize -= k.SizePerFile[fileID]
				delete(k.SizePerFile, fileID)
			}
		}
		k.Unlock()
	}
	return nil
}

func (k *kafkaPartitionReader) ConsumeFrom(offset int64) error {
	if k.partitionConsumer != nil {
		// close previous created partition consumer
		k.partitionConsumer.Close()
	}
	var err error
	k.partitionConsumer, err = k.consumer.ConsumePartition(k.Topic, int32(k.Shard), offset)
	if err != nil {
		return utils.StackError(err, "failed to consume from topic %s partition %d",
			k.Topic, k.Shard)
	}

	go func() {
		for {
			select {
			case msg, ok := <-k.partitionConsumer.Messages():
				if !ok {
					// consumer closed
					utils.GetLogger().With(
						"table", k.TableName,
						"shard", k.Shard).Error("partition consumer channel closed")
					return
				}
				if msg != nil {
					upsertBatch, err := common.NewUpsertBatch(msg.Value)
					if err != nil {
						utils.GetLogger().With(
							"table", k.TableName,
							"shard", k.Shard, "error", err.Error()).Error("failed to create upsert batch from msg")
						continue
					}
					k.batches <- upsertBatchBundle{upsertBatch, msg.Offset}
				}
			case err, ok := <-k.partitionConsumer.Errors():
				if !ok {
					// consumer closed
					utils.GetLogger().With(
						"table", k.TableName,
						"shard", k.Shard).Error("partition consumer error channel closed")
					return
				} else {
					utils.GetLogger().With("table", k.TableName, "shard", k.Shard, "error", err.Error()).
						Error("received consumer error")
				}
				continue
			case <-k.done:
				return
			}
		}
	}()
	return nil
}

func (k *kafkaPartitionReader) NextUpsertBatch() func() (*common.UpsertBatch, int64) {
	return func() (*common.UpsertBatch, int64) {
		for {
			select {
			case batchBundle := <-k.batches:
				return batchBundle.batch, batchBundle.kafkaOffset
			case <-k.done:
				// redolog manager closed
				return nil, 0
			}
		}
	}
}

func (k *kafkaPartitionReader) GetTotalSize() int {
	k.RLock()
	defer k.RUnlock()
	return int(k.TotalRedologSize)
}

func (k *kafkaPartitionReader) GetNumFiles() int {
	k.RLock()
	defer k.RUnlock()
	return len(k.SizePerFile)
}

func (k *kafkaPartitionReader) Close() {
	k.Lock()
	defer k.Unlock()
	if k.partitionConsumer != nil {
		k.partitionConsumer.Close()
		k.partitionConsumer = nil
	}
	close(k.done)
}

// MarshalJSON marshals a kafkaPartitionReader into json.
func (k *kafkaPartitionReader) MarshalJSON() ([]byte, error) {
	// Avoid json.Marshal loop calls.
	type alias kafkaPartitionReader
	k.RLock()
	defer k.RUnlock()
	return json.Marshal((*alias)(k))
}
