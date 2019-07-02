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

package redolog

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

// kafkaRedoLogManager is kafka partition level consumer, also implementation of RedoLogManager
type kafkaRedoLogManager struct {
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

	// is this redolog manager also used for recovery (true if no disk redolog)
	includeRecovery bool

	consumer          sarama.Consumer
	partitionConsumer sarama.PartitionConsumer

	done chan struct{}

	commitFunc              func(string, int, int64) error
	checkPointFunc          func(string, int, int64) error
	getCommitOffsetFunc     func(string, int) (int64, error)
	getCheckpointOffsetFunc func(string, int) (int64, error)

	// used for external blocking check if recovery done
	recoveryChan chan bool
	recoveryDone bool
	// batch recovered counts
	batchRecovered int
	batchReceived  int
}

// newKafkaRedoLogManager creates kafka redolog manager
func newKafkaRedoLogManager(namespace, table, suffix string, shard int, consumer sarama.Consumer, includeRecovery bool,
	commitFunc func(string, int, int64) error,
	checkPointFunc func(string, int, int64) error,
	getCommitOffsetFunc func(string, int) (int64, error),
	getCheckpointOffsetFunc func(string, int) (int64, error)) *kafkaRedoLogManager {
	topic := utils.GetTopicFromTable(namespace, table, suffix)
	return &kafkaRedoLogManager{
		TableName:               table,
		Shard:                   shard,
		Topic:                   topic,
		consumer:                consumer,
		includeRecovery:         includeRecovery,
		recoveryDone:            !includeRecovery,
		commitFunc:              commitFunc,
		checkPointFunc:          checkPointFunc,
		getCommitOffsetFunc:     getCommitOffsetFunc,
		getCheckpointOffsetFunc: getCheckpointOffsetFunc,
		MaxEventTimePerFile:     make(map[int64]uint32),
		FirstKafkaOffsetPerFile: make(map[int64]int64),
		SizePerFile:             make(map[int64]int),
		recoveryChan:            make(chan bool, 1),
		done:                    make(chan struct{}),
	}
}

// AppendToRedoLog to record upsertbatch info as redolog
func (k *kafkaRedoLogManager) AppendToRedoLog(upsertBatch *common.UpsertBatch) (int64, uint32) {
	panic("WriteUpsertBatch to kafka redolog manager is disabled")
}

func (k *kafkaRedoLogManager) UpdateMaxEventTime(eventTime uint32, fileID int64) {
	k.Lock()
	defer k.Unlock()
	if _, ok := k.MaxEventTimePerFile[fileID]; ok && eventTime <= k.MaxEventTimePerFile[fileID] {
		return
	}
	k.MaxEventTimePerFile[fileID] = eventTime
}

func (k *kafkaRedoLogManager) getFileOffset(kafkaOffset int64) (int64, uint32) {
	return kafkaOffset / maxBatchesPerFile, uint32(kafkaOffset % maxBatchesPerFile)
}

func (k *kafkaRedoLogManager) CheckpointRedolog(eventTimeCutoff uint32, fileIDCheckpointed int64, batchOffset uint32) error {
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
		err := k.checkPointFunc(k.TableName, k.Shard, firstKafkaOffset)
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

func (k *kafkaRedoLogManager) addMessage(fileID int64, kafkaOffset int64, size int) {
	k.Lock()
	defer k.Unlock()

	if currentFirstOffset, ok := k.FirstKafkaOffsetPerFile[fileID]; !ok || currentFirstOffset > kafkaOffset {
		k.FirstKafkaOffsetPerFile[fileID] = kafkaOffset
	}

	if _, exist := k.SizePerFile[fileID]; !exist {
		k.SizePerFile[fileID] = 0
	} else {
		k.SizePerFile[fileID] += size
	}
	k.TotalRedologSize += size

	if k.recoveryDone {
		k.batchReceived++
	} else {
		k.batchRecovered++
	}
	if k.batchReceived%commitInterval == (commitInterval - 1) {
		k.commitFunc(k.TableName, k.Shard, kafkaOffset)
	}
}

func (k *kafkaRedoLogManager) getKafkaOffsets() (int64, int64) {
	var offsetFrom, offsetTo int64
	var err error
	if k.includeRecovery {
		offsetFrom, err = k.getCheckpointOffsetFunc(k.TableName, k.Shard)
		if err != nil {
			utils.GetLogger().Fatal(err)
		}
		offsetTo, err = k.getCommitOffsetFunc(k.TableName, k.Shard)
		if err != nil {
			utils.GetLogger().Fatal(err)
		}
	} else {
		offsetFrom, err = k.getCommitOffsetFunc(k.TableName, k.Shard)
		offsetTo = offsetFrom
	}

	if offsetFrom == 0 {
		offsetFrom = sarama.OffsetNewest
	}
	if offsetTo < offsetFrom {
		offsetTo = offsetFrom
	}
	return offsetFrom, offsetTo
}

func (k *kafkaRedoLogManager) Iterator() (NextUpsertFunc, error) {
	if k.partitionConsumer != nil {
		// close previous created partition consumer
		k.partitionConsumer.Close()
	}
	offsetFrom, offsetTo := k.getKafkaOffsets()
	var err error
	k.partitionConsumer, err = k.consumer.ConsumePartition(k.Topic, int32(k.Shard), offsetFrom)
	if err != nil {
		utils.GetLogger().Panic("Failed to consumer kafka partition", err)
	}

	if k.includeRecovery {
		utils.GetLogger().With("action", "recover", "table", k.TableName, "shard", k.Shard, "offsetFrom", offsetFrom, "offsetTo", offsetTo).
			Info("start recover from kafka")
	} else {
		utils.GetLogger().With("action", "ingestion", "table", k.TableName, "shard", k.Shard, "offsetFrom", offsetFrom).
			Info("start play redolog from kafka")
	}

	return func() *NextUpsertBatchInfo {
		if k.partitionConsumer == nil {
			// partition consumer closed
			return nil
		}
		if !k.recoveryDone && (offsetTo == 0 || offsetTo <= offsetFrom) {
			k.setRecoveryDone()
		}
		for {
			select {
			case msg, ok := <-k.partitionConsumer.Messages():
				if !ok {
					// consumer closed
					utils.GetLogger().With(
						"table", k.TableName,
						"shard", k.Shard).Error("partition consumer channel closed")
					return nil
				}
				if msg != nil {
					upsertBatch, err := common.NewUpsertBatch(msg.Value)
					if err != nil {
						utils.GetLogger().With(
							"table", k.TableName,
							"shard", k.Shard, "error", err.Error()).Error("failed to create upsert batch from msg")
					}
					if !k.recoveryDone && msg.Offset > offsetTo {
						k.setRecoveryDone()
					}

					fileID, fileOffset := k.getFileOffset(msg.Offset)
					k.addMessage(fileID, msg.Offset, len(upsertBatch.GetBuffer()))
					return &NextUpsertBatchInfo{
						Batch:       upsertBatch,
						RedoLogFile: fileID,
						BatchOffset: fileOffset,
						Recovery:    !k.recoveryDone,
					}
				}
			case err, ok := <-k.partitionConsumer.Errors():
				if !ok {
					// consumer closed
					utils.GetLogger().With(
						"table", k.TableName,
						"shard", k.Shard).Error("partition consumer error channel closed")
					return nil
				} else {
					utils.GetLogger().With("table", k.TableName, "shard", k.Shard, "error", err.Error()).
						Error("received consumer error")
				}
			case <-k.done:
				return nil
			}
		}
	}, nil
}

func (k *kafkaRedoLogManager) setRecoveryDone() {
	k.Lock()
	defer k.Unlock()

	k.recoveryDone = true
	k.recoveryChan <- true

	utils.GetLogger().With("action", "recover", "table", k.TableName, "shard", k.Shard,
		"batchRecovered", k.batchRecovered).Info("Finished recovery from kafka")
}

func (k *kafkaRedoLogManager) WaitForRecoveryDone() {
	<-k.recoveryChan
}

func (k *kafkaRedoLogManager) GetTotalSize() int {
	k.RLock()
	defer k.RUnlock()
	return int(k.TotalRedologSize)
}

func (k *kafkaRedoLogManager) GetNumFiles() int {
	k.RLock()
	defer k.RUnlock()
	return len(k.SizePerFile)
}

func (k *kafkaRedoLogManager) GetBatchReceived() int {
	return k.batchReceived
}

func (k *kafkaRedoLogManager) GetBatchRecovered() int {
	return k.batchRecovered
}

func (k *kafkaRedoLogManager) Close() {
	k.Lock()
	defer k.Unlock()

	close(k.done)
	if k.partitionConsumer != nil {
		k.partitionConsumer.Close()
		k.partitionConsumer = nil
	}
}

// MarshalJSON marshals a kafkaRedoLogManager into json.
func (k *kafkaRedoLogManager) MarshalJSON() ([]byte, error) {
	// Avoid json.Marshal loop calls.
	type alias kafkaRedoLogManager
	k.RLock()
	defer k.RUnlock()
	return json.Marshal((*alias)(k))
}
