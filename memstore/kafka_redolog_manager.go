package memstore

import (
	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/uber/aresdb/utils"
	"math"
	"sync"
)

const maxBatchesPerFile = 5000

type upsertBatchBundle struct {
	batch       *UpsertBatch
	kafkaOffset int64
}

// kafkaRedologManager is the implementation of RedologFileRotator
type kafkaRedologManager struct {
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

	commitFunc func(offset int64) error
	done       chan struct{}
	batches    chan upsertBatchBundle
}

// NewKafkaRedologManager creates kafka redolog manager
func NewKafkaRedologManager(namespace, table string, shard int, consumer sarama.Consumer, commitFunc func(offset int64) error) KafkaRedologManager {
	topic := utils.GetTopicFromTable(namespace, table)
	return &kafkaRedologManager{
		TableName:               table,
		Shard:                   shard,
		Topic:                   topic,
		consumer:                consumer,
		commitFunc:              commitFunc,
		MaxEventTimePerFile:     make(map[int64]uint32),
		FirstKafkaOffsetPerFile: make(map[int64]int64),
		SizePerFile:             make(map[int64]int),
		done:                    make(chan struct{}),
		batches:                 make(chan upsertBatchBundle, 1),
	}
}

// WriteUpsertBatch to kafka redolog manager is disabled
func (k *kafkaRedologManager) WriteUpsertBatch(upsertBatch *UpsertBatch) (int64, uint32) {
	panic("WriteUpsertBatch to kafka redolog manager is disabled")
}

func (k *kafkaRedologManager) UpdateMaxEventTime(eventTime uint32, fileID int64) {
	k.Lock()
	defer k.Unlock()
	if _, ok := k.MaxEventTimePerFile[fileID]; ok && eventTime <= k.MaxEventTimePerFile[fileID] {
		return
	}
	k.MaxEventTimePerFile[fileID] = eventTime
}

func (k *kafkaRedologManager) addMessage(fileID int64, kafkaOffset int64, size int) {
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
}

func (k *kafkaRedologManager) NextUpsertBatch() func() (*UpsertBatch, int64, uint32) {
	return func() (*UpsertBatch, int64, uint32) {
		for {
			select {
			case batchBundle := <-k.batches:
				fileID := batchBundle.kafkaOffset / maxBatchesPerFile
				fileOffset := batchBundle.kafkaOffset % maxBatchesPerFile
				k.addMessage(fileID, batchBundle.kafkaOffset, len(batchBundle.batch.buffer))
				return batchBundle.batch, fileID, uint32(fileOffset)
			case <-k.done:
				// redolog manager closed
				return nil, 0, 0
			}
		}
	}
}

func (k *kafkaRedologManager) CheckpointRedolog(eventTimeCutoff uint32, fileIDCheckpointed int64, batchOffset uint32) error {
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
		err := k.commitFunc(firstKafkaOffset)
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

func (k *kafkaRedologManager) ConsumeFrom(offset int64) error {
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
					upsertBatch, err := NewUpsertBatch(msg.Value)
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

func (k *kafkaRedologManager) GetTotalSize() int {
	k.RLock()
	defer k.RUnlock()
	return int(k.TotalRedologSize)
}

func (k *kafkaRedologManager) GetNumFiles() int {
	k.RLock()
	defer k.RUnlock()
	return len(k.SizePerFile)
}

func (k *kafkaRedologManager) Close() {
	if k.partitionConsumer != nil {
		k.partitionConsumer.Close()
	}
	close(k.done)
}

// MarshalJSON marshals a fileRedologManager into json.
func (k *kafkaRedologManager) MarshalJSON() ([]byte, error) {
	// Avoid json.Marshal loop calls.
	type alias kafkaRedologManager
	k.RLock()
	defer k.RUnlock()
	return json.Marshal((*alias)(k))
}
