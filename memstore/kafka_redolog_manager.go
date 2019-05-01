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

	Topic               string           `json:"topic"`
	TableName           string           `json:"table"`
	Shard               int              `json:"shard"`

	MaxEventTimePerFile map[int64]uint32 `json:"maxEventTimePerFile"`

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
		TableName:           table,
		Shard:               shard,
		Topic:               topic,
		consumer:            consumer,
		commitFunc:          commitFunc,
		MaxEventTimePerFile: make(map[int64]uint32),
		done:                make(chan struct{}),
		batches:             make(chan upsertBatchBundle, 1),
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

// NextUpsertBatch creates iterator function for getting the next upsert batch
func (k *kafkaRedologManager) NextUpsertBatch() func() (*UpsertBatch, int64, uint32) {
	return func() (*UpsertBatch, int64, uint32) {
		for {
			select {
			case batchBundle := <-k.batches:
				fileID := batchBundle.kafkaOffset / maxBatchesPerFile
				fileOffset := batchBundle.kafkaOffset % maxBatchesPerFile
				return batchBundle.batch, fileID, uint32(fileOffset)
			case <-k.done:
				// redolog manager closed
				return nil, 0, 0
			}
		}
	}
}

func (k *kafkaRedologManager) PurgeRedologFileAndData(eventTimeCutoff uint32, fileIDCheckpointed int64, batchOffset uint32) error {
	k.RLock()
	var firstUnpurgeable int64 = math.MaxInt64
	for fileID, maxEventTime := range k.MaxEventTimePerFile {
		if maxEventTime >= eventTimeCutoff ||
			fileID > fileIDCheckpointed ||
			(fileID == fileIDCheckpointed && batchOffset != maxBatchesPerFile-1) {
			// file not purgeable
			if fileID < firstUnpurgeable {
				firstUnpurgeable = fileID
			}
		}
	}
	k.RUnlock()

	if firstUnpurgeable < math.MaxInt64 {
		err := k.commitFunc(firstUnpurgeable)
		if err != nil {
			return err
		}

		k.Lock()
		for fileID := range k.MaxEventTimePerFile {
			if fileID < firstUnpurgeable {
				delete(k.MaxEventTimePerFile, fileID)
			}
		}
		k.Unlock()
	}
	return nil
}

// ConsumeFrom starts kafka consumer from a offset
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
					return
				}
				if msg != nil {
					upsertBatch, err := NewUpsertBatch(msg.Value)
					if err != nil {
						utils.GetLogger().With(
							"table", k.TableName,
							"Shard", k.Shard).Error("failed to create upsert batch from msg")
						continue
					}
					k.batches <- upsertBatchBundle{upsertBatch, msg.Offset}
				}
			case <-k.done:
				return
			}
		}
	}()

	return nil
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
