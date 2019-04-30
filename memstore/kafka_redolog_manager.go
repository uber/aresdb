package memstore

import (
	"github.com/Shopify/sarama"
	"github.com/uber/aresdb/utils"
	"math"
	"sync"
)

const maxBatchesPerFile = 5000

type upsertBatchBundle struct {
	batch *UpsertBatch
	kafkaOffset int64
}

// kafkaRedologManager is the implementation of RedologFileRotator
type kafkaRedologManager struct {
	sync.RWMutex

	Topic string
	TableName string
	Shard int
	Address []string `json:"address"`

	MaxEventTimePerFile map[int64]uint32 `json:"maxEventTimePerFile"`
	consumer sarama.PartitionConsumer

	commitFunc func(offset int64) error
	done    chan struct{}
	batches chan upsertBatchBundle
}

// NewKafkaRedologManager creates kafka redolog manager
func NewKafkaRedologManager(namespace, table string, shard int, address []string, commitFunc func(offset int64) error) KafkaRedologManager {
	return &kafkaRedologManager{
		Topic: utils.GetTopicFromTable(namespace, table),
		Shard: shard,
		Address: address,
		commitFunc: commitFunc,
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
			(fileID == fileIDCheckpointed && batchOffset != maxBatchesPerFile - 1) {
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

// StartConsumer starts kafka consumer from a offset
func (k *kafkaRedologManager) StartConsumer(offset int64) error {
	if k.consumer != nil {
		k.consumer.Close()
	}

	config := sarama.NewConfig()
	consumer, err := sarama.NewConsumer(k.Address, config)
	if err != nil {
		return utils.StackError(err, "failed to initialize kafka consumer for topic %s",
			k.Topic)
	}

	k.consumer, err = consumer.ConsumePartition(k.Topic, int32(k.Shard), offset)
	if err != nil {
		return utils.StackError(err, "failed to consume from topic %s partition %d",
			k.Topic, k.Shard)
	}

	go func() {
		for {
			select {
			case msg, ok := <-k.consumer.Messages():
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
	if k.consumer != nil {
		if err := k.consumer.Close(); err != nil {
			utils.GetLogger().With(
				"topic", k.Topic,
				"partition", k.Shard).Error("failed to close consumer")

		}
	}
	close(k.done)
}
