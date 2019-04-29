package memstore

import (
	"github.com/Shopify/sarama"
	"github.com/uber/aresdb/utils"
	"math"
	"sync"
)

type upsertBatchBundle struct {
	batch  *UpsertBatch
	fileID int64
	offset uint32
}

// kafkaRedologManager is the implementation of RedologFileRotator
type kafkaRedologManager struct {
	sync.RWMutex

	Cfg RedologManagerConfig `json:"config"`

	BatchCountPerFile   map[int64]uint32 `json:"batchCountPerFile"`
	MaxEventTimePerFile map[int64]uint32 `json:"maxEventTimePerFile"`
	CurrentFileID       *int64           `json:"currentFileID"`

	consumer sarama.PartitionConsumer

	batches chan upsertBatchBundle
	done    chan struct{}
}

// NewKafkaRedologManager creates kafka redolog manager
func NewKafkaRedologManager(config RedologManagerConfig) KafkaRedologManager {
	return &kafkaRedologManager{
		Cfg:                 config,
		batches:             make(chan upsertBatchBundle, 1),
		done:                make(chan struct{}),
		BatchCountPerFile:   make(map[int64]uint32),
		MaxEventTimePerFile: make(map[int64]uint32),
	}
}

// WriteUpsertBatch to kafka redolog manager is disabled
func (k *kafkaRedologManager) WriteUpsertBatch(upsertBatch *UpsertBatch) (int64, uint32) {
	panic("WriteUpsertBatch to kafka redolog manager is disabled")
}

func (k *kafkaRedologManager) writeUpsertBatch(upsertBatch *UpsertBatch, offset int64) (int64, uint32) {
	k.Lock()
	if k.CurrentFileID == nil || k.BatchCountPerFile[*k.CurrentFileID] >= k.Cfg.Kafka.MaxBatchesPerFile {
		newFileID := offset
		k.CurrentFileID = &newFileID
	}
	if _, ok := k.BatchCountPerFile[*k.CurrentFileID]; !ok {
		k.BatchCountPerFile[*k.CurrentFileID] = 1
	} else {
		k.BatchCountPerFile[*k.CurrentFileID]++
	}
	k.Unlock()

	batchOffset := k.BatchCountPerFile[*k.CurrentFileID] - 1
	k.batches <- upsertBatchBundle{upsertBatch, *k.CurrentFileID, batchOffset}

	return *k.CurrentFileID, batchOffset
}

func (k *kafkaRedologManager) UpdateMaxEventTime(eventTime uint32, fileID int64) {
	k.Lock()
	defer k.Unlock()
	if _, ok := k.MaxEventTimePerFile[fileID]; ok && eventTime <= k.MaxEventTimePerFile[fileID] {
		return
	}
	k.MaxEventTimePerFile[fileID] = eventTime
}

func (k *kafkaRedologManager) NextUpsertBatch() func() (*UpsertBatch, int64, uint32) {
	return func() (*UpsertBatch, int64, uint32) {
		select {
		case bundle, ok := <-k.batches:
			if !ok {
				return nil, 0, 0
			}
			return bundle.batch, bundle.fileID, bundle.offset
		case <-k.done:
			return nil, 0, 0
		}
	}
}

func (k *kafkaRedologManager) PurgeRedologFileAndData(eventTimeCutoff uint32, fileIDCheckpointed int64, batchOffset uint32) error {
	k.RLock()
	var purgeableFileIDs []int64
	var firstUnpurgable int64 = math.MaxInt64
	for fileID, maxEventTime := range k.MaxEventTimePerFile {
		if k.checkPurgeConditionWithLock(fileID, maxEventTime, eventTimeCutoff, fileIDCheckpointed, batchOffset) {
			purgeableFileIDs = append(purgeableFileIDs, fileID)
		} else if fileID < firstUnpurgable {
			firstUnpurgable = fileID
		}
	}
	k.RUnlock()

	var maxPurgeableFileID int64 = 0
	// last check for purgeable fileIDs to exclude fileIDs > firstUnpurgable
	for _, purgeableFileID := range purgeableFileIDs {
		if purgeableFileID < firstUnpurgable && purgeableFileID > maxPurgeableFileID {
			maxPurgeableFileID = purgeableFileID
		}
	}
	err := k.Cfg.Kafka.CommitFunc(maxPurgeableFileID)
	if err != nil {
		return err
	}
	for _, purgeableFileID := range purgeableFileIDs {
		if purgeableFileID <= maxPurgeableFileID {
			k.Lock()
			delete(k.MaxEventTimePerFile, purgeableFileID)
			delete(k.BatchCountPerFile, purgeableFileID)
			k.Unlock()
		}
	}
	return nil
}

// StartConsume starts kafka consumer
func (k *kafkaRedologManager) StartConsumer(offset int64) error {
	config := sarama.NewConfig()
	consumer, err := sarama.NewConsumer(k.Cfg.Kafka.Address, config)
	if err != nil {
		return utils.StackError(err, "failed to initialize kafka consumer for table %s",
			k.Cfg.Table)
	}

	topic := utils.GetTopicFromTable(k.Cfg.Namespace, k.Cfg.Table)
	k.consumer, err = consumer.ConsumePartition(topic, k.Cfg.Shard, offset)
	if err != nil {
		return utils.StackError(err, "failed to consume from table %s partition %d",
			k.Cfg.Table, k.Cfg.Shard)
	}

	go func() {
		for {
			select {
			case <-k.done:
				err := k.consumer.Close()
				if err != nil {
					utils.GetLogger().With(
						"table", k.Cfg.Table,
						"shard", k.Cfg.Shard,
						"error", err.Error()).Error("failed to close consumer")
				}
				return
			case msg := <-k.consumer.Messages():
				if msg != nil {
					upsertBatch, err := NewUpsertBatch(msg.Value)
					if err != nil {
						utils.GetLogger().With(
							"table", k.Cfg.Table,
							"shard", k.Cfg.Shard).Error("failed to create upsert batch from msg")
					} else {
						k.writeUpsertBatch(upsertBatch, msg.Offset)
					}
				}
			case err := <-k.consumer.Errors():
				if err != nil {
					utils.GetLogger().With(
						"table", k.Cfg.Table,
						"shard", k.Cfg.Shard,
						"error", err.Error()).Error("received kafka consumer error")

				}
			}
		}
	}()
	return nil
}

func (k *kafkaRedologManager) Close() {
	close(k.done)
	k.CurrentFileID = nil
}

// check whether fileID matches purge condition
// assuming lock already held
func (k *kafkaRedologManager) checkPurgeConditionWithLock(fileID int64, maxEventTime, eventCutoff uint32, fileIDCheckpointed int64, batchOffset uint32) bool {
	// exclude current redo file since it's used by ingestion
	return (k.CurrentFileID == nil || fileID != *k.CurrentFileID) &&
		maxEventTime < eventCutoff &&
		(fileID < fileIDCheckpointed || (fileID == fileIDCheckpointed && k.BatchCountPerFile[fileIDCheckpointed] == batchOffset+1))
}
