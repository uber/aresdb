package memstore

import (
	"encoding/json"
	"github.com/uber/aresdb/metastore"
	"github.com/uber/aresdb/utils"
	"math"
	"sync"
)

const (
	maxBatchesPerFile 	  = 5000
	messageCommitInterval = 500
)

type offsetSaveFunc func(table string, shard int, offset int64) error
type offsetGetFunc func(table string, shard int) (int64, error)

// kafkaRedologManager is the implementation of RedologFileRotator
type kafkaRedologManager struct {
	sync.RWMutex

	Topic     string `json:"topic"`
	TableName string `json:"table"`
	Shard     int    `json:"shard"`

	// Pointer to the disk store for redo log access.
	metaStore metastore.MetaStore

	// MaxEventTime per virtual redolog file
	MaxEventTimePerFile map[int64]uint32 `json:"maxEventTimePerFile"`
	// FirstKafkaOffset per virtual redolog file
	FirstKafkaOffsetPerFile map[int64]int64 `json:"firstKafkaOffsetPerFile"`
	SizePerFile             map[int64]int   `json:"sizePerFile"`
	TotalRedologSize        int             `json:"totalRedologSize"`

	partitionIngestor       PartitionIngestor
	messageIngested			int64

	saveCommitOffsetFunc        offsetSaveFunc
	saveCheckpointOffsetFunc    offsetSaveFunc
	getCommitOffsetFunc         offsetGetFunc
	getCheckPointOffsetFunc     offsetGetFunc
}

// NewKafkaRedologManager creates kafka redolog manager
func NewKafkaRedologManager(namespace, table string, shard int, partitionIngestor PartitionIngestor, metaStore metastore.MetaStore) KafkaRedologManager {
	topic := utils.GetTopicFromTable(namespace, table)
	return &kafkaRedologManager{
		TableName:                   table,
		Shard:                       shard,
		Topic:                       topic,
		partitionIngestor:           partitionIngestor,
		metaStore:                   metaStore,
		MaxEventTimePerFile:         make(map[int64]uint32),
		FirstKafkaOffsetPerFile:     make(map[int64]int64),
		SizePerFile:                 make(map[int64]int),
		saveCommitOffsetFunc:        metaStore.UpdateIngestionCommitOffset,
		saveCheckpointOffsetFunc:    metaStore.UpdateIngestionCheckpointOffset,
		getCommitOffsetFunc:         metaStore.GetIngestionCommitOffset,
		getCheckPointOffsetFunc:     metaStore.GetIngestionCheckpointOffset,
	}
}

// RecordUpsertBatch to record kafka offset committed
func (k *kafkaRedologManager) RecordUpsertBatch(upsertBatch *UpsertBatch, offset int64) (int64, uint32) {
	k.messageIngested++
	if k.messageIngested % messageCommitInterval == 0 {
		k.saveCommitOffsetFunc(k.TableName, k.Shard, offset)
	}
	return offset / maxBatchesPerFile, uint32(offset % maxBatchesPerFile)
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
	run, err := k.Consume()
	if err != nil {
		utils.GetLogger().Panic("Failed to start kafka partition consumer", err)
	}

	if !run {
		// there are nothing to ingest
		return func() (*UpsertBatch, int64, uint32) {
			return nil, 0, 0
		}
	}

	return func() (*UpsertBatch, int64, uint32) {
		b := k.partitionIngestor.Next()

		if b == nil {
			return nil, 0, 0
		} else {
			fileID := b.Offset / maxBatchesPerFile
			fileOffset := b.Offset % maxBatchesPerFile
			k.addMessage(fileID, b.Offset, len(b.Batch.buffer))
			return b.Batch, fileID, uint32(fileOffset)
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
		err := k.saveCheckpointOffsetFunc(k.TableName, k.Shard, firstKafkaOffset)
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


func (k *kafkaRedologManager) Consume() (bool, error) {
	offsetCheckpointed, err := k.getCheckPointOffsetFunc(k.TableName, k.Shard)
	if err != nil {
		return false, utils.StackError(err, "Failed to read ingestion checkpoint offset, table: %s, shard: %d", k.TableName, k.Shard)
	}
	if offsetCheckpointed != 0 {
		offsetCommitted, err := k.getCommitOffsetFunc(k.TableName, k.Shard)
		if err != nil {
			return false, utils.StackError(err, "Failed to read ingestion commit offset, table: %s, shard: %d", k.TableName, k.Shard)
		}
		if offsetCommitted >= offsetCheckpointed {
			return true, k.partitionIngestor.Ingest(offsetCheckpointed, offsetCommitted, true)
		} else {
			return false, utils.StackError(nil, "Invalid ingestion commit offset: %d, checkpointed offset: %d, table: %s, shard:%d",
				offsetCommitted, offsetCheckpointed, k.TableName, k.Shard)
		}
	}
	// nothing checkpointed, nothing to recover
	return false, nil
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
	k.partitionIngestor.Close()
}

// MarshalJSON marshals a fileRedologManager into json.
func (k *kafkaRedologManager) MarshalJSON() ([]byte, error) {
	// Avoid json.Marshal loop calls.
	type alias kafkaRedologManager
	k.RLock()
	defer k.RUnlock()
	return json.Marshal((*alias)(k))
}
