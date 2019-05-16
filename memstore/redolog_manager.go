package memstore

import (
	"github.com/uber/aresdb/common"
	"github.com/uber/aresdb/diskstore"
	"github.com/uber/aresdb/metastore"
	"github.com/uber/aresdb/utils"
)

// RedologManager defines rodelog manager interface
type RedologManager interface {
	// RecordUpsertBatch writes upsert batch to the redolog manager or record offset
	RecordUpsertBatch(upsertBatch *UpsertBatch, offset int64) (int64, uint32)
	// UpdateMaxEventTime update max eventime of given redolog file
	UpdateMaxEventTime(eventTime uint32, redoFile int64)
	// NextUpsertBatch returns a function yielding the next upsert batch
	NextUpsertBatch() func() (*UpsertBatch, int64, uint32)
	// CheckpointRedolog checkpoint event time cutoff (from archiving) and redologFileID and batchOffset (from backfill)
	// to redolog manager
	CheckpointRedolog(cutoff uint32, redoFileCheckpointed int64, batchOffset uint32) error
	// GetTotalSize returns the total size of all redologs tracked in redolog manager
	GetTotalSize() int
	// Get the number of files tracked in redolog manager
	GetNumFiles() int
	// Close free resources held by redolog manager
	Close()
}

// KafkaRedologManager is interface for kafka based redolog manager
type KafkaRedologManager interface {
	RedologManager
	// ConsumeFrom start the consumer for kafka redolog manager from given kafka offset
	Consume() (bool, error)
}

// RedologManagerFactory factory to create RedoLogManager and maintains IngestionManager for RedoLogManager
type RedoLogManagerFactory struct {
	redologStorage 		string
	ingestConfig 		common.IngestionConfig
	ingestorManager     *IngestorManager
}

// NewRedoLogManagerFactory create RedoLogManagerFactory instance
func NewRedoLogManagerFactory(redologStorage string, ingestConfig common.IngestionConfig) (*RedoLogManagerFactory, error) {
	if redologStorage == common.KafkaRedolog {
		if ingestConfig.IngestionMode != common.KafkaIngestion {
			return nil, utils.StackError(nil, "Kafka redolog must use kafka ingestion")
		}
	}
	ingestionFactory, err := NewIngestorFactory(ingestConfig)
	if err != nil {
		return nil, utils.StackError(err, "Fail to create redolog ingestion factory")
	}

	ingestorManager := NewIngestorManager(ingestConfig.Namespace, ingestionFactory)
	return &RedoLogManagerFactory {
		redologStorage:  redologStorage,
		ingestConfig: 	 ingestConfig,
		ingestorManager: ingestorManager,
	}, nil
}

// NewRedologManager create RedologManager for tabe shard
func (f *RedoLogManagerFactory) NewRedologManager(tableName string, shard int, diskStore diskstore.DiskStore,
		rotationInterval, maxRedoLogSize int64, metaStore metastore.MetaStore) (RedologManager, error) {
	if f.redologStorage == common.KafkaRedolog {
		return NewKafkaRedologManager(f.ingestConfig.Namespace, tableName, shard, f.ingestorManager.IngestPartition(tableName, shard), metaStore), nil
	} else {
		return NewFileRedoLogManager(rotationInterval, maxRedoLogSize, diskStore, tableName, shard), nil
	}
}

// Close clear resource potentially used by the factory
func (f *RedoLogManagerFactory) Close() {
	f.ingestorManager.Close()
}
