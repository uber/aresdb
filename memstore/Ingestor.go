package memstore

// TODO better move to separate package, stay here due to dependency on UpsertBatch
// resturcture needed to isolate UpsertBatch and dependancy stuffs from memstore

import (
	"sync"
	"github.com/uber/aresdb/common"
)

const (
	IngestionAction   = "ingestion"
)

// UpsertBatchWithOffset is struct to contains UpsertBatch with offset from streaming (like kafka)
type UpsertBatchWithOffset struct {
	Batch       *UpsertBatch
	Offset 		int64
}

// IngestorFactory is factory interface to generate partition ingestor
type IngestorFactory interface {
	// NewPartitionIngestor create ingestor for table/partition
	NewPartitionIngestor(tableName string, shard int) PartitionIngestor
	// GetNamespace returns the table namespace used
	GetNamespace() string
	// Close close resources used by this factory
	Close()
}

// IngestorManager is the manager of partition ingestors
type IngestorManager struct {
	sync.RWMutex
	// namespace
	namespace string
	// caches all partition readers
	partitionIngestors map[string]map[int]PartitionIngestor
	// factory to create partition ingestor
	factory IngestorFactory
}

// PartitionIngestor is interface of Partition level ingestor
type PartitionIngestor interface {
	// Ingest start data ingestion from offset from to offset to
	Ingest(from, to int64, recover bool) error
	// Next return the next upsertbatch
	Next() *UpsertBatchWithOffset
	// Close close this partition ingestor
	Close()
}

// NewIngestorFactory create IngestorFactory based on the config
// currently only Kafka ingestor supported
func NewIngestorFactory(conf common.IngestionConfig) (IngestorFactory, error) {
	if conf.IngestionMode == common.KafkaIngestion {
		return NewKafkaIngestorFactory(conf.Brokers, conf.Namespace)
	} else {
		return nil, nil
	}
}

// NewIngestorManager create one IngestorManager using specified IngestorFactory
func NewIngestorManager(namespace string, factory IngestorFactory) *IngestorManager {
	return &IngestorManager{
		namespace:		namespace,
		factory:		factory,
	}
}

// IngestPartition create PartitionIngestor on one specified table/shard
func (m *IngestorManager) IngestPartition(tableName string, shard int) PartitionIngestor {
	if m == nil || m.factory == nil {
		return nil
	}
	m.Lock()
	defer m.Unlock()

	if m.partitionIngestors == nil {
		m.partitionIngestors = make(map[string]map[int]PartitionIngestor)
	}
	var tableIngestors map[int]PartitionIngestor
	var ok bool
	if tableIngestors, ok = m.partitionIngestors[tableName]; !ok || tableIngestors == nil {
		tableIngestors = make(map[int]PartitionIngestor)
		m.partitionIngestors[tableName] = tableIngestors
	}
	partitionIngestor, ok := tableIngestors[shard]
	if ok && partitionIngestor != nil {
		partitionIngestor.Close()
	}

	partitionIngestor = m.factory.NewPartitionIngestor(tableName, shard)
	tableIngestors[shard] = partitionIngestor
	return partitionIngestor
}

// IsValid check if this IngestorManager has valid IngestorFactory
func (m *IngestorManager) IsValid() bool {
	return m.factory != nil
}

// Close close all PartitionIngestor and factory resources
func (m *IngestorManager) Close() {

	m.Lock()
	defer m.Unlock()

	if m.partitionIngestors != nil {
		for _, tableIngestors := range m.partitionIngestors {
			if tableIngestors != nil {
				for _, partitionIngestor := range tableIngestors {
					if partitionIngestor != nil {
						partitionIngestor.Close()
					}
				}
			}
		}
	}
	m.partitionIngestors = nil

	if m.factory != nil {
		m.factory.Close()
		m.factory = nil
	}
}

