package memstore

import (
	"github.com/Shopify/sarama"
	"github.com/uber/aresdb/utils"
	"fmt"
)

// KafkaIngestorFactory Kafka specified IngestorFactory
type KafkaIngestorFactory struct {
	// Kafka consumer
	consumer sarama.Consumer
	// namespace used for this cluster
	namespace string
}

// KafkaPartitionIngestor Kafka partition level ingestor
type KafkaPartitionIngestor struct {
	consumer 			sarama.Consumer
	namespace   		string
	tableName   		string
	shard       		int
	partitionConsumer 	sarama.PartitionConsumer
	done        		chan struct{}
	batches    			chan UpsertBatchWithOffset
}

// NewKafkaIngestorFactory create Kafka IngestorFactory
func NewKafkaIngestorFactory(brokers []string, namespace string) (IngestorFactory, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("Invalid Kafka broker list")
	}
	consumer, err := sarama.NewConsumer(brokers, sarama.NewConfig())
	if err != nil {
		return nil, err
	}
	return &KafkaIngestorFactory{
		consumer:  consumer,
		namespace: namespace,
	}, nil
}

// NewPartitionIngestor create Kafka PartitionIngestor
func (f *KafkaIngestorFactory) NewPartitionIngestor(tableName string, shard int) PartitionIngestor {
	return &KafkaPartitionIngestor {
		consumer:   f.consumer,
		namespace:	f.namespace,
		tableName:  tableName,
		shard:      shard,
		done:       make(chan struct{}),
		batches:    make(chan UpsertBatchWithOffset, 1),
	}
}

// GetNamespace return namespace used for this IngestorFactory
func (f *KafkaIngestorFactory) GetNamespace() string {
	return f.namespace
}

// Close close kafka consumer
func (f *KafkaIngestorFactory) Close() {
	if f.consumer != nil {
		f.consumer.Close()
	}
	f.consumer = nil
}

// Ingest start the partition message consumption
func (p *KafkaPartitionIngestor) Ingest(from, to int64, recover bool) error {
	utils.GetLogger().With("action", IngestionAction, "table", p.tableName, "shard", p.shard).Info("Start ingestion")

	if from == 0 {
		from = sarama.OffsetNewest
	}
	if to <= from {
		return utils.StackError(nil, "Invalid kafka offsets")
	}

	topicName := utils.GetTopicFromTable(p.namespace, p.tableName)

	var err error
	if p.partitionConsumer != nil {
		return utils.StackError(err, "Kafka consumer is already running for topic %s shard %d of table %s",
			topicName, p.shard, p.tableName)
	}
	p.partitionConsumer, err = p.consumer.ConsumePartition(topicName, int32(p.shard), from)
	if err != nil {
		return utils.StackError(err, "failed to consume from topic %s shard %d for table %s",
			topicName, p.shard, p.tableName)
	}

	go func() {
		for {
			select {
			case msg, ok := <-p.partitionConsumer.Messages():
				if !ok {
					// consumer closed
					utils.GetLogger().With("action", IngestionAction, "table", p.tableName, "shard", p.shard).Error("shard consumer channel closed")
					return
				}
				if msg != nil {
					upsertBatch, err := NewUpsertBatch(msg.Value)
					if err != nil {
						utils.GetLogger().With("action", IngestionAction, "table", p.tableName, "shard", p.shard, "error", err.Error()).Error("failed to create upsert batch from msg")
						// TODO add metric for decode msg error
						continue
					}
					p.batches <- UpsertBatchWithOffset{upsertBatch, msg.Offset}
				}
				// if last offset reached for recovery, quit the consumer
				if msg.Offset >= to {
					utils.GetLogger().With("action", IngestionAction, "table", p.tableName, "shard", p.shard).Info("Partition consumer reached offset end")
					close(p.done)

					return
				}
			case err, ok := <-p.partitionConsumer.Errors():
				if !ok {
					// consumer closed
					utils.GetLogger().With("action", IngestionAction, "table", p.tableName, "shard", p.shard).Error("shard consumer error channel closed")
				} else {
					utils.GetLogger().With("action", IngestionAction, "table", p.tableName, "shard", p.shard, "error", err.Error()).Error("received consumer error")
				}
				continue
			case <-p.done:
				utils.GetLogger().With("action", IngestionAction, "table", p.tableName, "shard", p.shard).Info("Partition consumer closed")
				return
			}
		}
	}()
	return nil
}

// Next return the next Kafka message with offset
func (p *KafkaPartitionIngestor) Next() *UpsertBatchWithOffset {
	if p.partitionConsumer == nil {
		return nil
	}
	for {
		select {
		case batch := <-p.batches:
			return &batch
		case <-p.done:
			// redolog manager closed
			return nil
		}
	}
}

// Close close the partition consumer and channels
func (p *KafkaPartitionIngestor) Close() {
	if p.partitionConsumer != nil {
		p.partitionConsumer.Close()
		p.partitionConsumer = nil
	}
}
