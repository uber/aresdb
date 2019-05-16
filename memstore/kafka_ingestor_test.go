package memstore

import (
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber/aresdb/common"
	"github.com/uber/aresdb/utils"
	"github.com/Shopify/sarama/mocks"
	"github.com/Shopify/sarama"
)

var _ = ginkgo.Describe("Kafka ingestor tests", func() {

	ginkgo.It("Kafka ingestor factory will fail", func() {
		conf := common.IngestionConfig{
			Namespace:     "abc",
			IngestionMode: "kafka",
			Brokers:       []string{"localhost"},
		}
		f, err := NewIngestorFactory(conf)
		Ω(f).Should(BeNil())
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("Kafka ingestor consume data", func() {
		var t GinkgoTestReporter
		kc := mocks.NewConsumer(t, sarama.NewConfig())
		namespace := "ns1"
		table := "table1"
		shard := 0
		factory := KafkaIngestorFactory{
			namespace: namespace,
			consumer:   kc,
		}

		upsertBatchBytes := []byte{1, 0, 237, 254, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 51, 0, 0, 0, 57, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 2, 0, 123, 0, 1, 0, 0, 0, 0, 0, 135, 0, 0, 0, 0, 0, 0, 0}

		paritionConsumer := kc.ExpectConsumePartition(utils.GetTopicFromTable(namespace, table), int32(shard), mocks.AnyOffset)
		for i := 0; i < 10; i++ {
			paritionConsumer.YieldMessage(&sarama.ConsumerMessage{
				Value: upsertBatchBytes,
			})
		}

		p := factory.NewPartitionIngestor(table, shard)
		err := p.Ingest(sarama.OffsetNewest, 5, false)

		Ω(err).Should(BeNil())
		var i int64
		for i = 1; i<= 10; i++ {
			msg := p.Next()
			if msg == nil {
				break
			}
			Ω(msg.Batch.buffer).Should(Equal(upsertBatchBytes))
			Ω(msg.Offset).Should(Equal(int64(i)))
		}
		Ω(i).Should(Equal(int64(6)))
		p.Close()
	})
})
