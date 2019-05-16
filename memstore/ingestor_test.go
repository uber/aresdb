package memstore

import (
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber/aresdb/common"
)

var _ = ginkgo.Describe("Ingestor tests", func() {

	ginkgo.It("New Ingestor factory", func() {
		conf := common.IngestionConfig{
			IngestionMode: "abc",
		}
		f, err := NewIngestorFactory(conf)
		Ω(f).Should(BeNil())
		Ω(err).Should(BeNil())

		conf.IngestionMode = "kafka"
		f, err = NewIngestorFactory(conf)
		Ω(f).Should(BeNil())
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("IngestorManager test", func() {
		factory := &KafkaIngestorFactory{}
		manager := NewIngestorManager("namespace", factory)
		Ω(manager.IsValid()).Should(BeTrue())
		manager.IngestPartition("table1", 0)
		Ω(len(manager.partitionIngestors)).Should(Equal(1))
		manager.IngestPartition("table2", 0)
		Ω(len(manager.partitionIngestors)).Should(Equal(2))
		Ω(len(manager.partitionIngestors["table1"])).Should(Equal(1))
		// different partition
		manager.IngestPartition("table1", 1)
		Ω(len(manager.partitionIngestors["table1"])).Should(Equal(2))
		Ω(len(manager.partitionIngestors)).Should(Equal(2))

		//reingestion
		manager.IngestPartition("table1", 0)
		Ω(len(manager.partitionIngestors["table1"])).Should(Equal(2))
		Ω(len(manager.partitionIngestors)).Should(Equal(2))

		manager.Close()
		Ω(len(manager.partitionIngestors)).Should(Equal(0))
		Ω(manager.partitionIngestors).Should(BeNil())
	})
})
