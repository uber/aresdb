package memstore

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type GinkgoTestReporter struct{}

func (g GinkgoTestReporter) Errorf(format string, args ...interface{}) {
	ginkgo.Fail(fmt.Sprintf(format, args))
}

func (g GinkgoTestReporter) Fatalf(format string, args ...interface{}) {
	ginkgo.Fail(fmt.Sprintf(format, args))
}

var _ = ginkgo.Describe("kafka redolog manager", func() {
	var t GinkgoTestReporter

	ginkgo.It("NextUpsertBatch should work", func() {
		consumer := mocks.NewConsumer(t, nil)
		commitedOffset := make([]int64, 0)
		commitFunc := func(offset int64) error {
			commitedOffset = append(commitedOffset, offset)
			return nil
		}

		upsertBatchBytes := []byte{1, 0, 237, 254, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 51, 0, 0, 0, 57, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 2, 0, 123, 0, 1, 0, 0, 0, 0, 0, 135, 0, 0, 0, 0, 0, 0, 0}
		redoManager := &kafkaRedologManager{
			TableName:           "test",
			Shard:               0,
			Topic:               "test-test",
			consumer:            consumer,
			commitFunc:          commitFunc,
			MaxEventTimePerFile: make(map[int64]uint32),
			done:                make(chan struct{}),
			batches:             make(chan upsertBatchBundle, 1),
		}

		consumer.ExpectConsumePartition("test-test", 0, 0).
			YieldMessage(&sarama.ConsumerMessage{
				Value: upsertBatchBytes,
			})

		err := redoManager.ConsumeFrom(0)
		Ω(err).Should(BeNil())

		nextUpsertBatch := redoManager.NextUpsertBatch()
		upsertBatch, fileID, offset := nextUpsertBatch()
		Ω(upsertBatch.buffer).Should(Equal(upsertBatchBytes))
		Ω(fileID).Should(Equal(int64(1 / maxBatchesPerFile)))
		Ω(offset).Should(Equal(uint32(1 % maxBatchesPerFile)))

		redoManager.UpdateMaxEventTime(0, fileID)
		err = redoManager.PurgeRedologFileAndData(1, 1, 0)
		Ω(err).Should(BeNil())
		redoManager.Close()
	})

})
