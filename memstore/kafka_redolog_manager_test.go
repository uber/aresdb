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
		config := sarama.NewConfig()
		config.ChannelBufferSize = 2 * maxBatchesPerFile
		consumer := mocks.NewConsumer(t, config)
		commitedOffset := make([]int64, 0)
		commitFunc := func(offset int64) error {
			commitedOffset = append(commitedOffset, offset)
			return nil
		}

		upsertBatchBytes := []byte{1, 0, 237, 254, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 51, 0, 0, 0, 57, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 2, 0, 123, 0, 1, 0, 0, 0, 0, 0, 135, 0, 0, 0, 0, 0, 0, 0}
		redoManager := NewKafkaRedologManager("test", "test", 0, consumer, commitFunc)

		// create 2 * maxBatchesPerFile number of messages
		for i := 0; i < 2*maxBatchesPerFile; i++ {
			consumer.ExpectConsumePartition("test-test", 0, mocks.AnyOffset).
				YieldMessage(&sarama.ConsumerMessage{
					Value: upsertBatchBytes,
				})
		}

		err := redoManager.ConsumeFrom(0)
		Ω(err).Should(BeNil())

		// since offset starts from 1, we will have three files
		fileIDs := map[int64]struct{}{}
		nextUpsertBatch := redoManager.NextUpsertBatch()
		for i := 1; i <= 2*maxBatchesPerFile; i++ {
			upsertBatch, fileID, offset := nextUpsertBatch()
			fileIDs[fileID] = struct{}{}
			Ω(upsertBatch.buffer).Should(Equal(upsertBatchBytes))
			Ω(fileID).Should(Equal(int64(i / maxBatchesPerFile)))
			Ω(offset).Should(Equal(uint32(i % maxBatchesPerFile)))
			redoManager.UpdateMaxEventTime(0, fileID)
		}

		Ω(fileIDs).Should(HaveLen(3))
		Ω(fileIDs).Should(HaveKey(int64(0)))
		Ω(fileIDs).Should(HaveKey(int64(1)))
		Ω(fileIDs).Should(HaveKey(int64(2)))

		err = redoManager.CheckpointRedolog(1, 1, 0)
		Ω(err).Should(BeNil())
		Ω(commitedOffset).Should(ConsistOf(int64(maxBatchesPerFile)))
		redoManager.Close()

		upsertBatch, fileID, offset := nextUpsertBatch()
		Ω(upsertBatch).Should(BeNil())
		Ω(fileID).Should(BeEquivalentTo(0))
		Ω(offset).Should(BeEquivalentTo(0))
	})

})
