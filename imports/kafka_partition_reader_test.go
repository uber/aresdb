//  Copyright (c) 2017-2018 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package imports

import (
	"github.com/uber/aresdb/testing"
	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber/aresdb/utils"
)



var _ = ginkgo.Describe("kafka redolog manager", func() {
	var t testing.GinkgoTestReporter

	ginkgo.It("NextUpsertBatch should work", func() {
		config := sarama.NewConfig()
		config.ChannelBufferSize = 2 * maxBatchesPerFile
		consumer := mocks.NewConsumer(t, config)
		commitedOffset := make([]int64, 0)
		commitFunc := func(offset int64) error {
			commitedOffset = append(commitedOffset, offset)
			return nil
		}
		checkPointFunc := func(offset int64) error {
			return nil
		}

		upsertBatchBytes := []byte{1, 0, 237, 254, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 51, 0, 0, 0, 57, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 2, 0, 123, 0, 1, 0, 0, 0, 0, 0, 135, 0, 0, 0, 0, 0, 0, 0}
		redoManager := NewKafkaPartitionReader("test", "test", 0, consumer, commitFunc, checkPointFunc)

		// create 2 * maxBatchesPerFile number of messages
		for i := 0; i < 2*maxBatchesPerFile; i++ {
			consumer.ExpectConsumePartition(utils.GetTopicFromTable("test", "test"), 0, mocks.AnyOffset).
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
			upsertBatch, kafkaOffset := nextUpsertBatch()
			fileID, offset := redoManager.AppendToRedoLog(upsertBatch, kafkaOffset)
			fileIDs[fileID] = struct{}{}
			Ω(upsertBatch.GetBuffer()).Should(Equal(upsertBatchBytes))
			Ω(fileID).Should(Equal(int64(i / maxBatchesPerFile)))
			Ω(offset).Should(Equal(uint32(i % maxBatchesPerFile)))
		}

		Ω(fileIDs).Should(HaveLen(3))
		Ω(fileIDs).Should(HaveKey(int64(0)))
		Ω(fileIDs).Should(HaveKey(int64(1)))
		Ω(fileIDs).Should(HaveKey(int64(2)))

		err = redoManager.CheckpointRedolog(1, 1, 0)
		Ω(err).Should(BeNil())
		Ω(commitedOffset).Should(ContainElement(int64(maxBatchesPerFile)))
		redoManager.Close()

		upsertBatch, kafkaOffset := nextUpsertBatch()
		fileID, offset := redoManager.getFileOffset(kafkaOffset)

		Ω(upsertBatch).Should(BeNil())
		Ω(fileID).Should(BeEquivalentTo(0))
		Ω(offset).Should(BeEquivalentTo(0))
	})

})
