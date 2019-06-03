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

package redolog

import (
	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber/aresdb/common"
	metaCom "github.com/uber/aresdb/metastore/common"
	metaMocks "github.com/uber/aresdb/metastore/mocks"
	"github.com/uber/aresdb/testing"
	"github.com/uber/aresdb/utils"
)

var _ = ginkgo.Describe("kafka redolog manager", func() {
	var t testing.GinkgoTestReporter

	namespace := "ns1"
	table := "table1"
	shard := 0

	tableConfig := &metaCom.TableConfig{
		RedoLogRotationInterval: 1,
		MaxRedoLogFileSize:      100000,
	}

	redoLogCfg := &common.RedoLogConfig{
		DiskConfig: common.DiskRedoLogConfig{
			Disabled: true,
		},
		KafkaConfig: common.KafkaRedoLogConfig{
			Enabled: true,
			Brokers: []string{
				"host1",
				"host2",
			},
		},
	}
	metaStore := &metaMocks.MetaStore{}

	ginkgo.It("Iterator should work", func() {
		config := sarama.NewConfig()
		config.ChannelBufferSize = 2 * maxBatchesPerFile
		consumer := mocks.NewConsumer(t, config)
		commitedOffset := make([]int64, 0)
		commitFunc := func(table string, shard int, offset int64) error {
			commitedOffset = append(commitedOffset, offset)
			return nil
		}
		checkPointFunc := func(table string, shard int, offset int64) error {
			return nil
		}
		getCommitFunc := func(table string, shard int) (int64, error) {
			return 0, nil
		}
		getCheckpointFunc := func(table string, shard int) (int64, error) {
			return 0, nil
		}

		upsertBatchBytes := []byte{1, 0, 237, 254, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 51, 0, 0, 0, 57, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 2, 0, 123, 0, 1, 0, 0, 0, 0, 0, 135, 0, 0, 0, 0, 0, 0, 0}
		m, err := NewKafkaRedoLogManagerMaster(redoLogCfg, nil, metaStore, consumer)
		Ω(err).Should(BeNil())
		r, err := m.NewRedologManager(table, shard, tableConfig)
		Ω(err).Should(BeNil())
		Ω(r.(*kafkaRedoLogManager)).ShouldNot(BeNil())

		redoManager := newKafkaRedoLogManager(namespace, table, shard, consumer, true, commitFunc, checkPointFunc, getCommitFunc, getCheckpointFunc)
		// create 2 * maxBatchesPerFile number of messages
		for i := 0; i < 2*maxBatchesPerFile; i++ {
			consumer.ExpectConsumePartition(utils.GetTopicFromTable(namespace, table), 0, mocks.AnyOffset).
				YieldMessage(&sarama.ConsumerMessage{
					Value: upsertBatchBytes,
				})
		}

		// since offset starts from 1, we will have three files
		fileIDs := map[int64]struct{}{}
		nextUpsertBatch, err := redoManager.Iterator()
		for i := 1; i <= 2*maxBatchesPerFile; i++ {
			batchInfo := nextUpsertBatch()
			fileIDs[batchInfo.RedoLogFile] = struct{}{}
			Ω(batchInfo.Batch.GetBuffer()).Should(Equal(upsertBatchBytes))
			Ω(batchInfo.RedoLogFile).Should(Equal(int64(i / maxBatchesPerFile)))
			Ω(batchInfo.BatchOffset).Should(Equal(uint32(i % maxBatchesPerFile)))
		}

		Ω(fileIDs).Should(HaveLen(3))
		Ω(fileIDs).Should(HaveKey(int64(0)))
		Ω(fileIDs).Should(HaveKey(int64(1)))
		Ω(fileIDs).Should(HaveKey(int64(2)))

		err = redoManager.CheckpointRedolog(1, 1, 0)
		Ω(err).Should(BeNil())
		Ω(commitedOffset).Should(ContainElement(int64(maxBatchesPerFile - 1)))
		redoManager.Close()

		batchInfo := nextUpsertBatch()
		Ω(batchInfo).Should(BeNil())
	})

})
