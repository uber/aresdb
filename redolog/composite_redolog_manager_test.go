package redolog

import (
	"github.com/Shopify/sarama"
	kafkaMocks "github.com/Shopify/sarama/mocks"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
	"github.com/uber/aresdb/common"
	diskMocks "github.com/uber/aresdb/diskstore/mocks"
	memCom "github.com/uber/aresdb/memstore/common"
	metaCom "github.com/uber/aresdb/metastore/common"
	metaMocks "github.com/uber/aresdb/metastore/mocks"
	"github.com/uber/aresdb/testing"
	"github.com/uber/aresdb/utils"
)

func createMockMetaStore() *metaMocks.MetaStore {
	metaStore := &metaMocks.MetaStore{}
	metaStore.On("GetRedoLogCheckpointOffset", "table1", 0).Return(int64(1000), nil)
	metaStore.On("GetRedoLogCommitOffset", "table1", 0).Return(int64(2000), nil)
	metaStore.On("UpdateRedoLogCommitOffset", "table1", 0, mock.MatchedBy(func(offset int64) bool {
		return offset%100 == 99
	})).Return(nil)
	metaStore.On("UpdateRedoLogCheckpointOffset", "table1", 0, mock.Anything).Return(nil)

	return metaStore
}

var _ = ginkgo.Describe("composite redolog manager tests", func() {

	table := "table1"
	shard := 0
	namespace := "ns1"
	tableConfig := &metaCom.TableConfig{
		RedoLogRotationInterval: 1,
		MaxRedoLogFileSize:      100000,
	}
	redoLogConfig := &common.RedoLogConfig{
		Namespace: namespace,
		DiskConfig: common.DiskRedoLogConfig{
			Disabled: false,
		},
		KafkaConfig: common.KafkaRedoLogConfig{
			Enabled: true,
			Brokers: []string{
				"host1",
				"host2",
			},
		},
	}

	ginkgo.It("Test kafka only redolog manager", func() {
		redoLogConfig.DiskConfig.Disabled = true

		mockMetaStore := createMockMetaStore()
		consumer, _ := testing.MockKafkaConsumerFunc(nil)
		f, err := NewKafkaRedoLogManagerMaster(redoLogConfig, &diskMocks.DiskStore{}, mockMetaStore, consumer)
		Ω(err).Should(BeNil())
		Ω(f).ShouldNot(BeNil())
		m, err := f.NewRedologManager(table, shard, tableConfig)

		Ω(err).Should(BeNil())
		Ω(m).ShouldNot(BeNil())
		cm := m.(*kafkaRedoLogManager)
		Ω(cm).ShouldNot(BeNil())

		buffer, _ := memCom.NewUpsertBatchBuilder().ToByteArray()
		upsertBatch, _ := memCom.NewUpsertBatch(buffer)
		for i := 0; i < 2*maxBatchesPerFile; i++ {
			f.consumer.(*kafkaMocks.Consumer).ExpectConsumePartition(utils.GetTopicFromTable(namespace, table), int32(shard), kafkaMocks.AnyOffset).
				YieldMessage(&sarama.ConsumerMessage{
					Value: upsertBatch.GetBuffer(),
				})
		}

		nextUpsertBatch, err := cm.Iterator()
		for i := 1; i <= 2*maxBatchesPerFile; i++ {
			 nextUpsertBatch()
		}

		Ω(cm.batchReceived).Should(Equal(2*maxBatchesPerFile - 2000))
		Ω(cm.batchRecovered).Should(Equal(2000))
		Ω(cm.GetNumFiles()).Should(Equal(3))

		err = m.CheckpointRedolog(1, 1, 0)
		Ω(cm.GetNumFiles()).Should(Equal(3))
		Ω(cm.FirstKafkaOffsetPerFile[0]).Should(Equal(int64(1)))
		Ω(cm.FirstKafkaOffsetPerFile[1]).Should(Equal(int64(5000)))
		Ω(cm.FirstKafkaOffsetPerFile[2]).Should(Equal(int64(10000)))

		Ω(mockMetaStore.AssertNumberOfCalls(utils.TestingT, "UpdateRedoLogCommitOffset", 80)).Should(BeTrue())
	})

	ginkgo.It("Test kafka with local file redolog manager", func() {
		mockMetaStore := createMockMetaStore()

		buffer, _ := memCom.NewUpsertBatchBuilder().ToByteArray()

		file1 := &testing.TestReadWriteCloser{}
		streamWriter1 := utils.NewStreamDataWriter(file1)
		streamWriter1.WriteUint32(UpsertHeader)
		streamWriter1.WriteUint32(uint32(len(buffer)))
		streamWriter1.Write(buffer)
		streamWriter1.WriteUint32(uint32(len(buffer)))
		streamWriter1.Write(buffer)

		file2 := &testing.TestReadWriteCloser{}
		streamWriter2 := utils.NewStreamDataWriter(file2)
		streamWriter2.WriteUint32(UpsertHeader)
		streamWriter2.WriteUint32(uint32(len(buffer)))
		streamWriter2.Write(buffer)

		diskStore := &diskMocks.DiskStore{}
		diskStore.On("ListLogFiles", mock.Anything, mock.Anything).Return([]int64{1, 2}, nil)
		diskStore.On("OpenLogFileForAppend", mock.Anything, mock.Anything, mock.Anything).Return(&testing.TestReadWriteCloser{}, nil)
		diskStore.On("OpenLogFileForReplay", mock.Anything, mock.Anything, int64(1)).Return(file1, nil)
		diskStore.On("OpenLogFileForReplay", mock.Anything, mock.Anything, int64(2)).Return(file2, nil)

		redoLogConfig.DiskConfig.Disabled = false
		consumer, _ := testing.MockKafkaConsumerFunc(nil)
		f, err := NewKafkaRedoLogManagerMaster(redoLogConfig, diskStore, mockMetaStore, consumer)
		Ω(err).Should(BeNil())
		Ω(f).ShouldNot(BeNil())
		m, err := f.NewRedologManager(table, shard, tableConfig)
		Ω(err).Should(BeNil())
		Ω(m).ShouldNot(BeNil())

		cm := m.(*compositeRedoLogManager)
		Ω(cm).ShouldNot(BeNil())
		Ω(cm.fileRedoLogManager).ShouldNot(BeNil())
		Ω(cm.kafkaReader).ShouldNot(BeNil())

		upsertBatch, _ := memCom.NewUpsertBatch(buffer)
		for i := 0; i < 2*maxBatchesPerFile; i++ {
			f.consumer.(*kafkaMocks.Consumer).ExpectConsumePartition(utils.GetTopicFromTable(namespace, table), int32(shard), kafkaMocks.AnyOffset).
				YieldMessage(&sarama.ConsumerMessage{
					Value: upsertBatch.GetBuffer(),
				})
		}

		nextUpsertBatch, err := cm.Iterator()
		for i := 1; i <= 2*maxBatchesPerFile+3; i++ {
			 nextUpsertBatch()
		}

		Ω(cm.fileRedoLogManager.batchRecovered).Should(Equal(3))
		Ω(cm.kafkaReader.batchReceived).Should(Equal(2 * maxBatchesPerFile))

		Ω(mockMetaStore.AssertNumberOfCalls(utils.TestingT, "UpdateRedoLogCommitOffset", 100)).Should(BeTrue())
	})
})
