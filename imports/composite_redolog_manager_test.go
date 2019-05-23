package imports

import (
	"errors"
	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
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
	"time"
)

type mockLiveStore struct {
	m     RedologManager
	count int64
}

func (s *mockLiveStore) mockStoreFunc(batch *memCom.UpsertBatch, redoFile int64, offset uint32, skipBackFillRows bool) error {
	s.count++

	if s.count%1000 == 0 {
		return errors.New("purposed error")
	}
	s.m.UpdateMaxEventTime(uint32(time.Now().Unix()), redoFile)
	return nil
}

func createMockMetaStore() *metaMocks.MetaStore {
	metaStore := &metaMocks.MetaStore{}
	metaStore.On("GetIngestionCheckpointOffset", "table1", 0).Return(int64(5), nil)
	metaStore.On("GetIngestionCommitOffset", "table1", 0).Return(int64(11), nil)
	metaStore.On("UpdateIngestionCommitOffset", "table1", 0, mock.MatchedBy(func(offset int64) bool {
		return offset%10 == 0
	})).Return(nil)
	metaStore.On("UpdateIngestionCheckpointOffset", "table1", 0, mock.Anything).Return(nil)

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
	c := &common.ImportsConfig{
		Namespace: namespace,
		Source:    common.KafkaSoureOnly,
		RedoLog: common.RedoLogConfig{
			Disabled: true,
		},
		KafkaConfig: common.KafkaConfig{
			Brokers: []string{
				"host1",
				"host2",
			},
		},
	}
	ginkgo.It("Test original file redolog manager", func() {
		f, err := NewRedologManagerFactory(nil, nil, nil)
		Ω(err).Should(BeNil())
		Ω(f).ShouldNot(BeNil())
		Ω(f.enableLocalRedoLog).Should(BeTrue())
		m, err := f.NewRedologManager(table, shard, tableConfig, nil)
		Ω(err).Should(BeNil())
		Ω(m).ShouldNot(BeNil())
		cm := m.(*CompositeRedologManager)
		Ω(cm).ShouldNot(BeNil())
		Ω(cm.GetLocalFileRedologManager()).ShouldNot(BeNil())
		Ω(cm.GetKafkaReader()).Should(BeNil())
	})

	ginkgo.It("Test kafka only redolog manager", func() {
		store := &mockLiveStore{}

		mockMetaStore := createMockMetaStore()
		consumer, _ := testing.MockKafkaConsumerFunc(nil)
		f, err := NewKafkaRedologManagerFactory(c, &diskMocks.DiskStore{}, mockMetaStore, consumer)
		Ω(err).Should(BeNil())
		Ω(f).ShouldNot(BeNil())
		Ω(f.enableLocalRedoLog).Should(BeFalse())
		m, err := f.NewRedologManager(table, shard, tableConfig, store.mockStoreFunc)
		store.m = m

		Ω(err).Should(BeNil())
		Ω(m).ShouldNot(BeNil())
		cm := m.(*CompositeRedologManager)
		Ω(cm).ShouldNot(BeNil())
		Ω(cm.GetLocalFileRedologManager()).Should(BeNil())
		Ω(cm.GetKafkaReader()).ShouldNot(BeNil())

		buffer, _ := memCom.NewUpsertBatchBuilder().ToByteArray()
		upsertBatch, _ := memCom.NewUpsertBatch(buffer)
		for i := 0; i < 2*5000; i++ {
			f.consumer.(*mocks.Consumer).ExpectConsumePartition(utils.GetTopicFromTable(namespace, table), int32(shard), mocks.AnyOffset).
				YieldMessage(&sarama.ConsumerMessage{
					Value: upsertBatch.GetBuffer(),
				})
		}

		cm.Start(0, 0)

		for store.count < 2*maxBatchesPerFile {
			time.Sleep(100)
		}

		Ω(cm.msgReceived).Should(Equal(int64(2*maxBatchesPerFile - 10)))
		Ω(cm.msgRecovered).Should(Equal(int64(10)))
		Ω(store.count).Should(Equal(int64(2 * maxBatchesPerFile)))
		Ω(cm.GetNumFiles()).Should(Equal(3))

		err = m.CheckpointRedolog(1, 1, 0)
		Ω(cm.GetNumFiles()).Should(Equal(3))
		Ω(cm.kafkaReader.FirstKafkaOffsetPerFile[0]).Should(Equal(int64(11)))
		Ω(cm.kafkaReader.FirstKafkaOffsetPerFile[1]).Should(Equal(int64(5000)))
		Ω(cm.kafkaReader.FirstKafkaOffsetPerFile[2]).Should(Equal(int64(10000)))

		Ω(mockMetaStore.AssertNumberOfCalls(utils.TestingT, "UpdateIngestionCommitOffset", 99)).Should(BeTrue())
		Ω(mockMetaStore.AssertNumberOfCalls(utils.TestingT, "UpdateIngestionCheckpointOffset", 1)).Should(BeTrue())

		// add data for kafka only redolog should be fail
		batch, _ := memCom.NewUpsertBatch(buffer)
		err = m.WriteUpsertBatch(batch)
		Ω(err).ShouldNot(BeNil())

	})

	ginkgo.It("Test kafka with local file redolog manager", func() {
		store := &mockLiveStore{}
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

		c.RedoLog.Disabled = false
		consumer, _ := testing.MockKafkaConsumerFunc(nil)
		f, err := NewKafkaRedologManagerFactory(c, diskStore, mockMetaStore, consumer)
		Ω(err).Should(BeNil())
		Ω(f).ShouldNot(BeNil())
		Ω(f.enableLocalRedoLog).Should(BeTrue())
		m, err := f.NewRedologManager(table, shard, tableConfig, store.mockStoreFunc)
		Ω(err).Should(BeNil())
		Ω(m).ShouldNot(BeNil())
		store.m = m

		cm := m.(*CompositeRedologManager)
		Ω(cm).ShouldNot(BeNil())
		Ω(cm.GetLocalFileRedologManager()).ShouldNot(BeNil())
		Ω(cm.GetKafkaReader()).ShouldNot(BeNil())

		upsertBatch, _ := memCom.NewUpsertBatch(buffer)
		for i := 0; i < 2*5000; i++ {
			f.consumer.(*mocks.Consumer).ExpectConsumePartition(utils.GetTopicFromTable(namespace, table), int32(shard), mocks.AnyOffset).
				YieldMessage(&sarama.ConsumerMessage{
					Value: upsertBatch.GetBuffer(),
				})
		}

		cm.Start(0, 0)

		for store.count < 2*maxBatchesPerFile+3 {
			time.Sleep(100)
		}
		Ω(cm.msgReceived).Should(Equal(int64(2 * maxBatchesPerFile)))
		Ω(cm.msgRecovered).Should(Equal(int64(3)))
		Ω(store.count).Should(Equal(int64(2*maxBatchesPerFile + 3)))
		err = m.CheckpointRedolog(1, 1, 0)

		Ω(mockMetaStore.AssertNumberOfCalls(utils.TestingT, "UpdateIngestionCommitOffset", 100)).Should(BeTrue())
		Ω(mockMetaStore.AssertNumberOfCalls(utils.TestingT, "UpdateIngestionCheckpointOffset", 0)).Should(BeTrue())
	})
})
