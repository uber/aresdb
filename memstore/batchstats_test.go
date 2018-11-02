package memstore

import (
	"github.com/uber/aresdb/memstore/common"
	"github.com/onsi/ginkgo"
	"github.com/stretchr/testify/mock"

	metaMocks "github.com/uber/aresdb/metastore/mocks"
	. "github.com/onsi/gomega"
	"time"
)

var _ = ginkgo.Describe("batch stats should work", func() {
	metaStore := &metaMocks.MetaStore{}
	memStore := createMemStore("abc", 0, []common.DataType{common.Uint32, common.Uint8}, []int{0}, 10, true, metaStore, CreateMockDiskStore())

	batchStatsReporter := NewBatchStatsReporter(1, memStore, metaStore)

	ginkgo.It("batch stats report should work", func() {
		metaStore.On("GetOwnedShards", mock.Anything).Return([]int{0}, nil)
		metaStore.On("GetArchiveBatchVersion", mock.Anything, 0, mock.Anything, mock.Anything).Return(uint32(0), uint32(0), 0, nil)

		builder := common.NewUpsertBatchBuilder()
		// Put event time to the 2nd column.
		builder.AddColumn(1, common.Uint8)
		builder.AddColumn(0, common.Uint32)
		builder.AddRow()
		builder.SetValue(0, 1, uint32(23456))
		builder.SetValue(0, 0, uint8(123))
		buffer, _ := builder.ToByteArray()
		upsertBatch, _ := NewUpsertBatch(buffer)
		shard, err := memStore.GetTableShard("abc", 0)

		err = memStore.HandleIngestion("abc", 0, upsertBatch)
		Ω(err).Should(BeNil())
		Ω(shard.LiveStore.lastModifiedTimePerColumn).Should(Equal([]uint32{23456, 23456}))

		Ω(shard.LiveStore.LastReadRecord.BatchID).Should(Equal(BaseBatchID))

		go batchStatsReporter.Run()
		time.Sleep(time.Second * 2)
		batchStatsReporter.Stop()
	})
})
