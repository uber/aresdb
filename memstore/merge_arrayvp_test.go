package memstore

import (
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/memstore/list"
	"sync"
	"unsafe"
)

var _ = ginkgo.Describe("merge", func() {
	var cutoff uint32 = 200
	var batch0, batch1 *common.Batch

	var hostMemoryManager common.HostMemoryManager
	var shard *TableShard
	var patch *archivingPatch
	var base *ArchiveBatch
	var expectedBatch *common.Batch

	sortColumnVals01 := [][]uint16{
		{1, 4},
	}
	sortColumnVals02 := [][]uint16{
		{1, 3},
		{2, 1},
	}
	sortColumnVals11 := [][]uint16{
		{1, 2},
		{2, 2},
	}
	sortColumnVals12 := [][]uint16{
		{2, 2},
		{3, 2},
	}
	strVals1 := []string{
		"[11,12]",
		"[13,14]",
		"[15,16]",
		"[17,18]",
	}
	strVals2 := []string{
		"[21,22]",
		"[23,24]",
		"[25,26]",
		"[27,28]",
	}
	mergedCol0 := [][]uint16{
		{1, 7},
		{2, 1},
	}
	mergedCol1 := [][]uint16{
		{1, 2},
		{2, 4},
		{3, 1},
		{3, 1},
	}
	mergedVals := []string{
		"[11,12]",
		"[13,14]",
		"[15,16]",
		"[17,18]",
		"[21,22]",
		"[23,24]",
		"[25,26]",
		"[27,28]",
	}

	createArrayVP := func(dataType common.DataType, strVals []string, isLive bool) common.VectorParty {
		var vp common.VectorParty
		if isLive {
			vp = list.NewLiveVectorParty(len(strVals), dataType, hostMemoryManager)
		} else {
			valueBytes := len(strVals) * common.CalculateListElementBytes(dataType, 2)
			vp = list.NewArchiveVectorParty(len(strVals), dataType, int64(valueBytes), &sync.RWMutex{})
		}
		vp.Allocate(false)
		for i, str := range strVals {
			val, _ := common.ValueFromString(str, dataType)
			vp.SetDataValue(i, val, common.IgnoreCount)
		}
		return vp
	}

	createSortedVP := func(vals [][]uint16, isLive bool) common.VectorParty {
		if isLive {
			var length int
			for _, val := range vals {
				length += int(val[1])
			}
			vp := NewLiveVectorParty(length, common.Uint16, common.NullDataValue, hostMemoryManager)
			vp.Allocate(false)
			var index int
			for _, val := range vals {
				for j := 0; j < int(val[1]); j++ {
					vp.SetValue(index, unsafe.Pointer(&val[0]), true)
					index++
				}
			}
			return vp
		}

		vp := newArchiveVectorParty(len(vals), common.Uint16, common.NullDataValue, &sync.RWMutex{})
		vp.Allocate(true)
		var count uint32
		for index, val := range vals {
			dataValue := common.DataValue{
				OtherVal: unsafe.Pointer(&val[0]),
				Valid:    true,
				DataType: common.Uint16,
			}
			vp.SetDataValue(index, dataValue, common.IncrementCount)
			count += uint32(val[1])
			vp.SetCount(index, count)
		}
		return vp
	}

	ginkgo.BeforeEach(func() {
		hostMemoryManager = NewHostMemoryManager(GetFactory().NewMockMemStore(), 1<<32)
		shard = &TableShard{
			HostMemoryManager: hostMemoryManager,
		}

		batch0 = &common.Batch{
			RWMutex: &sync.RWMutex{},
			Columns: []common.VectorParty{
				createSortedVP(sortColumnVals01, false),
				createSortedVP(sortColumnVals11, false),
				createArrayVP(common.ArrayUint8, strVals1, false),
				createArrayVP(common.ArrayInt8, strVals1, false),
				createArrayVP(common.ArrayUint16, strVals1, false),
				createArrayVP(common.ArrayUint32, strVals1, false),
				createArrayVP(common.ArrayInt64, strVals1, false),
			},
		}
		batch1 = &common.Batch{
			RWMutex: &sync.RWMutex{},
			Columns: []common.VectorParty{
				createSortedVP(sortColumnVals02, true),
				createSortedVP(sortColumnVals12, true),
				createArrayVP(common.ArrayUint8, strVals2, true),
				createArrayVP(common.ArrayInt8, strVals2, true),
				createArrayVP(common.ArrayUint16, strVals2, true),
				createArrayVP(common.ArrayUint32, strVals2, true),
				createArrayVP(common.ArrayInt64, strVals2, true),
			},
		}
		vs := &LiveStore{
			LastReadRecord: common.RecordID{BatchID: -101, Index: 4},
			Batches: map[int32]*LiveBatch{
				-101: {
					Batch:     *batch1,
					Capacity:  4,
					liveStore: nil,
				},
			},
		}

		ss := vs.snapshot()

		patch = &archivingPatch{
			recordIDs: []common.RecordID{
				{BatchID: 0, Index: 0},
				{BatchID: 0, Index: 1},
				{BatchID: 0, Index: 2},
				{BatchID: 0, Index: 3},
			},
			sortColumns: []int{0, 1},
			data:        ss,
		}

		base = &ArchiveBatch{
			Version: 0,
			Size:    4,
			Batch:   *batch0,
			Shard:   shard,
		}

		expectedBatch = &common.Batch{
			Columns: []common.VectorParty{
				createSortedVP(mergedCol0, false),
				createSortedVP(mergedCol1, false),
				createArrayVP(common.ArrayUint8, mergedVals, false),
				createArrayVP(common.ArrayInt8, mergedVals, false),
				createArrayVP(common.ArrayUint16, mergedVals, false),
				createArrayVP(common.ArrayUint32, mergedVals, false),
				createArrayVP(common.ArrayInt64, mergedVals, false),
			},
		}
	})

	ginkgo.It("merge on array columns", func() {
		ctx := newMergeContext(base, patch,
			[]bool{false, false, false, false, false, false, false},
			[]common.DataType{common.Uint16, common.Uint16, common.ArrayUint8, common.ArrayInt8, common.ArrayUint16, common.ArrayUint32, common.ArrayInt64},
			[]*common.DataValue{&common.NullDataValue, &common.NullDataValue, &common.NullDataValue, &common.NullDataValue, &common.NullDataValue, &common.NullDataValue, &common.NullDataValue},
			nil)
		ctx.merge(cutoff, 0)

		for i, col := range ctx.merged.Columns {
			Ω(col.Equals(expectedBatch.Columns[i])).Should(BeTrue())
		}
	})
})
