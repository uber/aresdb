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

package memstore

import (
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber/aresdb/memstore/common"
	"sync"
)

var _ = ginkgo.Describe("merge", func() {
	var cutoff uint32 = 200
	var batch0, batch1, noSortColumnBatch *Batch
	var err error
	//var scheduler *schedulerImpl
	var hostMemoryManager common.HostMemoryManager
	var shard *TableShard
	var patch, noSortColumnPatch, patchWithDeletedColumns *archivingPatch
	var base, merged, noSortColumnBase, noSortColumnMerged, mergedWithDeletedColumnsMerged *ArchiveBatch

	ginkgo.BeforeEach(func() {

		//scheduler = newScheduler(getFactory().NewMockMemStore())
		hostMemoryManager = NewHostMemoryManager(getFactory().NewMockMemStore(), 1<<32)
		shard = &TableShard{
			HostMemoryManager: hostMemoryManager,
		}
		// this table describes the archivingPatch layout
		// please notice that record ids is in increasing order
		// for convenience, usually it should be out of order
		//+---------------+-------------------+-------------------------+-----------------+---------------------+-------------------------+
		//| Column1(Bool) | Column2 (Float32) | Column3 all nulls(bool) | Column4 (int32) | Column0 time column | Column5 all nulls(bool) |

		// batch 0
		//+---------------+-------------------+-------------------------+-----------------+---------------------+-------------------------+
		//| null          | -0.1              | null                    | 1               | 100                 | null                    |
		//+---------------+-------------------+-------------------------+-----------------+---------------------+-------------------------+
		//| null          | -0.1              | null                    | 2               | 110                 | null                    |
		//+---------------+-------------------+-------------------------+-----------------+---------------------+-------------------------+
		//| null          | 0.0               | null                    | 2               | 120                 | null                    |
		//+---------------+-------------------+-------------------------+-----------------+---------------------+-------------------------+
		//| null          | 0.1               | null                    | 2               | 130                 | null                    |
		//+---------------+-------------------+-------------------------+-----------------+---------------------+-------------------------+

		// batch 1
		//| false         | null              | null                    | 1               | 140                 | null                    |
		//+---------------+-------------------+-------------------------+-----------------+---------------------+-------------------------+
		//| false         | null              | null                    | 1               | 150                 | null                    |
		//+---------------+-------------------+-------------------------+-----------------+---------------------+-------------------------+
		//| true          | 0.1               | null                    | null            | 160                 | null                    |
		//+---------------+-------------------+-------------------------+-----------------+---------------------+-------------------------+
		//| true          | 0.1               | null                    | 2               | 170                 | null                    |
		//+---------------+-------------------+-------------------------+-----------------+---------------------+-------------------------+

		batch0, err = getFactory().ReadArchiveBatch("patchBatch0")
		Ω(err).Should(BeNil())
		batch1, err = getFactory().ReadArchiveBatch("patchBatch1")
		Ω(err).Should(BeNil())
		vs := &LiveStore{
			LastReadRecord: RecordID{-101, 4},
			Batches: map[int32]*LiveBatch{
				-110: {
					Batch:     *batch0,
					Capacity:  4,
					liveStore: nil,
				},
				-101: {
					Batch:     *batch1,
					Capacity:  4,
					liveStore: nil,
				},
			},
		}

		ss := vs.snapshot()

		patch = &archivingPatch{
			recordIDs: []RecordID{
				{0, 0},
				{0, 1},
				{0, 2},
				{0, 3},
				{1, 0},
				{1, 1},
				{1, 2},
				{1, 3},
			},
			sortColumns: []int{1, 2, 3, 4},
			data:        ss,
		}

		// Sorted column 0 (time column)
		//+--------+
		//| Values |
		//+--------+
		//| 0      |
		//+--------+
		//| 10     |
		//+--------+
		//| 20     |
		//+--------+
		//| 30     |
		//+--------+
		//| 40     |
		//+--------+

		// ArchiveBatch column 1
		//+--------+-------+--------+
		//| Values | Nulls | Counts |
		//+--------+-------+--------+
		//| null   | 0     | 0      |
		//+--------+-------+--------+
		//| false  | 1     | 3      |
		//+--------+-------+--------+
		//| true   | 1     | 4      |
		//+--------+-------+--------+
		//				   | 5      |
		//				   +--------+

		// ArchiveBatch Column 2
		//+--------+-------+--------+
		//| Values | Nulls | Counts |
		//+--------+-------+--------+
		//| 0.0    | 1     | 0      |
		//+--------+-------+--------+ null
		//| 0.05   | 1     | 1      |
		//+--------+-------+--------+
		//| 0.1    | 1     | 2      |
		//+--------+-------+--------+---------------
		//| 0.1    | 1     | 3      | false
		//+--------+-------+--------+---------------
		//| null   | 0     | 4      | true
		//+--------+-------+--------+---------------
		//                 | 5      |
		//				   +--------+

		// ArchiveBatch Column 3 mode 0
		//+--------+-------+--------+
		//| Values | Nulls | Counts |

		// ArchiveBatch Column 4
		//+--------+-------+--------+
		//| Values | Nulls | Counts |
		//+--------+-------+--------+
		//| 1      | 1     | 0      | null,0.0,null
		//+--------+-------+--------+--------------
		//| 2      | 1     | 1      | null,0.05,null
		//+--------+-------+--------+--------------
		//| 2      | 1     | 2      | null,0.1,null
		//+--------+-------+--------+--------------
		//| 1      | 1     | 3      | false,0.1,null
		//+--------+-------+--------+---------------
		//| 1      | 1     | 4      | true,null,null
		//+--------+-------+--------+--------------
		//                 | 5      |
		//				   +--------+
		tmpBatch, err := getFactory().ReadArchiveBatch("archiveBatch")
		Ω(err).Should(BeNil())
		base = &ArchiveBatch{
			Version: 0,
			Size:    5,
			Batch:   *tmpBatch,
			Shard:   shard,
		}

		// Expected Result
		// merged column 0 (time column)
		//+--------+
		//| Values |
		//+--------+
		//| 100    |
		//+--------+
		//| 110    |
		//+--------+
		//| 0      |
		//+--------+
		//| 120    |
		//+--------+
		//| 10     |
		//+--------+
		//| 20     |
		//+--------+
		//| 130    |
		//+--------+
		//| 140    |
		//+--------+
		//| 150    |
		//+--------+
		//| 30     |
		//+--------+
		//| 40     |
		//+--------+
		//| 160    |
		//+--------+
		//| 170    |
		//+--------+

		// merged column 1
		//+--------+-------+--------+
		//| Values | Nulls | Counts |
		//+--------+-------+--------+
		//| null   | 0     | 0      |
		//+--------+-------+--------+
		//| false  | 1     | 7      |
		//+--------+-------+--------+
		//| true   | 1     | 10     |
		//+--------+-------+--------+
		//				   | 13     |
		//				   +--------+

		// merged Column 2
		//+--------+-------+--------+
		//| Values | Nulls | Counts |
		//+--------+-------+--------+
		//| -0.1   | 1     | 0      |  null
		//+--------+-------+--------+
		//| 0.0    | 1     | 2      |
		//+--------+-------+--------+
		//| 0.05   | 1     | 4      |
		//+--------+-------+--------+
		//| 0.1    | 1     | 5      |
		//+--------+-------+--------+-----------------
		//| null   | 0     | 7      |
		//+--------+-------+--------+  false
		//| 0.1    | 1     | 9      |
		//+--------+-------+--------+-----------------
		//| null   | 0     | 10     |  true
		//+--------+-------+--------+
		//| 0.1    | 1     | 11     |
		//+--------+-------+--------+
		//                 | 13     |
		//				   +--------+

		// merged Column 4
		//+--------+-------+--------+
		//| Values | Nulls | Counts |
		//+--------+-------+--------+
		//| 1      | 1     | 0      |
		//+--------+-------+--------+ null,-0.1,null
		//| 2      | 1     | 1      |
		//+--------+-------+--------+---------------
		//| 1      | 1     | 2      |
		//+--------+-------+--------+
		//| 2      | 1     | 3      | null,0.0,null
		//+--------+-------+--------+----------------
		//| 2      | 1     | 4      | null,0.05,null
		//+--------+-------+--------+----------------
		//| 2      | 1     | 5      | null,0.1,null
		//+--------+-------+--------+----------------
		//| 1      | 1     | 7      | false,null,null
		//+--------+-------+--------+----------------
		//| 1      | 1     | 9      | false,1,null
		//+--------+-------+--------+----------------
		//| 1      | 1     | 10     | true,null,null
		//+--------+-------+--------+----------------
		//| null   | 0     | 11     | true,0.1,null
		//+--------+-------+--------+----------------
		//| 2      | 1     | 12     | true,0.1,null
		//+--------+-------+--------+----------------
		//                 | 13     |
		//

		tmpBatch, err = getFactory().ReadArchiveBatch("mergedBatch")
		Ω(err).Should(BeNil())
		merged = &ArchiveBatch{
			Version: cutoff,
			Size:    13,
			Batch:   *tmpBatch,
			Shard:   shard,
		}

		// Init test data for no sort columns test case.
		noSortColumnBatch, err = getFactory().ReadArchiveBatch("no-sort-columns/patchBatch")
		Ω(err).Should(BeNil())

		vs2 := &LiveStore{
			LastReadRecord: RecordID{-101, 4},
			Batches: map[int32]*LiveBatch{
				-101: {
					Batch:     *noSortColumnBatch,
					Capacity:  4,
					liveStore: nil,
				},
			},
		}

		ss2 := vs2.snapshot()

		noSortColumnPatch = &archivingPatch{
			recordIDs: []RecordID{
				{0, 0},
				{0, 1},
				{0, 2},
				{0, 3},
			},
			data: ss2,
		}

		tmpBatch, err = getFactory().ReadArchiveBatch("no-sort-columns/baseBatch")
		Ω(err).Should(BeNil())

		noSortColumnBase = &ArchiveBatch{
			Batch:   *tmpBatch,
			Version: 0,
			Size:    4,
			Shard:   shard,
		}

		tmpBatch, err = getFactory().ReadArchiveBatch("no-sort-columns/mergedBatch")
		Ω(err).Should(BeNil())
		noSortColumnMerged = &ArchiveBatch{
			Batch:   *tmpBatch,
			Version: 0,
			Size:    8,
			Shard:   shard,
		}

		// Test deleted columns.
		patchWithDeletedColumns = &archivingPatch{
			recordIDs: []RecordID{
				{0, 0},
				{0, 1},
				{0, 2},
				{0, 3},
				{1, 0},
				{1, 1},
				{1, 2},
				{1, 3},
			},
			sortColumns: []int{1, 2, 3, 4},
			data:        ss,
		}

		tmpBatch, err = getFactory().ReadArchiveBatch("merge-with-deleted-columns/mergedBatch")
		Ω(err).Should(BeNil())

		mergedWithDeletedColumnsMerged = &ArchiveBatch{
			Batch:   *tmpBatch,
			Version: 0,
			Size:    13,
			Shard:   shard,
		}
	})

	ginkgo.It("preallocate", func() {
		ctx := newMergeContext(base, patch,
			[]bool{false, false, false, false, false, false},
			[]common.DataType{common.Uint32, common.Bool, common.Float32, common.Bool, common.Int32, common.Bool},
			[]*common.DataValue{&common.NullDataValue, &common.NullDataValue, &common.NullDataValue, &common.NullDataValue, &common.NullDataValue, &common.NullDataValue},
			nil)
		ctx.mergeRecursive(
			0,
			7,
			8,
			ctx.preAllocate,
		)
		Ω(ctx.mergedLengths).Should(Equal(
			[]int{3, 8, 8, 11},
		))
	})

	ginkgo.It("merge", func() {
		ctx := newMergeContext(base, patch,
			[]bool{false, false, false, false, false, false},
			[]common.DataType{common.Uint32, common.Bool, common.Float32, common.Bool, common.Int32, common.Bool},
			[]*common.DataValue{&common.NullDataValue, &common.NullDataValue, &common.NullDataValue, &common.NullDataValue, &common.NullDataValue, &common.NullDataValue},
			nil)
		ctx.merge(cutoff, 0)
		Ω(ctx.outputBegins).Should(Equal(
			[]int{13, 3, 8, 8, 11, 13},
		))
		Ω(ctx.outputCounts).Should(Equal(
			[]uint32{13, 13, 13, 13},
		))
		Ω(ctx.unsortedColumns).Should(Equal(
			[]int{0, 5},
		))

		for i := 0; i < ctx.numColumns; i++ {
			Ω(ctx.merged.Columns[i].Equals(merged.Columns[i])).Should(BeTrue())
		}
		Ω(ctx.merged.Version).Should(BeEquivalentTo(cutoff))
		Ω(ctx.merged.Size).Should(BeEquivalentTo(13))

		// Vectors that's are mode 0 or mode 1 should be pruned.
		Ω(ctx.merged.Columns[3].(*archiveVectorParty).values).Should(BeNil())
		Ω(ctx.merged.Columns[3].(*archiveVectorParty).nulls).Should(BeNil())
		Ω(ctx.merged.Columns[3].(*archiveVectorParty).counts).Should(BeNil())
		Ω(ctx.merged.Columns[3].(*archiveVectorParty).length).Should(BeEquivalentTo(8))
		Ω(ctx.merged.Columns[5].(*archiveVectorParty).values).Should(BeNil())
		Ω(ctx.merged.Columns[5].(*archiveVectorParty).nulls).Should(BeNil())
		Ω(ctx.merged.Columns[5].(*archiveVectorParty).counts).Should(BeNil())
		Ω(ctx.merged.Columns[5].(*archiveVectorParty).length).Should(BeEquivalentTo(13))
	})

	ginkgo.It("test base iterator", func() {
		var baseEndCount uint32 = 5
		baseIter := newArchiveBatchColumnIterator(base, 1, nil)
		baseIter.setEndPosition(baseEndCount)
		Ω(baseIter.done()).Should(BeFalse())
		Ω(baseIter.currentPosition()).Should(BeEquivalentTo(0))
		Ω(baseIter.nextPosition()).Should(BeEquivalentTo(3))
		Ω(baseIter.index()).Should(BeEquivalentTo(0))
		Ω(baseIter.count()).Should(BeEquivalentTo(3))
		Ω(baseIter.value().Valid).Should(BeFalse())

		baseIter.next()
		Ω(baseIter.done()).Should(BeFalse())
		Ω(baseIter.currentPosition()).Should(BeEquivalentTo(3))
		Ω(baseIter.nextPosition()).Should(BeEquivalentTo(4))
		Ω(baseIter.index()).Should(BeEquivalentTo(1))
		Ω(baseIter.count()).Should(BeEquivalentTo(1))
		Ω(baseIter.value().Valid).Should(BeTrue())
		Ω(baseIter.value().BoolVal).Should(BeFalse())

		baseIter.next()
		Ω(baseIter.done()).Should(BeFalse())
		Ω(baseIter.currentPosition()).Should(BeEquivalentTo(4))
		Ω(baseIter.nextPosition()).Should(BeEquivalentTo(5))
		Ω(baseIter.index()).Should(BeEquivalentTo(2))
		Ω(baseIter.count()).Should(BeEquivalentTo(1))
		Ω(baseIter.value().Valid).Should(BeTrue())
		Ω(baseIter.value().BoolVal).Should(BeTrue())

		baseIter.next()
		Ω(baseIter.done()).Should(BeTrue())

		// test empty iterator
		baseIter = newArchiveBatchColumnIterator(base, 1, nil)
		baseIter.setEndPosition(0)
		Ω(baseIter.done()).Should(BeTrue())
	})

	ginkgo.It("test base iterator with deleted rows: case 1", func() {
		// mark row [0,2,4] as deleted
		ctx := newMergeContext(base, patch, []bool{false, false, false, false, false, false},
			[]common.DataType{common.Uint32, common.Bool, common.Float32, common.Bool, common.Int32, common.Bool},
			[]*common.DataValue{&common.NullDataValue, &common.NullDataValue, &common.NullDataValue, &common.NullDataValue, &common.NullDataValue, &common.NullDataValue},
			[]int{0, 2, 4})
		baseIter := ctx.baseIters[0]
		var baseEndCount uint32 = 5
		baseIter.setEndPosition(baseEndCount)

		Ω(baseIter.done()).Should(BeFalse())

		// value 'null' has count 1
		Ω(baseIter.currentPosition()).Should(BeEquivalentTo(0))
		Ω(baseIter.nextPosition()).Should(BeEquivalentTo(3))
		Ω(baseIter.index()).Should(BeEquivalentTo(0))
		Ω(baseIter.count()).Should(BeEquivalentTo(1))
		Ω(baseIter.value().Valid).Should(BeFalse())

		// value 'false' has count 1
		baseIter.next()
		for baseIter.count() == 0 {
			baseIter.next()
		}
		Ω(baseIter.done()).Should(BeFalse())
		Ω(baseIter.currentPosition()).Should(BeEquivalentTo(3))
		Ω(baseIter.nextPosition()).Should(BeEquivalentTo(4))
		Ω(baseIter.index()).Should(BeEquivalentTo(1))
		Ω(baseIter.count()).Should(BeEquivalentTo(1))
		Ω(baseIter.value().Valid).Should(BeTrue())
		Ω(baseIter.value().BoolVal).Should(BeFalse())

		baseIter.next()
		for baseIter.count() == 0 {
			baseIter.next()
		}
		Ω(baseIter.done()).Should(BeTrue())
	})

	ginkgo.It("test base iterator with deleted rows: case 2", func() {
		// mark row [0,2,4] as deleted
		ctx := newMergeContext(base, patch, []bool{false, false, false, false, false, false},
			[]common.DataType{common.Uint32, common.Bool, common.Float32, common.Bool, common.Int32, common.Bool},
			[]*common.DataValue{&common.NullDataValue, &common.NullDataValue, &common.NullDataValue, &common.NullDataValue, &common.NullDataValue, &common.NullDataValue},
			[]int{0, 1, 2, 4})
		baseIter := ctx.baseIters[0]
		var baseEndCount uint32 = 5
		baseIter.setEndPosition(baseEndCount)

		Ω(baseIter.done()).Should(BeFalse())

		for baseIter.count() == 0 {
			baseIter.next()
		}

		// value 'false' has count 1
		Ω(baseIter.currentPosition()).Should(BeEquivalentTo(3))
		Ω(baseIter.nextPosition()).Should(BeEquivalentTo(4))
		Ω(baseIter.index()).Should(BeEquivalentTo(1))
		Ω(baseIter.count()).Should(BeEquivalentTo(1))
		Ω(baseIter.value().Valid).Should(BeTrue())
		Ω(baseIter.value().BoolVal).Should(BeFalse())

		baseIter.next()

		for baseIter.count() == 0 {
			baseIter.next()
		}
		Ω(baseIter.done()).Should(BeTrue())
	})

	ginkgo.It("preallocate: base has deleted rows", func() {
		ctx := newMergeContext(base, patch, []bool{false, false, false, false, false, false},
			[]common.DataType{common.Uint32, common.Bool, common.Float32, common.Bool, common.Int32, common.Bool},
			[]*common.DataValue{&common.NullDataValue, &common.NullDataValue, &common.NullDataValue, &common.NullDataValue, &common.NullDataValue, &common.NullDataValue},
			[]int{0, 2, 4})
		ctx.mergeRecursive(
			0,
			7,
			8,
			ctx.preAllocate,
		)
		Ω(ctx.mergedLengths).Should(Equal(
			[]int{3, 7, 7, 9},
		))
	})

	ginkgo.It("merge: base has deleted rows", func() {
		tmpBatch, err := getFactory().ReadArchiveBatch("merge-with-deleted-rows/mergedBatch")
		Ω(err).Should(BeNil())

		mergedWithDeletedRows := &ArchiveBatch{
			Batch:   *tmpBatch,
			Version: 0,
			Size:    10,
			Shard:   shard,
		}

		ctx := newMergeContext(base, patch, []bool{false, false, false, false, false, false},
			[]common.DataType{common.Uint32, common.Bool, common.Float32, common.Bool, common.Int32, common.Bool},
			[]*common.DataValue{&common.NullDataValue, &common.NullDataValue, &common.NullDataValue, &common.NullDataValue, &common.NullDataValue, &common.NullDataValue},
			[]int{0, 2, 4})
		ctx.merge(cutoff, 0)
		Ω(ctx.outputBegins).Should(Equal(
			[]int{10, 3, 7, 7, 9, 10},
		))
		Ω(ctx.outputCounts).Should(Equal(
			[]uint32{10, 10, 10, 10},
		))
		Ω(ctx.unsortedColumns).Should(Equal(
			[]int{0, 5},
		))

		for i := 0; i < ctx.numColumns; i++ {
			Ω(ctx.merged.Columns[i].Equals(mergedWithDeletedRows.Columns[i])).Should(BeTrue())
		}

		Ω(ctx.merged.Version).Should(BeEquivalentTo(cutoff))
		Ω(ctx.merged.Size).Should(BeEquivalentTo(10))

		// Vectors that's are mode 0 or mode 1 should be pruned.
		Ω(ctx.merged.Columns[3].(*archiveVectorParty).values).Should(BeNil())
		Ω(ctx.merged.Columns[3].(*archiveVectorParty).nulls).Should(BeNil())
		Ω(ctx.merged.Columns[3].(*archiveVectorParty).counts).Should(BeNil())
		Ω(ctx.merged.Columns[3].(*archiveVectorParty).length).Should(BeEquivalentTo(7))
		Ω(ctx.merged.Columns[5].(*archiveVectorParty).values).Should(BeNil())
		Ω(ctx.merged.Columns[5].(*archiveVectorParty).nulls).Should(BeNil())
		Ω(ctx.merged.Columns[5].(*archiveVectorParty).counts).Should(BeNil())
		Ω(ctx.merged.Columns[5].(*archiveVectorParty).length).Should(BeEquivalentTo(10))
	})

	ginkgo.It("merge: unsorted column needs skip deleted rows", func() {
		// load base
		tmpBatch, err := getFactory().ReadArchiveBatch("merge-with-deleted-rows/baseBatch")
		Ω(err).Should(BeNil())
		baseBatch := &ArchiveBatch{
			Batch:   *tmpBatch,
			Version: 0,
			Size:    5,
			Shard:   shard,
		}

		// load patch
		patchData, err := getFactory().ReadArchiveBatch("merge-with-deleted-rows/patchBatch")
		Ω(err).Should(BeNil())

		vs := &LiveStore{
			LastReadRecord: RecordID{-101, 4},
			Batches: map[int32]*LiveBatch{
				-101: {
					Batch:     *patchData,
					Capacity:  4,
					liveStore: nil,
				},
			},
		}

		ss := vs.snapshot()

		archivingPatch := &archivingPatch{
			recordIDs: []RecordID{
				{0, 0},
				{0, 1},
				{0, 2},
				{0, 3},
			},
			sortColumns: []int{1},
			data:        ss,
		}

		// load merged batch
		tmpBatch, err = getFactory().ReadArchiveBatch("merge-with-deleted-rows/mergedBatch2")
		Ω(err).Should(BeNil())

		mergedWithDeletedRows := &ArchiveBatch{
			Batch:   *tmpBatch,
			Version: 0,
			Size:    10,
			Shard:   shard,
		}

		ctx := newMergeContext(baseBatch, archivingPatch,
			[]bool{false, false}, []common.DataType{common.Uint32, common.Bool},
			[]*common.DataValue{&common.NullDataValue, &common.NullDataValue},
			[]int{0, 2, 4})

		ctx.merge(cutoff, 0)
		Ω(ctx.outputBegins).Should(Equal(
			[]int{6, 3},
		))

		Ω(ctx.outputCounts).Should(Equal(
			[]uint32{6},
		))

		Ω(ctx.unsortedColumns).Should(Equal(
			[]int{0},
		))

		// verify merged column data
		// column0: 10, 30, 140, 150, 160, 170
		// column1: null:1, false:3, true:2
		for i := 0; i < ctx.numColumns; i++ {
			Ω(ctx.merged.Columns[i].Equals(mergedWithDeletedRows.Columns[i])).Should(BeTrue())
		}

		Ω(ctx.merged.Version).Should(BeEquivalentTo(cutoff))
		Ω(ctx.merged.Size).Should(BeEquivalentTo(6))
	})

	ginkgo.It("test archiving iterator", func() {
		patchIter := newArchivingPatchColumnIterator(patch, 1)
		patchIter.setEndPosition(8)
		Ω(patchIter.done()).Should(BeFalse())
		Ω(patchIter.currentPosition()).Should(BeEquivalentTo(0))
		Ω(patchIter.nextPosition()).Should(BeEquivalentTo(4))
		Ω(patchIter.index()).Should(BeEquivalentTo(0))
		Ω(patchIter.count()).Should(BeEquivalentTo(4))
		Ω(patchIter.value().Valid).Should(BeFalse())

		patchIter.next()
		Ω(patchIter.done()).Should(BeFalse())
		Ω(patchIter.currentPosition()).Should(BeEquivalentTo(4))
		Ω(patchIter.nextPosition()).Should(BeEquivalentTo(6))
		Ω(patchIter.index()).Should(BeEquivalentTo(4))
		Ω(patchIter.count()).Should(BeEquivalentTo(2))
		Ω(patchIter.value().Valid).Should(BeTrue())
		Ω(patchIter.value().BoolVal).Should(BeFalse())

		patchIter.next()
		Ω(patchIter.done()).Should(BeFalse())
		Ω(patchIter.currentPosition()).Should(BeEquivalentTo(6))
		Ω(patchIter.nextPosition()).Should(BeEquivalentTo(8))
		Ω(patchIter.index()).Should(BeEquivalentTo(6))
		Ω(patchIter.count()).Should(BeEquivalentTo(2))
		Ω(patchIter.value().Valid).Should(BeTrue())
		Ω(patchIter.value().BoolVal).Should(BeTrue())

		patchIter.next()
		Ω(patchIter.done()).Should(BeTrue())

		// test empty iterator
		patchIter = newArchivingPatchColumnIterator(patch, 1)
		patchIter.setEndPosition(0)
		Ω(patchIter.done()).Should(BeTrue())
	})

	ginkgo.It("merge with nil base", func() {
		ctx := newMergeContext(&ArchiveBatch{Shard: shard, Batch: Batch{RWMutex: &sync.RWMutex{}}}, patch,
			[]bool{false, false, false, false, false, false},
			[]common.DataType{common.Uint32, common.Bool, common.Float32, common.Bool, common.Int32, common.Bool},
			[]*common.DataValue{&common.NullDataValue, &common.NullDataValue, &common.NullDataValue, &common.NullDataValue, &common.NullDataValue, &common.NullDataValue},
			nil)
		ctx.merge(cutoff, 0)
		Ω(ctx.mergedLengths).Should(Equal(
			[]int{3, 5, 5, 7},
		))
		Ω(ctx.outputCounts).Should(Equal(
			[]uint32{8, 8, 8, 8},
		))
		Ω(ctx.outputBegins).Should(Equal(
			[]int{8, 3, 5, 5, 7, 8},
		))
		Ω(ctx.unsortedColumns).Should(Equal(
			[]int{0, 5},
		))

		Ω(ctx.merged.Version).Should(BeEquivalentTo(cutoff))
		Ω(ctx.merged.Size).Should(BeEquivalentTo(8))

		mergedNilBase, err := getFactory().ReadArchiveBatch("merge-nil-base")
		Ω(err).Should(BeNil())
		for i := 0; i < ctx.numColumns; i++ {
			Ω(ctx.merged.Columns[i].Equals(mergedNilBase.Columns[i])).Should(BeTrue())
		}
	})

	ginkgo.It("merge with no sort columns", func() {
		ctx := newMergeContext(noSortColumnBase, noSortColumnPatch,
			[]bool{false, false}, []common.DataType{common.Uint32, common.Bool},
			[]*common.DataValue{&common.NullDataValue, &common.NullDataValue},
			nil)
		ctx.merge(cutoff, 0)
		Ω(ctx.mergedLengths).Should(BeEmpty())
		Ω(ctx.outputCounts).Should(BeEmpty())
		Ω(ctx.outputBegins).Should(Equal([]int{8, 8}))
		Ω(ctx.unsortedColumns).Should(Equal([]int{0, 1}))

		Ω(ctx.merged.Version).Should(BeEquivalentTo(cutoff))
		Ω(ctx.merged.Size).Should(BeEquivalentTo(8))

		for i := 0; i < ctx.numColumns; i++ {
			Ω(ctx.merged.Columns[i].Equals(noSortColumnMerged.Columns[i])).Should(BeTrue())
		}
	})

	ginkgo.It("deleted columns should be short circuited for both sort "+
		"and non sort columns ", func() {
		ctx := newMergeContext(base, patchWithDeletedColumns, []bool{false, false, true, false, false, true},
			[]common.DataType{common.Uint32, common.Bool, common.Float32, common.Bool, common.Int32, common.Bool},
			[]*common.DataValue{&common.NullDataValue, &common.NullDataValue, &common.NullDataValue, &common.NullDataValue, &common.NullDataValue, &common.NullDataValue},
			nil)
		ctx.merge(cutoff, 0)
		// Columns 2 and 5 are deleted.
		Ω(ctx.mergedLengths).Should(Equal(
			[]int{3, 8, 8, 11},
		))

		// We still write to deleted sort column but skip deleted no sort column.
		Ω(ctx.outputBegins).Should(Equal(
			[]int{13, 3, 8, 8, 11, 0},
		))

		Ω(ctx.outputCounts).Should(Equal(
			[]uint32{13, 13, 13, 13},
		))

		for i := 0; i < ctx.numColumns; i++ {
			Ω(ctx.merged.Columns[i].Equals(mergedWithDeletedColumnsMerged.Columns[i])).Should(BeTrue())
		}

		ctx = newMergeContext(noSortColumnBase, noSortColumnPatch,
			[]bool{true, false}, []common.DataType{common.Uint32, common.Bool},
			[]*common.DataValue{&common.NullDataValue, &common.NullDataValue},
			nil)
		ctx.merge(cutoff, 0)
		Ω(ctx.mergedLengths).Should(BeEmpty())
		Ω(ctx.outputCounts).Should(BeEmpty())
		Ω(ctx.outputBegins).Should(Equal([]int{0, 8}))
		Ω(ctx.unsortedColumns).Should(Equal([]int{0, 1}))

		Ω(ctx.merged.Version).Should(BeEquivalentTo(cutoff))
		Ω(ctx.merged.Size).Should(BeEquivalentTo(8))

		// Column 0 should be a mode 0 vector.
		Ω(ctx.merged.Columns[0].GetLength()).Should(BeEquivalentTo(8))
		Ω(ctx.merged.Columns[0].(*archiveVectorParty).JudgeMode()).Should(Equal(common.AllValuesDefault))
		Ω(ctx.merged.Columns[1].Equals(noSortColumnMerged.Columns[1])).Should(BeTrue())
	})

	ginkgo.AfterEach(func() {
		batch0.SafeDestruct()
		batch1.SafeDestruct()
		base.SafeDestruct()
		merged.SafeDestruct()

		// Clean data for no sort columns merge test case.
		noSortColumnBatch.SafeDestruct()
		noSortColumnBase.SafeDestruct()
		noSortColumnMerged.SafeDestruct()

		// Clean data for merge with deleted columns test case
		mergedWithDeletedColumnsMerged.SafeDestruct()
	})
})
