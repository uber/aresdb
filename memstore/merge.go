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
	"github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/utils"
	"sync"
)

// mergeContext carries all context information used during merge
type mergeContext struct {
	base   *ArchiveBatch
	patch  *archivingPatch
	merged *ArchiveBatch

	// Number of total columns (including deleted columns).
	numColumns int
	// Number of records in base patch. If base is nil, size is 0.
	baseSize int
	// Number of records in base patch and patch.
	totalSize int

	// Iterators for sorted base. One iterator per sort column.
	baseIters []sortedColumnIterator
	// Iterators for archiving patch. One iterator per sort column.
	patchIters []sortedColumnIterator

	// Following fields will only be used during preallocate stage
	// length is len(sortColumns).
	mergedLengths []int

	// Following fields will be only used during merge stage stores the indexes to
	// write to the final merged archive batch for each column length is len(all columns).
	outputBegins []int
	// It stores the accumulated counts to write to the final merged archive batch for
	// each sorted column, length is len(all columns).
	outputCounts []uint32
	// Buffer the unsorted column for writing final uncompressed values.
	unsortedColumns []int

	// Stores list of rows that have been marked as deleted
	baseRowDeleted []int
	// This is used to short circuit merge for deleted columns.
	columnDeletions []bool

	// Needed during merging.
	dataTypes []common.DataType

	defaultValues []*common.DataValue

	// Keep track of total unmanaged memory space this merge process uses.
	unmanagedMemoryBytes int64
}

// newMergeContext creates a new context for merge existing batch and archive batch
// into a new batch. It's shared by pre-allocate stage and actual merge stage.
func newMergeContext(base *ArchiveBatch, patch *archivingPatch, columnDeletions []bool, dataTypes []common.DataType,
	defaultValues []*common.DataValue, baseRowDeleted []int) *mergeContext {
	numColumns := len(dataTypes)
	baseSize := 0
	if base != nil {
		baseSize = base.Size
	}

	ctx := &mergeContext{
		base:            base,
		patch:           patch,
		numColumns:      numColumns,
		baseSize:        baseSize,
		totalSize:       len(patch.recordIDs) + baseSize - len(baseRowDeleted),
		mergedLengths:   make([]int, len(patch.sortColumns)),
		outputBegins:    make([]int, numColumns),
		outputCounts:    make([]uint32, len(patch.sortColumns)),
		unsortedColumns: make([]int, 0, numColumns-len(patch.sortColumns)),
		baseRowDeleted:  baseRowDeleted,
		columnDeletions: columnDeletions,
		dataTypes:       dataTypes,
		defaultValues:   defaultValues,
	}
	ctx.initIters()

	return ctx
}

// initIters initialize patch and base iterators.
func (ctx *mergeContext) initIters() {
	ctx.patchIters = make([]sortedColumnIterator, len(ctx.patch.sortColumns))
	ctx.baseIters = make([]sortedColumnIterator, len(ctx.patch.sortColumns))
	for i, columnID := range ctx.patch.sortColumns {
		ctx.patchIters[i] = newArchivingPatchColumnIterator(ctx.patch, columnID)
		// archive batch iterator
		ctx.baseIters[i] = newArchiveBatchColumnIterator(ctx.base, columnID, ctx.baseRowDeleted)
	}
}

// sortedColumnIterator is the common interface to merge two sorted columns
type sortedColumnIterator interface {
	// Read values from current idx.
	read()
	// Advance the iterator.
	next()
	// Tells whether the iteration has been finished.
	done() bool
	// Returns current data value the iterator points to
	value() common.DataValue
	// Tells what's the current index of iterator in a vector
	index() int
	// Tells iterator where to start and stop in next sorted column.
	// For base iterator, it's current count.
	// For patch iterator, it's current index.
	// To make the interface consistent, we choose return uint32.
	// So for patch iterator, we need to convert it between int and uint32.
	currentPosition() uint32

	// For base iterator, it's next count.
	// For patch iterator, it's next index.
	nextPosition() uint32
	// Count of current value.
	count() uint32
	// For base iterator, it should be next end count. For patch iterator,
	// it should be next end index. This must be called before iterating
	// each slice
	setEndPosition(pos uint32)

	// list of rows that should be skipped for current value
	currentSkipRows() []int
}

type archiveBatchColumnIterator struct {
	// Column being iterated.
	vp common.ArchiveVectorParty

	// Iterator position.
	idx int

	// #
	currentCount uint32

	// rows from beginning to current value
	nextCount uint32

	// total rows of this vp
	endCount uint32
	val      common.DataValue

	// rows deleted
	rowsDeleted []int

	// rowsDeleted start position for current value
	currentRowsDeletedStart int

	// rowsDeleted end position(exclusive) for current value
	currentRowsDeletedEnd int
}

// newArchiveBatchColumnIterator creates a new iterator that iterates through
// a specific range of archive batch column.
func newArchiveBatchColumnIterator(base *ArchiveBatch, columnID int, rowsDeleted []int) sortedColumnIterator {
	var baseVP common.ArchiveVectorParty
	if base != nil && columnID < len(base.Columns) {
		baseVP = base.Columns[columnID].(common.ArchiveVectorParty)
	}
	return &archiveBatchColumnIterator{
		vp:          baseVP,
		rowsDeleted: rowsDeleted,
	}
}

func (itr *archiveBatchColumnIterator) done() bool {
	if itr.vp == nil {
		return true
	}
	return itr.idx >= itr.vp.GetLength() || itr.currentCount >= itr.endCount
}

func (itr *archiveBatchColumnIterator) read() {
	if itr.done() {
		return
	}

	// Sort columns of archive batch should be either mode 3 or mode 0.
	mode := itr.vp.(common.CVectorParty).GetMode()
	itr.currentRowsDeletedStart = itr.currentRowsDeletedEnd
	if mode == common.HasCountVector {
		itr.nextCount = itr.vp.GetCount(itr.idx)
	} else {
		itr.nextCount = itr.endCount
	}
	for itr.currentRowsDeletedEnd < len(itr.rowsDeleted) && uint32(itr.rowsDeleted[itr.currentRowsDeletedEnd]) < itr.nextCount {
		itr.currentRowsDeletedEnd++
	}
	itr.val = itr.vp.GetDataValue(itr.idx)
}

func (itr *archiveBatchColumnIterator) next() {
	// move to next value
	itr.idx++
	itr.currentCount = itr.nextCount
	itr.read()

}

func (itr *archiveBatchColumnIterator) index() int {
	return itr.idx
}

func (itr *archiveBatchColumnIterator) value() common.DataValue {
	return itr.val
}

func (itr *archiveBatchColumnIterator) currentPosition() uint32 {
	return itr.currentCount
}

func (itr *archiveBatchColumnIterator) nextPosition() uint32 {
	return itr.nextCount
}

func (itr *archiveBatchColumnIterator) count() uint32 {
	return itr.nextCount - itr.currentCount - uint32(itr.currentRowsDeletedEnd-itr.currentRowsDeletedStart)
}

func (itr *archiveBatchColumnIterator) currentSkipRows() []int {
	return itr.rowsDeleted[itr.currentRowsDeletedStart:itr.currentRowsDeletedEnd]
}

func (itr *archiveBatchColumnIterator) setEndPosition(pos uint32) {
	itr.endCount = pos

	// see to the first valid value
	itr.read()
}

type archivingPatchColumnIterator struct {
	patch    *archivingPatch
	columnID int

	endIdx int
	// Iterator position.
	idx     int
	nextIdx int
	val     common.DataValue
}

// newArchivingPatchColumnIterator creates a new iterator that iterates through a specific
// range of archive batch column.
func newArchivingPatchColumnIterator(patch *archivingPatch, columnID int) sortedColumnIterator {
	itr := &archivingPatchColumnIterator{
		patch:    patch,
		columnID: columnID,
	}
	return itr
}

func (itr *archivingPatchColumnIterator) done() bool {
	return itr.idx >= itr.endIdx
}

func (itr *archivingPatchColumnIterator) read() {
	if itr.done() {
		return
	}

	// Read current value.
	itr.val = itr.patch.GetDataValue(itr.idx, itr.columnID)

	// Find next value != current value.
	for itr.nextIdx++; itr.nextIdx < itr.endIdx; itr.nextIdx++ {
		val := itr.patch.GetDataValue(itr.nextIdx, itr.columnID)
		if itr.val.Compare(val) != 0 {
			break
		}
	}
}

func (itr *archivingPatchColumnIterator) next() {
	// Jump directly to index of next different value.
	itr.idx = itr.nextIdx
	itr.read()
}

func (itr *archivingPatchColumnIterator) value() common.DataValue {
	return itr.val
}

func (itr *archivingPatchColumnIterator) index() int {
	return itr.idx
}

func (itr *archivingPatchColumnIterator) currentPosition() uint32 {
	return uint32(itr.idx)
}

func (itr *archivingPatchColumnIterator) nextPosition() uint32 {
	return uint32(itr.nextIdx)
}

func (itr *archivingPatchColumnIterator) count() uint32 {
	return uint32(itr.nextIdx - itr.idx)
}

func (itr *archivingPatchColumnIterator) currentSkipRows() []int {
	return nil
}

func (itr *archivingPatchColumnIterator) setEndPosition(pos uint32) {
	itr.endIdx = int(pos)
	itr.read()
}

// merge an live batch with a archive batch and store the merged data into a new archive batch.
// It has two stages:
// 	1. preallocate: attempt to merge two batches but only calculate how much space merged data needs.
//  2. merge: based on the calculated size, allocate space and do actual merge.
// This algorithm will do merge on sorted columns first and based on the positions of last sorted column,
// it will copy non-sorted columns data into final result.
// The parameters will be used as cutoff and seqNum for the merged batch.
func (ctx *mergeContext) merge(cutoff uint32, seqNum uint32) {
	// We preallocate space in 1st pass to avoid allocate unnecessary memory.
	ctx.mergeRecursive(0, uint32(ctx.baseSize), len(ctx.patch.recordIDs), ctx.preAllocate)

	// Allocate space for merged archive batch.
	ctx.allocate(cutoff, seqNum)

	// Reset iterators to begin 2nd pass.
	ctx.initIters()

	// Do actual merge and write to output vector party.
	ctx.mergeRecursive(0, uint32(ctx.baseSize), len(ctx.patch.recordIDs), ctx.writeOutput)

	// If sort columns is empty, we need to dump all values in batch first and then dump patch values.
	if len(ctx.patch.sortColumns) == 0 {
		// Write base.
		ctx.writeUnsortedColumns(0, ctx.baseSize, ctx.base, ctx.baseRowDeleted)

		// Write patch.
		ctx.writeUnsortedColumns(0, len(ctx.patch.recordIDs), ctx.patch, nil)
	}

	// Scan through all columns for mode 0 and 1 columns and remove unnecessary vectors.
	for columnID := 0; columnID < len(ctx.merged.Columns); columnID++ {
		column := ctx.merged.Columns[columnID]
		column.(common.ArchiveVectorParty).Prune()
	}
}

// allocate space for merged archive batch based on calculated mergedLengths.
func (ctx *mergeContext) allocate(cutoff uint32, seqNum uint32) {
	columns := make([]common.VectorParty, ctx.numColumns)
	// Need to create batch in advance otherwise vector party's allUsersDone will have nil value.
	ctx.merged = &ArchiveBatch{
		Version: cutoff,
		SeqNum:  seqNum,
		Size:    ctx.totalSize,
		BatchID: ctx.base.BatchID,
		Shard:   ctx.base.Shard,
		Batch: Batch{RWMutex: &sync.RWMutex{}},
	}

	for columnID := 0; columnID < ctx.numColumns; columnID++ {
		dataType := ctx.dataTypes[columnID]
		defaultValue := *ctx.defaultValues[columnID]

		var bytes int64
		if i := utils.IndexOfInt(ctx.patch.sortColumns, columnID); i >= 0 {
			// Sort columns.
			bytes = int64(CalculateVectorPartyBytes(dataType, ctx.mergedLengths[i], true, true))
			ctx.base.Shard.HostMemoryManager.ReportUnmanagedSpaceUsageChange(bytes)
			columns[columnID] = newArchiveVectorParty(ctx.mergedLengths[i], dataType, defaultValue, &ctx.merged.RWMutex)
			columns[columnID].Allocate(true)
		} else {
			// Non-sort columns.
			bytes = int64(CalculateVectorPartyBytes(dataType, ctx.totalSize, true, false))
			ctx.base.Shard.HostMemoryManager.ReportUnmanagedSpaceUsageChange(bytes)
			columns[columnID] = newArchiveVectorParty(ctx.totalSize, dataType, defaultValue, &ctx.merged.RWMutex)
			if !ctx.columnDeletions[columnID] {
				columns[columnID].Allocate(false)
			}
			ctx.unsortedColumns = append(ctx.unsortedColumns, columnID)
		}
		ctx.unmanagedMemoryBytes += bytes
	}
	ctx.merged.Columns = columns
}

// common function signature for both preallocate and merge.
type mergeAction func(baseIter, patchIter sortedColumnIterator, sortColIdx, compareRes int, mergedVP common.ArchiveVectorParty)

// preAllocate called during first pass.
func (ctx *mergeContext) preAllocate(baseIter, patchIter sortedColumnIterator, sortColIdx, compareRes int, mergedVP common.ArchiveVectorParty) {
	ctx.mergedLengths[sortColIdx]++
}

func (ctx *mergeContext) writeUnsortedColumns(start, end int, reader BatchReader, skipRows []int) {
	// Base batch is possible to be nil for a particular day.
	if reader != nil {
		for i := start; i < end; i++ {
			if len(skipRows) > 0 && skipRows[0] == i {
				skipRows = skipRows[1:]
				continue
			}

			for _, columnID := range ctx.unsortedColumns {
				// We will skip writing to deleted columns so that it will have all null values.
				if !ctx.columnDeletions[columnID] {
					mergedVP := ctx.merged.Columns[columnID]
					val := reader.GetDataValueWithDefault(int(i), columnID, *ctx.defaultValues[columnID])
					mergedVP.SetDataValue(ctx.outputBegins[columnID], val, IncrementCount)
					ctx.outputBegins[columnID]++
				}
			}
		}
	}
}

func (ctx *mergeContext) writeOutput(baseIter, patchIter sortedColumnIterator, sortColIdx, compareRes int, mergedVP common.ArchiveVectorParty) {
	var val common.DataValue
	var count uint32
	ifWriteUnsortedColumns := sortColIdx == len(ctx.patch.sortColumns)-1
	// if patch value is less, we read from patch
	if compareRes < 0 {
		val = patchIter.value()
		count = patchIter.count()
		ctx.outputCounts[sortColIdx] += count
		if ifWriteUnsortedColumns {
			ctx.writeUnsortedColumns(int(patchIter.currentPosition()), int(patchIter.nextPosition()), ctx.patch, nil)
		}
	} else if compareRes == 0 {
		val = baseIter.value()
		count = patchIter.count() + baseIter.count()
		ctx.outputCounts[sortColIdx] += count
		if ifWriteUnsortedColumns {
			ctx.writeUnsortedColumns(int(baseIter.currentPosition()), int(baseIter.nextPosition()), ctx.base, baseIter.currentSkipRows())
			ctx.writeUnsortedColumns(int(patchIter.currentPosition()), int(patchIter.nextPosition()), ctx.patch, nil)
		}
	} else {
		val = baseIter.value()
		count = baseIter.count()
		ctx.outputCounts[sortColIdx] += count
		if ifWriteUnsortedColumns {
			ctx.writeUnsortedColumns(int(baseIter.currentPosition()), int(baseIter.nextPosition()), ctx.base, baseIter.currentSkipRows())
		}
	}

	columnID := ctx.patch.sortColumns[sortColIdx]
	// Set value on the mergedVP.
	mergedVP.SetDataValue(ctx.outputBegins[columnID], val, IncrementCount, count)
	mergedVP.SetCount(ctx.outputBegins[columnID], ctx.outputCounts[sortColIdx])
	ctx.outputBegins[columnID]++
}

// mergeRecursive does merge on base and patch iterators on a given sort column. baseEndPos is the end count
// for base iter to stop. patchEndPos is the end index for patch iter to stop. Any end pos with 0 value means
// for this iterator it's an empty range.
func (ctx *mergeContext) mergeRecursive(sortColIdx int, baseEndPos uint32, patchEndPos int, f mergeAction) {
	if sortColIdx >= len(ctx.patch.sortColumns) {
		return
	}

	columnID := ctx.patch.sortColumns[sortColIdx]
	var mergedVP common.ArchiveVectorParty

	// Only used by merge stage during preallocate stage ctx.merged will be nil
	if ctx.merged != nil {
		mergedVP = ctx.merged.Columns[columnID].(common.ArchiveVectorParty)
	}

	baseIter := ctx.baseIters[sortColIdx]
	baseIter.setEndPosition(baseEndPos)
	patchIter := ctx.patchIters[sortColIdx]
	patchIter.setEndPosition(uint32(patchEndPos))

	for !baseIter.done() && !patchIter.done() {
		// ignore values if the count is 0.
		if baseIter.count() == 0 {
			baseIter.next()
			continue
		}

		if patchIter.count() == 0 {
			patchIter.next()
			continue
		}

		res := patchIter.value().Compare(baseIter.value())
		f(baseIter, patchIter, sortColIdx, res, mergedVP)
		if res < 0 {
			// New value from patch.
			ctx.mergeRecursive(
				sortColIdx+1,
				0,
				int(patchIter.nextPosition()),
				f,
			)

			patchIter.next()
		} else if res == 0 {
			ctx.mergeRecursive(
				sortColIdx+1,
				baseIter.nextPosition(),
				int(patchIter.nextPosition()),
				f,
			)

			baseIter.next()
			patchIter.next()
		} else {
			ctx.mergeRecursive(
				sortColIdx+1,
				baseIter.nextPosition(),
				0,
				f,
			)
			baseIter.next()
		}
	}

	for !patchIter.done() {
		if patchIter.count() > 0 {
			// This is equal to compareRes < 0.
			f(baseIter, patchIter, sortColIdx, -1, mergedVP)
			ctx.mergeRecursive(
				sortColIdx+1,
				0,
				int(patchIter.nextPosition()),
				f,
			)
		}
		patchIter.next()
	}

	for !baseIter.done() {
		if baseIter.count() > 0 {
			// This is equal to compareRes > 0.
			f(baseIter, patchIter, sortColIdx, 1, mergedVP)
			ctx.mergeRecursive(
				sortColIdx+1,
				baseIter.nextPosition(),
				0,
				f,
			)
		}
		baseIter.next()
	}
}
