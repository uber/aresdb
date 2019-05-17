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

package query

import (
	"github.com/uber/aresdb/memutils"
	queryCom "github.com/uber/aresdb/query/common"
	"unsafe"
	"time"
)

// NonAggrBatchExecutorImpl is batch executor implementation for non-aggregation query
type NonAggrBatchExecutorImpl struct {
	*BatchExecutorImpl
}

// project for non-aggregation query will only calculate the selected columns
// dimension calculation, reduce will be skipped, once the generated result reaches limit, it will return and cancel all other ongoing processing.
func (e *NonAggrBatchExecutorImpl) project() {
	// Prepare for dimension evaluation.
	e.prepareForDimEval(e.qc.OOPK.DimRowBytes, e.qc.OOPK.NumDimsPerDimWidth, e.stream)

	e.qc.reportTimingForCurrentBatch(e.stream, &e.start, prepareForDimAndMeasureTiming)
	// for non-aggregation query, we always write from start for dimension output
	e.evalDimensions(0)
	// uncompress the result from baseCount
	e.expandDimensions(e.qc.OOPK.NumDimsPerDimWidth)
	// wait for stream to clean up non used buffer before final aggregation
	memutils.WaitForCudaStream(e.stream, e.qc.Device)
	e.qc.OOPK.currentBatch.cleanupBeforeAggregation()
}

func (e *NonAggrBatchExecutorImpl) prepareForDimEval(
	dimRowBytes int, numDimsPerDimWidth queryCom.DimCountsPerDimWidth, stream unsafe.Pointer) {
	bc := &e.qc.OOPK.currentBatch
	// only allocate dimension vector once
	if bc.resultCapacity == 0 {
		bc.resultCapacity = e.qc.maxBatchSizeAfterPrefilter
		// Extra budget for future proofing.
		bc.resultCapacity += bc.resultCapacity / 8
		bc.dimensionVectorD = [2]devicePointer{
			deviceAllocate(bc.resultCapacity*dimRowBytes, bc.device),
			deviceAllocate(bc.resultCapacity*dimRowBytes, bc.device),
		}
	}
}

func (e *NonAggrBatchExecutorImpl) expandDimensions(numDims queryCom.DimCountsPerDimWidth) {
	bc := &e.qc.OOPK.currentBatch

	lenWanted := e.getNumberOfRecordsNeeded()

	if bc.size != 0 && !bc.baseCountD.isNull() {
		e.qc.doProfile(func() {
			e.qc.OOPK.currentBatch.expand(numDims, e.stream, e.qc.Device)
			e.qc.reportTimingForCurrentBatch(e.stream, &e.start, expandEvalTiming)
		}, "expand", e.stream)
	} else {
		bc.resultSize = bc.size
	}
	if bc.resultSize > lenWanted {
		bc.resultSize = lenWanted
	}
}

func (e *NonAggrBatchExecutorImpl) postExec(start time.Time) {
	// TODO: @shz experiment with on demand flush when next batch can not fit in buffer
	bc := e.qc.OOPK.currentBatch
	// transfer current batch result from device to host
	e.qc.OOPK.dimensionVectorH = memutils.HostAlloc(bc.resultSize * e.qc.OOPK.DimRowBytes)
	asyncCopyDimensionVector(e.qc.OOPK.dimensionVectorH, bc.dimensionVectorD[0].getPointer(), bc.resultSize, 0,
		e.qc.OOPK.NumDimsPerDimWidth, bc.resultSize, bc.resultCapacity,
		memutils.AsyncCopyDeviceToHost, e.qc.cudaStreams[0], e.qc.Device)
	memutils.WaitForCudaStream(e.qc.cudaStreams[0], e.qc.Device)

	// flush current batches results to result buffer
	e.qc.OOPK.ResultSize = bc.resultSize
	e.qc.numberOfRowsWritten += bc.resultSize
	if e.getNumberOfRecordsNeeded() <= 0 {
		e.qc.OOPK.done = true
	}
	e.qc.flushResultBuffer()

	bc.size = 0
	e.qc.reportTimingForCurrentBatch(e.stream, &start, cleanupTiming)
	e.qc.reportBatch(e.batchID > 0)

	// Only profile one batch.
	e.qc.Profiling = ""
}


func (e *NonAggrBatchExecutorImpl) reduce() {
	// nothing need to do for non-aggregation query
}

// getNumberOfRecordsNeeded is a helper function
func (e *NonAggrBatchExecutorImpl) getNumberOfRecordsNeeded() int {
	return e.qc.Query.Limit - e.qc.numberOfRowsWritten
}