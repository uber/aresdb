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
	lenWanted := e.qc.Query.Limit - e.qc.numberOfRowsWritten
	if bc.size > lenWanted {
		bc.size = lenWanted
	}

	if bc.resultCapacity == 0 {
		bc.resultCapacity = bc.size
		bc.dimensionVectorD = [2]devicePointer{
			deviceAllocate(bc.resultCapacity*dimRowBytes, bc.device),
			deviceAllocate(bc.resultCapacity*dimRowBytes, bc.device),
		}
	}
}

func (e *NonAggrBatchExecutorImpl) expandDimensions(numDims queryCom.DimCountsPerDimWidth) {
	bc := &e.qc.OOPK.currentBatch

	if bc.size != 0 && !bc.baseCountD.isNull() {
		e.qc.doProfile(func() {
			e.qc.OOPK.currentBatch.expand(numDims, bc.size, e.stream, e.qc.Device)
			e.qc.reportTimingForCurrentBatch(e.stream, &e.start, expandEvalTiming)
		}, "expand", e.stream)
	}
}

func (e *NonAggrBatchExecutorImpl) postExec(start time.Time) {
	// transfer current batch result from device to host
	e.qc.OOPK.dimensionVectorH = memutils.HostAlloc(e.qc.OOPK.currentBatch.size * e.qc.OOPK.DimRowBytes)
	asyncCopyDimensionVector(e.qc.OOPK.dimensionVectorH, e.qc.OOPK.currentBatch.dimensionVectorD[0].getPointer(), e.qc.OOPK.currentBatch.size, 0,
		e.qc.OOPK.NumDimsPerDimWidth, e.qc.OOPK.currentBatch.size, e.qc.OOPK.currentBatch.resultCapacity,
		memutils.AsyncCopyDeviceToHost, e.qc.cudaStreams[0], e.qc.Device)
	memutils.WaitForCudaStream(e.qc.cudaStreams[0], e.qc.Device)

	// flush current results from current batch
	e.qc.OOPK.ResultSize = e.qc.OOPK.currentBatch.size
	e.qc.flushResultBuffer()
	e.qc.numberOfRowsWritten += e.qc.OOPK.currentBatch.size
	if e.qc.numberOfRowsWritten >= e.qc.Query.Limit {
		e.qc.OOPK.done = true
	}

	e.BatchExecutorImpl.postExec(start)
}


func (e *NonAggrBatchExecutorImpl) reduce() {
	// nothing need to do for non-aggregation query
}