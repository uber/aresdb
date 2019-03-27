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
)

// NonAggrBatchExecutorImpl is batch executor implementation for non-aggregation query
type NonAggrBatchExecutorImpl struct {
	*BatchExecutorImpl
}

// project for non-aggregation query will only calculate the selected columns
// measure calculation, reduce will be skipped, once the generated result reaches limit, it will return and cancel all other ongoing processing.
func (e *NonAggrBatchExecutorImpl) project() {
	// Prepare for dimension and measure evaluation.
	e.prepareForDimEval(e.qc.OOPK.DimRowBytes, e.qc.OOPK.NumDimsPerDimWidth, e.stream)

	e.qc.reportTimingForCurrentBatch(e.stream, &e.start, prepareForDimAndMeasureTiming)
	// for non-aggregation query, we always write from start for dimension output
	e.evalDimensions(0)
	// uncompress the result from baseCount
	e.expandDimensions(e.qc.OOPK.NumDimsPerDimWidth)
	// wait for stream to clean up non used buffer before final aggregation
	memutils.WaitForCudaStream(e.stream, e.qc.Device)
	e.qc.OOPK.currentBatch.cleanupBeforeAggregation()

	if e.qc.OOPK.currentBatch.resultSize >= e.qc.Query.Limit {
		e.qc.OOPK.done = true
	}
}

func (e *NonAggrBatchExecutorImpl) reduce() {
	// nothing need to do for non-aggregation query
}

func (e *NonAggrBatchExecutorImpl) expandDimensions(numDims queryCom.DimCountsPerDimWidth) {
	bc := &e.qc.OOPK.currentBatch
	if bc.size == 0 {
		//nothing to do
		return
	}

	if bc.baseCountD.isNull() {
		// baseCountD is null, no uncompression is needed
		asyncCopyDimensionVector(bc.dimensionVectorD[1].getPointer(), bc.dimensionVectorD[0].getPointer(), bc.size, bc.resultSize,
			numDims, bc.resultCapacity, bc.resultCapacity, memutils.AsyncCopyDeviceToDevice, e.stream, e.qc.Device)
		bc.resultSize += bc.size
		return
	}

	e.qc.doProfile(func() {
		e.qc.OOPK.currentBatch.expand(numDims, bc.size, e.stream, e.qc.Device)
		e.qc.reportTimingForCurrentBatch(e.stream, &e.start, expandEvalTiming)
	}, "expand", e.stream)
}

func (e *NonAggrBatchExecutorImpl) prepareForDimEval(
	dimRowBytes int, numDimsPerDimWidth queryCom.DimCountsPerDimWidth, stream unsafe.Pointer) {

	bc := &e.qc.OOPK.currentBatch
	if bc.resultCapacity == 0 {
		bc.resultCapacity = e.qc.Query.Limit
		bc.dimensionVectorD = [2]devicePointer{
			deviceAllocate(bc.resultCapacity*dimRowBytes, bc.device),
			deviceAllocate(bc.resultCapacity*dimRowBytes, bc.device),
		}
	}
	// to keep the consistency of the output dimension vector
	bc.dimensionVectorD[0], bc.dimensionVectorD[1] = bc.dimensionVectorD[1], bc.dimensionVectorD[0]
	// maximum rows needed from filter result
	lenWanted := bc.resultCapacity - bc.resultSize
	if bc.size > lenWanted {
		bc.size = lenWanted
	}
}
