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

	if e.qc.OOPK.currentBatch.resultSize == e.qc.Query.Limit {
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

	// maximum rows needed for this run as we may have results from previous run
	lenWanted := bc.resultCapacity - bc.resultSize
	if bc.size < lenWanted {
		lenWanted = bc.size
	}

	if bc.baseCountD.isNull() {
		// baseCountD is null, no uncompression is needed
		asyncCopyDimensionVector(bc.dimensionVectorD[1].getPointer(), bc.dimensionVectorD[0].getPointer(), lenWanted, bc.resultSize,
			numDims, bc.resultCapacity, bc.resultCapacity, memutils.AsyncCopyDeviceToDevice, e.stream, e.qc.Device)
		bc.resultSize += lenWanted
		return
	}

	e.qc.doProfile(func() {
		e.qc.OOPK.currentBatch.expand(numDims, lenWanted, e.stream, e.qc.Device)
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
}
