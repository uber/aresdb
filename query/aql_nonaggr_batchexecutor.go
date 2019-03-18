package query

import (
	"github.com/uber/aresdb/memutils"
	queryCom "github.com/uber/aresdb/query/common"
	"github.com/uber/aresdb/utils"
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

	e.evalDimensions()

	// uncompress the result from baseCount
	e.expandAction(e.qc.OOPK.NumDimsPerDimWidth)

	// wait for stream to clean up non used buffer before final aggregation
	memutils.WaitForCudaStream(e.stream, e.qc.Device)
	e.qc.OOPK.currentBatch.cleanupBeforeAggregation()

	if e.qc.OOPK.currentBatch.resultSize == e.qc.Query.Limit {
		e.qc.OOPK.done = true
	}
}

func (e *NonAggrBatchExecutorImpl) expandAction(numDims queryCom.DimCountsPerDimWidth) {
	bc := &e.qc.OOPK.currentBatch
	if bc.size == 0 {
		//nothing to do
		return
	}
	// maximum rows needed for this run as we may have results from previous run
	lenWanted := bc.resultCapacity - bc.resultSize

	if bc.baseCountD.getPointer() == nil {
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

	lenWanted := bc.size
	if lenWanted+bc.resultSize > bc.resultCapacity {
		lenWanted = bc.resultCapacity - bc.resultSize
	}
	// to keep the consistency of the output dimension vector
	e.qc.OOPK.currentBatch.swapResultBufferForNextBatch()
	bc.size = lenWanted
}

// Run is the function to run the whole process for a batch
func (e *NonAggrBatchExecutorImpl) Run(isLastBatch bool) {
	e.isLastBatch = isLastBatch
	start := utils.Now()
	// initialize index vector.
	initIndexVector(e.qc.OOPK.currentBatch.indexVectorD.getPointer(), 0, e.qc.OOPK.currentBatch.size, e.stream, e.qc.Device)

	e.qc.reportTimingForCurrentBatch(e.stream, &start, initIndexVectorTiming)

	e.filter()

	e.join()

	e.project()

	// swap result buffer before next batch
	e.qc.OOPK.currentBatch.swapResultBufferForNextBatch()
	e.qc.reportTimingForCurrentBatch(e.stream, &start, cleanupTiming)
	e.qc.reportBatch(e.batchID > 0)

	// Only profile one batch.
	e.qc.Profiling = ""
}
