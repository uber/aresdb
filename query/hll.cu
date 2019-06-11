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

#include <thrust/iterator/discard_iterator.h>
#include <thrust/transform.h>
#include <algorithm>
#include "query/algorithm.hpp"
#include "query/memory.hpp"

CGoCallResHandle HyperLogLog(DimensionVector prevDimOut,
                             DimensionVector curDimOut,
                             uint32_t *prevValuesOut,
                             uint32_t *curValuesOut,
                             int prevResultSize,
                             int curBatchSize,
                             bool isLastBatch,
                             uint8_t **hllVectorPtr,
                             size_t *hllVectorSizePtr,
                             uint16_t **hllRegIDCountPerDimPtr,
                             void *stream,
                             int device) {
  CGoCallResHandle resHandle = {nullptr, nullptr};
  try {
#ifdef RUN_ON_DEVICE
    cudaSetDevice(device);
#endif
    cudaStream_t cudaStream = reinterpret_cast<cudaStream_t>(stream);
    resHandle.res =
        reinterpret_cast<void *>(ares::hyperloglog(prevDimOut,
                                                   curDimOut,
                                                   prevValuesOut,
                                                   curValuesOut,
                                                   prevResultSize,
                                                   curBatchSize,
                                                   isLastBatch,
                                                   hllVectorPtr,
                                                   hllVectorSizePtr,
                                                   hllRegIDCountPerDimPtr,
                                                   cudaStream));
    CheckCUDAError("HyperLogLog");
    return resHandle;
  }
  catch (std::exception &e) {
    std::cerr << "Exception happend when doing HyperLogLog:" << e.what()
              << std::endl;
    resHandle.pStrErr = strdup(e.what());
  }
  return resHandle;
}

namespace ares {

// hll sort batch sort current batch using higher 48bits of the hash produced
// from dim values + 16bit reg_id from hll value,
// Note: we store the actual dim values of the current batch into the
// prevDimOut.DimValues vector, but we write the index vector, hash vector, hll
// value vector into the curDimOut, index vector will be initialized to range
// [prevResultSize, preResultSize + curBatchSize)
void sortCurrentBatch(uint8_t *dimValues, uint64_t *hashValues,
                      uint32_t *indexVector,
                      uint8_t numDimsPerDimWidth[NUM_DIM_WIDTH],
                      int vectorCapacity, uint32_t *curValuesOut,
                      int prevResultSize, int curBatchSize,
                      cudaStream_t cudaStream) {
  DimensionHashIterator<> hashIter(
      dimValues, numDimsPerDimWidth, vectorCapacity, indexVector);
  auto zippedValueIter = thrust::make_zip_iterator(
      thrust::make_tuple(indexVector, curValuesOut));
  thrust::transform(
      GET_EXECUTION_POLICY(cudaStream),
      hashIter, hashIter + curBatchSize, curValuesOut, hashValues,
      HLLHashFunctor());
  thrust::stable_sort_by_key(
      GET_EXECUTION_POLICY(cudaStream),
      hashValues, hashValues + curBatchSize,
      zippedValueIter);
}

// prepareHeadFlags prepares dimHeadFlags determines whether a element is the
// head of a dimension partition
template<typename DimHeadIter>
void prepareHeadFlags(uint64_t *hashVector, DimHeadIter dimHeadFlags,
                      int resultSize, cudaStream_t cudaStream) {
  HLLDimNotEqualFunctor dimNotEqual;
  // TODO(jians): if we see performance issue here, we can try to use custome
  // kernel to utilize shared memory
  thrust::transform(
      GET_EXECUTION_POLICY(cudaStream),
      hashVector, hashVector + resultSize - 1, hashVector + 1, dimHeadFlags + 1,
      dimNotEqual);
}

// createAndCopyHLLVector creates the hll vector based on
// scanned count of reg_id counts per dimension and copy hll value
// reg_id < 4096, copy hll measure value in sparse format
// reg_id >= 4096, copy hll measure value in dense format
void createAndCopyHLLVector(uint64_t *hashVector,
                            uint8_t **hllVectorPtr,
                            size_t *hllVectorSizePtr,
                            uint16_t **hllRegIDCountPerDimPtr,
                            unsigned int *dimCumCount,
                            uint32_t *values,
                            int resultSizeWithRegIDs,
                            int resultSize,
                            cudaStream_t cudaStream) {
  HLLRegIDHeadFlagIterator regIDHeadFlagIterator(hashVector);
  // allocate dimRegIDCount vector
  ares::deviceMalloc(reinterpret_cast<void **>(hllRegIDCountPerDimPtr),
             (size_t)resultSize * sizeof(uint16_t));
  // produce dimRegIDCount vector
  thrust::reduce_by_key(
      GET_EXECUTION_POLICY(cudaStream),
      dimCumCount, dimCumCount + resultSizeWithRegIDs,
      regIDHeadFlagIterator, thrust::make_discard_iterator(),
      *hllRegIDCountPerDimPtr);

  // iterator for get byte count for each dim according to reg id count
  auto hllDimByteCountIter = thrust::make_transform_iterator(
      *hllRegIDCountPerDimPtr, HLLDimByteCountFunctor());

  auto hllDimRegIDCountIter = thrust::make_transform_iterator(
      *hllRegIDCountPerDimPtr, CastFunctor<uint16_t, uint64_t>());
  // get dim reg id cumulative count (cumulative count of reg_id per each
  // dimension value)
  ares::device_vector<uint64_t> hllDimRegIDCumCount(resultSize + 1, 0);
  ares::device_vector<uint64_t> hllVectorOffsets(resultSize + 1, 0);
  ares::device_vector<uint64_t> hllRegIDCumCount(resultSizeWithRegIDs);
  thrust::inclusive_scan(
      GET_EXECUTION_POLICY(cudaStream),
      hllDimRegIDCountIter, hllDimRegIDCountIter + resultSize,
      hllDimRegIDCumCount.begin() + 1);
  thrust::inclusive_scan(
      GET_EXECUTION_POLICY(cudaStream),
      hllDimByteCountIter, hllDimByteCountIter + resultSize,
      hllVectorOffsets.begin() + 1);
  thrust::inclusive_scan(
      GET_EXECUTION_POLICY(cudaStream),
      regIDHeadFlagIterator,
      regIDHeadFlagIterator + resultSizeWithRegIDs,
      hllRegIDCumCount.begin());
  *hllVectorSizePtr = hllVectorOffsets[resultSize];
  HLLValueOutputIterator hllValueOutputIter(
      dimCumCount, values, thrust::raw_pointer_cast(hllRegIDCumCount.data()),
      thrust::raw_pointer_cast(hllDimRegIDCumCount.data()),
      thrust::raw_pointer_cast(hllVectorOffsets.data()));

  // allocate dense vector
  deviceMalloc(reinterpret_cast<void **>(hllVectorPtr), *hllVectorSizePtr);
  deviceMemset(*hllVectorPtr, 0, *hllVectorSizePtr);
  thrust::transform_if(
      GET_EXECUTION_POLICY(cudaStream),
      hllValueOutputIter, hllValueOutputIter + resultSizeWithRegIDs,
      regIDHeadFlagIterator, thrust::make_discard_iterator(),
      CopyHLLFunctor(*hllVectorPtr), thrust::identity<unsigned int>());
}

// copyDim is the same as regular dimension copy in regular reduce operations
void copyDim(DimensionVector inputKeys,
             DimensionVector outputKeys, int outputLength,
             cudaStream_t cudaStream) {
  DimensionColumnPermutateIterator iterIn(
      inputKeys.DimValues, outputKeys.IndexVector, inputKeys.VectorCapacity,
      outputLength, inputKeys.NumDimsPerDimWidth);

  DimensionColumnOutputIterator iterOut(outputKeys.DimValues,
                                        inputKeys.VectorCapacity, outputLength,
                                        inputKeys.NumDimsPerDimWidth, 0);

  int numDims = 0;
  for (int i = 0; i < NUM_DIM_WIDTH; i++) {
    numDims += inputKeys.NumDimsPerDimWidth[i];
  }

  thrust::copy(GET_EXECUTION_POLICY(cudaStream),
               iterIn, iterIn + numDims * 2 * outputLength, iterOut);
}

// merge merges previous batch results with current batch results
// based on hash value (asce) and hll value (desc)
void merge(uint64_t *inputHashValues, uint64_t *outputHashValues,
           uint32_t *inputValues, uint32_t *outputValues,
           uint32_t *inputIndexVector, uint32_t *outputIndexVector,
           int prevResultSize, int curBatchResultSize,
           cudaStream_t cudaStream) {
  auto zippedPrevBatchMergeKey = thrust::make_zip_iterator(
      thrust::make_tuple(inputHashValues, inputValues));
  auto zippedCurBatchMergeKey = thrust::make_zip_iterator(thrust::make_tuple(
      inputHashValues + prevResultSize, inputValues + prevResultSize));
  auto zippedOutputKey = thrust::make_zip_iterator(
      thrust::make_tuple(outputHashValues, outputValues));

  thrust::merge_by_key(
      GET_EXECUTION_POLICY(cudaStream),
      zippedPrevBatchMergeKey, zippedPrevBatchMergeKey + prevResultSize,
      zippedCurBatchMergeKey, zippedCurBatchMergeKey + curBatchResultSize,
      inputIndexVector, inputIndexVector + prevResultSize, zippedOutputKey,
      outputIndexVector, HLLMergeComparator());
}

int reduceCurrentBatch(uint64_t *inputHashValues,
                       uint32_t *inputIndexVector,
                       uint32_t *inputValues,
                       uint64_t *outputHashValues,
                       uint32_t *outputIndexVector,
                       uint32_t *outputValues,
                       int length,
                       cudaStream_t cudaStream) {
  thrust::equal_to<uint64_t> binaryPred;
  thrust::maximum<uint32_t> maxOp;
  ReduceByHashFunctor<thrust::maximum<uint32_t> > reduceFunc(maxOp);
  auto zippedInputIter = thrust::make_zip_iterator(
      thrust::make_tuple(inputIndexVector, inputValues));
  auto zippedOutputIter = thrust::make_zip_iterator(
      thrust::make_tuple(outputIndexVector, outputValues));
  auto resEnd = thrust::reduce_by_key(
      GET_EXECUTION_POLICY(cudaStream),
      inputHashValues, inputHashValues + length, zippedInputIter,
      outputHashValues, zippedOutputIter, binaryPred, reduceFunc);
  return thrust::get<0>(resEnd) - outputHashValues;
}

int makeHLLVector(uint64_t *hashValues, uint32_t *indexVector,
                  uint32_t *values, int resultSize, uint8_t **hllVectorPtr,
                  size_t *hllVectorSizePtr, uint16_t **hllRegIDCountPerDimPtr,
                  cudaStream_t cudaStream) {
  ares::device_vector<unsigned int> dimHeadFlags(resultSize, 1);
  prepareHeadFlags(hashValues, dimHeadFlags.begin(), resultSize, cudaStream);

  int reducedResultSize =
      thrust::remove_if(
          GET_EXECUTION_POLICY(cudaStream),
          indexVector, indexVector + resultSize, dimHeadFlags.begin(),
          thrust::detail::equal_to_value<unsigned int>(0)) -
      indexVector;
  thrust::inclusive_scan(
      GET_EXECUTION_POLICY(cudaStream),
      dimHeadFlags.begin(), dimHeadFlags.end(), dimHeadFlags.begin());
  createAndCopyHLLVector(hashValues, hllVectorPtr, hllVectorSizePtr,
                         hllRegIDCountPerDimPtr,
                         thrust::raw_pointer_cast(dimHeadFlags.data()), values,
                         resultSize, reducedResultSize, cudaStream);
  return reducedResultSize;
}

// the steps for hyperloglog:
// 1. sort current batch
// 2. reduce current batch
// 3. merge current batch result with result from previous batches
// 4. (last batch only) create dense hll vector
// 5. copy dimension values
int hyperloglog(DimensionVector prevDimOut,
                DimensionVector curDimOut, uint32_t *prevValuesOut,
                uint32_t *curValuesOut, int prevResultSize, int curBatchSize,
                bool isLastBatch, uint8_t **hllVectorPtr,
                size_t *hllVectorSizePtr, uint16_t **hllRegIDCountPerDimPtr,
                cudaStream_t cudaStream) {
  sortCurrentBatch(prevDimOut.DimValues, curDimOut.HashValues,
                   curDimOut.IndexVector, curDimOut.NumDimsPerDimWidth,
                   curDimOut.VectorCapacity, curValuesOut, prevResultSize,
                   curBatchSize, cudaStream);
  int curResultSize = reduceCurrentBatch(
      curDimOut.HashValues, curDimOut.IndexVector, curValuesOut,
      prevDimOut.HashValues + prevResultSize,
      prevDimOut.IndexVector + prevResultSize, prevValuesOut + prevResultSize,
      curBatchSize, cudaStream);

  merge(prevDimOut.HashValues, curDimOut.HashValues, prevValuesOut,
        curValuesOut, prevDimOut.IndexVector, curDimOut.IndexVector,
        prevResultSize, curResultSize, cudaStream);

  int resSize = prevResultSize + curResultSize;
  if (isLastBatch && resSize > 0) {
    resSize = makeHLLVector(
        curDimOut.HashValues, curDimOut.IndexVector, curValuesOut, resSize,
        hllVectorPtr, hllVectorSizePtr, hllRegIDCountPerDimPtr, cudaStream);
  }
  copyDim(prevDimOut, curDimOut, resSize, cudaStream);
  return resSize;
}

}  // namespace ares
