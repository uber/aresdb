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

#include <thrust/iterator/zip_iterator.h>
#include <thrust/iterator/discard_iterator.h>
#include <thrust/gather.h>
#include <thrust/transform.h>
#include <cstring>
#include <algorithm>
#include <exception>
#include "query/algorithm.hpp"
#include "query/iterator.hpp"
#include "query/time_series_aggregate.h"


CGoCallResHandle Sort(DimensionColumnVector keys,
                      int length,
                      void *cudaStream,
                      int device) {
  CGoCallResHandle resHandle = {nullptr, nullptr};
  try {
#ifdef RUN_ON_DEVICE
    cudaSetDevice(device);
#endif
    ares::sort(keys, length, cudaStream);
    CheckCUDAError("Sort");
  }
  catch (std::exception &e) {
    std::cerr << "Exception happend when doing Sort:" << e.what()
              << std::endl;
    resHandle.pStrErr = strdup(e.what());
  }
  return resHandle;
}

CGoCallResHandle Reduce(DimensionColumnVector inputKeys,
                        uint8_t *inputValues,
                        DimensionColumnVector outputKeys,
                        uint8_t *outputValues,
                        int valueBytes,
                        int length,
                        AggregateFunction aggFunc,
                        void *cudaStream,
                        int device) {
  CGoCallResHandle resHandle = {nullptr, nullptr};
  try {
#ifdef RUN_ON_DEVICE
    cudaSetDevice(device);
#endif
    resHandle.res = reinterpret_cast<void *>(ares::reduce(inputKeys,
                                                          inputValues,
                                                          outputKeys,
                                                          outputValues,
                                                          valueBytes,
                                                          length,
                                                          aggFunc,
                                                          cudaStream));
    CheckCUDAError("Reduce");
    return resHandle;
  }
  catch (std::exception &e) {
    std::cerr << "Exception happend when doing Reduce:" << e.what()
              << std::endl;
    resHandle.pStrErr = strdup(e.what());
  }
  return resHandle;
}


CGoCallResHandle Expand(DimensionColumnVector inputKeys,
                        DimensionColumnVector outputKeys,
                        uint32_t *baseCounts,
                        uint32_t *indexVector,
                        int indexVectorLen,
                        int outputOccupiedLen,
                        void *cudaStream,
                        int device) {
  CGoCallResHandle resHandle = {nullptr, nullptr};

  try {
    SET_DEVICE(device)
    resHandle.res = reinterpret_cast<void *>(ares::expand(inputKeys,
                                                          outputKeys,
                                                          baseCounts,
                                                          indexVector,
                                                          indexVectorLen,
                                                          outputOccupiedLen,
                                                          cudaStream));
    CheckCUDAError("Expand");
    return resHandle;
  }
  catch (std::exception &e) {
    std::cerr << "Exception happend when doing Expand:" << e.what()
              << std::endl;
    resHandle.pStrErr = strdup(e.what());
  }

  return resHandle;
}

namespace ares {

// sort based on DimensionColumnVector
void sort(DimensionColumnVector keys,
          int length,
          void *cudaStream) {
  DimensionHashIterator hashIter(keys.DimValues,
                                 keys.IndexVector,
                                 keys.NumDimsPerDimWidth,
                                 keys.VectorCapacity);
#ifdef RUN_ON_DEVICE
  thrust::copy(thrust::cuda::par.on(reinterpret_cast<cudaStream_t>(cudaStream)),
               hashIter, hashIter + length, keys.HashValues);
  thrust::stable_sort_by_key(
      thrust::cuda::par.on(reinterpret_cast<cudaStream_t>(cudaStream)),
      keys.HashValues, keys.HashValues + length, keys.IndexVector);
#else
  thrust::copy(thrust::host,
               hashIter,
               hashIter + length,
               keys.HashValues);
  thrust::stable_sort_by_key(thrust::host,
                             keys.HashValues,
                             keys.HashValues + length,
                             keys.IndexVector);
#endif
}

template<typename Value, typename AggFunc>
int reduceInternal(uint64_t *inputHashValues, uint32_t *inputIndexVector,
                   uint8_t *inputValues, uint64_t *outputHashValues,
                   uint32_t *outputIndexVector, uint8_t *outputValues,
                   int length, void *cudaStream) {
  thrust::equal_to<uint64_t> binaryPred;
  AggFunc aggFunc;
  ReduceByHashFunctor<AggFunc> reduceFunc(aggFunc);
  auto zippedInputIter = thrust::make_zip_iterator(thrust::make_tuple(
      inputIndexVector,
      thrust::make_permutation_iterator(reinterpret_cast<Value *>(inputValues),
                                        inputIndexVector)));
  auto zippedOutputIter = thrust::make_zip_iterator(thrust::make_tuple(
      outputIndexVector, reinterpret_cast<Value *>(outputValues)));
#ifdef RUN_ON_DEVICE
  auto resEnd = thrust::reduce_by_key(
      thrust::cuda::par.on(reinterpret_cast<cudaStream_t>(cudaStream)),
      inputHashValues, inputHashValues + length, zippedInputIter,
      thrust::make_discard_iterator(), zippedOutputIter, binaryPred,
      reduceFunc);
  return thrust::get<1>(resEnd) - zippedOutputIter;
#else
  auto resEnd = thrust::reduce_by_key(thrust::host,
                                      inputHashValues,
                                      inputHashValues + length,
                                      zippedInputIter,
                                      thrust::make_discard_iterator(),
                                      zippedOutputIter,
                                      binaryPred,
                                      reduceFunc);
  return thrust::get<1>(resEnd) - zippedOutputIter;
#endif
}

struct rolling_avg {
  typedef uint64_t first_argument_type;
  typedef uint64_t second_argument_type;
  typedef uint64_t result_type;

  __host__  __device__ uint64_t operator()(
      uint64_t lhs, uint64_t rhs) const {
    uint32_t lCount = lhs >> 32;
    uint32_t rCount = rhs >> 32;
    uint32_t totalCount = lCount + rCount;
    if (totalCount == 0) {
      return 0;
    }

    uint64_t res = 0;
    *(reinterpret_cast<uint32_t *>(&res) + 1) = totalCount;
    // do division first to avoid overflow.
    *reinterpret_cast<float_t*>(&res) =
        *reinterpret_cast<float_t*>(&lhs) / totalCount * lCount +
        *reinterpret_cast<float_t*>(&rhs) / totalCount * rCount;
    return res;
  }
};

int bindValueAndAggFunc(uint64_t *inputHashValues,
                        uint32_t *inputIndexVector,
                        uint8_t *inputValues,
                        uint64_t *outputHashValues,
                        uint32_t *outputIndexVector,
                        uint8_t *outputValues,
                        int valueBytes,
                        int length,
                        AggregateFunction aggFunc,
                        void *cudaStream) {
  switch (aggFunc) {
    #define REDUCE_INTERNAL(ValueType, AggFunc) \
      return reduceInternal< ValueType, AggFunc >( \
            inputHashValues, \
            inputIndexVector, \
            inputValues, \
            outputHashValues, \
            outputIndexVector, \
            outputValues, \
            length, \
            cudaStream);

    case AGGR_SUM_UNSIGNED:
      if (valueBytes == 4) {
        REDUCE_INTERNAL(uint32_t, thrust::plus<uint32_t>)
      } else {
        REDUCE_INTERNAL(uint64_t, thrust::plus<uint64_t>)
      }
    case AGGR_SUM_SIGNED:
      if (valueBytes == 4) {
        REDUCE_INTERNAL(int32_t, thrust::plus<int32_t>)
      } else {
        REDUCE_INTERNAL(int64_t, thrust::plus<int64_t>)
      }
    case AGGR_SUM_FLOAT:
      if (valueBytes == 4) {
        REDUCE_INTERNAL(float_t, thrust::plus<float_t>)
      } else {
        REDUCE_INTERNAL(double_t, thrust::plus<double_t>)
      }
    case AGGR_MIN_UNSIGNED:
      REDUCE_INTERNAL(uint32_t, thrust::minimum<uint32_t>)
    case AGGR_MIN_SIGNED:
      REDUCE_INTERNAL(int32_t, thrust::minimum<int32_t>)
    case AGGR_MIN_FLOAT:
      REDUCE_INTERNAL(float_t, thrust::minimum<float_t>)
    case AGGR_MAX_UNSIGNED:
      REDUCE_INTERNAL(uint32_t, thrust::maximum<uint32_t>)
    case AGGR_MAX_SIGNED:
      REDUCE_INTERNAL(int32_t, thrust::maximum<int32_t>)
    case AGGR_MAX_FLOAT:
      REDUCE_INTERNAL(float_t, thrust::maximum<float_t>)
    case AGGR_AVG_FLOAT:
      REDUCE_INTERNAL(uint64_t, rolling_avg)
    default:
      throw std::invalid_argument("Unsupported aggregation function type");
  }
}

int reduce(DimensionColumnVector inputKeys, uint8_t *inputValues,
           DimensionColumnVector outputKeys, uint8_t *outputValues,
           int valueBytes, int length, AggregateFunction aggFunc,
           void *cudaStream) {
  int outputLength = bindValueAndAggFunc(
      inputKeys.HashValues,
      inputKeys.IndexVector,
      inputValues,
      outputKeys.HashValues,
      outputKeys.IndexVector,
      outputValues,
      valueBytes,
      length,
      aggFunc,
      cudaStream);
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
// copy dim values into output
#ifdef RUN_ON_DEVICE
  thrust::copy(thrust::cuda::par.on(reinterpret_cast<cudaStream_t>(cudaStream)),
                                    iterIn, iterIn + numDims * 2 * outputLength,
                                    iterOut);
#else
  thrust::copy(thrust::host, iterIn, iterIn + numDims * 2 * outputLength,
               iterOut);
#endif
  return outputLength;
}


int expand(DimensionColumnVector inputKeys,
           DimensionColumnVector outputKeys,
           uint32_t *baseCounts,
           uint32_t *indexVector,
           int indexVectorLen,
           int outputOccupiedLen,
           void *cudaStream) {
  // create count interator from baseCount and indexVector
  IndexCountIterator countIter = IndexCountIterator(baseCounts, indexVector);

  // total item counts by adding counts together
  uint32_t totalCount = thrust::reduce(HOST_DEVICE_STRATEGY(cudaStream),
                                       countIter,
                                       countIter+indexVectorLen);

  // scan the counts to obtain output offsets for each input element
  HOST_DEVICE_VECTOR(uint32_t) offsets(indexVectorLen);
  thrust::exclusive_scan(HOST_DEVICE_STRATEGY(cudaStream),
                         countIter,
                         countIter+indexVectorLen,
                         offsets.begin());

  // scatter the nonzero counts into their corresponding output positions
  HOST_DEVICE_VECTOR(uint32_t) indices(totalCount);
  thrust::scatter_if(HOST_DEVICE_STRATEGY(cudaStream),
                     thrust::counting_iterator<uint32_t>(0),
                     thrust::counting_iterator<uint32_t>(indexVectorLen),
                     offsets.begin(),
                     countIter,
                     indices.begin());

  // compute max-scan over the indices, filling in the holes
  thrust::inclusive_scan(HOST_DEVICE_STRATEGY(cudaStream),
                         indices.begin(),
                         indices.end(),
                         indices.begin(),
                         thrust::maximum<uint32_t>());

  // get the raw pointer from device/host vector
  uint32_t * newIndexVector = thrust::raw_pointer_cast(&indices[0]);

  int outputLen = min(totalCount, outputKeys.VectorCapacity - outputOccupiedLen);
  // start the real copy operation
  DimensionColumnPermutateIterator iterIn(
      inputKeys.DimValues, newIndexVector, inputKeys.VectorCapacity,
      outputLen, inputKeys.NumDimsPerDimWidth);

  DimensionColumnOutputIterator iterOut(outputKeys.DimValues,
                                        outputKeys.VectorCapacity, outputLen,
                                        inputKeys.NumDimsPerDimWidth, outputOccupiedLen);

  int numDims = 0;
  for (int i = 0; i < NUM_DIM_WIDTH; i++) {
      numDims += inputKeys.NumDimsPerDimWidth[i];
  }
  // copy dim values into output
  thrust::copy(HOST_DEVICE_STRATEGY(cudaStream), iterIn, iterIn + numDims * 2 * outputLen,
               iterOut);
  // return total count in the output dimensionVector
  return outputLen + outputOccupiedLen;
}

}  // namespace ares
