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
#include <thrust/transform.h>
#include <cstring>
#include <algorithm>
#include <exception>
#include "query/algorithm.hpp"
#include "query/iterator.hpp"
#include "query/time_series_aggregate.h"

CGoCallResHandle Sort(DimensionColumnVector keys,
                      uint8_t *values,
                      int valueBytes,
                      int length,
                      void *cudaStream,
                      int device) {
  CGoCallResHandle resHandle = {nullptr, nullptr};
  try {
#ifdef RUN_ON_DEVICE
    cudaSetDevice(device);
#endif
    ares::sort(keys, values, valueBytes, length,
        reinterpret_cast<cudaStream_t>(cudaStream));
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
                        void *stream,
                        int device) {
  CGoCallResHandle resHandle = {nullptr, nullptr};
  try {
#ifdef RUN_ON_DEVICE
    cudaSetDevice(device);
#endif
    cudaStream_t cudaStream = reinterpret_cast<cudaStream_t>(stream);
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

namespace ares {

template<typename ValueType>
void sortInternal(DimensionColumnVector vector,
                  ValueType values,
                  int length,
                  cudaStream_t cudaStream) {
  DimensionHashIterator hashIter(vector.DimValues,
                                 vector.IndexVector,
                                 vector.NumDimsPerDimWidth,
                                 vector.VectorCapacity);
  thrust::copy(GET_EXECUTION_POLICY(cudaStream),
               hashIter,
               hashIter + length,
               vector.HashValues);
  thrust::stable_sort_by_key(GET_EXECUTION_POLICY(cudaStream),
                             vector.HashValues,
                             vector.HashValues + length,
                             vector.IndexVector);
}

// sort based on DimensionColumnVector
void sort(DimensionColumnVector keys,
          uint8_t *values,
          int valueBytes,
          int length,
          cudaStream_t cudaStream) {
  switch (valueBytes) {
#define  SORT_INTERNAL(ValueType) \
      sortInternal<ValueType>(keys, reinterpret_cast<ValueType>(values), \
                               length, cudaStream); \
      break;

    case 4:
      SORT_INTERNAL(uint32_t *)
    case 8:
      SORT_INTERNAL(uint64_t *)
    default:throw std::invalid_argument("ValueBytes is invalid");
  }
}

template<typename Value, typename AggFunc>
int reduceInternal(uint64_t *inputHashValues, uint32_t *inputIndexVector,
                   uint8_t *inputValues, uint64_t *outputHashValues,
                   uint32_t *outputIndexVector, uint8_t *outputValues,
                   int length, cudaStream_t cudaStream) {
  thrust::equal_to<uint64_t> binaryPred;
  AggFunc aggFunc;
  ReduceByHashFunctor<AggFunc> reduceFunc(aggFunc);
  auto zippedInputIter = thrust::make_zip_iterator(thrust::make_tuple(
      inputIndexVector,
      thrust::make_permutation_iterator(reinterpret_cast<Value *>(inputValues),
                                        inputIndexVector)));
  auto zippedOutputIter = thrust::make_zip_iterator(thrust::make_tuple(
      outputIndexVector, reinterpret_cast<Value *>(outputValues)));
  auto resEnd = thrust::reduce_by_key(GET_EXECUTION_POLICY(cudaStream),
                                      inputHashValues,
                                      inputHashValues + length,
                                      zippedInputIter,
                                      thrust::make_discard_iterator(),
                                      zippedOutputIter,
                                      binaryPred,
                                      reduceFunc);
  return thrust::get<1>(resEnd) - zippedOutputIter;
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
                        cudaStream_t cudaStream) {
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
           cudaStream_t cudaStream) {
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
                                        inputKeys.NumDimsPerDimWidth);

  int numDims = 0;
  for (int i = 0; i < NUM_DIM_WIDTH; i++) {
    numDims += inputKeys.NumDimsPerDimWidth[i];
  }
  // copy dim values into output
  thrust::copy(GET_EXECUTION_POLICY(cudaStream),
      iterIn, iterIn + numDims * 2 * outputLength, iterOut);
  return outputLength;
}

}  // namespace ares
