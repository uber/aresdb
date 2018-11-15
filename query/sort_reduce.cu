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
    ares::sort(keys, values, valueBytes, length, cudaStream);
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

namespace ares {

template<typename ValueType>
void sortInternal(DimensionColumnVector vector,
                  ValueType values,
                  int length,
                  void *cudaStream) {
  DimensionHashIterator hashIter(vector.DimValues,
                                 vector.IndexVector,
                                 vector.NumDimsPerDimWidth,
                                 vector.VectorCapacity);
#ifdef RUN_ON_DEVICE
  thrust::copy(thrust::cuda::par.on(reinterpret_cast<cudaStream_t>(cudaStream)),
               hashIter, hashIter + length, vector.HashValues);
  thrust::stable_sort_by_key(
      thrust::cuda::par.on(reinterpret_cast<cudaStream_t>(cudaStream)),
      vector.HashValues, vector.HashValues + length, vector.IndexVector);
#else
  thrust::copy(thrust::host,
               hashIter,
               hashIter + length,
               vector.HashValues);
  thrust::stable_sort_by_key(thrust::host,
                             vector.HashValues,
                             vector.HashValues + length,
                             vector.IndexVector);
#endif
}

// sort based on DimensionColumnVector
void sort(DimensionColumnVector keys,
          uint8_t *values,
          int valueBytes,
          int length,
          void *cudaStream) {
  switch (valueBytes) {
    case 4:
      sortInternal<uint32_t *>(keys,
                               reinterpret_cast<uint32_t *>(values),
                               length,
                               cudaStream);
      break;
    case 8:
      sortInternal<uint64_t *>(keys,
                               reinterpret_cast<uint64_t *>(values),
                               length,
                               cudaStream);
      break;
    default:throw std::invalid_argument("ValueBytes is invalid");
  }
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
    case AGGR_SUM_UNSIGNED:
      if (valueBytes == 4) {
        return reduceInternal<uint32_t, thrust::plus<uint32_t> >(
            inputHashValues,
            inputIndexVector,
            inputValues,
            outputHashValues,
            outputIndexVector,
            outputValues,
            length,
            cudaStream);
      } else {
        return reduceInternal<uint64_t, thrust::plus<uint64_t> >(
            inputHashValues,
            inputIndexVector,
            inputValues,
            outputHashValues,
            outputIndexVector,
            outputValues,
            length,
            cudaStream);
      }
    case AGGR_SUM_SIGNED:
      if (valueBytes == 4) {
        return reduceInternal<int32_t, thrust::plus<int32_t> >(
            inputHashValues,
            inputIndexVector,
            inputValues,
            outputHashValues,
            outputIndexVector,
            outputValues,
            length,
            cudaStream);
      } else {
        return reduceInternal<int64_t, thrust::plus<int64_t> >(
            inputHashValues,
            inputIndexVector,
            inputValues,
            outputHashValues,
            outputIndexVector,
            outputValues,
            length,
            cudaStream);
      }
    case AGGR_SUM_FLOAT:
      if (valueBytes == 4) {
        return reduceInternal<float_t, thrust::plus<float_t> >(
            inputHashValues,
            inputIndexVector,
            inputValues,
            outputHashValues,
            outputIndexVector,
            outputValues,
            length,
            cudaStream);
      } else {
        return reduceInternal<double_t, thrust::plus<double_t> >(
            inputHashValues,
            inputIndexVector,
            inputValues,
            outputHashValues,
            outputIndexVector,
            outputValues,
            length,
            cudaStream);
      }
    case AGGR_MIN_UNSIGNED:
      return reduceInternal<uint32_t, thrust::minimum<uint32_t> >(
          inputHashValues, inputIndexVector, inputValues, outputHashValues,
          outputIndexVector, outputValues, length, cudaStream);
    case AGGR_MIN_SIGNED:
      return reduceInternal<int32_t, thrust::minimum<int32_t> >(
          inputHashValues, inputIndexVector, inputValues, outputHashValues,
          outputIndexVector, outputValues, length, cudaStream);
    case AGGR_MIN_FLOAT:
      return reduceInternal<float_t, thrust::minimum<float_t> >(
          inputHashValues, inputIndexVector, inputValues, outputHashValues,
          outputIndexVector, outputValues, length, cudaStream);
    case AGGR_MAX_UNSIGNED:
      return reduceInternal<uint32_t, thrust::maximum<uint32_t> >(
          inputHashValues, inputIndexVector, inputValues, outputHashValues,
          outputIndexVector, outputValues, length, cudaStream);
    case AGGR_MAX_SIGNED:
      return reduceInternal<int32_t, thrust::maximum<int32_t> >(
          inputHashValues, inputIndexVector, inputValues, outputHashValues,
          outputIndexVector, outputValues, length, cudaStream);
    case AGGR_MAX_FLOAT:
      return reduceInternal<float_t, thrust::maximum<float_t> >(
          inputHashValues, inputIndexVector, inputValues, outputHashValues,
          outputIndexVector, outputValues, length, cudaStream);
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
                                        inputKeys.NumDimsPerDimWidth);

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

}  // namespace ares
