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

#ifdef SUPPORT_HASH_REDUCTION
#include <groupby/aggregation_operations.hpp>
#include <hash/concurrent_unordered_map.cuh>
#include <hash/hash_functions.cuh>
#endif
#include <thrust/pair.h>
#include <thrust/iterator/counting_iterator.h>
#include <cstring>
#include <algorithm>
#include <exception>
#include "algorithm.hpp"
#include "iterator.hpp"
#include "memory.hpp"
#include "query/time_series_aggregate.h"
#include "utils.hpp"

CGoCallResHandle HashReduce(DimensionVector inputKeys,
                            uint8_t *inputValues,
                            DimensionVector outputKeys,
                            uint8_t *outputValues,
                            int valueBytes,
                            int length,
                            AggregateFunction aggFunc,
                            void *stream,
                            int device) {
  CGoCallResHandle resHandle = {nullptr, nullptr};
  try {
#ifdef SUPPORT_HASH_REDUCTION
#ifdef RUN_ON_DEVICE
    cudaSetDevice(device);
#endif
    cudaStream_t cudaStream = reinterpret_cast<cudaStream_t>(stream);
    resHandle.res = reinterpret_cast<void *>(ares::hash_reduction(inputKeys,
                                                                  inputValues,
                                                                  outputKeys,
                                                                  outputValues,
                                                                  valueBytes,
                                                                  length,
                                                                  aggFunc,
                                                                  cudaStream));
#else
    resHandle.res = reinterpret_cast<void *>(0);
#endif
    CheckCUDAError("HashReduce");
    return resHandle;
  }
  catch (std::exception &e) {
    std::cerr << "Exception happend when doing Reduce:" << e.what()
              << std::endl;
    resHandle.pStrErr = strdup(e.what());
  }
  return resHandle;
}

#ifdef SUPPORT_HASH_REDUCTION
namespace ares {

template<typename map_type>
struct ExtractGroupByResultFunctor {
  typedef typename map_type::key_type key_type;
  typedef typename map_type::mapped_type element_type;
  typedef thrust::pair<key_type, element_type> *key_value_ptr_type;

  explicit ExtractGroupByResultFunctor(
      map_type *const __restrict__ map,
      uint8_t *dimInputValues,
      size_t vectorCapacity,
      uint8_t *dimOutputValues,
      uint8_t dimWidthPrefixSum[NUM_DIM_WIDTH],
      uint8_t *measureValues,
      uint32_t *global_write_index) : map(map), dimInputValues(dimInputValues),
                                      vectorCapacity(vectorCapacity),
                                      dimOutputValues(dimOutputValues),
                                      measureValues(measureValues),
                                      global_write_index(
                                          global_write_index) {
    memcpy(this->dimWidthPrefixSum,
           dimWidthPrefixSum, sizeof(uint8_t) * NUM_DIM_WIDTH);
  }

  map_type *map;
  uint8_t *dimInputValues;
  uint32_t *global_write_index;
  uint8_t *measureValues;
  uint8_t *dimOutputValues;
  size_t vectorCapacity;
  uint8_t dimWidthPrefixSum[NUM_DIM_WIDTH];

  // copy_dim_values moves the inputDimValues at inputIdx to curDimOutput
  // at outputIdx.
  __host__ __device__
  void copy_dim_values(uint32_t inputIdx, size_t outputIdx) {
    uint8_t * curDimInput = dimInputValues;
    uint8_t * curDimOutput = dimOutputValues;

    // idx in numDimsPerDimWidth;
    int widthIdx = 0;
    uint8_t totalDims = dimWidthPrefixSum[NUM_DIM_WIDTH - 1];
    // write values
    // pointer address for dim value d on row r is
    // (base_ptr + accumulatedValueBytes * vectorCapacity) + r * dimValueBytes.
    for (uint8_t dimIndex = 0; dimIndex < totalDims; dimIndex += 1) {
      // find correct widthIdx.
      while (widthIdx < NUM_DIM_WIDTH
          && dimIndex >= dimWidthPrefixSum[widthIdx]) {
        widthIdx++;
      }

      uint16_t dimValueBytes = 1 << (NUM_DIM_WIDTH - widthIdx - 1);
      curDimInput += dimValueBytes * inputIdx;
      curDimOutput += dimValueBytes * outputIdx;
      setDimValue(curDimOutput, curDimInput, dimValueBytes);

      // set to the start of next dim value
      curDimInput += (vectorCapacity - inputIdx) * dimValueBytes;
      curDimOutput += (vectorCapacity - outputIdx) * dimValueBytes;
    }

    // write nulls.
    // Now both curDimInput and curDimOutput should point to start of
    // null vector.
    for (uint8_t dimIndex = 0; dimIndex < totalDims; dimIndex += 1) {
      curDimInput += inputIdx;
      curDimOutput += outputIdx;

      *curDimOutput = *curDimInput;

      // set to the start of next dim null vector
      curDimInput += vectorCapacity - inputIdx;
      curDimOutput += vectorCapacity - outputIdx;
    }
  }

  __host__ __device__
  void operator()(const int i) {
    key_type unusedKey = map->get_unused_key();
    key_value_ptr_type mapValues = map->data();
    size_t mapSize = map->capacity();
    if (i < mapSize) {
      key_type current_key = mapValues[i].first;
      if (current_key != unusedKey) {
        uint32_t outputIdx = ares::atomicAdd(global_write_index, (uint32_t)1);
        copy_dim_values(static_cast<uint32_t>(current_key), outputIdx);
        element_type value = mapValues[i].second;
        reinterpret_cast<element_type *>(measureValues)[outputIdx] = value;
      }
    }
  }
};

// hasher is the struct to return the hash value for a given key of concurrent
// map. It just returns the higher 32 bits of the key as the hash value. Note
// we cannot use lambda for hasher since the concurrent_unordered_map need
// hash_value_type defined in the struct.
struct Higher32BitsHasher {
  using result_type = uint32_t;
  using key_type = int64_t;

  __host__ __device__
  result_type operator()(key_type key) const {
    return static_cast<result_type>(key >> 32);
  }
};

// HashReductionContext wraps the concurrent_unordered_map for hash reduction.
// The key of the hash map is a 8 byte integer where the first 4 bytes are the
// hash_value and second 4 bytes are the dim row index, hash value of the map is
// pre-calculated during insertion because we use the same hash value for
// equality check as well when collision happens. Therefore the hash function
// of the hash map will be just extract the 1st item of the pair.
// value_type of the hash map is just the measure value. Therefore we can use
// atomic functions for most aggregate functions.
// After the reduction part is done, we need to output the result in the format
// of <index, aggregated_values>. Notes we don't need to output hash values.
template<typename value_type, typename agg_func>
class HashReductionContext {
  using key_type = int64_t;

 private:
  agg_func f;
  uint32_t length;
  value_type identity;

 public:
  explicit HashReductionContext(uint32_t length,
                                value_type identity)
      : length(length), identity(identity) {
    f = agg_func();
  }

  // reduce reduces dimension value with same hash key into a single element
  // using corresponding aggregation function. Note the key is actually
  // < hash_value, index> pair but the equability check is only on
  // hash_value. Therefore the first index paired with the hash value will
  // be the final output value.
  template<typename map_type>
  void reduce(map_type *map,
              DimensionVector inputKeys,
              uint8_t *inputValues,
              cudaStream_t cudaStream) {
    DimensionHashIterator<32,
                          thrust::counting_iterator<uint32_t>>
        hashIter(inputKeys.DimValues,
                 inputKeys.NumDimsPerDimWidth,
                 inputKeys.VectorCapacity);

    auto rawMapKeyIter = thrust::make_zip_iterator(
        thrust::make_tuple(hashIter, thrust::counting_iterator<uint32_t>(0)));

    auto hashIndexFusionFunc = []
    __host__ __device__(
        typename decltype(rawMapKeyIter)::value_type tuple) {
      return (static_cast<int64_t>(thrust::get<0>(tuple)) << 32)
          | (static_cast<int64_t>(thrust::get<1>(tuple)));
    };

    auto mapKeyIter = thrust::make_transform_iterator(rawMapKeyIter,
                                                      hashIndexFusionFunc);

    auto mapKeyValuePairIter = thrust::make_zip_iterator(
        thrust::make_tuple(
            mapKeyIter,
            reinterpret_cast<value_type *>(inputValues)));

    auto equality = []
    __host__ __device__(key_type lhs, key_type rhs) {
      return lhs >> 32 == rhs >> 32;
    };

    auto insertionFunc = [=]
    __host__ __device__(
        thrust::tuple<key_type, value_type> key_value_tuple) {
      map->insert(tuple2pair(key_value_tuple), f, equality);
    };

    thrust::for_each_n(GET_EXECUTION_POLICY(cudaStream),
                       mapKeyValuePairIter,
                       length,
                       insertionFunc);
  }

  template<typename map_type>
  int output(map_type *map,
             DimensionVector inputKeys,
             DimensionVector outputKeys, uint8_t *outputValue,
             cudaStream_t cudaStream) {
    // calculate prefix sum of NumDimsPerDimWidth so that it will be easier
    // to tell the valueBytes given a dim index. The prefix sum is inclusive.
    uint8_t
    dimWidthPrefixSum[NUM_DIM_WIDTH];
    for (int i = 0; i < NUM_DIM_WIDTH; i++) {
      dimWidthPrefixSum[i] = inputKeys.NumDimsPerDimWidth[i];
      if (i > 0) {
        dimWidthPrefixSum[i] += dimWidthPrefixSum[i - 1];
      }
    }

    device_unique_ptr<uint32_t> globalWriteIndexDevice =
        make_device_unique<uint32_t>(cudaStream);

    ExtractGroupByResultFunctor<map_type> extractorFunc(
        map, inputKeys.DimValues, inputKeys.VectorCapacity,
        outputKeys.DimValues, dimWidthPrefixSum,
        outputValue, globalWriteIndexDevice.get());

    thrust::for_each_n(GET_EXECUTION_POLICY(cudaStream),
                       thrust::counting_iterator<uint32_t>(0),
                       length, extractorFunc);
    // copy the reduced count back from GPU.
    uint32_t
    globalWriteIndexHost;
    ares::asyncCopyDeviceToHost(
        reinterpret_cast<void *>(&globalWriteIndexHost),
        reinterpret_cast<void *>(globalWriteIndexDevice.get()),
        sizeof(uint32_t), cudaStream);
    ares::waitForCudaStream(cudaStream);
    return globalWriteIndexHost;
  }

  // execute reduces the dimension values with measures first and then extract
  // aggregated result.
  int execute(DimensionVector inputKeys, uint8_t *inputValues,
              DimensionVector outputKeys, uint8_t *outputValues,
              cudaStream_t cudaStream) {
    auto equality = []
    __host__ __device__(key_type lhs, key_type rhs) {
      return (lhs >> 32) == (rhs >> 32);
    };

    typedef concurrent_unordered_map<key_type, value_type, Higher32BitsHasher,
        decltype(equality)>
        map_type;

    host_unique_ptr<map_type>
        mapHost = make_host_unique<map_type>(length, identity, 0,
                                             Higher32BitsHasher(), equality);

    device_unique_ptr<map_type>
        mapDevice = make_device_unique<map_type>(cudaStream, mapHost.get());
    reduce(mapDevice.get(), inputKeys, inputValues, cudaStream);
    return output(mapDevice.get(),
                  inputKeys,
                  outputKeys,
                  outputValues,
                  cudaStream);
  }
};

// hashReduceInternal reduces the dimension value using the aggregation
// function into a concurrent hash map. After reduction it will extract
// the dimension and measure values from hash map to corresponding
// dimension vector and value vector.
template<typename value_type, typename agg_func_type>
int hashReduceInternal(DimensionVector inputKeys,
                       uint8_t *inputValues,
                       DimensionVector outputKeys,
                       uint8_t *outputValues,
                       int length,
                       value_type identity,
                       cudaStream_t cudaStream) {
  HashReductionContext<value_type, agg_func_type> context(length, identity);
  return context.execute(inputKeys,
                         inputValues,
                         outputKeys,
                         outputValues,
                         cudaStream);
}

// This function simply binds the measure value type and aggregate function
// type.
int hash_reduction(DimensionVector inputKeys, uint8_t *inputValues,
                   DimensionVector outputKeys, uint8_t *outputValues,
                   int valueBytes, int length, AggregateFunction aggFunc,
                   cudaStream_t cudaStream) {
  int outputLength = 0;
  switch (aggFunc) {
#define REDUCE_INTERNAL(value_type, agg_func_type) \
      outputLength = hashReduceInternal< \
          value_type, agg_func_type>( \
            inputKeys, \
            inputValues, \
            outputKeys, \
            outputValues, \
            length, \
            get_identity_value<value_type>(aggFunc), \
            cudaStream); break;
    case AGGR_SUM_UNSIGNED:
      if (valueBytes == 4) {
        REDUCE_INTERNAL(uint32_t, sum_op <uint32_t>)
      } else {
        REDUCE_INTERNAL(uint64_t, sum_op < uint64_t >)
      }
    case AGGR_SUM_SIGNED:
      if (valueBytes == 4) {
        REDUCE_INTERNAL(int32_t, sum_op < int32_t >)
      } else {
        REDUCE_INTERNAL(int64_t, sum_op < int64_t >)
      }
    case AGGR_SUM_FLOAT:
      if (valueBytes == 4) {
        REDUCE_INTERNAL(float_t, sum_op < float_t >)
      } else {
        REDUCE_INTERNAL(double_t, sum_op < double_t >)
      }
    case AGGR_MIN_UNSIGNED:REDUCE_INTERNAL(uint32_t, min_op < uint32_t >)
    case AGGR_MIN_SIGNED:REDUCE_INTERNAL(int32_t, min_op < int32_t >)
    case AGGR_MIN_FLOAT:REDUCE_INTERNAL(float_t, min_op < float_t >)
    case AGGR_MAX_UNSIGNED:REDUCE_INTERNAL(uint32_t, max_op < uint32_t >)
    case AGGR_MAX_SIGNED:REDUCE_INTERNAL(int32_t, max_op < int32_t >)
    case AGGR_MAX_FLOAT:REDUCE_INTERNAL(float_t, max_op < float_t >)
    case AGGR_AVG_FLOAT:REDUCE_INTERNAL(uint64_t, RollingAvgFunctor)
    default:
      throw std::invalid_argument("Unsupported aggregation function type");
  }
  return outputLength;
}

}  // namespace ares
#endif