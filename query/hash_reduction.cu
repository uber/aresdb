#include <thrust/pair.h>
#include <cstring>
#include <algorithm>
#include <exception>
#include <hash/concurrent_unordered_map.cuh>
#include <hash/hash_functions.cuh>
#include <groupby/aggregation_operations.hpp>
#include "algorithm.hpp"
#include "iterator.hpp"
#include "memory.hpp"
#include "time_series_aggregate.h"
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

namespace ares {

template<typename key_type, typename value_type>
struct ExtractGroupByResultFunctor {
  typedef const value_type *const value_ptr_type;

  explicit ExtractGroupByResultFunctor(
      key_type unusedKey,
      value_ptr_type mapValues,
      size_t mapSize,
      uint8_t *dimInputValues,
      size_t vectorCapacity,
      uint8_t *dimOutputValues,
      uint8_t dimWidthPrefixSum[NUM_DIM_WIDTH],
      uint8_t *measureValues,
      uint32_t *const global_write_index) : unusedKey(unusedKey),
                                            mapValues(mapValues),
                                            mapSize(mapSize),
                                            dimInputValues(dimInputValues),
                                            vectorCapacity(vectorCapacity),
                                            dimOutputValues(dimOutputValues),
                                            dimWidthPrefixSum(dimWidthPrefixSum),
                                            measureValues(measureValues),
                                            global_write_index(
                                                global_write_index) {
  }

  key_type unusedKey;
  value_ptr_type mapValues;
  size_t mapSize;
  uint8_t *dimInputValues;
  size_t vectorCapacity;
  uint8_t *dimOutputValues;
  uint8_t dimWidthPrefixSum[NUM_DIM_WIDTH];
  uint8_t *measureValues;
  uint32_t *const global_write_index;

  // copy_dim_values moves the inputDimValues at inputIdx to outputDimValues
  // at outputIdx.
  __host__ __device__
  void copy_dim_values(uint32_t inputIdx, size_t outputIdx) {
    // idx in numDimsPerDimWidth;
    int widthIdx = 0;
    uint8_t totalDims = dimWidthPrefixSum[NUM_DIM_WIDTH-1];
    // write values
    // pointer address for dim value d on row r is
    // (base_ptr + accumulatedValueBytes * vectorCapacity) + r * dimValueBytes.
    for (uint8_t dimIndex = 0; dimIndex < totalDims; dimIndex += 1) {
      // find correct widthIdx.
      while (widthIdx < NUM_DIM_WIDTH
          && dimIndex >= dimWidthPrefixSum[widthIdx]) {
        widthIdx += 1;
      }

      uint16_t dimValueBytes = 1 << (NUM_DIM_WIDTH - widthIdx - 1);
      inputDimValues += dimValueBytes * inputIdx;
      outputDimValues += dimValueBytes * outputIdx;
      setDimValue(inputDimValues, outputDimValues, dimValueBytes);

      // set to the start of next dim value
      inputDimValues += (vectorCapacity - inputIdx) * dimValueBytes;
      outputDimValues += (vectorCapacity - outputIdx) * dimValueBytes;
    }

    // write nulls.
    // Now both inputDimValues and outputDimValues should point to start of
    // null vector.
    for (uint8_t dimIndex = 0; dimIndex < totalDims; dimIndex += 1) {
      inputDimValues += inputIdx;
      outputDimValues += outputIdx;
      *inputDimValues = *outputDimValues;

      // set to the start of next dim null vector
      inputDimValues += vectorCapacity - inputIdx;
      outputDimValues += vectorCapacity - outputIdx;
    }
  }

  __host__ __device__
  void operator(const int i) const {
    if (i < map_size) {
      key_type current_key = mapValues[i].first;
      if (current_key != unused_key) {
        const uint32_t outputIdx = ares::atomicAdd(global_write_index, 1);
        copy_dim_values(current_key.second, outputIdx)
        value_type value = hashtabl_values[i].second;
        reinterpret_cast<value_type *>(measureValues)[outputIdx] = value;
      }
    }
  }
};

// HashReductionContext wraps the concurrent_unordered_map for hash reduction.
// The key of the hash map is a pair of <hash_value, index>, the hash_value is
// pre-calculated during insertion because we use the same hash value for
// equality check as well when collision happens. Therefore the hash function
// of the hash map will be just extract the 1st item of the pair.
// value_type of the hash map is just the measure value. Therefore we can use atomic
// functions for most aggregate functions.
// After the reduction part is done, we need to output the result in the format
// of <index, aggregated_values>. Notes we don't need to output hash values.
template<typename value_type, typename agg_func, value_type identity_value>
class HashReductionContext {
  using hash_val_type = uint64_t;
  using index_type = int64_t;
  using key_type = thrust::pair<hash_val_type, index_type>;
  using hasher_type = std::function<hash_value_type(key_type)>;
  using equality_type = std::function<bool(key_type, key_type)>;

  // TODO(lucafuji): using allocator according to memory type.
  // We are using default_allocator for now.
  using map_type =
  concurrent_unordered_multimap<key_type,
                                value_type,
                                hasher_type,
                                equality_type>;
 private:
  std::unique_ptr<map_type> map;
  agg_func f;

 public:
  explicit HashReductionContext(uint32_t length) {
    constexpr int init_hash_map_size_divider = 1000;
    auto hasher = [](key_type key) {
      return static_cast<hash_value_type>(thrust::get<0>(key));
    };

    auto equality = [](
        key_type lhs, key_type
    rhs) {
      return thrust::get<0>(lhs) == thrust::get<0>(rhs);
    };

    map = std::unique_ptr(
        map_type(length / init_hash_map_size_divider,
                 identity_value,
                 thrust::make_pair(0, -1),
                 hasher,
                 equality)));
    f = agg_func();
  }

  // reduce reduces dimension value with same hash key into a single element
  // using corresponding aggregation function. Note the key is actually
  // < hash_value, index> pair but the equability check is only on
  // hash_value. Therefore the first index paired with the hash value will
  // be the final output value.
  int reduce(DimensionVector inputKeys, uint8_t *inputMeasureValues,
             int length, cudaStream_t cudaStream) {
    DimensionHashIterator hashIter(inputKeys.DimValues,
                                   inputKeys.IndexVector,
                                   keys.NumDimsPerDimWidth,
                                   keys.VectorCapacity);
    auto mapKeyIter = thrust::make_zip_iterator(
        thrust::make_tuple(hashIter, inputKeys.IndexVector));

    auto mapKeyValuePairIter = thrust::make_zip_iterator(
        thrust::make_tuple(
            mapKeyIter,
            reinterpret_cast<value_type *>(inputMeasureValues))
    );

    auto insertionFunc = [](
        thrust::pair<key_type, value_type> key_value_pair) {
        map.insert(key_value_pair, f);
    };

    return thrust::for_each_n(GET_EXECUTION_POLICY(cudaStream), length,
                         mapKeyValuePairIter, insertionFunc);
  }

  int output(DimensionVector inputKeys,
             DimensionVector outputKeys, uint8_t *outputValue,
             cudaStream_t cudaStream) {
    // calculate prefix sum of NumDimsPerDimWidth so that it will be easier
    // to tell the valueBytes given a dim index. The prefix sum is inclusive.
    uint8_t dimWidthPrefixSum[NUM_DIM_WIDTH];
    for (int i = 0; i < NUM_DIM_WIDTH; i++) {
      dimWidthPrefixSum[i] = inputKeys.NumDimsPerDimWidth[i];
      if (i > 0) {
        dimWidthPrefixSum[i] += dimWidthPrefixSum[i - 1];
      }
    }

    uint32_t * globalWriteIndexDevice = nullptr;
    ares::deviceMalloc(reinterpret_cast<void **>(
                 &globalWriteIndexDevice), sizeof(uint32_t));
    ares::deviceMemset(globalWriteIndexDevice, 0, sizeof(uint32_t));

    ExtractGroupByResultFunctor<key_type, value_type>(
        map->get_unused_key(),
        map->data(),
        map->capacity(),
        inputKeys.DimValues,
        inputKeys.VectorCapacity,
        outputKeys.DimValues,
        dimWidthPrefixSum,
        outputValue,
        globalWriteIndexDevice) f;

    thrust::for_each_n(GET_EXECUTION_POLICY(cudaStream),
                              thrust::counting_iterator<uint32_t>(0),
                              map->capacity(), f);

    // copy the reduced count back from GPU.
    uint32_t globalWriteIndexHost;
    ares::asyncCopyHostToDevice(
        reinterpret_cast<void *>(globalWriteIndexDevice),
        reinterpret_cast<void *>(&globalWriteIndexHost),
        sizeof(uint32_t), stream);

    return globalWriteIndexHost;
  }
};

// hashReduceInternal reduces the dimension value using the aggregation
// function into a concurrent hash map. After reduction it will extract
// the dimension and measure values from hash map to corresponding
// dimension vector and value vector.
template<typename value_type, typename agg_func_type, value_type identity>
int hashReduceInternal(DimensionVector inputKeys, uint8_t *inputValues,
                       DimensionVector outputKeys, uint8_t *outputValues,
                       int length, cudaStream_t cudaStream) {
  HashReductionContext<value_type, agg_func_type, identity> context;
  context.reduce(inputKeys, inputValues, length, cudaStream);
  return context.output(inputKeys, outputKeys, outputValues, cudaStream);
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
          value_type, agg_func_type, get_identity_value<value_type>(aggFunc)>( \
            inputKeys, \
            inputValues, \
            outputKeys, \
            outputValues, \
            length, \
            cudaStream);
    case AGGR_SUM_UNSIGNED:
      if (valueBytes == 4) {
        REDUCE_INTERNAL(uint32_t, sum_op < uint32_t >)
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
    default:throw std::invalid_argument("Unsupported aggregation function type");
  }
  return outputLength;
}

}