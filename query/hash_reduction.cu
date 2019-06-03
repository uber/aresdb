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

// HashReductionMap wraps the concurrent_unordered_map for hash reduction.
// The key of the hash map is a pair of <hash_value, index>, the hash_value is
// pre-calculated during insertion because we use the same hash value for
// equality check as well when collision happens. Therefore the hash function
// of the hash map will be just extract the 1st item of the pair.
// Value of the hash map is just the measure value. Therefore we can use atomic
// functions for most aggregate functions.
// After the reduction part is done, we need to output the result in the format of
// <index, aggregated_values>. Notes we don't need to output hash values.
template<typename value_type, typename agg_func, value_type identity_value>
class HashReductionMap {
  using hash_val_type = uint64_t;
  using index_type = int64_t;
  using key_type = thrust::pair<hash_val_type, index_type>;
  using hasher_type = std::function<hash_value_type(key_type)>;
  using equality_type = std::function<bool(key_type, key_type)>;

  using map_type =
  concurrent_unordered_multimap<key_type,
                                value_type,
                                hasher_type,
                                equality_type>;
 private:
  std::unique_ptr<map_type> map;
  agg_func f;

 public:
  HashReductionMap(uint32_t length) {
    auto hasher = [](key_type key) {
      return static_cast<hash_value_type>(thrust::get<0>(key));
    };

    auto equality = [](
        key_type lhs, key_type
    rhs) {
      return thrust::get<0>(lhs) == thrust::get<0>(rhs);
    };

    map = std::move(
        map_type(length / 1000,
                 identity_value,
                 thrust::make_pair(0, -1),
                 hasher,
                 equality)));
    f = agg_func();
  }

  int reduce();
  int output();
};



// hashReduceInternal reduces the dimension value and  
template<typename Value, typename AggFunc>
int hashReduceInternal(uint32_t *inputIndexVector,
                   uint8_t *inputValues, uint32_t *outputIndexVector,
                   uint8_t *outputValues,
                   int length, cudaStream_t cudaStream) {

}

int bindHashValueAndAggFunc(uint32_t *inputIndexVector,
                        uint8_t *inputValues,
                        uint32_t *outputIndexVector,
                        uint8_t *outputValues,
                        int valueBytes,
                        int length,
                        AggregateFunction aggFunc,
                        cudaStream_t cudaStream) {
  switch (aggFunc) {
    #define REDUCE_INTERNAL(ValueType, AggFunc) \
      return hashReduceInternal< ValueType, AggFunc >( \
            inputIndexVector, \
            inputValues, \
            outputIndexVector, \
            outputValues, \
            length, \
            cudaStream);

    case AGGR_SUM_UNSIGNED:
      if (valueBytes == 4) {
        REDUCE_INTERNAL(uint32_t, sum_op<uint32_t>)
      } else {
        REDUCE_INTERNAL(uint64_t, sum_op<uint64_t>)
      }
    case AGGR_SUM_SIGNED:
      if (valueBytes == 4) {
        REDUCE_INTERNAL(int32_t, sum_op<int32_t>)
      } else {
        REDUCE_INTERNAL(int64_t, sum_op<int64_t>)
      }
    case AGGR_SUM_FLOAT:
      if (valueBytes == 4) {
        REDUCE_INTERNAL(float_t, sum_op<float_t>)
      } else {
        REDUCE_INTERNAL(double_t, sum_op<double_t>)
      }
    case AGGR_MIN_UNSIGNED:
      REDUCE_INTERNAL(uint32_t, min_op<uint32_t>)
    case AGGR_MIN_SIGNED:
      REDUCE_INTERNAL(int32_t, min_op<int32_t>)
    case AGGR_MIN_FLOAT:
      REDUCE_INTERNAL(float_t, min_op<float_t>)
    case AGGR_MAX_UNSIGNED:
      REDUCE_INTERNAL(uint32_t, max_op<uint32_t>)
    case AGGR_MAX_SIGNED:
      REDUCE_INTERNAL(int32_t, max_op<int32_t>)
    case AGGR_MAX_FLOAT:
      REDUCE_INTERNAL(float_t, max_op<float_t>)
    case AGGR_AVG_FLOAT:
      REDUCE_INTERNAL(uint64_t, RollingAvgFunctor)
    default:
      throw std::invalid_argument("Unsupported aggregation function type");
  }
}

int hash_reduction(DimensionVector inputKeys, uint8_t *inputValues,
                   DimensionVector outputKeys, uint8_t *outputValues,
                   int valueBytes, int length, AggregateFunction aggFunc,
                   cudaStream_t cudaStream) {
  int outputLength = bindValueAndAggFunc(
      inputKeys.IndexVector, inputValues, outputKeys.IndexVector, outputValues,
      valueBytes, length, aggFunc, cudaStream);

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
  thrust::copy(GET_EXECUTION_POLICY(cudaStream),
               iterIn, iterIn + numDims * 2 * outputLength, iterOut);
  return outputLength;
}

}