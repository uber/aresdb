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

#ifndef QUERY_ITERATOR_HPP_
#define QUERY_ITERATOR_HPP_

#include <cuda_runtime.h>
#include <thrust/detail/internal_functional.h>
#include <thrust/iterator/constant_iterator.h>
#include <thrust/iterator/iterator_adaptor.h>
#include <thrust/iterator/permutation_iterator.h>
#include <thrust/iterator/zip_iterator.h>
#include <thrust/tuple.h>
#include <thrust/iterator/detail/normal_iterator.h>
#include <cfloat>
#include <cmath>
#include <tuple>
#include "query/time_series_aggregate.h"
#include "query/utils.hpp"

namespace ares {

// VectorPartyIterator iterates <value,validity> tuples against a column
// (vector parties) under 3 modes:
// 1: all values are present
// 2: uncompressed
// 3: compressed
// based on the presences of 3 vectors (values, nulls, counts).
// Only 4 type of values vector is supported here:
// Uint32, Int32, Float32 and Bool. Bool vector is packed into 1 bit.
// More details
// can be found here:
// https://github.com/uber/aresdb/wiki/VectorStore
//
// This struct aims at reduce overall struct size so that we will use as few
// registers per thread as possible when launching kernels.
// We made following efforts to reduce overall size:
// 1. Put in a single struct to avoid too much padding.
// 2. Passing values pointers only and passing nulls and counts as offset to
// value pointers. When allocating device space, we will store values, nulls
// and counts consecutively.
// 3. Base counts and start count will share the same memory address.
//
// Mode 0 vp is modeled as constant vector so value pointer always presents.
// Length is only used by compressed column.
//
// The mode of the column is judged by following logic:
// If valuesOffset == 0, it means it’s a mode 1 vector.
// Otherwise if nullsOffset, it’s a mode 2 vector.
// Otherwise it’s a mode 3 vector.
template <typename Value>
class VectorPartyIterator
    : public thrust::iterator_adaptor<
          VectorPartyIterator<Value>, uint32_t *, thrust::tuple<Value, bool>,
          thrust::use_default, thrust::use_default, thrust::tuple<Value, bool>,
          thrust::use_default> {
 public:
  friend class thrust::iterator_core_access;

  typedef thrust::iterator_adaptor<VectorPartyIterator<Value>,
                                   uint32_t *, thrust::tuple<Value, bool>,
                                   thrust::use_default, thrust::use_default,
                                   thrust::tuple<Value, bool>,
                                   thrust::use_default> super_t;

  __host__ __device__ VectorPartyIterator() {}

  // base is counts vector if mode 0. nulls vector if mode 2, values vector
  // if mode 0.
  __host__ __device__
  VectorPartyIterator(
      uint32_t *baseCounts,
      const uint32_t startCount,
      uint8_t *basePtr,
      uint32_t nullsOffset,
      uint32_t valuesOffset,
      uint32_t length,
      uint8_t stepInBytes,
      uint8_t nullBitOffset)
      : super_t(baseCounts != nullptr
                ? baseCounts : reinterpret_cast<uint32_t *>(startCount)),
        basePtr(basePtr),
        nullsOffset(nullsOffset),
        valuesOffset(valuesOffset),
        length(length),
        currentPtrIndex(0),
        stepInBytes(stepInBytes),
        nullBitOffset(nullBitOffset) {
    mode = judge_mode();
    hasBaseCounts = baseCounts != nullptr;
  }

 private:
  uint8_t *basePtr;
  uint32_t nullsOffset;  // Offset of nullsPtr.
  uint32_t valuesOffset;  // Offset of basePtr.
  uint32_t currentPtrIndex;
  uint32_t length;
  uint8_t stepInBytes;
  uint8_t hasBaseCounts;  // Whether the base iterator is a base count vector or
  // a start count.
  uint8_t nullBitOffset;  // Starting bit offset in null vector.
  uint8_t mode;  // For convenience we store here as this does not add to the
  // final space of this iterator.

  __host__ __device__
  uint8_t judge_mode() const {
    if (valuesOffset == 0) {
      return 1;
    }
    if (nullsOffset == 0) {
      return 2;
    }
    return 3;
  }

  // offset must be largert than 0, otherwise it may lead to access to undesired
  // location.
  __host__ __device__
  bool get_null() const {
    if (mode >= 2) {
      return get_bool(basePtr + nullsOffset);
    }
    return true;
  }

  __host__ __device__
  bool get_bool(uint8_t *ptr) const {
    uint32_t nullIndex = currentPtrIndex + nullBitOffset;
    return ptr[nullIndex / 8] & (1 << nullIndex % 8);
  }

  // overloaded functions for different value types.
  __host__ __device__
  uint32_t get_value(uint32_t *values) const {
    uint8_t *ptr =
        reinterpret_cast<uint8_t *>(values) + currentPtrIndex * stepInBytes;
    switch (stepInBytes) {
      case 2:return *reinterpret_cast<uint16_t *>(ptr);
      case 4:return *reinterpret_cast<uint32_t *>(ptr);
      default:return *ptr;
    }
  }

  __host__ __device__
  int32_t get_value(int32_t *values) const {
    int8_t *ptr =
        reinterpret_cast<int8_t *>(values) + currentPtrIndex * stepInBytes;
    switch (stepInBytes) {
      case 2:return *reinterpret_cast<int16_t *>(ptr);
      case 4:return *reinterpret_cast<int32_t *>(ptr);
      default:return *ptr;
    }
  }

  __host__ __device__
  float_t get_value(float_t *values) const {
    return values[currentPtrIndex];
  }

  __host__ __device__
  GeoPointT get_value(GeoPointT *values) const {
    return values[currentPtrIndex];
  }

  __host__ __device__
  int64_t get_value(int64_t *values) const {
    return values[currentPtrIndex];
  }

  __host__ __device__
  UUIDT get_value(UUIDT *values) const {
    return values[currentPtrIndex];
  }

  __host__ __device__
  bool get_value(bool *values) const {
    return get_bool(reinterpret_cast<uint8_t *>(values));
  }

  __host__ __device__
  Value get_value() const {
    return get_value(reinterpret_cast<Value *>(
                         basePtr + valuesOffset));
  }

  // Should be called only if it's a mode 3 vector.
  __host__ __device__
  uint32_t get_count(uint32_t index) const {
    return reinterpret_cast<uint32_t *>(basePtr)[currentPtrIndex + index];
  }

  __host__ __device__
  thrust::tuple<Value, bool> dereference() const {
    return thrust::make_tuple(get_value(),
                              get_null());
  }

  // For compressed vp, moving strategy is like following steps:
  //  1. if the offset is less than next count, we will not move the iterator.
  //  2. if the offset is less than the warp size(32 or 64), we will do a
  // linear search.
  //  3. otherwise, we will do a binary search to go to the correct position.
  // It works best if n is larger than 0 since we will do quick check and
  // linear check, otherwise we will do binary search for negative n.
  __host__ __device__
  void advance(typename super_t::difference_type n) {
    if (hasBaseCounts) {
      this->base_reference() += n;
    } else {
      this->base_reference() = reinterpret_cast<uint32_t *>(
          reinterpret_cast<uintptr_t >(this->base_reference()) + n);
    }

    if (mode == 3) {
      uint32_t newIndex;
      if (hasBaseCounts) {
        newIndex = *this->base_reference();
      } else {
        newIndex = reinterpret_cast<uintptr_t>(this->base_reference());
      }

      // quick check to avoid binary search
      if (newIndex >= get_count(0)
          && newIndex < get_count(1)) {
        return;
      }

      // do linear search if n is less than or equal to wrap size.
      if (n <= WARP_SIZE && n >= 0) {
        for (int i = 1; i < length - currentPtrIndex; i++) {
          if (newIndex >= get_count(i)
              && newIndex < get_count(i + 1)) {
            currentPtrIndex += i;
            return;
          }
        }
      }

      uint32_t first = currentPtrIndex;
      uint32_t last = length;

      if (n < 0) {
        first = 0;
        last = currentPtrIndex;
      }

      // this algorithm computes upper_bound - 1 of [first, last),
      // which is the last element in the [first, last) that is
      // smaller or equal to the given index
      while (first < last) {
        uint32_t mid = first + (last - first) / 2;
        if (get_count(mid) > newIndex) {
          last = mid;
        } else {
          first = mid + 1;
        }
      }
      currentPtrIndex = first - 1;
    } else {
      // If this column is not compressed, this means the counting
      // iterator is simply thrust counting iterator since base count
      // column should be also uncompressed as well. We can safely
      // advance our zip iterator with n;
      currentPtrIndex += n;
    }
  }

  __host__ __device__
  void increment() {
    advance(1);
  }

  __host__ __device__
  void decrement() {
    advance(-1);
  }
};

template <>
class VectorPartyIterator<GeoPointT>
    : public thrust::iterator_adaptor<
          VectorPartyIterator<GeoPointT>, uint32_t *,
          thrust::tuple<GeoPointT, bool>, thrust::use_default,
          thrust::use_default, thrust::tuple<GeoPointT, bool>,
          thrust::use_default> {
 public:
  friend class thrust::iterator_core_access;

  typedef thrust::iterator_adaptor<
      VectorPartyIterator<GeoPointT>, uint32_t *,
      thrust::tuple<GeoPointT, bool>, thrust::use_default, thrust::use_default,
      thrust::tuple<GeoPointT, bool>, thrust::use_default>
      super_t;

  __host__ __device__ VectorPartyIterator() {}

  __host__ __device__ VectorPartyIterator(
      uint32_t *baseCounts, const uint32_t startCount, uint8_t *basePtr,
      uint32_t nullsOffset, uint32_t valuesOffset, uint32_t length,
      uint8_t stepInBytes, uint8_t nullBitOffset)
      : super_t(reinterpret_cast<uint32_t *>(startCount)),
        basePtr(basePtr),
        valuesOffset(valuesOffset),
        currentPtrIndex(0),
        nullBitOffset(nullBitOffset) {
  }

 private:
  uint8_t *basePtr;
  uint32_t currentPtrIndex;
  uint32_t valuesOffset;
  uint8_t nullBitOffset;

  __host__ __device__ bool get_null() const {
    // mode 1
    if (valuesOffset == 0) {
      return true;
    }
    return get_bool(basePtr);
  }

  __host__ __device__ GeoPointT get_value() const {
    return reinterpret_cast<GeoPointT *>(basePtr+valuesOffset)[currentPtrIndex];
  }

  __host__ __device__ bool get_bool(uint8_t *ptr) const {
    uint32_t nullIndex = currentPtrIndex + nullBitOffset;
    return ptr[nullIndex / 8] & (1 << nullIndex % 8);
  }

  __host__ __device__ thrust::tuple<GeoPointT, bool> dereference() const {
    return thrust::make_tuple(get_value(), get_null());
  }

  __host__ __device__ void advance(typename super_t::difference_type n) {
    this->base_reference() = reinterpret_cast<uint32_t *>(
        reinterpret_cast<uintptr_t>(this->base_reference()) + n);
    currentPtrIndex += n;
  }

  __host__ __device__ void increment() { advance(1); }

  __host__ __device__ void decrement() { advance(-1); }
};

template<typename Value>
using ColumnIterator =
thrust::permutation_iterator<VectorPartyIterator<Value>, uint32_t *>;

// Helper function for creating VectorPartyIterator without specifying
// Value template argument.
template <class Value>
ColumnIterator<Value> make_column_iterator(
    uint32_t *indexVector, uint32_t *baseCounts, uint32_t startCount,
    uint8_t *basePtr, uint32_t nullsOffset, uint32_t valuesOffset,
    uint32_t length, uint8_t stepInBytes, uint8_t nullBitOffset) {
  return thrust::make_permutation_iterator(
      VectorPartyIterator<Value>(baseCounts, startCount, basePtr, nullsOffset,
                                 valuesOffset, length, stepInBytes,
                                 nullBitOffset),
      indexVector);
}

// SimpleIterator combines interators in 4 different cases:
// 1. Constant value.
// 2. Mode 0 column, equivalent to constant value.
// 3. Scratch space.
//
// We will use a value pointer and null offset to represent values
// and nulls. If values or nulls are missing, we will reuse the space
// to store constant values.
// Only 4 Value types are supported: Uint32, Int32, Float32 and Bool.
// Each bool value take 1 byte.
// We will use the first 4 bytes of the value pointer to store default value
// and rest 4 bytes as the position of the iterator.
template<typename Value>
class SimpleIterator :
    public thrust::iterator_adaptor<SimpleIterator<Value>,
                                    Value *, thrust::tuple<Value, bool>,
                                    thrust::use_default, thrust::use_default,
                                    thrust::tuple<Value, bool>,
                                    thrust::use_default> {
 public:
  friend class thrust::iterator_core_access;

  typedef thrust::iterator_adaptor<SimpleIterator<Value>,
                                   Value *, thrust::tuple<Value, bool>,
                                   thrust::use_default, thrust::use_default,
                                   thrust::tuple<Value, bool>,
                                   thrust::use_default> super_t;

  __host__ __device__ SimpleIterator() {}

  SimpleIterator(
      Value *values,
      uint32_t nullsOffset,
      Value defaultValue,
      bool defaultNull
  )
      : super_t(values != nullptr
                ? values : reinterpret_cast<Value *>(
                    *reinterpret_cast<uintptr_t *>(&defaultValue) << 32)),
        useDefault(values == nullptr) {
    if (useDefault) {
      nulls.defaultNull = defaultNull;
    } else {
      nulls.nullsOffset = nullsOffset;
    }
  }

 private:
  union {
    uint32_t nullsOffset;
    bool defaultNull;
  } nulls;
  bool useDefault;

  __host__ __device__
  thrust::tuple<Value, bool> dereference() const {
    if (useDefault) {
      uintptr_t val = reinterpret_cast<uintptr_t>(this->base_reference()) >> 32;
      return thrust::make_tuple(*reinterpret_cast<Value *>(&val),
                                nulls.defaultNull);
    }
    return thrust::make_tuple(*this->base_reference(),
                              *(reinterpret_cast<bool *>(this->base_reference())
                                  + nulls.nullsOffset));
  }

  __host__ __device__
  void advance(typename super_t::difference_type n) {
    this->base_reference() += n;
    if (!useDefault) {
      nulls.nullsOffset += (1 - sizeof(Value)) * n;
    }
  }

  __host__ __device__
  void increment() {
    advance(1);
  }

  __host__ __device__
  void decrement() {
    advance(-1);
  }
};

template <typename Value>
struct ConstantIterator {
  typedef typename std::conditional<
      std::is_same<Value, UUIDT>::value || std::is_same<Value, int64_t>::value,
      thrust::constant_iterator<thrust::tuple<Value, bool>>,
      SimpleIterator<Value>>::type type;
};

template <typename Value>
inline typename ConstantIterator<Value>::type make_constant_iterator(
    Value defaultValue, bool defaultNull) {
  return SimpleIterator<Value>(nullptr, 0, defaultValue, defaultNull);
}

template <>
inline typename ConstantIterator<int64_t>::type make_constant_iterator(
    int64_t defaultValue, bool defaultNull) {
  return thrust::make_constant_iterator(
      thrust::make_tuple<int64_t, bool>(defaultValue, defaultNull));
}

template <>
inline typename ConstantIterator<UUIDT>::type make_constant_iterator(
    UUIDT defaultValue, bool defaultNull) {
  return thrust::make_constant_iterator(
      thrust::make_tuple<UUIDT, bool>(defaultValue, defaultNull));
}

using GeoSimpleIterator = thrust::constant_iterator<thrust::tuple<GeoPointT,
                                                                  bool>>;

// DimensionOutputIterator is for writing individual dimension transform
// results into a consecutive chunk of memory which later will be used
// as the key for sort and reduce.
template<typename Value>
using DimensionOutputIterator = thrust::zip_iterator<
    thrust::tuple<thrust::detail::normal_iterator<Value *>,
                  thrust::detail::normal_iterator<bool *> > >;

template<typename Value>
DimensionOutputIterator<Value> make_dimension_output_iterator(
    uint8_t *dimValues, uint8_t *dimNulls) {
  return thrust::make_zip_iterator(
      thrust::make_tuple(reinterpret_cast<Value *>(dimValues),
                         reinterpret_cast<bool *>(dimNulls)));
}

template<typename Value>
SimpleIterator<Value> make_scratch_space_input_iterator(
    uint8_t *valueIter, uint32_t nullOffset) {
  return SimpleIterator<Value>(reinterpret_cast<Value *>(valueIter),
                               nullOffset, 0, 0);
}

template<typename Value>
using ScratchSpaceOutputIterator = thrust::zip_iterator<
    thrust::tuple<
        thrust::detail::normal_iterator<Value *>,
        thrust::detail::normal_iterator<bool *> > >;

// Helper function for creating ScratchSpaceIterator without specifying
// Value template argument.
template<typename Value>
ScratchSpaceOutputIterator<Value> make_scratch_space_output_iterator(
    Value *v, uint32_t nullOffset) {
  return thrust::make_zip_iterator(
      thrust::make_tuple(v, reinterpret_cast<bool *>(v) + nullOffset));
}

template<typename Value>
class MeasureProxy {
 public:
  __host__ __device__
  MeasureProxy(Value *outputIter, Value identity, uint32_t count, bool isAvg)
      : outputIter(outputIter),
        count(count),
        identity(identity), isAvg(isAvg) {
  }

  __host__ __device__
  MeasureProxy operator=(thrust::tuple<Value, bool> t) const {
    if (!thrust::get<1>(t)) {
      *outputIter = identity;
    } else if (isAvg) {
      return assignAvg(t);
    } else {
      *outputIter = thrust::get<0>(t) * count;
    }
    return *this;
  }

  __host__ __device__
  MeasureProxy assignAvg(thrust::tuple<Value, bool> t) const {
    // Each operand is 64bits where higher 32bits are used for count and
    // lower bits are for intermediate average.
    float_t v = thrust::get<0>(t);
    *reinterpret_cast<float_t *>(outputIter) = v;
    *(reinterpret_cast<uint32_t *>(outputIter) + 1) = count;
    return *this;
  }

 private:
  Value *outputIter;
  Value identity;
  uint32_t count;
  bool isAvg;
};

// MeasureOutputIterator is the output iterator for the final step of measure
// transformation and as the input for aggregation. It needs to take care
// of 2 things:
//  1. Given a <value, validity> tuple, write to a single value vector while
// write null value as the identity value for different aggregation function
//  2. If base column is a compressed column, also need to recover the original
// value instead of the compressed value. e.g. if we have a value 3 with count 3
// and the aggregation function is sum. We need to write 3 * 3 to measure
// output iterator.
template<typename Value>
class MeasureOutputIterator : public thrust::iterator_adaptor<
    MeasureOutputIterator<Value>, Value *, thrust::tuple<Value, bool>,
    thrust::use_default, thrust::use_default, MeasureProxy<Value>,
    thrust::use_default> {
 public:
  friend class thrust::iterator_core_access;

  // shorthand for the name of the iterator_adaptor we're deriving from.
  typedef thrust::iterator_adaptor<MeasureOutputIterator<Value>,
                                   Value *,
                                   thrust::tuple<Value, bool>,
                                   thrust::use_default,
                                   thrust::use_default,
                                   MeasureProxy<Value>,
                                   thrust::use_default> super_t;

  __host__ __device__
  MeasureOutputIterator(Value *base, const uint32_t *baseCounts,
                        const uint32_t *indexVector,
                        AggregateFunction aggFunc)
      : super_t(base),
        baseCounts(baseCounts),
        indexVector(indexVector) {
    // We don't need to do anything for this value if it's not sum.
    if (!((aggFunc >= AGGR_SUM_UNSIGNED && aggFunc <= AGGR_SUM_FLOAT)
        || (aggFunc == AGGR_AVG_FLOAT))) {
      skipCount = true;
    }
    isAvg = aggFunc == AGGR_AVG_FLOAT;
    identity = get_identity_value<Value>(aggFunc);
  }

 private:
  const uint32_t *baseCounts;
  const uint32_t *indexVector;
  Value identity;
  bool skipCount = false;
  bool isAvg = false;

  __host__ __device__
  typename super_t::reference dereference() const {
    uint32_t index = *indexVector;
    uint32_t count = !skipCount && baseCounts != nullptr ? baseCounts[index + 1]
        - baseCounts[index] : 1;
    return MeasureProxy<Value>(this->base_reference(), identity, count, isAvg);
  }

  __host__ __device__
  void advance(typename super_t::difference_type n) {
    this->base_reference() += n;
    indexVector += n;
  }

  __host__ __device__
  void increment() {
    advance(1);
  }

  __host__ __device__
  void decrement() {
    advance(-1);
  }
};

// Helper function for creating MeasureOutputIterator without specifying
// Value template argument.
template<typename Value>
MeasureOutputIterator<Value> make_measure_output_iterator(
    Value *v, uint32_t *indexVector, uint32_t *baseCounts,
    AggregateFunction aggFunc) {
  return MeasureOutputIterator<Value>(v, baseCounts,
                                      indexVector, aggFunc);
}

class IndexProxy {
 public:
  __host__ __device__
  explicit IndexProxy(uint32_t *outputIndex)
      : outputIndex(outputIndex) {
  }

  // Parameter is a tuple of <index row number, index value>.
  __host__ __device__
  IndexProxy operator=(thrust::tuple<uint32_t, uint32_t> t) const {
    *outputIndex = thrust::get<1>(t);
    return *this;
  }

 private:
  uint32_t *outputIndex;
};

// IndexOutputIterator is the output iterator for writing filtered index
// to. The input will be a tuple of <index row number, index value>. We
// will discard the index row number and only write the filtered index value
// into the output index value vector which is a uint32_t pointer.
class IndexOutputIterator :
    public thrust::iterator_adaptor<IndexOutputIterator,
                                    uint32_t *,
                                    thrust::tuple<
                                        uint32_t,
                                        uint32_t>,
                                    thrust::use_default,
                                    thrust::use_default,
                                    IndexProxy,
                                    thrust::use_default> {
 public:
  friend class thrust::iterator_core_access;

  // shorthand for the name of the iterator_adaptor we're deriving from.
  typedef thrust::iterator_adaptor<IndexOutputIterator,
                                   uint32_t *,
                                   thrust::tuple<uint32_t, uint32_t>,
                                   thrust::use_default,
                                   thrust::use_default,
                                   IndexProxy,
                                   thrust::use_default> super_t;

  __host__ __device__
  explicit IndexOutputIterator(uint32_t *base)
      : super_t(base) {
  }

 private:
  __host__ __device__
  typename super_t::reference dereference() const {
    return IndexProxy(this->base_reference());
  }
};

// The column mode for foreign tables are either 0 or 2, which is
// presented by two different type of iterators: SimpleIterator and
// VectorPartyIterator. And the mode of batch columns will be interleaving.
// To make all iterators have the same size, we put both cases into a
// single union and has a boolean flag to tell which iterator it is.
template <typename Value>
struct ForeignTableIterator {
  __host__ __device__ ForeignTableIterator() {}

  __host__ __device__
  ForeignTableIterator(const ForeignTableIterator<Value> &other) {
    this->isConst = other.isConst;
    if (this->isConst) {
      this->iter.constantIter = other.iter.constantIter;
    } else {
      this->iter.columnIter = other.iter.columnIter;
    }
  }

  __host__ __device__ ForeignTableIterator &operator=(
      const ForeignTableIterator<Value> &other) {
    this->isConst = other.isConst;
    if (this->isConst) {
      this->iter.constantIter = other.iter.constantIter;
    } else {
      this->iter.columnIter = other.iter.columnIter;
    }
    return *this;
  }

  explicit ForeignTableIterator(VectorPartyIterator<Value> columnIter)
      : isConst(false) {
    iter.columnIter = columnIter;
  }

  explicit ForeignTableIterator(
      typename ConstantIterator<Value>::type constantIter)
      : isConst(true) {
    iter.constantIter = constantIter;
  }

  union ForeignBatchIter {
    VectorPartyIterator<Value> columnIter;
    typename ConstantIterator<Value>::type constantIter;
    __host__ __device__ ForeignBatchIter() {}
  } iter;
  bool isConst;
};

// RecordIDJoinIterator reads the foreign table column.
template<typename Value>
class RecordIDJoinIterator
    : public thrust::iterator_adaptor<
        RecordIDJoinIterator<Value>, RecordID *,
        thrust::tuple<Value, bool>, thrust::use_default, thrust::use_default,
        thrust::tuple<Value, bool>,
        thrust::use_default> {
 public:
  friend class thrust::iterator_core_access;

  typedef ForeignTableIterator<Value> ValueIter;

  typedef thrust::iterator_adaptor<
      RecordIDJoinIterator<Value>, RecordID *,
      thrust::tuple<Value, bool>, thrust::use_default, thrust::use_default,
      thrust::tuple<Value, bool>, thrust::use_default>
      super_t;

  __host__ __device__ RecordIDJoinIterator(
      RecordID *base,
      int32_t numBatches,
      int32_t baseBatchID,
      ValueIter *batches,
      int32_t numRecordsInLastBatch,
      int16_t *timezoneLookupTable,
      int16_t timezoneLookupSize)
      : super_t(base),
        batches(batches),
        baseBatchID(baseBatchID),
        numBatches(numBatches),
        numRecordsInLastBatch(numRecordsInLastBatch),
        timezoneLookupTable(timezoneLookupTable),
        timezoneLookupSize(timezoneLookupSize) {
  }

 private:
  ValueIter *batches;
  int32_t baseBatchID;
  int32_t numBatches;
  int32_t numRecordsInLastBatch;
  int16_t *timezoneLookupTable;
  int16_t timezoneLookupSize;

  __host__ __device__
  thrust::tuple<GeoPointT, bool> timezoneLookup(
      thrust::tuple<GeoPointT, bool> val) const {
    return val;
  }

  __host__ __device__ thrust::tuple<UUIDT, bool> timezoneLookup(
      thrust::tuple<UUIDT, bool> val) const {
    return val;
  }

  template<typename V>
  __host__ __device__
  thrust::tuple<V, bool> timezoneLookup(
      thrust::tuple<V, bool> val) const {
    if (timezoneLookupTable) {
      // if timezoneLookup is not null, we must be iterating a enum column
      int enumVal = static_cast<int>(thrust::get<0>(val));
      if (enumVal < timezoneLookupSize) {
        return thrust::make_tuple(
            timezoneLookupTable[enumVal], thrust::get<1>(val));
      } else {
        return thrust::make_tuple(0, thrust::get<1>(val));
      }
    }
    return val;
  }

  __host__ __device__ typename super_t::reference dereference() const {
    RecordID recordID = *this->base_reference();
    if (recordID.batchID && (recordID.batchID - baseBatchID < numBatches - 1 ||
        recordID.index < numRecordsInLastBatch)) {
      ForeignTableIterator<Value>
          iter = batches[recordID.batchID - baseBatchID];
      if (iter.isConst) {
        return iter.iter.constantIter[recordID.index];
      }
      thrust::tuple<Value, bool> val = iter.iter.columnIter[recordID.index];
      return timezoneLookup(val);
    }
    Value defaultValue;
    return thrust::make_tuple(defaultValue, false);
  }
};

// DimensionHashIterator reads DimensionColumnVector and produce hashed values
class DimensionHashIterator
    : public thrust::iterator_adaptor<
        DimensionHashIterator, uint32_t *, uint64_t, thrust::use_default,
        thrust::use_default, uint64_t, thrust::use_default> {
 public:
  friend class thrust::iterator_core_access;
  typedef thrust::iterator_adaptor<DimensionHashIterator, uint32_t *, uint64_t,
                                   thrust::use_default, thrust::use_default,
                                   uint64_t, thrust::use_default>
      super_t;

  __host__ __device__
  DimensionHashIterator(uint8_t *dimValues, uint32_t *indexVector,
                        uint8_t _numDimsPerDimWidth[NUM_DIM_WIDTH], int length)
      : super_t(indexVector), dimValues(dimValues), length(length) {
    totalNumDims = 0;
    rowBytes = 0;
    for (int i = 0; i < NUM_DIM_WIDTH; i++) {
      numDimsPerDimWidth[i] = _numDimsPerDimWidth[i];
      totalNumDims += _numDimsPerDimWidth[i];
      uint8_t dimBytes = 1 << (NUM_DIM_WIDTH - 1 - i);
      rowBytes += dimBytes * numDimsPerDimWidth[i];
    }
    nullValues = dimValues + rowBytes * length;
    // include null values, 1 byte per each dim
    rowBytes += totalNumDims;
  }

 private:
  uint8_t *dimValues;
  uint8_t *nullValues;
  uint8_t numDimsPerDimWidth[NUM_DIM_WIDTH];
  uint8_t totalNumDims;
  uint8_t rowBytes;
  int length;

  __host__ __device__ typename super_t::reference dereference() const {
    uint32_t index = *this->base_reference();
    uint8_t dimRow[MAX_DIMENSION_BYTES] = {0};
    uint64_t hashedOutput[2];
    // read from
    uint8_t *inputValueStart = dimValues;
    uint8_t *inputNullStart = nullValues;
    // write to
    uint8_t *outputValuePtr = dimRow;
    uint8_t *outputNullPtr = outputValuePtr + (rowBytes - totalNumDims);
    uint8_t numDims = 0;
    for (int i = 0; i < NUM_DIM_WIDTH; i++) {
      uint8_t dimBytes = 1 << (NUM_DIM_WIDTH - 1 - i);
      for (int j = numDims; j < numDims + numDimsPerDimWidth[i]; j++) {
        switch (dimBytes) {
          case 16:
            *reinterpret_cast<UUIDT *>(outputValuePtr) =
                reinterpret_cast<UUIDT *>(inputValueStart)[index];
            break;
          case 8:
            *reinterpret_cast<uint64_t *>(outputValuePtr) =
                reinterpret_cast<uint64_t *>(inputValueStart)[index];
            break;
          case 4:
            *reinterpret_cast<uint32_t *>(outputValuePtr) =
                reinterpret_cast<uint32_t *>(inputValueStart)[index];
            break;
          case 2:
            *reinterpret_cast<uint16_t *>(outputValuePtr) =
                reinterpret_cast<uint16_t *>(inputValueStart)[index];
            break;
          case 1:
            *outputValuePtr = inputValueStart[index];
            break;
        }
        outputValuePtr += dimBytes;
        inputValueStart += dimBytes * length;
        *outputNullPtr = inputNullStart[index];
        outputNullPtr++;
        inputNullStart += length;
      }
      numDims += numDimsPerDimWidth[i];
    }
    murmur3sum128(dimRow, rowBytes, 0, hashedOutput);
    // only use the first 64bit of the 128bit hash
    return hashedOutput[0];
  }
};

class DimValueProxy {
 public:
  __host__ __device__ DimValueProxy(uint8_t *ptr, int dimBytes)
      : ptr(ptr), dimBytes(dimBytes) {}

  __host__ __device__ DimValueProxy operator=(DimValueProxy t) {
    switch (dimBytes) {
      case 16:
        *reinterpret_cast<UUIDT *>(ptr) = *reinterpret_cast<UUIDT *>(t.ptr);
      case 8:
        *reinterpret_cast<uint64_t *>(ptr) =
            *reinterpret_cast<uint64_t *>(t.ptr);
      case 4:
        *reinterpret_cast<uint32_t *>(ptr) =
            *reinterpret_cast<uint32_t *>(t.ptr);
      case 2:
        *reinterpret_cast<uint16_t *>(ptr) =
            *reinterpret_cast<uint16_t *>(t.ptr);
      case 1:*ptr = *t.ptr;
    }
    return *this;
  }

 private:
  uint8_t *ptr;
  int dimBytes;
};

class DimensionColumnPermutateIterator
    : public thrust::iterator_adaptor<DimensionColumnPermutateIterator,
                                      uint32_t *, DimValueProxy,
                                      thrust::use_default, thrust::use_default,
                                      DimValueProxy, thrust::use_default> {
 public:
  friend class thrust::iterator_core_access;
  typedef thrust::iterator_adaptor<DimensionColumnPermutateIterator, uint32_t *,
                                   DimValueProxy, thrust::use_default,
                                   thrust::use_default, DimValueProxy,
                                   thrust::use_default>
      super_t;

  __host__ __device__ DimensionColumnPermutateIterator(
      uint8_t *values, uint32_t *indexVector, int dimInputLength,
      int dimOutputLength, uint8_t _numDimsPerDimWidth[NUM_DIM_WIDTH])
      : super_t(indexVector),
        begin(indexVector),
        values(values),
        dimInputLength(dimInputLength),
        dimOutputLength(dimOutputLength) {
    for (int i = 0; i < NUM_DIM_WIDTH; i++) {
      numDimsPerDimWidth[i] = _numDimsPerDimWidth[i];
    }
  }

 private:
  uint8_t *values;
  uint32_t *begin;
  int dimInputLength;
  int dimOutputLength;
  uint8_t numDimsPerDimWidth[NUM_DIM_WIDTH];

  __host__ __device__ typename super_t::reference dereference() const {
    int baseIndex = this->base_reference() - begin;
    int dimIndex = baseIndex / dimOutputLength;
    // index in current dimension vector
    int localIndex = *(begin + (baseIndex % dimOutputLength));
    int bytes = 0;
    uint8_t numDims = 0;
    uint8_t dimBytes = 0;
    int i = 0;
    for (; i < NUM_DIM_WIDTH; i++) {
      dimBytes = 1 << (NUM_DIM_WIDTH - i - 1);
      if (dimIndex < numDims + numDimsPerDimWidth[i]) {
        bytes +=
            ((dimIndex - numDims) * dimInputLength + localIndex) * dimBytes;
        break;
      } else {
        bytes += numDimsPerDimWidth[i] * dimInputLength * dimBytes;
      }
      numDims += numDimsPerDimWidth[i];
    }
    // null vector
    if (i == NUM_DIM_WIDTH) {
      bytes += ((dimIndex - numDims) * dimInputLength + localIndex) * dimBytes;
      return DimValueProxy(values + bytes, dimBytes);
    }
    return DimValueProxy(values + bytes, dimBytes);
  }
};

// DimensionColumnOutputIterator skips certain protion of the vector and proceed
class DimensionColumnOutputIterator
    : public thrust::iterator_adaptor<
        DimensionColumnOutputIterator, thrust::counting_iterator<int>,
        DimValueProxy, thrust::use_default, thrust::use_default,
        DimValueProxy, thrust::use_default> {
 public:
  friend class thrust::iterator_core_access;
  typedef thrust::iterator_adaptor<
      DimensionColumnOutputIterator, thrust::counting_iterator<int>,
      DimValueProxy, thrust::use_default, thrust::use_default, DimValueProxy,
      thrust::use_default>
      super_t;

  __host__ __device__ DimensionColumnOutputIterator(
      uint8_t *values, int dimInputLength, int dimOutputLength,
      uint8_t _numDimsPerDimWidth[NUM_DIM_WIDTH])
      : super_t(thrust::make_counting_iterator<int>(0)),
        values(values),
        dimOutputLength(dimOutputLength),
        dimInputLength(dimInputLength) {
    for (int i = 0; i < NUM_DIM_WIDTH; i++) {
      numDimsPerDimWidth[i] = _numDimsPerDimWidth[i];
    }
  }

 private:
  uint8_t *values;
  int dimInputLength;
  int dimOutputLength;
  uint8_t numDimsPerDimWidth[NUM_DIM_WIDTH];

  __host__ __device__ typename super_t::reference dereference() const {
    int baseIndex = *this->base_reference();
    int dimIndex = baseIndex / dimOutputLength;
    int globalIndex = baseIndex + (dimInputLength - dimOutputLength) * dimIndex;
    uint8_t numDims = 0;
    int bytes = 0;
    uint8_t dimBytes = 0;
    int i = 0;
    for (; i < NUM_DIM_WIDTH; i++) {
      dimBytes = 1 << (NUM_DIM_WIDTH - i - 1);
      if (dimIndex < numDims + numDimsPerDimWidth[i]) {
        bytes += (globalIndex - numDims * dimInputLength) * dimBytes;
        break;
      } else {
        bytes += numDimsPerDimWidth[i] * dimInputLength * dimBytes;
      }
      numDims += numDimsPerDimWidth[i];
    }
    if (i == NUM_DIM_WIDTH) {
      bytes += (globalIndex - numDims * dimInputLength) * dimBytes;
      return DimValueProxy(values + bytes, dimBytes);
    }
    return DimValueProxy(values + bytes, dimBytes);
  }
};

// HLLRegIDHeadFlagIterator
class HLLRegIDHeadFlagIterator
    : public thrust::iterator_adaptor<
        HLLRegIDHeadFlagIterator, thrust::counting_iterator<int>,
        unsigned int, thrust::use_default, thrust::use_default, unsigned int,
        thrust::use_default> {
 public:
  friend class thrust::iterator_core_access;
  typedef thrust::iterator_adaptor<HLLRegIDHeadFlagIterator,
                                   thrust::counting_iterator<int>, unsigned int,
                                   thrust::use_default, thrust::use_default,
                                   unsigned int, thrust::use_default>
      super_t;

  __host__ __device__ HLLRegIDHeadFlagIterator(uint64_t *hashValues)
      : super_t(thrust::make_counting_iterator(0)), hashValues(hashValues) {}

 private:
  uint64_t *hashValues;

  __host__ __device__ typename super_t::reference dereference() const {
    int index = *this->base_reference();
    return (index == 0 || hashValues[index] != hashValues[index - 1]);
  }
};

// HLLValueOutputIterator
class HLLValueOutputIterator
    : public thrust::iterator_adaptor<
        HLLValueOutputIterator, thrust::counting_iterator<int>,
        thrust::tuple<uint64_t, uint32_t, int>, thrust::use_default,
        thrust::use_default, thrust::tuple<uint64_t, uint32_t, int>,
        thrust::use_default> {
 public:
  friend class thrust::iterator_core_access;
  typedef thrust::iterator_adaptor<
      HLLValueOutputIterator, thrust::counting_iterator<int>,
      thrust::tuple<uint64_t, uint32_t, int>, thrust::use_default,
      thrust::use_default, thrust::tuple<uint64_t, uint32_t, int>,
      thrust::use_default>
      super_t;

  __host__ __device__ HLLValueOutputIterator(unsigned int *dimCount,
                                             uint32_t *hllMeasureValues,
                                             uint64_t *hllRegIDCumCount,
                                             uint64_t *hllDimRegIDCumCount,
                                             uint64_t *hllVectorOffsets)
      : super_t(thrust::make_counting_iterator(0)),
        dimCount(dimCount),
        hllMeasureValues(hllMeasureValues),
        hllRegIDCumCount(hllRegIDCumCount),
        hllDimRegIDCumCount(hllDimRegIDCumCount),
        hllVectorOffsets(hllVectorOffsets) {}

 private:
  unsigned int *dimCount;
  uint32_t *hllMeasureValues;
  uint64_t *hllRegIDCumCount;
  uint64_t *hllDimRegIDCumCount;
  uint64_t *hllVectorOffsets;

  // get the reg id within dim group
  // index:               0 1 2 3 4 5 6 7 8 9 10 11
  // regHeadIter:         1 0 0 0 1 0 0 0 1 0 0 0
  // hllRegIDCumCount:    1 1 1 1 2 2 2 2 3 3 3 3
  // dimCount:            1 1 1 1 1 1 1 1 2 2 2 2
  // hllDimRegIDCumCount: 0 2 3
  // hllVectorOffsets:    0 8 12
  __host__ __device__ typename super_t::reference dereference() const {
    int index = *this->base_reference();
    unsigned int dimIndex = dimCount[index] - 1;
    int dimRegIDCount =
        hllDimRegIDCumCount[dimIndex + 1] - hllDimRegIDCumCount[dimIndex];
    uint64_t hllVectorOffset = hllVectorOffsets[dimIndex];
    if (dimRegIDCount < HLL_DENSE_THRESHOLD) {
      // sparse mode
      uint64_t regIDCumCount = hllRegIDCumCount[index];
      uint64_t dimRegIDcumCount = hllDimRegIDCumCount[dimIndex];
      int regIDIndexWithinDim = regIDCumCount - dimRegIDcumCount - 1;
      return thrust::make_tuple(hllVectorOffset + regIDIndexWithinDim * 4,
                                hllMeasureValues[index], 4);
    } else {
      // dense mode
      int regID = hllMeasureValues[index] & 0x3FFF;
      return thrust::make_tuple(hllVectorOffset + regID,
                                hllMeasureValues[index], 1);
    }
  }
};

// GeoPredicateIterator reads geo intersection output predicate vector.
// It output the first intersected geoshape index, or -1 if no intersected
// geoshape found.
class GeoPredicateIterator
    : public thrust::iterator_adaptor<GeoPredicateIterator, uint32_t *, int8_t,
                                      thrust::use_default, thrust::use_default,
                                      int8_t, thrust::use_default> {
 public:
  friend class thrust::iterator_core_access;

  typedef thrust::iterator_adaptor<GeoPredicateIterator, uint32_t *, int8_t,
                                   thrust::use_default, thrust::use_default,
                                   int8_t, thrust::use_default>
      super_t;

  __host__ __device__ GeoPredicateIterator(uint32_t *predicateIter,
                                           uint8_t stepInWords)
      : super_t(predicateIter), stepInWords(stepInWords) {}

 private:
  uint8_t stepInWords;

  __host__ __device__ int8_t get_first_none_zero(uint32_t word) const {
    int i = 0;
    while (i < 32) {
      if ((word >> i) & 1) {
        return i;
      }
      i++;
    }
    return -1;
  }

  __host__ __device__ typename super_t::reference dereference() const {
    for (int i = 0; i < stepInWords; i++) {
      int8_t index = get_first_none_zero(this->base_reference()[i]);
      if (index >= 0) {
        return (int8_t)(i * 32 + index);
      }
    }
    return -1;
  }

  __host__ __device__ void advance(typename super_t::difference_type n) {
    this->base_reference() += n * stepInWords;
  }

  __host__ __device__ void increment() { advance(1); }

  __host__ __device__ void decrement() { advance(-1); }

  __host__ __device__ typename super_t::difference_type distance_to(
      const GeoPredicateIterator &other) const {
    typename super_t::difference_type dist =
        other.base_reference() - this->base_reference();
    return dist / stepInWords;
  }
};

// Used as void value and reference type.
struct EmptyStruct {};

// GeoBatchIntersectIterator is the iterator to compute whether the
// semi-infinite ray horizontally emitted from the geo point crosses a single
// edge of the geoshape. Note value and dereference type of this iterator are
// all default which means accessing and assigning value to this iterator has no
// meaning. The dereference function will directly write to the output predicate
// vector using atomicXor.
template <typename GeoInputIterator>
class GeoBatchIntersectIterator
    : public thrust::iterator_adaptor<
        GeoBatchIntersectIterator<GeoInputIterator>,
        GeoInputIterator, EmptyStruct,
        thrust::use_default, thrust::use_default,
        EmptyStruct, thrust::use_default> {
 public:
  friend class thrust::iterator_core_access;

  typedef thrust::iterator_adaptor<GeoBatchIntersectIterator<GeoInputIterator>,
                                   GeoInputIterator, EmptyStruct,
                                   thrust::use_default, thrust::use_default,
                                   EmptyStruct, thrust::use_default>
      super_t;

  __host__ __device__ GeoBatchIntersectIterator() {}

  __host__ __device__ GeoBatchIntersectIterator(
      GeoInputIterator geoPoints, GeoShapeBatch geoShapes,
      uint32_t *outputPredicate, bool inOrOut)
      : super_t(geoPoints),
        geoShapes(geoShapes),
        outputPredicate(outputPredicate),
        pointIndex(0),
        inOrOut(inOrOut) {}

 private:
  GeoShapeBatch geoShapes;
  uint32_t *outputPredicate;
  int32_t pointIndex;
  bool inOrOut;

  __host_or_device__
  typename super_t::reference dereference() const {
    // offset to shapeVector in Bytes is totalNumberPoints * 2 * 4
    uint8_t shapeIndex =
        geoShapes.LatLongs[geoShapes.TotalNumPoints * 2 * 4 + pointIndex];
    EmptyStruct emptyRes;
    // nothing need to be done for the last point of a shape.
    if (pointIndex >= geoShapes.TotalNumPoints - 1) {
      return emptyRes;
    }

    // shapeIndex change marks the last point of a shape, therefore nothing
    // needs to be done
    if (shapeIndex !=
        geoShapes.LatLongs[geoShapes.TotalNumPoints * 2 * 4 + pointIndex + 1]) {
      return emptyRes;
    }

    auto testPoint = *this->base_reference();
    // testPoint is null, we just write false to the output predicate.
    if (!thrust::get<1>(testPoint)) {
      // only first pointer is responsible for write.
      if (pointIndex == 0) {
        for (int i = 0; i < geoShapes.TotalWords; i++) {
          outputPredicate[i] = !inOrOut;
        }
      }
      return emptyRes;
    }

    float testLat = thrust::get<0>(testPoint).Lat;
    float testLong = thrust::get<0>(testPoint).Long;
    // the latitude of first point of the edge.
    float edgeLat1 = reinterpret_cast<float *>(geoShapes.LatLongs)[pointIndex];
    // the latitude of second point of the edge.
    float edgeLat2 =
        reinterpret_cast<float *>(geoShapes.LatLongs)[pointIndex + 1];
    if (edgeLat1 < FLT_MAX && edgeLat2 < FLT_MAX) {
      float edgeLong1 = reinterpret_cast<float *>(
          geoShapes.LatLongs)[geoShapes.TotalNumPoints + pointIndex];
      float edgeLong2 = reinterpret_cast<float *>(
          geoShapes.LatLongs)[geoShapes.TotalNumPoints + pointIndex + 1];
      if (((edgeLong1 > testLong) != (edgeLong2 > testLong)) &&
          (testLat < (edgeLat2 - edgeLat1) * (testLong - edgeLong1) /
                             (edgeLong2 - edgeLong1) +
                         edgeLat1)) {
#ifdef RUN_ON_DEVICE
        atomicXor(outputPredicate + (shapeIndex / 32),
                  (1 << (shapeIndex % 32)));
#else
        // When we are running in host mode, we are running sequentially.
        // So non atomic access is ok for now.
        // If we switch to parallel host execution in future,
        // we need to find out how to do atomic operation on an existing
        // pointer in c++.
        outputPredicate[shapeIndex / 32] ^= (1 << (shapeIndex % 32));
#endif
      }
    }
    return emptyRes;
  }

  __host__ __device__ void advance(typename super_t::difference_type n) {
    int64_t newPointIndex = (int64_t)pointIndex + n;
    if (newPointIndex >= geoShapes.TotalNumPoints) {
      int steps = newPointIndex / geoShapes.TotalNumPoints;
      this->base_reference() += steps;
      pointIndex = newPointIndex %= geoShapes.TotalNumPoints;
      outputPredicate += steps * geoShapes.TotalWords;
    } else if (newPointIndex < 0) {
      int steps = (newPointIndex - geoShapes.TotalNumPoints + 1) /
                  geoShapes.TotalNumPoints;
      this->base_reference() += steps;
      pointIndex = newPointIndex -= steps * geoShapes.TotalNumPoints;
      outputPredicate += steps * geoShapes.TotalWords;
    } else {
      pointIndex = (int32_t)newPointIndex;
    }
  }

  __host__ __device__ void increment() { advance(1); }

  __host__ __device__ void decrement() { advance(-1); }

  __host__ __device__ typename super_t::difference_type distance_to(
      const GeoBatchIntersectIterator &other) const {
    typename super_t::difference_type dist =
        other.base_reference() - this->base_reference();
    return dist * geoShapes.TotalNumPoints +
           (other.pointIndex - this->pointIndex);
  }
};

template<typename GeoInputIterator>
GeoBatchIntersectIterator<GeoInputIterator> make_geo_batch_intersect_iterator(
    GeoInputIterator points, GeoShapeBatch geoShape,
    uint32_t *outputPredicate, bool inOrOut) {
  return GeoBatchIntersectIterator<GeoInputIterator>(points,
                                                     geoShape,
                                                     outputPredicate,
                                                     inOrOut);
}

}  // namespace ares

namespace thrust {
namespace detail {
// For execution policy with cuda, it requires the output iterator's reference
// type to be a actual reference. However when using MeasureProxy and
// IndexProxy, they are value types. So we need to specialize the
// is_non_const_reference to traits to treat all MeasureProxy template classes
// as reference type.
template<>
struct is_non_const_reference<
    ares::MeasureProxy<int32_t> > : public true_type {
};

template<>
struct is_non_const_reference<
    ares::MeasureProxy<uint32_t> > : public true_type {
};

template<>
struct is_non_const_reference<
    ares::MeasureProxy<float_t> > : public true_type {
};

template<>
struct is_non_const_reference<
    ares::MeasureProxy<int64_t> > : public true_type {
};

template<>
struct is_non_const_reference<
    ares::MeasureProxy<double_t> > : public true_type {
};

template<>
struct is_non_const_reference<ares::IndexProxy> : public true_type {
};

template<>
struct is_non_const_reference<ares::DimValueProxy> : public true_type {
};
}  // namespace detail
}  // namespace thrust
#endif  // QUERY_ITERATOR_HPP_
