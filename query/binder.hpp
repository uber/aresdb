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

#ifndef QUERY_BINDER_HPP_
#define QUERY_BINDER_HPP_
#include <cuda_runtime.h>
#include <cfloat>
#include <cstdint>
#include <tuple>
#include <type_traits>
#include <vector>
#include "query/algorithm.hpp"
#include "query/functor.hpp"
#include "query/iterator.hpp"
#include "query/time_series_aggregate.h"

namespace ares {

template<typename Value>
ForeignTableIterator<Value> *prepareForeignTableIterators(
    int32_t numBatches,
    VectorPartySlice *vpSlices,
    size_t stepBytes,
    bool hasDefault,
    Value defaultValue, cudaStream_t stream);

// Forward declaration;
template<typename Context, int NumVectors, int NumUnboundIterators>
class InputVectorBinderBase;

// InputVectorBinder will bind NumVectors InputVector struct to different type
// of iterators. When there is no more input vector to bind, it will call
// context's run function with all the bound iterator.
// A example usage is like:
//    ares::FilterContext<UnaryFunctorType> ctx(predicateVector,
//                                                  indexVectorLength,
//                                                  foreignTableRecordIDVectors,
//                                                  numForeignTables,
//                                                  functorType,
//                                                  cudaStream);
//        std::vector<InputVector> inputVectors = {input};
//        ares::InputVectorBinder<ares::FilterContext<UnaryFunctorType>, 1>
//            binder(ctx, inputVectors, indexVector, baseCounts, startCount);
//        resHandle.res =
//            reinterpret_cast<void *>(binder.bind());
//
// If the caller need to specialize the binder with a context, they will always
template<typename Context, int NumVectors>
class InputVectorBinder : public InputVectorBinderBase<Context,
                                                       NumVectors,
                                                       NumVectors> {
  typedef InputVectorBinderBase<Context, NumVectors, NumVectors> super_t;
 public:
  explicit InputVectorBinder(Context context,
                             std::vector<InputVector> inputVectors,
                             uint32_t *indexVector, uint32_t *baseCounts,
                             uint32_t startCount) : super_t(context,
                                                            inputVectors,
                                                            indexVector,
                                                            baseCounts,
                                                            startCount) {
  }
};

// InputIteratorBinderBase is the class to bind InputVector structs into
// individual input iterators. It will bind one input vector at one time until
// N becomes zero.
template<typename Context, int NumVectors, int NumUnboundIterators>
class InputVectorBinderBase {
 public:
  explicit InputVectorBinderBase(Context context,
                                 std::vector<InputVector> inputVectors,
                                 uint32_t *indexVector,
                                 uint32_t *baseCounts,
                                 uint32_t startCount) :
      context(context),
      inputVectors(inputVectors),
      indexVector(indexVector),
      baseCounts(baseCounts),
      startCount(startCount) {}

 protected:
  Context context;
  std::vector<InputVector> inputVectors;
  uint32_t *indexVector;
  uint32_t *baseCounts;
  uint32_t startCount;

  template<typename ...InputIterators>
  int bindGeneric(InputIterators... boundInputIterators) {
    InputVectorBinderBase<Context, NumVectors, NumUnboundIterators - 1>
        nextBinder(context, inputVectors, indexVector, baseCounts, startCount);

    InputVector input = inputVectors[NumVectors - NumUnboundIterators];
    if (input.Type == ConstantInput) {
      ConstantVector constant = input.Vector.Constant;
      if (constant.DataType == ConstInt) {
        return nextBinder.bind(
            boundInputIterators...,
            make_constant_iterator(
                constant.Value.IntVal, constant.IsValid));
      } else if (constant.DataType == ConstFloat) {
        return nextBinder.bind(
            boundInputIterators...,
            make_constant_iterator(constant.Value.FloatVal,
                                   constant.IsValid));
      }
    } else if (input.Type == ScratchSpaceInput) {
      ScratchSpaceVector scratchSpace = input.Vector.ScratchSpace;
      uint32_t nullsOffset = scratchSpace.NullsOffset;
      switch (scratchSpace.DataType) {
        case Int32:
          return nextBinder.bind(
              boundInputIterators...,
              make_scratch_space_input_iterator<int32_t>(
                  scratchSpace.Values,
                  nullsOffset));
        case Uint32:
          return nextBinder.bind(
              boundInputIterators...,
              make_scratch_space_input_iterator<uint32_t>(
                  scratchSpace.Values,
                  nullsOffset));
        case Float32:
          return nextBinder.bind(
              boundInputIterators...,
              make_scratch_space_input_iterator<float_t>(
                  scratchSpace.Values,
                  nullsOffset));
        default:
          throw std::invalid_argument(
              "Unsupported data type for ScratchSpaceInput");
      }
    } else if (input.Type == ForeignColumnInput) {
      // Note: for now foreign vectors are dimension table columns
      // that are not compressed nor pre sliced
      RecordID *recordIDs = input.Vector.ForeignVP.RecordIDs;
      const int32_t numBatches = input.Vector.ForeignVP.NumBatches;
      const int32_t baseBatchID = input.Vector.ForeignVP.BaseBatchID;
      VectorPartySlice *vpSlices = input.Vector.ForeignVP.Batches;
      const int32_t numRecordsInLastBatch =
          input.Vector.ForeignVP.NumRecordsInLastBatch;
      int16_t *const timezoneLookup = input.Vector.ForeignVP.TimezoneLookup;
      int16_t timezoneLookupSize = input.Vector.ForeignVP.TimezoneLookupSize;
      DataType dataType = input.Vector.ForeignVP.DataType;
      bool hasDefault = input.Vector.ForeignVP.DefaultValue.HasDefault;
      DefaultValue defaultValueStruct = input.Vector.ForeignVP.DefaultValue;
      uint8_t stepInBytes = getStepInBytes(dataType);
      int res;

      switch (dataType) {
        case Bool: {
          ForeignTableIterator<bool> *vpIters = prepareForeignTableIterators(
              numBatches,
              vpSlices,
              stepInBytes,
              hasDefault,
              defaultValueStruct.Value.BoolVal,
              context.getStream());
          res =
              nextBinder.bind(boundInputIterators...,
                              RecordIDJoinIterator<bool>(
                                  recordIDs,
                                  numBatches,
                                  baseBatchID,
                                  vpIters,
                                  numRecordsInLastBatch,
                                  timezoneLookup, timezoneLookupSize));
          release(vpIters);
          break;
        }
        case Int8:
        case Int16:
        case Int32: {
          ForeignTableIterator<int32_t> *vpIters = prepareForeignTableIterators(
              numBatches,
              vpSlices,
              stepInBytes,
              hasDefault,
              defaultValueStruct.Value.Int32Val,
              context.getStream());
          res = nextBinder.bind(boundInputIterators...,
                                RecordIDJoinIterator<int32_t>(
                                    recordIDs,
                                    numBatches,
                                    baseBatchID,
                                    vpIters,
                                    numRecordsInLastBatch,
                                    timezoneLookup, timezoneLookupSize));
          release(vpIters);
          break;
        }
        case Uint8:
        case Uint16:
        case Uint32: {
          ForeignTableIterator<uint32_t>
              *vpIters = prepareForeignTableIterators(
              numBatches,
              vpSlices,
              stepInBytes,
              hasDefault,
              defaultValueStruct.Value.Uint32Val,
              context.getStream());
          res = nextBinder.bind(boundInputIterators...,
                                RecordIDJoinIterator<uint32_t>(
                                    recordIDs,
                                    numBatches,
                                    baseBatchID,
                                    vpIters,
                                    numRecordsInLastBatch,
                                    timezoneLookup, timezoneLookupSize));
          release(vpIters);
          break;
        }
        case Float32: {
          ForeignTableIterator<float_t> *vpIters = prepareForeignTableIterators(
              numBatches,
              vpSlices,
              stepInBytes,
              hasDefault,
              defaultValueStruct.Value.FloatVal,
              context.getStream());
          res = nextBinder.bind(
              boundInputIterators...,
              RecordIDJoinIterator<float_t>(recordIDs,
                                            numBatches,
                                            baseBatchID,
                                            vpIters,
                                            numRecordsInLastBatch,
                                            timezoneLookup,
                                            timezoneLookupSize));
          release(vpIters);
          break;
        }
        default:
          throw std::invalid_argument(
              "Unsupported data type for VectorPartyInput: " +
                  std::to_string(__LINE__));
      }
      return res;
    }

    VectorPartySlice inputVP = input.Vector.VP;
    bool hasDefault = inputVP.DefaultValue.HasDefault;
    bool isConstant = inputVP.BasePtr == nullptr;
    DefaultValue defaultValue = inputVP.DefaultValue;

    if (isConstant) {
      switch (inputVP.DataType) {
        case Bool:
          return nextBinder.bind(boundInputIterators..., make_constant_iterator(
              defaultValue.Value.BoolVal, hasDefault));
        case Int8:
        case Int16:
        case Int32:
          return nextBinder.bind(boundInputIterators..., make_constant_iterator(
              defaultValue.Value.Int32Val, hasDefault));
        case Uint8:
        case Uint16:
        case Uint32:
          return nextBinder.bind(boundInputIterators..., make_constant_iterator(
              defaultValue.Value.Uint32Val, hasDefault));
        case Float32:
          return nextBinder.bind(boundInputIterators..., make_constant_iterator(
              defaultValue.Value.FloatVal, hasDefault));
        default:
          throw std::invalid_argument(
              "Unsupported data type for VectorPartyInput: " +
                  std::to_string(__LINE__));
      }
    }

    // Non constant.
    uint8_t *basePtr = inputVP.BasePtr;
    uint32_t nullsOffset = inputVP.NullsOffset;
    uint32_t valuesOffset = inputVP.ValuesOffset;
    uint8_t startingIndex = inputVP.StartingIndex;
    uint8_t stepInBytes = getStepInBytes(inputVP.DataType);
    uint32_t length = inputVP.Length;
    switch (inputVP.DataType) {
      case Bool:
        return nextBinder.bind(boundInputIterators...,
                               make_column_iterator<bool>(indexVector,
                                                          baseCounts,
                                                          startCount,
                                                          basePtr,
                                                          nullsOffset,
                                                          valuesOffset,
                                                          length,
                                                          stepInBytes,
                                                          startingIndex));
      case Int8:
      case Int16:
      case Int32:
        return nextBinder.bind(boundInputIterators...,
                               make_column_iterator<int32_t>(indexVector,
                                                             baseCounts,
                                                             startCount,
                                                             basePtr,
                                                             nullsOffset,
                                                             valuesOffset,
                                                             length,
                                                             stepInBytes,
                                                             startingIndex));
      case Uint8:
      case Uint16:
      case Uint32:
        return nextBinder.bind(boundInputIterators...,
                               make_column_iterator<uint32_t>(indexVector,
                                                              baseCounts,
                                                              startCount,
                                                              basePtr,
                                                              nullsOffset,
                                                              valuesOffset,
                                                              length,
                                                              stepInBytes,
                                                              startingIndex));
      case Float32:
        return nextBinder.bind(boundInputIterators...,
                               make_column_iterator<float_t>(indexVector,
                                                             baseCounts,
                                                             startCount,
                                                             basePtr,
                                                             nullsOffset,
                                                             valuesOffset,
                                                             length,
                                                             stepInBytes,
                                                             startingIndex));
      default:
        throw std::invalid_argument(
            "Unsupported data type for VectorPartyInput: " +
                std::to_string(__LINE__));
    }
  }

  template<typename GeoIterator>
  int bindGeoPoint(GeoIterator geoIter) {
    InputVectorBinderBase<Context, NumVectors, NumUnboundIterators - 1>
        nextBinder(context, inputVectors, indexVector, baseCounts, startCount);

    InputVector input = inputVectors[NumVectors - NumUnboundIterators];
    if (input.Type == ConstantInput) {
      ConstantVector constant = input.Vector.Constant;
      if (constant.DataType == ConstGeoPoint) {
        return nextBinder.bind(
            geoIter,
            thrust::make_constant_iterator(
                thrust::make_tuple<GeoPointT, bool>(
                    constant.Value.GeoPointVal, constant.IsValid)));
      }
    }
    throw std::invalid_argument(
        "Unsupported data type " + std::to_string(__LINE__)
            + "when value type of first input iterator is GeoPoint");
  }

 public:
  template<typename ...InputIterators>
  int bind(InputIterators... boundInputIterators) {
    return bindGeneric(boundInputIterators...);
  }

  // when this is the first input iterator, we allow geo point iterator and uuid
  // iterator.
  int bind() {
    InputVectorBinderBase<Context, NumVectors, NumUnboundIterators - 1>
        nextBinder(context, inputVectors, indexVector, baseCounts, startCount);
    InputVector input = inputVectors[NumVectors - NumUnboundIterators];
    if (input.Type == VectorPartyInput) {
      VectorPartySlice inputVP = input.Vector.VP;
      DataType dataType = inputVP.DataType;
      uint8_t *basePtr = inputVP.BasePtr;
      bool hasDefault = inputVP.DefaultValue.HasDefault;
      DefaultValue defaultValue = inputVP.DefaultValue;
      uint32_t nullsOffset = inputVP.NullsOffset;
      uint32_t valuesOffset = inputVP.ValuesOffset;
      uint8_t startingIndex = inputVP.StartingIndex;
      uint8_t stepInBytes = getStepInBytes(inputVP.DataType);
      uint32_t length = inputVP.Length;
      if (dataType == GeoPoint) {
        // Treat mode 0 as constant vector.
        if (basePtr == nullptr) {
          return nextBinder.bind(thrust::make_constant_iterator(
              thrust::make_tuple<GeoPointT, bool>(
                  defaultValue.Value.GeoPointVal, hasDefault)));
        }
        return nextBinder.bind(make_column_iterator<GeoPointT>(
            indexVector, baseCounts, startCount, basePtr, nullsOffset,
            valuesOffset, length, stepInBytes, startingIndex));
      } else if (dataType == UUID) {
        if (basePtr == nullptr) {
          return nextBinder.bind(
              thrust::make_constant_iterator(thrust::make_tuple<UUIDT, bool>(
                  defaultValue.Value.UUIDVal, hasDefault)));
        }
        return nextBinder.bind(make_column_iterator<UUIDT>(
            indexVector, baseCounts, startCount, basePtr, nullsOffset,
            valuesOffset, length, stepInBytes, startingIndex));
      } else if (dataType == Int64) {
        if (basePtr == nullptr) {
          return nextBinder.bind(
              thrust::make_constant_iterator(thrust::make_tuple<int64_t, bool>(
                  defaultValue.Value.Int64Val, hasDefault)));
        }
        return nextBinder.bind(make_column_iterator<int64_t>(
            indexVector, baseCounts, startCount, basePtr, nullsOffset,
            valuesOffset, length, stepInBytes, startingIndex));
      }
    } else if (input.Type == ForeignColumnInput) {
      RecordID *recordIDs = input.Vector.ForeignVP.RecordIDs;
      const int32_t numBatches = input.Vector.ForeignVP.NumBatches;
      const int32_t baseBatchID = input.Vector.ForeignVP.BaseBatchID;
      VectorPartySlice *vpSlices = input.Vector.ForeignVP.Batches;
      const int32_t numRecordsInLastBatch =
          input.Vector.ForeignVP.NumRecordsInLastBatch;
      DataType dataType = input.Vector.ForeignVP.DataType;
      bool hasDefault = input.Vector.ForeignVP.DefaultValue.HasDefault;
      DefaultValue defaultValueStruct = input.Vector.ForeignVP.DefaultValue;
      uint8_t stepInBytes = getStepInBytes(dataType);
      if (dataType == UUID) {
        ForeignTableIterator<UUIDT> *vpIters = prepareForeignTableIterators(
            numBatches, vpSlices, stepInBytes, hasDefault,
            defaultValueStruct.Value.UUIDVal, context.getStream());
        int res = nextBinder.bind(RecordIDJoinIterator<UUIDT>(
            recordIDs, numBatches, baseBatchID, vpIters, numRecordsInLastBatch,
            nullptr, 0));
        release(vpIters);
        return res;
      } else if (dataType == Int64) {
        ForeignTableIterator<int64_t> *vpIters = prepareForeignTableIterators(
            numBatches, vpSlices, stepInBytes, hasDefault,
            defaultValueStruct.Value.Int64Val, context.getStream());
        int res = nextBinder.bind(RecordIDJoinIterator<int64_t>(
            recordIDs, numBatches, baseBatchID, vpIters, numRecordsInLastBatch,
            nullptr, 0));
        release(vpIters);
        return res;
      }
    }
    return bindGeneric();
  }

  // UUID data type is only supported in UnaryTransform
  template <typename UUIDIterator>
  typename std::enable_if<
      std::is_same<typename UUIDIterator::value_type::head_type, UUIDT>::value,
      int>::type
  bind(UUIDIterator uuidIter) {
    throw std::invalid_argument(
        "UUID data type is only supported in UnaryTransform" +
        std::to_string(__LINE__));
  }

  // Int64 data type is only supported in UnaryTransform
  template <typename Int64Iterator>
  typename std::enable_if<
      std::is_same<typename Int64Iterator::value_type::head_type,
                   int64_t>::value,
      int>::type
  bind(Int64Iterator int64Iter) {
    throw std::invalid_argument(
        "int64 data type is only supported in UnaryTransform" +
        std::to_string(__LINE__));
  }

  // Special handling if the first input iter is a geo iter.
  template<typename GeoIterator>
  typename std::enable_if<
      std::is_same<typename GeoIterator::value_type::head_type,
                   GeoPointT>::value, int>::type bind(
      GeoIterator geoIter) {
    return bindGeoPoint(geoIter);
  }
};

// This class is called when there is no more unbound iterators. It will just
// call context.run to do actual calculation.
template<typename Context, int NumVectors>
class InputVectorBinderBase<Context, NumVectors, 0> {
 public:
  explicit InputVectorBinderBase(Context context,
                                 std::vector<InputVector> inputVectors,
                                 uint32_t *indexVector, uint32_t *baseCounts,
                                 uint32_t startCount) :
      context(context),
      inputVectors(inputVectors),
      indexVector(indexVector),
      baseCounts(baseCounts),
      startCount(startCount) {}

 protected:
  Context context;
  std::vector<InputVector> inputVectors;
  uint32_t *indexVector;
  uint32_t *baseCounts;
  uint32_t startCount;

 public:
  // Special handling if the first input iter is a geo iter.
  template<typename GeoIterator>
  typename std::enable_if<
      std::is_same<typename GeoIterator::value_type::head_type,
                   GeoPointT>::value && NumVectors == 1,
      int>::type bind(
      GeoIterator geoIter) {
    throw std::invalid_argument(
        "GeoPoint data type is not supported when doing UnaryTransform "
            + std::to_string(__LINE__));
  }

  template<typename ...InputIterators>
  int bind(InputIterators... boundInputIterators) {
    return context.run(indexVector, boundInputIterators...);
  }
};

template<typename Value>
ForeignTableIterator<Value> *prepareForeignTableIterators(
    int32_t numBatches,
    VectorPartySlice *vpSlices,
    size_t stepBytes,
    bool hasDefault,
    Value defaultValue, cudaStream_t stream) {
  typedef ForeignTableIterator<Value> ValueIter;
  int totalSize = sizeof(ValueIter) * numBatches;
  ValueIter *batches =
      reinterpret_cast<ValueIter *>(malloc(totalSize));
  for (int i = 0; i < numBatches; i++) {
    VectorPartySlice inputVP = vpSlices[i];
    if (inputVP.BasePtr == nullptr) {
      batches[i] = ValueIter(
          make_constant_iterator(defaultValue,
                                 hasDefault));
    } else {
      batches[i] =
          ValueIter(
              VectorPartyIterator<Value>(
                  nullptr,
                  0,
                  inputVP.BasePtr,
                  inputVP.NullsOffset,
                  inputVP.ValuesOffset,
                  inputVP.Length,
                  stepBytes,
                  inputVP.StartingIndex));
    }
  }
#ifdef RUN_ON_DEVICE
  ValueIter *vpItersDevice;
  cudaMalloc(reinterpret_cast<void **>(
                 &vpItersDevice), totalSize);
  cudaMemcpyAsync(reinterpret_cast<void *>(vpItersDevice),
                  reinterpret_cast<void *>(batches),
                  totalSize,
                  cudaMemcpyHostToDevice,
                  stream);
  free(batches);
  batches = vpItersDevice;
#endif
  return batches;
}

// IndexZipIteratorMapper is the mapper to map number of foreign tables to
// the actual zip iterator
template<int NumTotalForeignTables>
struct IndexZipIteratorMapper {
  typedef thrust::zip_iterator<
      thrust::tuple < thrust::counting_iterator
          < uint32_t>, uint32_t*>> type;
};

template<>
struct IndexZipIteratorMapper<1> {
  typedef thrust::zip_iterator<
      thrust::tuple < thrust::counting_iterator
          < uint32_t>, uint32_t*, RecordID*>> type;
};

template<>
struct IndexZipIteratorMapper<2> {
  typedef thrust::zip_iterator<
      thrust::tuple < thrust::counting_iterator
          < uint32_t>, uint32_t*, RecordID*, RecordID*>> type;
};

template<>
struct IndexZipIteratorMapper<3> {
  typedef thrust::zip_iterator<
      thrust::tuple < thrust::counting_iterator
          < uint32_t>, uint32_t*, RecordID*, RecordID*, RecordID*>> type;
};

template<>
struct IndexZipIteratorMapper<4> {
  typedef thrust::zip_iterator<
      thrust::tuple < thrust::counting_iterator
          < uint32_t>, uint32_t*,
  RecordID*, RecordID*, RecordID*, RecordID*>> type;
};

template<>
struct IndexZipIteratorMapper<5> {
  typedef thrust::zip_iterator<
      thrust::tuple < thrust::counting_iterator
          < uint32_t>, uint32_t*,
  RecordID*, RecordID*, RecordID*, RecordID*, RecordID*>> type;
};

template<>
struct IndexZipIteratorMapper<6> {
  typedef thrust::zip_iterator<
      thrust::tuple < thrust::counting_iterator
          < uint32_t>, uint32_t*,
  RecordID*, RecordID*, RecordID*,
  RecordID*, RecordID*, RecordID*>> type;
};

template<>
struct IndexZipIteratorMapper<7> {
  typedef thrust::zip_iterator<
      thrust::tuple < thrust::counting_iterator
          < uint32_t>, uint32_t*,
  RecordID*, RecordID*, RecordID*,
  RecordID*, RecordID*, RecordID*, RecordID*>> type;
};

template<>
struct IndexZipIteratorMapper<8> {
  typedef thrust::zip_iterator<
      thrust::tuple < thrust::counting_iterator
          < uint32_t>, uint32_t*, RecordID*, RecordID*, RecordID*, RecordID*,
  RecordID*, RecordID*, RecordID*, RecordID*>> type;
};

// IndexZipIteratorMaker is the factory to make the index zip iterator given
// a counting iterator, a main table index vector and 0 or more foreign table
// record id vector. It binds one record id vector at one time from the
// unboundForeignTableRecordIDVectors. If the NUnboundForeignTable is 0, it will
// just return the tuple
template<int NumTotalForeignTables, int NumUnboundForeignTables>
struct IndexZipIteratorMakerBase {
  template<typename... RecordIDVector>
  typename IndexZipIteratorMapper<NumTotalForeignTables>::type
  make(uint32_t *index_vector,
       RecordID **unboundForeignTableRecordIDVectors,
       RecordIDVector... boundForeignRecordIDVectors) {
    IndexZipIteratorMakerBase<NumTotalForeignTables,
                              NumUnboundForeignTables - 1>
        nextMaker;
    return nextMaker.make(
        index_vector,
        unboundForeignTableRecordIDVectors,
        boundForeignRecordIDVectors...,
        unboundForeignTableRecordIDVectors[
            NumTotalForeignTables
                - NumUnboundForeignTables]);
  }
};

// Specialized IndexZipIteratorMakerBase with NUnboundForeignTable to be zero.
// Just bind everything together using thrust::make_tuple and return.
template<int NumTotalForeignTables>
struct IndexZipIteratorMakerBase<NumTotalForeignTables, 0> {
  template<typename... RecordIDVector>
  typename IndexZipIteratorMapper<NumTotalForeignTables>::type
  make(uint32_t *indexVector,
       RecordID **unboundForeignTableRecordIDVectors,
       RecordIDVector... boundForeignRecordIDVectors) {
    return thrust::make_zip_iterator(thrust::make_tuple(
        thrust::counting_iterator<uint32_t>(0), indexVector,
        boundForeignRecordIDVectors...));
  }
};

template<int NumTotalForeignTables>
struct IndexZipIteratorMaker : public IndexZipIteratorMakerBase<
    NumTotalForeignTables,
    NumTotalForeignTables> {
};

}  // namespace ares
#endif  // QUERY_BINDER_HPP_
