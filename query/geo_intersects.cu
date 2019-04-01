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
#include <vector>
#include "query/algorithm.hpp"
#include "query/binder.hpp"
#include "query/memory.hpp"

namespace ares {
class GeoIntersectionContext {
 public:
  GeoIntersectionContext(GeoShapeBatch geoShapes,
             int indexVectorLength,
             uint32_t startCount,
             RecordID **recordIDVectors,
             int numForeignTables,
             uint32_t *outputPredicate,
             bool inOrOut,
             void *cudaStream)
      : geoShapes(geoShapes),
          indexVectorLength(indexVectorLength),
          startCount(startCount),
          recordIDVectors(recordIDVectors),
          numForeignTables(numForeignTables),
          outputPredicate(outputPredicate),
          inOrOut(inOrOut),
          cudaStream(reinterpret_cast<cudaStream_t>(cudaStream)) {}

 private:
  GeoShapeBatch geoShapes;
  int indexVectorLength;
  uint32_t startCount;
  RecordID **recordIDVectors;
  int numForeignTables;
  uint32_t *outputPredicate;
  bool inOrOut;
  cudaStream_t cudaStream;

  template<typename IndexZipIterator>
  int executeRemoveIf(IndexZipIterator indexZipIterator);

 public:
  cudaStream_t getStream() const {
    return cudaStream;
  }

  template<typename InputIterator>
  int run(uint32_t *indexVector, InputIterator inputIterator);
};

// Specialized for GeoIntersectionContext.
template <>
class InputVectorBinder<GeoIntersectionContext, 1> :
    public InputVectorBinderBase<GeoIntersectionContext, 1, 1> {
  typedef InputVectorBinderBase<GeoIntersectionContext, 1, 1> super_t;

 public:
  explicit InputVectorBinder(GeoIntersectionContext context,
                                std::vector<InputVector> inputVectors,
                                uint32_t *indexVector, uint32_t *baseCounts,
                                uint32_t startCount) : super_t(context,
                                                               inputVectors,
                                                               indexVector,
                                                               baseCounts,
                                                               startCount) {
  }
 public:
  template<typename ...InputIterators>
  int bind(InputIterators... boundInputIterators);
};

}  // namespace ares

CGoCallResHandle GeoBatchIntersects(
    GeoShapeBatch geoShapes, InputVector points, uint32_t *indexVector,
    int indexVectorLength, uint32_t startCount, RecordID **recordIDVectors,
    int numForeignTables, uint32_t *outputPredicate, bool inOrOut,
    void *cudaStream, int device) {
  CGoCallResHandle resHandle = {0, nullptr};
  try {
#ifdef RUN_ON_DEVICE
    cudaSetDevice(device);
#endif
    ares::GeoIntersectionContext
        ctx(geoShapes, indexVectorLength, startCount,
            recordIDVectors, numForeignTables, outputPredicate, inOrOut,
            cudaStream);
    std::vector<InputVector> inputVectors = {points};
    ares::InputVectorBinder<ares::GeoIntersectionContext, 1>
        binder(ctx, inputVectors, indexVector, nullptr, startCount);
    resHandle.res = reinterpret_cast<void *>(binder.bind());
    CheckCUDAError("GeoBatchIntersects");
    return resHandle;
  } catch (const std::exception &e) {
    std::cerr << "Exception happened when doing GeoBatchIntersects:" << e.what()
              << std::endl;
    resHandle.pStrErr = strdup(e.what());
  }
  return resHandle;
}

CGoCallResHandle WriteGeoShapeDim(
    int shapeTotalWords, DimensionOutputVector dimOut,
    int indexVectorLengthBeforeGeo,
    uint32_t *outputPredicate, void *cudaStream, int device) {
  CGoCallResHandle resHandle = {nullptr, nullptr};
  try {
#ifdef RUN_ON_DEVICE
    cudaSetDevice(device);
#endif
    ares::write_geo_shape_dim(shapeTotalWords, dimOut,
                              indexVectorLengthBeforeGeo,
                              outputPredicate,
                              reinterpret_cast<cudaStream_t>(cudaStream));
    CheckCUDAError("WriteGeoShapeDim");
    return resHandle;
  } catch (const std::exception &e) {
    std::cerr << "Exception happened when doing GeoIntersectsJoin:" << e.what()
              << std::endl;
    resHandle.pStrErr = strdup(e.what());
  }
  return resHandle;
}

namespace ares {

template<typename ...InputIterators>
int InputVectorBinder<GeoIntersectionContext, 1>::bind(
    InputIterators... boundInputIterators) {
  InputVector input = super_t::inputVectors[0];
  uint32_t *indexVector = super_t::indexVector;
  uint32_t startCount = super_t::startCount;
  GeoIntersectionContext context = super_t::context;

  if (input.Type == VectorPartyInput) {
    VectorPartySlice points = input.Vector.VP;
    if (points.DataType != GeoPoint) {
      throw std::invalid_argument(
          "only geo point column are allowed in geo_intersects");
    }

    if (points.BasePtr == nullptr) {
      return 0;
    }

    uint8_t *basePtr = points.BasePtr;
    uint32_t nullsOffset = points.NullsOffset;
    uint32_t valueOffset = points.ValuesOffset;
    uint8_t startingIndex = points.StartingIndex;
    uint8_t stepInBytes = 8;
    uint32_t length = points.Length;
    auto columnIter = make_column_iterator<GeoPointT>(
        indexVector, nullptr, startCount, basePtr, nullsOffset, valueOffset,
        length, stepInBytes, startingIndex);
    return context.run(indexVector, columnIter);
  } else if (input.Type == ForeignColumnInput) {
    DataType dataType = input.Vector.ForeignVP.DataType;

    if (dataType != GeoPoint) {
      throw std::invalid_argument(
          "only geo point column are allowed in geo_intersects");
    }
    // Note: for now foreign vectors are dimension table columns
    // that are not compressed nor pre sliced
    RecordID *recordIDs = input.Vector.ForeignVP.RecordIDs;
    const int32_t numBatches = input.Vector.ForeignVP.NumBatches;
    const int32_t baseBatchID = input.Vector.ForeignVP.BaseBatchID;
    VectorPartySlice *vpSlices = input.Vector.ForeignVP.Batches;
    const int32_t numRecordsInLastBatch =
        input.Vector.ForeignVP.NumRecordsInLastBatch;
    bool hasDefault = input.Vector.ForeignVP.DefaultValue.HasDefault;
    DefaultValue defaultValueStruct = input.Vector.ForeignVP.DefaultValue;
    uint8_t stepInBytes = getStepInBytes(dataType);

    ForeignTableIterator<GeoPointT> *vpIters = prepareForeignTableIterators(
        numBatches,
        vpSlices,
        stepInBytes,
        hasDefault,
        defaultValueStruct.Value.GeoPointVal,
        context.getStream());
    int res =
        context.run(indexVector, RecordIDJoinIterator<GeoPointT>(
            recordIDs,
            numBatches,
            baseBatchID,
            vpIters,
            numRecordsInLastBatch,
            nullptr, 0));
    deviceFree(vpIters);
    return res;
  }
  throw std::invalid_argument(
      "Unsupported data type " + std::to_string(__LINE__)
          + "for geo intersection contexts");
}

// GeoRemoveFilter
template<typename Value>
struct GeoRemoveFilter {
  explicit GeoRemoveFilter(GeoPredicateIterator predicates, bool inOrOut)
      : predicates(predicates), inOrOut(inOrOut) {}

  GeoPredicateIterator predicates;
  bool inOrOut;

  __host__ __device__
  bool operator()(const Value &index) {
    return inOrOut == predicates[thrust::get<0>(index)] < 0;
  }
};

// actual function for executing geo filter in batch.
template<typename IndexZipIterator>
int GeoIntersectionContext::executeRemoveIf(IndexZipIterator indexZipIterator) {
  GeoPredicateIterator predIter(outputPredicate, geoShapes.TotalWords);
  GeoRemoveFilter<
      typename IndexZipIterator::value_type> removeFilter(predIter, inOrOut);

  return thrust::remove_if(GET_EXECUTION_POLICY(cudaStream), indexZipIterator,
                           indexZipIterator + indexVectorLength, removeFilter) -
      indexZipIterator;
}

// run intersection algorithm for points and 1 geoshape, side effect is
// modifying output predicate vector
template<typename InputIterator>
void calculateBatchIntersection(GeoShapeBatch geoShapes,
                                InputIterator geoPoints, uint32_t *indexVector,
                                int indexVectorLength, uint32_t startCount,
                                uint32_t *outputPredicate, bool inOrOut,
                                cudaStream_t cudaStream) {
  auto geoIter = make_geo_batch_intersect_iterator(geoPoints, geoShapes,
                                                   outputPredicate, inOrOut);
  int64_t iterLength = (int64_t) indexVectorLength * geoShapes.TotalNumPoints;

  thrust::for_each(GET_EXECUTION_POLICY(cudaStream), geoIter,
      geoIter + iterLength, VoidFunctor());
}

template<typename InputIterator>
int GeoIntersectionContext::run(uint32_t *indexVector,
                                InputIterator inputIterator) {
  calculateBatchIntersection(geoShapes, inputIterator,
                             indexVector, indexVectorLength,
                             startCount, outputPredicate,
                             inOrOut, cudaStream);

  switch (numForeignTables) {
    #define EXECUTE_GEO_REMOVE_IF(NumTotalForeignTables) \
    case NumTotalForeignTables: { \
      IndexZipIteratorMaker<NumTotalForeignTables> maker; \
      return executeRemoveIf(maker.make(indexVector, recordIDVectors)); \
    }

    EXECUTE_GEO_REMOVE_IF(0)
    EXECUTE_GEO_REMOVE_IF(1)
    EXECUTE_GEO_REMOVE_IF(2)
    EXECUTE_GEO_REMOVE_IF(3)
    EXECUTE_GEO_REMOVE_IF(4)
    EXECUTE_GEO_REMOVE_IF(5)
    EXECUTE_GEO_REMOVE_IF(6)
    EXECUTE_GEO_REMOVE_IF(7)
    EXECUTE_GEO_REMOVE_IF(8)
    default:throw std::invalid_argument("only support up to 8 foreign tables");
  }
}

struct is_non_negative {
  __host__ __device__
  bool operator()(const int val) {
    return val >= 0;
  }
};

void write_geo_shape_dim(
    int shapeTotalWords,
    DimensionOutputVector dimOut, int indexVectorLengthBeforeGeo,
    uint32_t *outputPredicate, cudaStream_t cudaStream) {
  typedef thrust::tuple<int8_t, uint8_t> DimensionOutputIterValue;
  GeoPredicateIterator geoPredicateIter(outputPredicate, shapeTotalWords);

  auto zippedShapeIndexIter = thrust::make_zip_iterator(thrust::make_tuple(
      geoPredicateIter, thrust::constant_iterator<uint8_t>(1)));

  thrust::copy_if(
      GET_EXECUTION_POLICY(cudaStream),
      zippedShapeIndexIter, zippedShapeIndexIter + indexVectorLengthBeforeGeo,
      geoPredicateIter,
      ares::make_dimension_output_iterator<uint8_t>(dimOut.DimValues,
                                                    dimOut.DimNulls),
                                                    is_non_negative());
}

}  // namespace ares
