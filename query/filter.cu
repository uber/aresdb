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

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <exception>
#include <vector>
#include <initializer_list>
#include "query/transform.hpp"
#include "query/binder.hpp"

namespace ares {

// FilterContext is doing the actual filter after binding one or two
// input iterators.
template<typename FunctorType>
class FilterContext {
 public:
  FilterContext(
      uint8_t *predicateVector, int indexVectorLength,
      RecordID **foreignTableRecordIDVectors,
      int numForeignTables, FunctorType functorType,
      void *cudaStream)
      : predicateVector(predicateVector),
        indexVectorLength(indexVectorLength),
        foreignTableRecordIDVectors(foreignTableRecordIDVectors),
        numForeignTables(numForeignTables),
        functorType(functorType),
        cudaStream(reinterpret_cast<cudaStream_t>(cudaStream)) {}

  cudaStream_t getStream() const {
    return cudaStream;
  }

  template<typename InputIterator>
  int run(uint32_t *indexVector, InputIterator inputIterator) {
    switch (numForeignTables) {
      #define EXECUTE_UNARY_REMOVE_IF(NumTotalForeignTables) \
      case NumTotalForeignTables: { \
        IndexZipIteratorMaker<NumTotalForeignTables> maker; \
        return executeRemoveIf(inputIterator, \
                             maker.make(indexVector, \
                                        foreignTableRecordIDVectors)); \
      }

      EXECUTE_UNARY_REMOVE_IF(0)
      EXECUTE_UNARY_REMOVE_IF(1)
      EXECUTE_UNARY_REMOVE_IF(2)
      EXECUTE_UNARY_REMOVE_IF(3)
      EXECUTE_UNARY_REMOVE_IF(4)
      EXECUTE_UNARY_REMOVE_IF(5)
      EXECUTE_UNARY_REMOVE_IF(6)
      EXECUTE_UNARY_REMOVE_IF(7)
      EXECUTE_UNARY_REMOVE_IF(8)
      default:throw std::invalid_argument("only support up to 8 foreign tables");
    }
  }

  template<typename LHSIterator, typename RHSIterator>
  struct supported_combination {
  static constexpr bool value =
    ((std::is_same<typename LHSIterator::value_type::head_type, UUIDT*>::value &&
      (std::is_same<typename RHSIterator::value_type::head_type, UUIDT>::value ||
          std::is_same<typename RHSIterator::value_type::head_type, uint32_t>::value)) ||
    (std::is_same<typename LHSIterator::value_type::head_type, GeoPointT*>::value &&
      (std::is_same<typename RHSIterator::value_type::head_type, GeoPointT>::value ||
          std::is_same<typename RHSIterator::value_type::head_type, uint32_t>::value)) ||
    (!std::is_same<typename LHSIterator::value_type::head_type, UUIDT*>::value &&
      !std::is_same<typename LHSIterator::value_type::head_type, GeoPointT*>::value &&
      !std::is_same<typename RHSIterator::value_type::head_type, UUIDT*>::value &&
      !std::is_same<typename RHSIterator::value_type::head_type, GeoPointT*>::value));
  };

  template<typename LHSIterator, typename RHSIterator>
  typename std::enable_if<supported_combination<LHSIterator, RHSIterator>::value, int>::type
  run(uint32_t *indexVector, LHSIterator lhsIter, RHSIterator rhsIter) {
    switch (numForeignTables) {
      #define EXECUTE_BINARY_REMOVE_IF(NumTotalForeignTables) \
      case NumTotalForeignTables: { \
        IndexZipIteratorMaker<NumTotalForeignTables> maker; \
        return executeRemoveIf(lhsIter, rhsIter, maker.make(indexVector, \
                                foreignTableRecordIDVectors)); \
      }

      EXECUTE_BINARY_REMOVE_IF(0)
      EXECUTE_BINARY_REMOVE_IF(1)
      EXECUTE_BINARY_REMOVE_IF(2)
      EXECUTE_BINARY_REMOVE_IF(3)
      EXECUTE_BINARY_REMOVE_IF(4)
      EXECUTE_BINARY_REMOVE_IF(5)
      EXECUTE_BINARY_REMOVE_IF(6)
      EXECUTE_BINARY_REMOVE_IF(7)
      EXECUTE_BINARY_REMOVE_IF(8)
      default:throw std::invalid_argument("only support up to 8 foreign tables");
    }
  }

  template<typename LHSIterator, typename RHSIterator>
  typename std::enable_if<!supported_combination<LHSIterator, RHSIterator>::value, int>::type
  run(uint32_t *indexVector, LHSIterator lhsIter, RHSIterator rhsIter) {
    throw std::invalid_argument(
              "Unsupported data type combination" + std::to_string(__LINE__)
                  + "in filter context");
  }

 private:
  uint8_t *predicateVector;
  int indexVectorLength;
  RecordID **foreignTableRecordIDVectors;
  int numForeignTables;
  FunctorType functorType;
  cudaStream_t cudaStream;

  template<typename LHSIterator, typename RHSIterator,
      typename IndexZipIterator>
  int executeRemoveIf(LHSIterator lhsIter,
                      RHSIterator rhsIter,
                      IndexZipIterator indexZipIterator);

  template<typename InputIterator, typename IndexZipIterator>
  int executeRemoveIf(InputIterator inputIter,
                      IndexZipIterator indexZipIterator);
};

}  // namespace ares

CGoCallResHandle UnaryFilter(InputVector input,
                             uint32_t *indexVector,
                             uint8_t *predicateVector,
                             int indexVectorLength,
                             RecordID **foreignTableRecordIDVectors,
                             int numForeignTables,
                             uint32_t *baseCounts,
                             uint32_t startCount,
                             UnaryFunctorType functorType,
                             void *cudaStream,
                             int device) {
  CGoCallResHandle resHandle = {nullptr, nullptr};
  try {
#ifdef RUN_ON_DEVICE
    cudaSetDevice(device);
#endif
    ares::FilterContext<UnaryFunctorType> ctx(predicateVector,
                                              indexVectorLength,
                                              foreignTableRecordIDVectors,
                                              numForeignTables,
                                              functorType,
                                              cudaStream);
    std::vector<InputVector> inputVectors = {input};
    ares::InputVectorBinder<ares::FilterContext<UnaryFunctorType>, 1>
        binder(ctx, inputVectors, indexVector, baseCounts, startCount);
    resHandle.res =
        reinterpret_cast<void *>(binder.bind());
    CheckCUDAError("UnaryFilter");
  }
  catch (std::exception &e) {
    std::cerr << "Exception happend when doing UnaryFilter:" << e.what()
              << std::endl;
    resHandle.pStrErr = strdup(e.what());
  }
  return resHandle;
}

CGoCallResHandle BinaryFilter(InputVector lhs,
                              InputVector rhs,
                              uint32_t *indexVector,
                              uint8_t *predicateVector,
                              int indexVectorLength,
                              RecordID **foreignTableRecordIDVectors,
                              int numForeignTables,
                              uint32_t *baseCounts,
                              uint32_t startCount,
                              BinaryFunctorType functorType,
                              void *cudaStream,
                              int device) {
  CGoCallResHandle resHandle = {nullptr, nullptr};
  try {
#ifdef RUN_ON_DEVICE
    cudaSetDevice(device);
#endif
    ares::FilterContext<BinaryFunctorType> ctx(predicateVector,
                                               indexVectorLength,
                                               foreignTableRecordIDVectors,
                                               numForeignTables,
                                               functorType,
                                               cudaStream);
    std::vector<InputVector> inputVectors = {lhs, rhs};
    ares::InputVectorBinder<ares::FilterContext<BinaryFunctorType>, 2> binder(
        ctx, inputVectors, indexVector, baseCounts, startCount);

    resHandle.res =
        reinterpret_cast<void *>(binder.bind());
    CheckCUDAError("BinaryFilter");
  }
  catch (std::exception &e) {
    std::cerr << "Exception happend when doing BinaryFilter:" << e.what()
              << std::endl;
    resHandle.pStrErr = strdup(e.what());
  }
  return resHandle;
}

namespace ares {

// Filter template function for unary transform filter.
template<typename FunctorType>
template<typename InputIterator, typename IndexZipIterator>
int FilterContext<FunctorType>::executeRemoveIf(
    InputIterator inputIter,
    IndexZipIterator indexZipIterator) {
  typedef typename InputIterator::value_type::head_type InputValueType;
  UnaryPredicateFunctor<bool, InputValueType> f(functorType);
  RemoveFilter<typename IndexZipIterator::value_type, uint8_t> removeFilter(
      predicateVector);
  // first compute the predicate values.
  thrust::transform(GET_EXECUTION_POLICY(cudaStream), inputIter,
                    inputIter + indexVectorLength, predicateVector, f);
  // then we use the predicate values to remove indexes in place.
  return thrust::remove_if(GET_EXECUTION_POLICY(cudaStream), indexZipIterator,
                           indexZipIterator + indexVectorLength, removeFilter) -
         indexZipIterator;
}

// run binary filter.
template<typename FunctorType>
template<typename LHSIterator, typename RHSIterator, typename IndexZipIterator>
int FilterContext<FunctorType>::executeRemoveIf(
    LHSIterator lhsIter,
    RHSIterator rhsIter,
    IndexZipIterator indexZipIterator) {

  typedef typename input_iterator_value_type<
        typename LHSIterator::value_type::head_type,
        typename RHSIterator::value_type::head_type>::type InputValueType1;
  typedef typename input_iterator_value_type<
        typename RHSIterator::value_type::head_type,
        typename LHSIterator::value_type::head_type>::type InputValueType2;

  BinaryPredicateFunctor<bool, InputValueType1, InputValueType2> f(functorType);
  RemoveFilter<typename IndexZipIterator::value_type, uint8_t> removeFilter(
      predicateVector);

  // first compute the predicate values.
  thrust::transform(GET_EXECUTION_POLICY(cudaStream), lhsIter,
      lhsIter + indexVectorLength, rhsIter, predicateVector, f);
  // then we use the predicate values to remove indexes in place.
  return thrust::remove_if(GET_EXECUTION_POLICY(cudaStream), indexZipIterator,
                           indexZipIterator + indexVectorLength, removeFilter) -
         indexZipIterator;
}
}  // namespace ares
