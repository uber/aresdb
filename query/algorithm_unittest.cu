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

#include <thrust/transform.h>
#include <cstdint>
#include <cfloat>
#include <algorithm>
#include <cmath>
#include <exception>
#include <iterator>
#include <iostream>
#include <type_traits>
#include "gtest/gtest.h"
#include "query/algorithm.hpp"
#include "query/iterator.hpp"
#include "query/time_series_aggregate.h"
#include "query/transform.hpp"
#include "query/unittest_utils.hpp"

namespace ares {

template<typename TypeA, typename TypeB, typename ExpectedType>
void checkCommonTypeAsExpected() {
  bool isSame = std::is_same<typename common_type<TypeA, TypeB>::type,
                             ExpectedType>::value;
  EXPECT_TRUE(isSame);
}

// cppcheck-suppress *
TEST(CommonTypeTraitsTest, CheckCommonType) {
  checkCommonTypeAsExpected<float_t, float_t, float_t>();
  checkCommonTypeAsExpected<int16_t, float_t, float_t>();
  checkCommonTypeAsExpected<float_t, float_t, float_t>();
  checkCommonTypeAsExpected<float_t, uint32_t, float_t>();
  checkCommonTypeAsExpected<int16_t, uint32_t, int32_t>();
  checkCommonTypeAsExpected<int32_t, uint32_t, int32_t>();
  checkCommonTypeAsExpected<int32_t, bool, int32_t>();
  checkCommonTypeAsExpected<uint32_t, bool, uint32_t>();
  checkCommonTypeAsExpected<uint16_t, bool, uint32_t>();
}

// cppcheck-suppress *
TEST(CGoCallResHandleTest, CheckCGoCallResHandle) {
  CGoCallResHandle resHandle = {0, nullptr};
  try {
    throw std::invalid_argument("test");
  }
  catch (std::exception &e) {
    resHandle.pStrErr = strdup(e.what());
  }

  EXPECT_STREQ(resHandle.pStrErr, "test");
  EXPECT_TRUE(resHandle.pStrErr != nullptr);
  free(const_cast<char *>(resHandle.pStrErr));
  CGoCallResHandle resHandle2 = {0, nullptr};
  try {
    printf("test CGoCallResHandle\n");
  }
  catch (std::exception &e) {
    resHandle2.pStrErr = strdup(e.what());
  }

  EXPECT_TRUE(resHandle2.pStrErr == nullptr);
}

// cppcheck-suppress *
TEST(UnaryTransformTest, CheckInt) {
  const int size = 3;
  uint32_t indexVectorH[size * 2] = {0, 1, 2};
  int inputValuesH[size] = {-1, 1, 0};
  // T T F
  uint8_t inputNullsH[1 + ((size - 1) / 8)] = {0x03};

  int outputValuesH[size] = {0, 0, 0};
  bool outputNullsH[size] = {false, false, false};

  uint32_t *indexVector = allocate(&indexVectorH[0], size * 2);
  uint8_t *basePtr =
      allocate_column(nullptr, &inputNullsH[0], &inputValuesH[0], 0, 1, 12);

  uint8_t *outputBasePtr =
      allocate_column(nullptr, reinterpret_cast<uint8_t *>(&outputValuesH[0]),
                      &outputNullsH[0], 0, 12, 3);

  DefaultValue defaultValue = {false, {.Int32Val = 0}};
  VectorPartySlice inputVP = {basePtr, 0, 8, 0, Int32, defaultValue};

  ScratchSpaceVector outputScratchSpace = {outputBasePtr, 16, Int32};

  InputVector input = {{.VP = inputVP}, VectorPartyInput};
  OutputVector output =
      {{.ScratchSpace = outputScratchSpace}, ScratchSpaceOutput};

  UnaryTransform(input, output, indexVector, size, nullptr, 0,
                 Negate, 0, 0);

  int expectedValues[size] = {1, -1, 0};
  bool expectedNulls[size] = {true, true, false};
  int *outputValues = reinterpret_cast<int *>(outputBasePtr);
  bool *outputNulls = reinterpret_cast<bool *>(outputBasePtr) + 16;
  EXPECT_TRUE(
      equal(outputValues, outputValues + size,
            &expectedValues[0]));

  EXPECT_TRUE(
      equal(outputNulls, outputNulls + size,
            &expectedNulls[0]));

  release(basePtr);
  release(outputBasePtr);
  release(indexVector);
}

// cppcheck-suppress *
TEST(UnaryTransformTest, CheckConstant) {
  const int size = 3;
  uint32_t indexVectorH[size * 2] = {0, 1, 2};
  ConstantVector constant = {
      {.IntVal = 1}, true, ConstInt,
  };

  int outputValuesH[size] = {0, 0, 0};
  bool outputNullsH[size] = {false, false, false};

  uint32_t *indexVector = allocate(&indexVectorH[0], size * 2);

  uint8_t *outputBasePtr =
      allocate_column(nullptr, reinterpret_cast<uint8_t *>(&outputValuesH[0]),
                      &outputNullsH[0], 0, 12, 3);
  ScratchSpaceVector outputScratchSpace = {outputBasePtr, 16, Int32};

  InputVector input = {{.Constant = constant}, ConstantInput};
  OutputVector output =
      {{.ScratchSpace = outputScratchSpace}, ScratchSpaceOutput};

  UnaryTransform(input, output, &indexVector[0], size, nullptr, 0,
                 Negate, 0, 0);
  int expectedValues[3] = {-1, -1, -1};
  bool expectedNulls[3] = {true, true, true};
  int *outputValues = reinterpret_cast<int *>(outputBasePtr);
  bool *outputNulls = reinterpret_cast<bool *>(outputBasePtr) + 16;
  EXPECT_TRUE(
      equal(outputValues, outputValues + size,
            &expectedValues[0]));

  EXPECT_TRUE(
      equal(outputNulls, outputNulls + size,
            &expectedNulls[0]));

  release(outputBasePtr);
  release(indexVector);
}

// cppcheck-suppress *
TEST(UnaryTransformTest, CheckMeasureOutputIteratorForAvg) {
  const int size = 3;
  uint32_t indexVectorH[size * 2] = {0, 1, 2};
  uint32_t baseCountsH[size + 1] = {0, 3, 9, 12};
  int inputValuesH[size] = {-1, 1, 0};
  // T T F
  uint8_t inputNullsH[1 + ((size - 1) / 8)] = {0x03};

  int64_t outputValuesH[size] = {0, 0, 0};

  uint32_t *indexVector = allocate(&indexVectorH[0], size * 2);
  uint32_t *baseCounts = allocate(&baseCountsH[0], size + 1);
  uint8_t *basePtr =
      allocate_column(nullptr, &inputNullsH[0], &inputValuesH[0], 0, 1, 12);
  int64_t *outputValues = allocate(&outputValuesH[0], size);

  DefaultValue defaultValue = {false, {.Int32Val = 0}};
  VectorPartySlice inputVP = {basePtr, 0, 8, 0, Int32, defaultValue};

  MeasureOutputVector outputMeasure = {
      reinterpret_cast<uint32_t *>(outputValues), Int64,
      AGGR_SUM_SIGNED};

  InputVector input = {{.VP = inputVP}, VectorPartyInput};
  OutputVector output = {{.Measure = outputMeasure}, MeasureOutput};

  UnaryTransform(input, output, indexVector, size, baseCounts, 0,
                 Negate, 0, 0);
  int64_t expectedValues[3] = {3, -6, 0};
  EXPECT_TRUE(equal(outputValues, outputValues + size,
                    &expectedValues[0]));

  // Test NOOP functor
  UnaryTransform(input, output, indexVector, size, baseCounts, 0,
                 Noop, 0, 0);
  int64_t expectedValues2[3] = {-3, 6, 0};
  EXPECT_TRUE(equal(outputValues, outputValues + size,
                    &expectedValues2[0]));

  // Test avg aggregation.
  MeasureOutputVector outputMeasure2 = {
      reinterpret_cast<uint32_t *>(outputValues), Float64,
      AGGR_AVG_FLOAT};

  OutputVector output2 = {{.Measure = outputMeasure2}, MeasureOutput};

  UnaryTransform(input, output2, indexVector, size, baseCounts, 0,
                 Noop, 0, 0);
  float_t expectedValues3[6] = {-1.0, 0, 1.0, 0, 0, 0};
  *reinterpret_cast<uint32_t*>(&expectedValues3[1]) = 3;
  *reinterpret_cast<uint32_t*>(&expectedValues3[3]) = 6;
  EXPECT_TRUE(equal_print(outputValues, outputValues + size,
                    reinterpret_cast<int64_t*>(&expectedValues3[0])));

  release(basePtr);
  release(outputValues);
  release(indexVector);
  release(baseCounts);
}

// cppcheck-suppress *
TEST(UnaryTransformTest, CheckDimensionOutputIterator) {
  const int size = 3;
  uint32_t indexVectorH[size * 2] = {0, 1, 2};
  int16_t inputValuesH[size] = {-1, 1, 0};
  // T T F
  uint8_t inputNullsH[1 + ((size - 1) / 8)] = {0x03};

  uint8_t outputValuesH[3 * 3];
  thrust::fill(std::begin(outputValuesH), std::end(outputValuesH), 0);

  uint8_t *basePtr =
      allocate_column(nullptr, &inputNullsH[0], &inputValuesH[0], 0, 1, 6);
  uint32_t *indexVector = allocate(&indexVectorH[0], size * 2);
  uint8_t *outputValues = allocate(&outputValuesH[0], 9);

  DefaultValue defaultValue = {false, {.Int32Val = 0}};
  VectorPartySlice inputVP = {basePtr, 0, 8, 0, Int16, defaultValue};

  DimensionOutputVector outputDimension = {
      outputValues, outputValues + 6, Int16};

  InputVector input = {{.VP = inputVP}, VectorPartyInput};
  OutputVector output =
      {{.Dimension = outputDimension}, DimensionOutput};

  UnaryTransform(input, output, indexVector, size, nullptr, 0,
                 Negate, 0, 0);

  uint8_t expectedValues[9] = {1, 0, 0xFF, 0xFF, 0, 0, 1, 1, 0};
  EXPECT_TRUE(equal(outputValues, outputValues + 9, &expectedValues[0]));

  // Test NOOP functor
  UnaryTransform(input, output, indexVector, size, nullptr, 0, Noop, 0, 0);

  uint8_t expectedValues2[9] = {0xFF, 0xFF, 1, 0, 0, 0, 1, 1, 0};
  EXPECT_TRUE(equal(outputValues, outputValues + 9, &expectedValues2[0]));

  release(basePtr);
  release(outputValues);
  release(indexVector);
}

// cppcheck-suppress *
TEST(UnaryTransformTest, CheckGeoPoint) {
  const int size = 3;
  uint32_t indexVectorH[size * 2] = {0, 1, 2};
  GeoPointT lhsValuesH[size] = {{1, 1}, {1, 0}, {0, 0}};
  // T T F
  uint8_t lhsNullsH[1 + ((size - 1) / 8)] = {0x03};

  uint8_t *inputBasePtr =
      allocate_column(nullptr, &lhsNullsH[0], &lhsValuesH[0], 0, 1, 24);

  int outputValuesH[size] = {false, false, false};
  bool outputNullsH[size] = {false, false, false};
  uint8_t *outputBasePtr =
      allocate_column(nullptr, reinterpret_cast<uint8_t *>(&outputValuesH[0]),
                      &outputNullsH[0], 0, 12, 3);

  uint32_t *indexVector = allocate(&indexVectorH[0], size * 2);

  DefaultValue defaultValue = {false, {.Int32Val = 0}};
  VectorPartySlice
      inputColumn = {inputBasePtr, 0, 8, 0, GeoPoint, defaultValue};
  ScratchSpaceVector outputScratchSpace = {outputBasePtr, 3, Int32};

  InputVector input = {{.VP = inputColumn}, VectorPartyInput};
  OutputVector output = {{.ScratchSpace = outputScratchSpace},
                         ScratchSpaceOutput};

  CGoCallResHandle resHandle = {0, nullptr};
  resHandle = UnaryTransform(input, output, indexVector, size, nullptr, 0,
      Negate, 0, 0);
  EXPECT_TRUE(resHandle.pStrErr != nullptr);
  free(const_cast<char *>(resHandle.pStrErr));
  release(inputBasePtr);
  release(outputBasePtr);
  release(indexVector);
}

// cppcheck-suppress *
TEST(UnaryFilterTest, CheckFilter) {
  const int size = 3;
  uint32_t indexVectorH[size] = {0, 1, 2};
  RecordID recordIDVectorH[3];
  for (int i = 0; i < size; i++) {
    RecordID recordID = {(int32_t) 0, (uint32_t) i};
    recordIDVectorH[i] = recordID;
  }
  uint8_t boolVectorH[size] = {0, 0, 0};

  int inputValuesH[size] = {1, 0, 1};
  uint8_t inputNullsH[size] = {1, 0, 1};

  uint8_t *basePtr =
      allocate_column(nullptr, reinterpret_cast<uint8_t *>(&inputValuesH[0]),
                      &inputNullsH[0], 0, 12, 3);

  uint32_t *indexVector = allocate(&indexVectorH[0], size);
  uint8_t *boolVector = allocate(&boolVectorH[0], size);
  RecordID *recordIDVector = allocate(&recordIDVectorH[0], size);

  ScratchSpaceVector inputVP = {basePtr, 16, Int32};

  InputVector input = {{.ScratchSpace = inputVP}, ScratchSpaceInput};
  RecordID *recordIDVectors[1] = {recordIDVector};
  CGoCallResHandle resHandle = UnaryFilter(input,
                                           indexVector,
                                           boolVector,
                                           size,
                                           recordIDVectors,
                                           1,
                                           nullptr,
                                           0,
                                           IsNotNull,
                                           0,
                                           0);
  EXPECT_EQ(reinterpret_cast<int64_t>(resHandle.res), 2);
  EXPECT_EQ(resHandle.pStrErr, nullptr);

  int length = reinterpret_cast<int64_t>(resHandle.res);

  uint32_t expectedValues[2] = {0, 2};
  RecordID recordID0 = {(int32_t) 0, (int32_t) 0};
  RecordID recordID1 = {(int32_t) 0, (int32_t) 2};
  RecordID expectedRecordIDs[2] = {recordID0, recordID1};
  EXPECT_TRUE(equal(indexVector, indexVector + length, &expectedValues[0]));
  EXPECT_TRUE(equal(
      reinterpret_cast<uint8_t *>(recordIDVector),
      reinterpret_cast<uint8_t *>(recordIDVector) + sizeof(RecordID) * length,
      reinterpret_cast<uint8_t *>(&expectedRecordIDs[0])));
  release(basePtr);
  release(boolVector);
  release(indexVector);
  release(recordIDVector);
}

// cppcheck-suppress *
TEST(UnaryFilterTest, AllEmpty) {
  const int size = 3;
  uint32_t indexVectorH[size] = {0, 1, 2};
  uint8_t boolVectorH[size] = {0, 0, 0};

  int inputValuesH[size] = {0, 0, 0};
  uint8_t inputNullsH[size] = {1, 1, 1};

  uint8_t *basePtr =
      allocate_column(nullptr, reinterpret_cast<uint8_t *>(&inputValuesH[0]),
                      &inputNullsH[0], 0, 12, 3);

  uint32_t *indexVector = allocate(&indexVectorH[0], size);
  uint8_t *boolVector = allocate(&boolVectorH[0], size);

  ScratchSpaceVector inputVP = {basePtr, 16, Int32};

  InputVector input = {{.ScratchSpace = inputVP}, ScratchSpaceInput};
  CGoCallResHandle resHandle = UnaryFilter(input, indexVector, boolVector, size,
                                           nullptr, 0, nullptr,
                                           0, Negate, 0, 0);
  EXPECT_EQ(reinterpret_cast<int64_t>(resHandle.res), 0);
  EXPECT_EQ(resHandle.pStrErr, nullptr);
  release(basePtr);
  release(indexVector);
}

// cppcheck-suppress *
TEST(BinaryTransformTest, CheckInt) {
  const int size = 3;
  uint32_t indexVectorH[size * 2] = {0, 1, 2};
  int lhsValuesH[size] = {-1, 1, 0};
  // T T F
  uint8_t lhsNullsH[1 + ((size - 1) / 8)] = {0x03};

  uint8_t *lhsBasePtr =
      allocate_column(nullptr, &lhsNullsH[0], &lhsValuesH[0], 0, 1, 12);

  int rhsValuesH[size] = {0, 1, -1};
  // F T T
  uint8_t rhsNullsH[1 + ((size - 1) / 8)] = {0x06};

  uint8_t *rhsBasePtr =
      allocate_column(nullptr, &rhsNullsH[0], &rhsValuesH[0], 0, 1, 12);

  int outputValuesH[size] = {0, 0, 0};
  bool outputNullsH[size] = {false, false, false};
  uint8_t *outputBasePtr =
      allocate_column(nullptr, reinterpret_cast<uint8_t *>(&outputValuesH[0]),
                      &outputNullsH[0], 0, 12, 3);

  uint32_t *indexVector = allocate(&indexVectorH[0], size * 2);

  DefaultValue defaultValue = {false, {.Int32Val = 0}};
  VectorPartySlice
      lhsColumn = {lhsBasePtr, 0, 8, 0, Int32, defaultValue};
  VectorPartySlice
      rhsColumn = {rhsBasePtr, 0, 8, 0, Int32, defaultValue};

  ScratchSpaceVector outputScratchSpace = {outputBasePtr, 16, Int32};

  InputVector lhs = {{.VP = lhsColumn}, VectorPartyInput};
  InputVector rhs = {{.VP = rhsColumn}, VectorPartyInput};
  OutputVector output = {{.ScratchSpace = outputScratchSpace},
                         ScratchSpaceOutput};

  BinaryTransform(lhs, rhs, output, indexVector, size, nullptr, 0,
                  Plus, 0, 0);

  int expectedValues[3] = {0, 2, 0};
  bool expectedNulls[3] = {false, true, false};

  int *outputValues = reinterpret_cast<int *>(outputBasePtr);
  bool *outputNulls = reinterpret_cast<bool *>(outputBasePtr) + 16;

  EXPECT_TRUE(
      equal(outputValues, outputValues + size,
            &expectedValues[0]));

  EXPECT_TRUE(
      equal(outputNulls, outputNulls + size,
            &expectedNulls[0]));

  release(lhsBasePtr);
  release(rhsBasePtr);
  release(outputBasePtr);
  release(indexVector);
}

// cppcheck-suppress *
TEST(BinaryTransformTest, CheckGeoPoint) {
  const int size = 3;
  uint32_t indexVectorH[size * 2] = {0, 1, 2};
  GeoPointT lhsValuesH[size] = {{1, 1}, {1, 0}, {0, 0}};
  // T T F
  uint8_t lhsNullsH[1 + ((size - 1) / 8)] = {0x03};

  uint8_t *lhsBasePtr =
      allocate_column(nullptr, &lhsNullsH[0], &lhsValuesH[0], 0, 1, 24);

  ConstantVector rhsConstant = {{.GeoPointVal = {1, 1}}, true, ConstGeoPoint};

  uint32_t outputValuesH[size] = {false, false, false};
  bool outputNullsH[size] = {false, false, false};
  uint8_t *outputBasePtr =
      allocate_column(nullptr, reinterpret_cast<uint8_t *>(&outputValuesH[0]),
                      &outputNullsH[0], 0, 12, 3);

  uint32_t *indexVector = allocate(&indexVectorH[0], size * 2);

  DefaultValue defaultValue = {false, {.Int32Val = 0}};
  VectorPartySlice
      lhsColumn = {lhsBasePtr, 0, 8, 0, GeoPoint, defaultValue};
  ScratchSpaceVector outputScratchSpace = {outputBasePtr, 16, Uint32};

  InputVector lhs = {{.VP = lhsColumn}, VectorPartyInput};
  InputVector rhs = {{.Constant = rhsConstant}, ConstantInput};
  OutputVector output = {{.ScratchSpace = outputScratchSpace},
                         ScratchSpaceOutput};
  BinaryTransform(lhs, rhs, output, indexVector, size, nullptr, 0,
                  Equal, 0, 0);

  uint32_t expectedValues[3] = {true, false, false};
  bool expectedNulls[3] = {true, true, false};

  uint32_t *outputValues = reinterpret_cast<uint32_t *>(outputBasePtr);
  bool *outputNulls = reinterpret_cast<bool *>(outputBasePtr) + 16;

  EXPECT_TRUE(
      equal(outputValues, outputValues + size,
            &expectedValues[0]));

  EXPECT_TRUE(
      equal(outputNulls, outputNulls + size,
            &expectedNulls[0]));

  int rhsValuesH[size] = {0, 1, -1};
  // F T T
  uint8_t rhsNullsH[1 + ((size - 1) / 8)] = {0x06};

  uint8_t *rhsBasePtr =
      allocate_column(nullptr, &rhsNullsH[0], &rhsValuesH[0], 0, 1, 12);

  VectorPartySlice
      rhsColumn = {rhsBasePtr, 0, 8, 0, Int32, defaultValue};

  InputVector rhsVector = {{.VP = rhsColumn}, VectorPartyInput};

  CGoCallResHandle resHandle = {0, nullptr};
  resHandle = BinaryTransform(lhs, rhsVector, output, indexVector, size,
      nullptr, 0, Equal, 0, 0);
  EXPECT_TRUE(resHandle.pStrErr != nullptr);
  free(const_cast<char *>(resHandle.pStrErr));
  release(lhsBasePtr);
  release(rhsBasePtr);
  release(outputBasePtr);
  release(indexVector);
}

// cppcheck-suppress *
TEST(BinaryTransformTest, CheckFloatAndUnpackedBoolIter) {
  const int size = 3;
  uint32_t indexVectorH[size * 2] = {0, 1, 2};
  int lhsValuesH[size] = {-1, 1, 0};
  uint8_t lhsNullsH[size] = {1, 1, 1};

  uint8_t *lhsBasePtr =
      allocate_column(nullptr,
                      reinterpret_cast<uint8_t *>(&lhsValuesH[0]),
                      &lhsNullsH[0], 0, 12, 3);

  float rhsValuesH[size] = {1.1, -1.1, 0.1};
  uint8_t rhsNullsH[size] = {1, 1, 1};

  uint8_t *rhsBasePtr =
      allocate_column(nullptr,
                      reinterpret_cast<uint8_t *>(&rhsValuesH[0]),
                      &rhsNullsH[0], 0, 12, 3);

  float outputValuesH[size] = {200, 0, 0};
  bool outputNullsH[size] = {false, false, false};
  uint8_t *outputBasePtr =
      allocate_column(nullptr, reinterpret_cast<uint8_t *>(&outputValuesH[0]),
                      &outputNullsH[0], 0, 12, 3);

  uint32_t *indexVector = allocate(&indexVectorH[0], size * 2);

  ScratchSpaceVector lhsColumn = {lhsBasePtr, 16, Int32};
  ScratchSpaceVector rhsColumn = {rhsBasePtr, 16, Float32};
  ScratchSpaceVector outputScratchSpace = {
      outputBasePtr, 16, Float32};

  InputVector lhs = {{.ScratchSpace = lhsColumn}, ScratchSpaceInput};
  InputVector rhs = {{.ScratchSpace = rhsColumn}, ScratchSpaceInput};
  OutputVector output = {{.ScratchSpace = outputScratchSpace},
                         ScratchSpaceOutput};

  BinaryTransform(lhs, rhs, output, indexVector, size, nullptr, 0, Minus, 0, 0);

  float expectedValues[3] = {-2.1, 2.1, -0.1};
  bool expectedNulls[3] = {true, true, true};

  float_t *outputValues = reinterpret_cast<float_t *>(outputBasePtr);
  bool *outputNulls = reinterpret_cast<bool *>(outputBasePtr) + 16;

  EXPECT_TRUE(
      equal(outputValues, outputValues + size,
            &expectedValues[0]));

  EXPECT_TRUE(
      equal(outputNulls, outputNulls + size,
            &expectedNulls[0]));

  // transform using multiply functor.
  BinaryTransform(lhs, rhs, output, indexVector, size, nullptr, 0,
                  Multiply, 0, 0);
  float expectedValues2[3] = {-1.1, -1.1, -0};
  bool expectedNulls2[3] = {true, true, true};

  EXPECT_TRUE(
      equal(outputValues, outputValues + size,
            &expectedValues2[0]));

  EXPECT_TRUE(
      equal(outputNulls, outputNulls + size,
            &expectedNulls2[0]));

  release(lhsBasePtr);
  release(rhsBasePtr);
  release(outputBasePtr);
  release(indexVector);
}

// cppcheck-suppress *
TEST(BinaryTransformTest, CheckConstantIterator) {
  const int size = 3;
  uint32_t indexVectorH[size * 2] = {0, 1, 2};
  int columnValuesH[size] = {-1, 1, 0};
  uint8_t columnNullsH[size] = {1, 1, 1};

  uint8_t *columnBasePtr =
      allocate_column(nullptr,
                      reinterpret_cast<uint8_t *>(&columnValuesH[0]),
                      &columnNullsH[0], 0, 12, 3);

  float outputValuesH[size] = {0, 0, 0};
  uint8_t outputNullsH[size] = {false, false, false};

  uint8_t *outputBasePtr =
      allocate_column(nullptr, reinterpret_cast<uint8_t *>(&outputValuesH[0]),
                      &outputNullsH[0], 0, 12, 3);

  uint32_t *indexVector = allocate(&indexVectorH[0], size * 2);

  ScratchSpaceVector lhsColumn = {columnBasePtr, 16, Int32};
  ConstantVector rhsConstant = {{.FloatVal = 0.1}, true, ConstFloat};
  ScratchSpaceVector outputScratchSpace = {outputBasePtr, 16, Float32};

  InputVector lhs = {{.ScratchSpace = lhsColumn}, ScratchSpaceInput};
  InputVector rhs = {{.Constant = rhsConstant}, ConstantInput};
  OutputVector output = {{.ScratchSpace = outputScratchSpace},
                         ScratchSpaceOutput};

  BinaryTransform(lhs, rhs, output, indexVector, size, nullptr, 0,
                  Plus, 0, 0);

  float expectedValues[3] = {-0.9, 1.1, 0.1};
  bool expectedNulls[3] = {true, true, true};

  float_t *outputValues = reinterpret_cast<float_t *>(outputBasePtr);
  bool *outputNulls = reinterpret_cast<bool *>(outputBasePtr) + 16;

  EXPECT_TRUE(
      equal(outputValues, outputValues + size,
            &expectedValues[0]));

  EXPECT_TRUE(
      equal(outputNulls, outputNulls + size,
            &expectedNulls[0]));

  release(columnBasePtr);
  release(outputBasePtr);
  release(indexVector);
}

// cppcheck-suppress *
TEST(BinaryFilterTest, CheckFilter) {
  const int size = 3;
  int lhsValuesH[size] = {0, 1, 2};
  uint8_t lhsNullsH[size] = {1, 1, 1};

  uint8_t *lhsBasePtr =
      allocate_column(nullptr,
                      reinterpret_cast<uint8_t *>(&lhsValuesH[0]),
                      &lhsNullsH[0], 0, 12, 3);

  float rhsValuesH[size] = {0.1, 0.9, 1.9};
  uint8_t rhsNullsH[size] = {1, 1, 1};

  uint8_t *rhsBasePtr =
      allocate_column(nullptr,
                      reinterpret_cast<uint8_t *>(&rhsValuesH[0]),
                      &rhsNullsH[0], 0, 12, 3);

  uint32_t indexVectorH[size * 2] = {0, 1, 2};
  uint8_t boolVectorH[size] = {0, 0, 0};

  uint32_t *indexVector = allocate(&indexVectorH[0], size);
  uint8_t *boolVector = allocate(&boolVectorH[0], size);

  ScratchSpaceVector lhsColumn = {lhsBasePtr, 16, Int32};
  ScratchSpaceVector rhsColumn = {rhsBasePtr, 16, Float32};

  InputVector lhs = {{.ScratchSpace = lhsColumn}, ScratchSpaceInput};
  InputVector rhs = {{.ScratchSpace = rhsColumn}, ScratchSpaceInput};

  CGoCallResHandle resHandle = BinaryFilter(lhs, rhs, indexVector, boolVector,
                                            size, nullptr, 0, nullptr, 0,
                                            GreaterThan, 0, 0);
  EXPECT_EQ(reinterpret_cast<int64_t>(resHandle.res), 2);
  EXPECT_EQ(resHandle.pStrErr, nullptr);

  int length = reinterpret_cast<int64_t>(resHandle.res);

  uint32_t expectedValues[2] = {1, 2};
  EXPECT_TRUE(
      equal(indexVector, indexVector + length,
            &expectedValues[0]));

  release(lhsBasePtr);
  release(rhsBasePtr);
  release(indexVector);
  release(boolVector);
}

// cppcheck-suppress *
TEST(BinaryTransformTest, CheckMeasureOutputIterator) {
  const int size = 3;
  uint32_t indexVectorH[size * 2] = {0, 1, 2};
  uint32_t baseCountsH[size + 1] = {0, 3, 9, 10};

  int lhsValuesH[size] = {-1, 1, 0};
  uint8_t lhsNullsH[size] = {1, 1, 0};

  uint8_t *lhsBasePtr =
      allocate_column(nullptr,
                      reinterpret_cast<uint8_t *>(&lhsValuesH[0]),
                      &lhsNullsH[0], 0, 12, 3);

  float rhsValuesH[size] = {1.1, -1.1, 0.1};
  uint8_t rhsNullsH[size] = {1, 1, 1};

  uint8_t *rhsBasePtr =
      allocate_column(nullptr,
                      reinterpret_cast<uint8_t *>(&rhsValuesH[0]),
                      &rhsNullsH[0], 0, 12, 3);

  float outputValuesH[size] = {0, 0, 0};

  float *outputValues = allocate(&outputValuesH[0], size);
  uint32_t *indexVector = allocate(&indexVectorH[0], size);
  uint32_t *baseCounts = allocate(&baseCountsH[0], size);

  ScratchSpaceVector lhsColumn = {lhsBasePtr, 16, Int32};

  ScratchSpaceVector rhsColumn = {rhsBasePtr, 16, Float32};

  MeasureOutputVector outputMeasure = {
      reinterpret_cast<uint32_t *>(outputValues), Float32,
      AGGR_SUM_FLOAT};

  InputVector lhs = {{.ScratchSpace = lhsColumn}, ScratchSpaceInput};
  InputVector rhs = {{.ScratchSpace = rhsColumn}, ScratchSpaceInput};
  OutputVector output = {{.Measure = outputMeasure}, MeasureOutput};

  BinaryTransform(lhs, rhs, output, indexVector, size, baseCounts,
                  0, Minus, 0, 0);
  float expectedValues[3] = {-6.3, 12.6, 0.0};

  EXPECT_TRUE(
      equal(outputValues, outputValues + 3,
            &expectedValues[0]));

  release(lhsBasePtr);
  release(rhsBasePtr);
  release(outputValues);
  release(indexVector);
  release(baseCounts);
}

// cppcheck-suppress *
TEST(InitIndexVectorTest, InitIndexVector) {
  const int size = 3;
  uint32_t indexVectorH[size * 2] = {0, 0, 0};
  uint32_t *indexVector = allocate(&indexVectorH[0], 3);
  InitIndexVector(indexVector, 0, 3, 0, 0);
  uint32_t expectedValues[3] = {0, 1, 2};
  EXPECT_TRUE(
      equal(indexVector, indexVector + size, &expectedValues[0]));
  release(indexVector);
}

// cppcheck-suppress *
TEST(HashLookupTest, CheckLookup) {
  // hash index created in golang cuckoo_hash_index
  uint8_t bucketsH[312] = {
      0, 0, 0, 0, 16, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0,
      0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 5, 0,
      0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 17,
      0, 0, 0, 0, 0, 0, 0, 11, 0, 0, 0, 0, 0, 0, 0,
      12, 0, 0, 0, 140, 236, 116, 56, 157, 195, 184, 133, 16, 0, 0,
      0, 3, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0, 6, 0,
      0, 0, 17, 0, 0, 0, 11, 0, 0, 0, 12, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0,
      0, 0, 0, 0, 15, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0,
      0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 10, 0,
      0, 0, 0, 0, 0, 0, 14, 0, 0, 0, 0, 0, 0, 0, 2,
      0, 0, 0, 121, 236, 209, 218, 160, 185, 109, 187, 0, 0, 0, 0,
      8, 0, 0, 0, 15, 0, 0, 0, 9, 0, 0, 0, 1, 0, 0,
      0, 10, 0, 0, 0, 14, 0, 0, 0, 2, 0, 0, 0, 0, 0,
      0, 0, 13, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 13, 0, 0, 0, 7,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

  uint8_t *buckets = allocate(bucketsH, 312);
  CuckooHashIndex hashIndex = {
      buckets,
      {(uint32_t) 2596996162, (uint32_t) 4039455774, (uint32_t) 2854263694,
       (uint32_t) 1879968118},
      4,
      4,
      2};
  const int size = 18;

  uint32_t indexVectorH[size] = {0, 1, 2, 3, 4, 5, 6, 7, 8,
                                 9, 10, 11, 12, 13, 14, 15, 16, 17};
  uint32_t inputValuesH[size] = {0, 1, 2, 3, 4, 5, 6, 7, 8,
                                 9, 10, 11, 12, 13, 14, 15, 16, 17};
  uint8_t inputNullsH[3] = {0xFF, 0xFF, 0xFF};

  uint8_t *basePtr =
      allocate_column(nullptr,
                      &inputNullsH[0], &inputValuesH[0], 0, 3, size * 4);

  RecordID outputValuesH[18];

  uint32_t *indexVector = allocate(&indexVectorH[0], size);

  RecordID *outputValues = allocate(&outputValuesH[0], size);

  DefaultValue defaultValue = {false, {.Int32Val = 0}};
  VectorPartySlice inputVP = {basePtr, 0, 8, 0, Int32, defaultValue};

  InputVector input = {{.VP = inputVP}, VectorPartyInput};
  HashLookup(input, outputValues, indexVector,
             size, nullptr, 0, hashIndex, 0, 0);

  RecordID expectedOutputValues[18];
  for (int i = 0; i < size; i++) {
    RecordID recordID = {(int32_t) 0, (uint32_t) i};
    expectedOutputValues[i] = recordID;
  }

  EXPECT_TRUE(
      equal(reinterpret_cast<uint8_t *>(outputValues),
            reinterpret_cast<uint8_t *>(outputValues) + sizeof(RecordID) * size,
            reinterpret_cast<uint8_t *>(&expectedOutputValues[0])));
  release(buckets);
  release(indexVector);
  release(basePtr);
  release(outputValues);
}

// cppcheck-suppress *
TEST(HashLookupTest, CheckUUID) {
  // hash index created in golang cuckoo_hash_index
  uint8_t bucketsH[600] = {
      0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 154, 0, 0, 0, 0, 0, 0, 0, 2, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 28, 22, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0};

  uint8_t *buckets = allocate(bucketsH, 600);
  CuckooHashIndex hashIndex = {
      buckets,
      {(uint32_t) 1904795661, (uint32_t) 1908039658, (uint32_t) 3167437076,
       (uint32_t) 4232957548},
      16,
      4,
      2};
  const int size = 3;

  uint32_t indexVectorH[size] = {0, 1, 2};
  UUIDT inputValuesH[size] = {{0, 0}, {1, 0}, {2, 0}};
  uint8_t inputNullsH[1] = {0xFF};

  uint8_t *basePtr =
      allocate_column(nullptr,
                      &inputNullsH[0], &inputValuesH[0], 0, 1, size * 16);

  RecordID outputValuesH[3];

  uint32_t *indexVector = allocate(&indexVectorH[0], size);

  RecordID *outputValues = allocate(&outputValuesH[0], size);

  DefaultValue defaultValue = {false, {}};
  VectorPartySlice inputVP = {basePtr, 0, 8, 0, UUID, defaultValue};

  InputVector input = {{.VP = inputVP}, VectorPartyInput};
  HashLookup(input, outputValues, indexVector,
             size, nullptr, 0, hashIndex, 0, 0);

  RecordID expectedOutputValues[3];
  for (int i = 0; i < size; i++) {
    RecordID recordID = {(int32_t) 0, (uint32_t) i};
    expectedOutputValues[i] = recordID;
  }

  EXPECT_TRUE(
      equal(reinterpret_cast<uint8_t *>(outputValues),
            reinterpret_cast<uint8_t *>(outputValues) + sizeof(RecordID) * size,
            reinterpret_cast<uint8_t *>(&expectedOutputValues[0])));
  release(buckets);
  release(indexVector);
  release(basePtr);
  release(outputValues);
}

// cppcheck-suppress *
TEST(ForeignTableColumnTransformTest, CheckTransform) {
  const int numBatches = 5;
  const int kNumRecords = 5;
  const int numRecordsInLastBatch = 5;
  int16_t* const timezoneLookup = nullptr;
  int16_t timezoneLookupSize = 0;
  int32_t baseBatchID = -2147483648;
  RecordID recordIDsH[kNumRecords];
  VectorPartySlice batches[kNumRecords];
  for (int i = 0; i < kNumRecords; i++) {
    RecordID recordID = {(int32_t) i + baseBatchID, (uint32_t) i};
    recordIDsH[i] = recordID;
  }
  RecordID *recordIDs = allocate(&recordIDsH[0], kNumRecords);
  int valuesH[5][5] = {{1, 0, 0, 0, 0},
                       {0, 2, 0, 0, 0},
                       {0, 0, 3, 0, 0},
                       {0, 0, 0, 4, 0},
                       {0, 0, 0, 0, 5}};

  uint8_t nullsH[1] = {0xFF};

  uint8_t *basePtrs[5];
  for (int i = 0; i < numBatches; i++) {
    uint8_t
        *basePtr = allocate_column(nullptr, nullsH, &valuesH[i][0], 0, 1, 20);
    VectorPartySlice foreignVP = {basePtr, 0, 8, 0, Int32};
    batches[i] = foreignVP;
    basePtrs[i] = basePtr;
  }

  DefaultValue defaultValue = {false, {.Int32Val = 0}};
  ForeignColumnVector foreignColumnVP = {recordIDs,
                                          batches,
                                          baseBatchID,
                                          (int32_t) numBatches,
                                          (int32_t) numRecordsInLastBatch,
                                          timezoneLookup,
                                          timezoneLookupSize,
                                          Int32,
                                          defaultValue};
  InputVector input = {{.ForeignVP = foreignColumnVP},
                       ForeignColumnInput};
  int outputValuesH[kNumRecords];
  bool outputNullsH[kNumRecords];

  uint8_t *outputBasePtr =
      allocate_column(nullptr, reinterpret_cast<uint8_t *>(&outputValuesH[0]),
                      &outputNullsH[0], 0, 20, 5);

  ScratchSpaceVector outputScratchSpace = {outputBasePtr, 24, Int32};
  OutputVector output = {{.ScratchSpace = outputScratchSpace},
                         ScratchSpaceOutput};

  int expectedValues[kNumRecords] = {-1, -2, -3, -4, -5};
  bool expectedNulls[kNumRecords] = {true, true, true, true, true};

  int *outputValues = reinterpret_cast<int *>(outputBasePtr);
  bool *outputNulls = reinterpret_cast<bool *>(outputBasePtr) + 24;

  UnaryTransform(input, output, nullptr, kNumRecords, nullptr, 0,
                 Negate, 0, 0);

  ForeignTableIterator<int> *vpIters = prepareForeignTableIterators(
      numBatches,
      batches,
      4,
      false,
      0, 0);
  RecordIDJoinIterator<int> inputIter(
      recordIDs,
      numBatches,
      baseBatchID,
      vpIters,
      numRecordsInLastBatch,
      timezoneLookup,
      timezoneLookupSize);

  outputValues = reinterpret_cast<int *>(outputBasePtr);
  outputNulls = reinterpret_cast<bool *>(outputBasePtr) + 24;

  EXPECT_TRUE(
      equal(outputValues, outputValues + kNumRecords, &expectedValues[0]));
  EXPECT_TRUE(equal(outputNulls, outputNulls + kNumRecords, &expectedNulls[0]));

  release(recordIDs);

  for (int i = 0; i < numBatches; i++) {
    release(basePtrs[i]);
  }
  release(outputBasePtr);
}

// cppcheck-suppress *
TEST(SortDimColumnVectorTest, CheckSort) {
  // test with 1 4-byte dim, 1 2-byte dim and 1 1-byte dim
  // with length = 3
  // numBytes=(4+2+1+3)*3=30
  uint8_t keysH[30] = {0};
  reinterpret_cast<uint32_t *>(keysH)[0] = 1;
  reinterpret_cast<uint32_t *>(keysH)[1] = 2;
  reinterpret_cast<uint32_t *>(keysH)[2] = 1;
  reinterpret_cast<uint16_t *>(keysH + 12)[0] = 1;
  reinterpret_cast<uint16_t *>(keysH + 12)[1] = 2;
  reinterpret_cast<uint16_t *>(keysH + 12)[2] = 1;
  (keysH + 18)[0] = 1;
  (keysH + 18)[1] = 2;
  (keysH + 18)[2] = 1;

  uint32_t indexH[3] = {0, 1, 2};
  uint64_t hashValuesH[3] = {0};

  uint8_t *keys = allocate(&keysH[0], 30);
  uint32_t *index = allocate(&indexH[0], 3);
  uint64_t *hashValues = allocate(&hashValuesH[0], 3);

  const int vectorCapacity = 3;
  const int length = 3;
  DimensionColumnVector keyColVector = {
      keys,
      hashValues,
      index,
      vectorCapacity,
      {(uint8_t)0, (uint8_t)0, (uint8_t)1, (uint8_t)1, (uint8_t)1}};
  Sort(keyColVector, length, 0, 0);

  uint32_t expectedIndex1[3] = {1, 0, 2};
  uint32_t expectedIndex2[3] = {0, 2, 1};
  EXPECT_TRUE(equal(index, index + 3, expectedIndex1) ||
      equal(index, index + 3, expectedIndex2));
}

// cppcheck-suppress *
TEST(ReduceDimColumnVectorTest, CheckReduce) {
  // test with 3 dimensions (4-byte, 2-byte, 1-byte)
  // each dimension vector has 6 elements with values assigned as [1,2,3,2,3,1]
  uint8_t inputDimValuesH[60] = {1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 2, 0, 0,
                                 0, 3, 0, 0, 0, 1, 0, 0, 0, 1, 0, 2, 0, 3, 0,
                                 2, 0, 3, 0, 1, 0, 1, 2, 3, 2, 3, 1, 1, 1, 1,
                                 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
  uint64_t inputHashValuesH[6] = {1, 1, 2, 2, 3, 3};
  uint32_t inputIndexVectorH[6] = {1, 3, 2, 4, 0, 5};
  uint32_t inputValuesH[6] = {5, 1, 3, 2, 4, 6};

  uint8_t outputDimValuesH[60] = {0};
  uint64_t outputHashValuesH[6] = {0};
  uint32_t outputIndexVectorH[6] = {0};
  uint32_t outputValuesH[6] = {0};

  uint8_t *inputDimValues = allocate(inputDimValuesH, 60);
  uint64_t *inputHashValues = allocate(inputHashValuesH, 6);
  uint32_t *inputIndexVector = allocate(inputIndexVectorH, 6);
  uint32_t *inputValues = allocate(inputValuesH, 6);

  uint8_t *outputDimValues = allocate(outputDimValuesH, 60);
  uint64_t *outputHashValues = allocate(outputHashValuesH, 6);
  uint32_t *outputIndexVector = allocate(outputIndexVectorH, 6);
  uint32_t *outputValues = allocate(outputValuesH, 6);

  uint32_t expectedValues[3] = {3, 7, 11};
  uint32_t expectedIndex[3] = {1, 2, 0};
  // output dimension values should be [2,3,1] for each dim vector
  uint8_t expectedDimValues[60] = {
      2, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 2, 0, 3, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 3, 1, 0,
      0, 0, 1, 1, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0,
  };
  int length = 6;
  int vectorCapacity = 6;
  DimensionColumnVector inputKeys = {
      inputDimValues,
      inputHashValues,
      inputIndexVector,
      vectorCapacity,
      {(uint8_t)0, (uint8_t)0, (uint8_t)1, (uint8_t)1, (uint8_t)1}};

  DimensionColumnVector outputKeys = {
      outputDimValues,
      outputHashValues,
      outputIndexVector,
      vectorCapacity,
      {(uint8_t)0, (uint8_t)0, (uint8_t)1, (uint8_t)1, (uint8_t)1}};
  CGoCallResHandle
      resHandle = Reduce(inputKeys,
                         reinterpret_cast<uint8_t *>(inputValues),
                         outputKeys,
                         reinterpret_cast<uint8_t *>(outputValues),
                         4,
                         length,
                         AGGR_SUM_UNSIGNED,
                         0,
                         0);
  EXPECT_EQ(reinterpret_cast<int64_t>(resHandle.res), 3);
  EXPECT_EQ(resHandle.pStrErr, nullptr);

  EXPECT_TRUE(equal(outputValues, outputValues + 3, expectedValues));
  EXPECT_TRUE(equal(outputIndexVector, outputIndexVector + 3, expectedIndex));
  EXPECT_TRUE(equal(outputDimValues, outputDimValues + 60, expectedDimValues));
}

TEST(SortAndReduceTest, CheckReduceByAvg) {
  // 6 elements 1 2 3 2 3 1
  uint8_t inputDimValuesH[30] = {1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 2, 0, 0,
                                 0, 3, 0, 0, 0, 1, 0, 0, 0, 1, 1, 1, 1, 1, 1};
  uint64_t inputHashValuesH[6] = {1, 1, 2, 2, 3, 3};
  uint32_t inputIndexVectorH[6] = {1, 3, 2, 4, 0, 5};
  float_t inputValuesH[12] = {5.0, 0, 1.0, 0, 3.0, 0, 2.0, 0, 4.0, 0, 6.0, 0};
  // set count to be 1.
  for (int i = 0; i < 6; i++) {
    *reinterpret_cast<uint32_t*>(&inputValuesH[i*2+1]) = uint32_t(1);
  }

  uint8_t outputDimValuesH[30] = {0};
  uint64_t outputHashValuesH[6] = {0};
  uint32_t outputIndexVectorH[6] = {0};
  uint64_t outputValuesH[6] = {0};

  uint8_t *inputDimValues = allocate(inputDimValuesH, 30);
  uint64_t *inputHashValues = allocate(inputHashValuesH, 6);
  uint32_t *inputIndexVector = allocate(inputIndexVectorH, 6);
  uint32_t *inputValues =
      allocate(reinterpret_cast<uint32_t*>(&inputValuesH[0]), 12);

  uint8_t *outputDimValues = allocate(outputDimValuesH, 30);
  uint64_t *outputHashValues = allocate(outputHashValuesH, 6);
  uint32_t *outputIndexVector = allocate(outputIndexVectorH, 6);
  uint64_t *outputValues = allocate(outputValuesH, 6);

  float_t expectedValuesF[6] = {1.5, 0, 3.5, 0, 5.5, 0};
  uint64_t* expectedValues = reinterpret_cast<uint64_t*>(&expectedValuesF[0]);
  for (int i = 0; i < 3; i++) {
    *(reinterpret_cast<uint32_t *>(&expectedValues[i]) + 1) = 2;
  }

  uint32_t expectedIndex[3] = {1, 2, 0};
  // output dimension values should be [2,3,1] for each dim vector
  uint8_t expectedDimValues[30] = {
      2, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0,
  };

  int length = 6;
  int vectorCapacity = 6;
  DimensionColumnVector inputKeys = {
      inputDimValues,
      inputHashValues,
      inputIndexVector,
      vectorCapacity,
      {(uint8_t)0, (uint8_t)0, (uint8_t)1, (uint8_t)0, (uint8_t)0}};

  DimensionColumnVector outputKeys = {
      outputDimValues,
      outputHashValues,
      outputIndexVector,
      vectorCapacity,
      {(uint8_t)0, (uint8_t)0, (uint8_t)1, (uint8_t)0, (uint8_t)0}};
  CGoCallResHandle
      resHandle = Reduce(inputKeys,
                         reinterpret_cast<uint8_t *>(inputValues),
                         outputKeys,
                         reinterpret_cast<uint8_t *>(outputValues),
                         8,
                         length,
                         AGGR_AVG_FLOAT, 0, 0);
  EXPECT_EQ(reinterpret_cast<int64_t>(resHandle.res), 3);
  EXPECT_EQ(resHandle.pStrErr, nullptr);

  EXPECT_TRUE(equal(outputValues, outputValues + 3, expectedValues));
  EXPECT_TRUE(equal(outputIndexVector, outputIndexVector + 3, expectedIndex));
  EXPECT_TRUE(equal(outputDimValues, outputDimValues + 30, expectedDimValues));
}

// cppcheck-suppress *
TEST(SortAndReduceTest, CheckHash) {
  int size = 8;

  uint8_t dimValuesH[16] = {2, 1, 0, 3, 0, 1, 2, 3, 1, 1, 1, 1, 1, 1, 1, 1};
  uint32_t measureValuesH[8] = {1, 1, 1, 1, 1, 1, 1, 1};
  uint64_t hashValuesH[8] = {0};
  uint32_t indexValuesH[8] = {0};

  uint8_t dimValuesOutH[16] = {0};
  uint32_t measureValuesOutH[8] = {0};
  uint64_t hashValuesOutH[8] = {0};
  uint32_t indexValuesOutH[8] = {0};

  uint8_t *dimValues = allocate(dimValuesH, 16);
  uint32_t *measureValues = allocate(measureValuesH, 8);
  uint64_t *hashValues = allocate(hashValuesH, 8);
  uint32_t *indexValues = allocate(indexValuesH, 8);

  uint8_t *dimValuesOut = allocate(dimValuesOutH, 16);
  uint32_t *measureValuesOut = allocate(measureValuesOutH, 8);
  uint64_t *hashValuesOut = allocate(hashValuesOutH, 8);
  uint32_t *indexValuesOut = allocate(indexValuesOutH, 8);

  InitIndexVector(indexValues, 0, size, 0, 0);

  const int length = 8;
  const int vectorCapacity = 8;
  DimensionColumnVector inputKeyColVector = {
      dimValues,
      hashValues,
      indexValues,
      vectorCapacity,
      {(uint8_t)0, (uint8_t)0, (uint8_t)0, (uint8_t)0, (uint8_t)1}};

  DimensionColumnVector outputKeyColVector = {
      dimValuesOut,
      hashValuesOut,
      indexValuesOut,
      vectorCapacity,
      {(uint8_t)0, (uint8_t)0, (uint8_t)0, (uint8_t)0, (uint8_t)1}};

  Sort(inputKeyColVector, length, 0, 0);
  CGoCallResHandle resHandle =
      Reduce(inputKeyColVector, reinterpret_cast<uint8_t *>(measureValues),
             outputKeyColVector, reinterpret_cast<uint8_t *>(measureValuesOut),
             4, length, AGGR_SUM_UNSIGNED, nullptr, 0);

  uint8_t expectedDims[16] = {2, 0, 3, 1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0};
  uint32_t expectedMeasures[8] = {2, 2, 2, 2, 0, 0, 0, 0};
  uint32_t expectedIndexes[8] = {0, 2, 3, 1, 0, 0, 0, 0};

  EXPECT_EQ(reinterpret_cast<int64_t>(resHandle.res), 4);
  EXPECT_EQ(resHandle.pStrErr, nullptr);

  EXPECT_TRUE(equal(dimValuesOut, dimValuesOut + 16, &expectedDims[0]));
  EXPECT_TRUE(
      equal(measureValuesOut, measureValuesOut + 8, &expectedMeasures[0]));
  EXPECT_TRUE(equal(indexValuesOut, indexValuesOut + 8, &expectedIndexes[0]));
  release(dimValues);
  release(measureValues);
  release(hashValues);
  release(indexValues);
  release(dimValuesOut);
  release(measureValuesOut);
  release(hashValuesOut);
  release(indexValuesOut);
}

// cppcheck-suppress *
TEST(HyperLogLogTest, CheckSparseMode) {
  int prevResultSize = 0;
  int curBatchSize = 8;
  const int vectorCapacity = 8;

  uint8_t prevDimH[16] = {1, 1, 2, 2, 3, 3, 4, 4, 1, 1, 1, 1, 1, 1, 1, 1};
  uint32_t prevValuesH[8] = {0};
  uint64_t prevHashH[8] = {0};
  uint32_t prevIndexH[8] = {0};

  uint8_t curDimH[16] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
  uint32_t curValuesH[8] = {0x010001, 0x020002, 0x010002, 0x020002,
                            0x010003, 0x020003, 0x010004, 0x020004};
  uint32_t curIndexH[8] = {0, 1, 2, 3, 4, 5, 6, 7};
  uint64_t curHashH[8] = {0};

  uint8_t *prevDim = allocate(prevDimH, 16);
  uint32_t *prevValues = allocate(prevValuesH, 8);
  uint64_t *prevHash = allocate(prevHashH, 8);
  uint32_t *prevIndex = allocate(prevIndexH, 8);

  uint8_t *curDim = allocate(curDimH, 16);
  uint32_t *curValues = allocate(curValuesH, 8);
  uint64_t *curHash = allocate(curHashH, 8);
  uint32_t *curIndex = allocate(curIndexH, 8);

  DimensionColumnVector prevDimOut = {
      prevDim,
      prevHash,
      prevIndex,
      vectorCapacity,
      {(uint8_t)0, (uint8_t)0, (uint8_t)0, (uint8_t)0, (uint8_t)1}};

  DimensionColumnVector curDimOut = {
      curDim,
      curHash,
      curIndex,
      vectorCapacity,
      {(uint8_t)0, (uint8_t)0, (uint8_t)0, (uint8_t)0, (uint8_t)1}};

  uint8_t *hllVector;
  size_t hllVectorSize;
  uint16_t *hllDimRegIDCount;
  CGoCallResHandle
      resHandle = HyperLogLog(prevDimOut, curDimOut, prevValues, curValues,
                              prevResultSize, curBatchSize, true, &hllVector,
                              &hllVectorSize, &hllDimRegIDCount, 0, 0);

  EXPECT_EQ(reinterpret_cast<int64_t>(resHandle.res), 4);
  EXPECT_EQ(resHandle.pStrErr, nullptr);

  int resSize = reinterpret_cast<int64_t>(resHandle.res);

  EXPECT_EQ(resSize, 4);
  EXPECT_EQ(hllVectorSize, 20);
  uint8_t expectedDims[16] = {2, 4, 3, 1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0};
  uint8_t expectedHLL[20] = {2, 0, 3, 0, 4, 0, 3, 0, 3, 0,
                             3, 0, 1, 0, 2, 0, 2, 0, 3, 0};
  uint16_t expectedDimRegIDCount[4] = {1, 1, 1, 2};
  EXPECT_TRUE(equal(curDim, curDim + 16, &expectedDims[0]));
  EXPECT_TRUE(equal(hllVector, hllVector + hllVectorSize, &expectedHLL[0]));
  EXPECT_TRUE(equal(hllDimRegIDCount, hllDimRegIDCount + resSize,
                    &expectedDimRegIDCount[0]));
  release(prevDim);
  release(curDim);
  release(prevValues);
  release(prevHash);
  release(prevIndex);
  release(curValues);
  release(curHash);
  release(curIndex);
  release(hllVector);
  release(hllDimRegIDCount);
}

// cppcheck-suppress *
TEST(HyperLogLogTest, CheckDenseMode) {
  int prevResultSize = 0;
  int curBatchSize = 5000;
  const int vectorCapacity = 5000;

  uint8_t prevDimH[10000] = {0};
  // {1,1,2,2,0,0, ... }
  prevDimH[0] = 1;
  prevDimH[1] = 1;
  prevDimH[2] = 2;
  prevDimH[3] = 2;
  for (int i = 5000; i < 10000; i++) {
    prevDimH[i] = 1;
  }
  uint32_t prevValuesH[5000] = {0};
  uint64_t prevHashH[5000] = {0};
  uint32_t prevIndexH[5000] = {0};

  uint8_t curDimH[10000] = {0};
  uint32_t curValuesH[5000] = {0};
  curValuesH[0] = 0x010001;
  curValuesH[1] = 0x020002;
  curValuesH[2] = 0x010002;
  curValuesH[3] = 0x020002;
  for (int i = 4; i < 5000; i++) {
    // 4996 registers
    curValuesH[i] = (0x010000 | (i - 4));
  }
  uint32_t curIndexH[5000] = {0};
  for (int i = 0; i < 5000; i++) {
    curIndexH[i] = i;
  }
  uint64_t curHashH[5000] = {0};

  uint8_t *prevDim = allocate(prevDimH, 10000);
  uint32_t *prevValues = allocate(prevValuesH, 5000);
  uint64_t *prevHash = allocate(prevHashH, 5000);
  uint32_t *prevIndex = allocate(prevIndexH, 5000);

  uint8_t *curDim = allocate(curDimH, 10000);
  uint32_t *curValues = allocate(curValuesH, 5000);
  uint64_t *curHash = allocate(curHashH, 5000);
  uint32_t *curIndex = allocate(curIndexH, 5000);

  DimensionColumnVector prevDimOut = {
      prevDim,
      prevHash,
      prevIndex,
      vectorCapacity,
      {(uint8_t)0, (uint8_t)0, (uint8_t)0, (uint8_t)0, (uint8_t)1}};

  DimensionColumnVector curDimOut = {
      curDim,
      curHash,
      curIndex,
      vectorCapacity,
      {(uint8_t)0, (uint8_t)0, (uint8_t)0, (uint8_t)0, (uint8_t)1}};

  uint8_t *hllVector;
  size_t hllVectorSize;
  uint16_t *hllDimRegIDCount;
  CGoCallResHandle
      resHandle = HyperLogLog(prevDimOut, curDimOut, prevValues, curValues,
                              prevResultSize, curBatchSize, true, &hllVector,
                              &hllVectorSize, &hllDimRegIDCount, 0, 0);

  EXPECT_EQ(reinterpret_cast<int64_t>(resHandle.res), 3);
  EXPECT_EQ(resHandle.pStrErr, nullptr);

  int resSize = reinterpret_cast<int64_t>(resHandle.res);

  EXPECT_EQ(resSize, 3);
  EXPECT_EQ(hllVectorSize, 16396);
  uint8_t expectedDims[10000] = {0};
  expectedDims[0] = 2;
  expectedDims[1] = 0;
  expectedDims[2] = 1;
  expectedDims[5000] = 1;
  expectedDims[5001] = 1;
  expectedDims[5002] = 1;
  uint8_t expectedHLL[16396] = {0};
  expectedHLL[0] = 2;
  expectedHLL[1] = 0;
  expectedHLL[2] = 3;
  expectedHLL[3] = 0;
  for (int i = 4; i < 5000; i++) {
    expectedHLL[i] = 2;
  }
  expectedHLL[16388] = 1;
  expectedHLL[16389] = 0;
  expectedHLL[16390] = 2;
  expectedHLL[16391] = 0;
  expectedHLL[16392] = 2;
  expectedHLL[16393] = 0;
  expectedHLL[16394] = 3;
  expectedHLL[16395] = 0;

  uint16_t expectedDimRegIDCount[3] = {1, 4996, 2};
  EXPECT_TRUE(equal(curDim, curDim + 10000, &expectedDims[0]));
  EXPECT_TRUE(equal(hllVector, hllVector + hllVectorSize, &expectedHLL[0]));
  EXPECT_TRUE(equal(hllDimRegIDCount, hllDimRegIDCount + resSize,
                    &expectedDimRegIDCount[0]));
  release(prevDim);
  release(curDim);
  release(prevValues);
  release(prevHash);
  release(prevIndex);
  release(curValues);
  release(curHash);
  release(curIndex);
  release(hllVector);
  release(hllDimRegIDCount);
}

// cppcheck-suppress *
TEST(DateFunctorsTest, CheckGetStarts) {
  // First ensure the constant memory is properly initialized.
  BootstrapDevice();

  const int size = 3;
  uint32_t indexVectorH[size * 2] = {0, 1, 2};
  uint32_t *indexVector = allocate(&indexVectorH[0], size * 2);

  uint32_t inputValuesH[size] = {0, get_ts(2018, 6, 11), get_ts(1970, 1, 1)};
  bool inputNullsH[size] = {false, true, true};
  uint8_t *basePtr =
      allocate_column(nullptr,
                      reinterpret_cast<uint8_t *>(&inputValuesH[0]),
                      &inputNullsH[0], 0, 12, 3);

  uint32_t outputValuesH[size] = {0, 0, 0};
  bool outputNullsH[size] = {false, false, false};
  uint8_t *outputBasePtr =
      allocate_column(nullptr, reinterpret_cast<uint8_t *>(&outputValuesH[0]),
                      &outputNullsH[0], 0, 12, 3);

  ScratchSpaceVector inputScratchSpace = {basePtr, 16, Int32};
  ScratchSpaceVector outputScratchSpace = {outputBasePtr, 16, Int32};

  InputVector
      input = {{.ScratchSpace = inputScratchSpace}, ScratchSpaceInput};
  OutputVector output = {{.ScratchSpace = outputScratchSpace},
                         ScratchSpaceOutput};

  UnaryTransform(input, output, indexVector, size, nullptr, 0,
                 GetMonthStart, 0, 0);

  uint32_t *outputValues = reinterpret_cast<uint32_t *>(outputBasePtr);
  bool *outputNulls = reinterpret_cast<bool *>(outputBasePtr) + 16;

  uint32_t expectedValues[3] = {0, get_ts(2018, 6, 1), get_ts(1970, 1, 1)};
  bool expectedNulls[3] = {false, true, true};

  EXPECT_TRUE(
      equal(outputValues, outputValues + size,
                  &expectedValues[0]));

  EXPECT_TRUE(
      equal(outputNulls, outputNulls + size,
                  &expectedNulls[0]));

  // Check get quarter start.
  UnaryTransform(input, output, indexVector, size, nullptr, 0,
                 GetQuarterStart, 0, 0);

  uint32_t expectedValues2[3] = {0, get_ts(2018, 4, 1), get_ts(1970, 1, 1)};
  bool expectedNulls2[3] = {false, true, true};

  EXPECT_TRUE(
      equal(outputValues, outputValues + size,
                  &expectedValues2[0]));

  EXPECT_TRUE(
      equal(outputNulls, outputNulls + size,
                  &expectedNulls2[0]));

  // Check get year start.
  UnaryTransform(input, output, indexVector, size, nullptr, 0,
                 GetYearStart, 0, 0);

  uint32_t expectedValues3[3] = {0, get_ts(2018, 1, 1), get_ts(1970, 1, 1)};
  bool expectedNulls3[3] = {false, true, true};

  EXPECT_TRUE(
      equal(outputValues, outputValues + size,
            &expectedValues3[0]));

  EXPECT_TRUE(
      equal(outputNulls, outputNulls + size,
            &expectedNulls3[0]));

  release(basePtr);
  release(outputBasePtr);
  release(indexVector);
}

// cppcheck-suppress *
TEST(GeoBatchIntersectTest, CheckInShape) {
  // 3 shapes
  // 1. Square with (1,1), (1,-1), (-1,-1), (-1, 1)
  // 2. Triangle with (3,3),(2,2),(4,2)
  // 3. Square with a hole (0,6),(3,6),(3,3),(0,3), hole (1,5),(2,5),(2,4),(1,4)
  float shapeLatsH[20] = {1, 1, -1, -1, 1,       3, 2, 4, 3, 0,
                           3, 3, 0,  0,  FLT_MAX, 1, 2, 2, 1, 1};
  float shapeLongsH[20] = {1, -1, -1, 1, 1,       3, 2, 2, 3, 6,
                            6, 3,  3,  6, FLT_MAX, 5, 5, 4, 4, 5};
  uint8_t shapeIndexsH[20] = {0, 0, 0, 0, 0, 1, 1, 1, 1, 2,
                              2, 2, 2, 2, 2, 2, 2, 2, 2, 2};
  GeoShapeBatch geoShapes =
      get_geo_shape_batch(shapeLatsH, shapeLongsH, shapeIndexsH, 3, 20);

  uint32_t indexVectorH[5] = {0, 1, 2, 3, 4};
  uint32_t *indexVector = allocate(indexVectorH, 5);

  // 5 points (0,0),(3,2.5),(1.5, 3.5),(1.5,4.5),null
  //           in 1   in 2    in 3       out      out
  GeoPointT pointsH[5] = {{0, 0}, {3, 2.5}, {1.5, 3.5}, {1.5, 4.5}, {0, 0}};
  uint8_t nullsH[1] = {0x0F};

  uint32_t outputPredicateH[5] = {0};
  uint32_t *outputPredicate = allocate(outputPredicateH, 5);

  // in.
  bool inOrOut = true;

  uint8_t *basePtr =
      allocate_column(nullptr, &nullsH[0], &pointsH[0], 0, 1, 40);

  DefaultValue defaultValue = {false};
  VectorPartySlice inputVP = {basePtr, 0, 8, 0, GeoPoint, defaultValue, 5};
  InputVector points = {{.VP = inputVP}, VectorPartyInput};

  CGoCallResHandle resHandle =
      GeoBatchIntersects(geoShapes, points, indexVector, 5, 0, nullptr, 0,
                         outputPredicate, inOrOut, 0, 0);

  EXPECT_EQ(reinterpret_cast<int64_t>(resHandle.res), 3);
  EXPECT_EQ(resHandle.pStrErr, nullptr);
  uint32_t expectedOutputPredicate[5] = {1, 2, 4, 0, 0};
  EXPECT_TRUE(
      equal(outputPredicate, outputPredicate + 5, expectedOutputPredicate));

  release(outputPredicate);
  release(indexVector);
  release(basePtr);
  release(geoShapes);
}

// cppcheck-suppress *
TEST(GeoBatchIntersectTest, CheckRecordIDJoinIterator) {
  // 3 shapes
  // 1. Square with (1,1), (1,-1), (-1,-1), (-1, 1)
  // 2. Triangle with (3,3),(2,2),(4,2)
  // 3. Square with a hole (0,6),(3,6),(3,3),(0,3), hole (1,5),(2,5),(2,4),(1,4)
  float shapeLatsH[20] = {1, 1, -1, -1, 1,       3, 2, 4, 3, 0,
                           3, 3, 0,  0,  FLT_MAX, 1, 2, 2, 1, 1};
  float shapeLongsH[20] = {1, -1, -1, 1, 1,       3, 2, 2, 3, 6,
                            6, 3,  3,  6, FLT_MAX, 5, 5, 4, 4, 5};
  uint8_t shapeIndexsH[20] = {0, 0, 0, 0, 0, 1, 1, 1, 1, 2,
                              2, 2, 2, 2, 2, 2, 2, 2, 2, 2};
  GeoShapeBatch geoShapes =
      get_geo_shape_batch(shapeLatsH, shapeLongsH, shapeIndexsH, 3, 20);

  uint32_t indexVectorH[5] = {0, 1, 2, 3, 4};
  uint32_t *indexVector = allocate(indexVectorH, 5);

  // 5 points (0,0),(3,2.5),(1.5, 3.5),(1.5,4.5),null
  //           in 1   in 2    in 3       out      out
  const int numBatches = 5;
  const int kNumRecords = 5;
  const int numRecordsInLastBatch = 5;
  int32_t baseBatchID = -2147483648;
  RecordID recordIDsH[kNumRecords];
  VectorPartySlice batches[kNumRecords];
  for (int i = 0; i < kNumRecords; i++) {
    RecordID recordID = {(int32_t) i + baseBatchID, (uint32_t) i};
    recordIDsH[i] = recordID;
  }
  RecordID *recordIDs = allocate(&recordIDsH[0], kNumRecords);
  GeoPointT valuesH[5][5];
  GeoPointT pointsH[5] = {{0, 0}, {3, 2.5}, {1.5, 3.5}, {1.5, 4.5}, {0, 0}};
  for (int i = 0; i < kNumRecords; i++) {
    valuesH[i][i] = pointsH[i];
  }
  uint8_t nullsH[1] = {0x0F};

  uint8_t *basePtrs[5];
  for (int i = 0; i < numBatches; i++) {
    uint8_t *basePtr =
        allocate_column(nullptr, nullsH, &valuesH[i][0], 0, 1, 40);
    VectorPartySlice foreignVP = {basePtr, 0, 8, 0, GeoPoint};
    batches[i] = foreignVP;
    basePtrs[i] = basePtr;
  }

  DefaultValue defaultValue = {false, {}};
  ForeignColumnVector foreignColumnVP = {recordIDs,
                                         batches,
                                         baseBatchID,
                                         (int32_t) numBatches,
                                         (int32_t) numRecordsInLastBatch,
                                         nullptr,
                                         0,
                                         GeoPoint,
                                         defaultValue};
  InputVector points = {{.ForeignVP = foreignColumnVP},
                       ForeignColumnInput};

  uint32_t outputPredicateH[5] = {0};
  uint32_t *outputPredicate = allocate(outputPredicateH, 5);

  CGoCallResHandle resHandle =
      GeoBatchIntersects(geoShapes, points, indexVector, 5, 0, nullptr, 0,
                         outputPredicate, true, 0, 0);

  EXPECT_EQ(reinterpret_cast<int64_t>(resHandle.res), 3);
  EXPECT_EQ(resHandle.pStrErr, nullptr);
  uint32_t expectedOutputPredicate[5] = {1, 2, 4, 0, 0};
  EXPECT_TRUE(
      equal(outputPredicate, outputPredicate + 5, expectedOutputPredicate));

  release(outputPredicate);
  release(indexVector);
  release(recordIDs);
  release(geoShapes);
  for (int i = 0; i < numBatches; i++) {
    release(basePtrs[i]);
  }
}

// cppcheck-suppress *
TEST(GeoBatchIntersectTest, CheckNotInShape) {
  // 3 shapes
  // 1. Square with (1,1), (1,-1), (-1,-1), (-1, 1)
  // 2. Triangle with (3,3),(2,2),(4,2)
  // 3. Square with a hole (0,6),(3,6),(3,3),(0,3), hole (1,5),(2,5),(2,4),(1,4)
  float shapeLatsH[20] = {1, 1, -1, -1, 1,       3, 2, 4, 3, 0,
                          3, 3, 0,  0,  FLT_MAX, 1, 2, 2, 1, 1};
  float shapeLongsH[20] = {1, -1, -1, 1, 1,       3, 2, 2, 3, 6,
                           6, 3,  3,  6, FLT_MAX, 5, 5, 4, 4, 5};
  uint8_t shapeIndexsH[20] = {0, 0, 0, 0, 0, 1, 1, 1, 1, 2,
                              2, 2, 2, 2, 2, 2, 2, 2, 2, 2};
  GeoShapeBatch geoShapes =
      get_geo_shape_batch(shapeLatsH, shapeLongsH, shapeIndexsH, 3, 20);

  uint32_t indexVectorH[5] = {0, 1, 2, 3, 4};
  uint32_t *indexVector = allocate(indexVectorH, 5);

  // 5 points (0,0),(3,2.5),(1.5, 3.5),(1.5,4.5),null
  //           in 1   in 2    in 3       out      out
  GeoPointT pointsH[5] = {{0, 0}, {3, 2.5}, {1.5, 3.5}, {1.5, 4.5}, {0, 0}};
  uint8_t nullsH[1] = {0x0F};

  uint32_t outputPredicateH[5] = {0};
  uint32_t *outputPredicate = allocate(outputPredicateH, 5);

  // in.
  bool inOrOut = false;

  uint8_t *basePtr =
      allocate_column(nullptr, &nullsH[0], &pointsH[0], 0, 1, 40);

  DefaultValue defaultValue = {false};
  VectorPartySlice inputVP = {basePtr, 0, 8, 0, GeoPoint, defaultValue, 5};
  InputVector points = {{.VP = inputVP}, VectorPartyInput};
  CGoCallResHandle resHandle =
      GeoBatchIntersects(geoShapes, points, indexVector, 5, 0, nullptr, 0,
                         outputPredicate, inOrOut, 0, 0);

  EXPECT_EQ(reinterpret_cast<int64_t>(resHandle.res), 1);
  EXPECT_EQ(resHandle.pStrErr, nullptr);
  uint32_t expectedOutputPredicate[5] = {1, 2, 4, 0, 1};
  EXPECT_TRUE(
      equal(outputPredicate, outputPredicate + 5, expectedOutputPredicate));

  release(outputPredicate);
  release(indexVector);
  release(basePtr);
  release(geoShapes);
}

// cppcheck-suppress *
TEST(GeoBatchIntersectionJoinTest, DimensionWriting) {
  float shapeLatsH[15] = {
      1,  1, -1,        -1,        1,         2,         2,        -2,
      -2, 2, 1.5 + 0.1, 1.5 + 0.1, 1.5 - 0.1, 1.5 - 0.1, 1.5 + 0.1};
  float shapeLongsH[15] = {
      1, -1, -1,        1,         1,         2,         -2,       -2,
      2, 2,  3.5 + 0.1, 3.5 - 0.1, 3.5 + 0.1, 3.5 - 0.1, 3.5 + 0.1};
  uint8_t shapeIndexsH[15] = {0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2};
  GeoShapeBatch geoShapes =
      get_geo_shape_batch(shapeLatsH, shapeLongsH, shapeIndexsH, 3, 15);
  uint32_t indexVectorH[5] = {0, 1, 2, 3, 4};
  uint32_t *indexVector = allocate(indexVectorH, 5);

  // 5 points  (1.5,1.5) (0,0) (1.5,4.5) (1.5, 3.5)   null
  //             in2     in1,2  out            in3        out
  GeoPointT pointsH[5] = {{1.5, 1.5}, {0, 0}, {1.5, 4.5}, {1.5, 3.5}, {0, 0}};
  uint8_t nullsH[1] = {0x0F};

  uint32_t outputPredicateH[5] = {0};
  uint32_t *outputPredicate = allocate(outputPredicateH, 5);

  uint8_t *basePtr =
      allocate_column(nullptr, &nullsH[0], &pointsH[0], 0, 1, 40);

  DefaultValue defaultValue = {false};
  VectorPartySlice inputVP = {basePtr, 0, 8, 0, GeoPoint, defaultValue, 5};

  uint8_t outputValuesH[10];
  thrust::fill(std::begin(outputValuesH), std::end(outputValuesH), 0);
  uint8_t *outputValues = allocate(&outputValuesH[0], 10);
  DimensionOutputVector outputDimension = {outputValues, outputValues + 5,
                                           Uint8};

  InputVector points = {{.VP = inputVP}, VectorPartyInput};
  CGoCallResHandle resHandle =
      GeoBatchIntersects(geoShapes, points, indexVector, 5,
                         0, nullptr, 0, outputPredicate, true, 0, 0);

  EXPECT_EQ(reinterpret_cast<int64_t>(resHandle.res), 3);
  EXPECT_EQ(resHandle.pStrErr, nullptr);

  resHandle = WriteGeoShapeDim(1, outputDimension, 5, outputPredicate, 0, 0);

  uint32_t expectedOutputPredicate[5] = {2, 3, 0, 4, 0};
  GeoPredicateIterator geoIter(expectedOutputPredicate, 1);
  EXPECT_TRUE(
      equal(outputPredicate, outputPredicate + 5, expectedOutputPredicate));
  uint8_t expectedDimValues[5] = {1, 0, 2, 0, 0};
  uint8_t expectedDimNulls[5] = {1, 1, 1, 0, 0};
  EXPECT_TRUE(equal(outputDimension.DimValues,
      outputDimension.DimValues + 5, expectedDimValues));
  EXPECT_TRUE(equal(outputDimension.DimNulls,
      outputDimension.DimNulls + 5, expectedDimNulls));

  release(outputPredicate);
  release(indexVector);
  release(basePtr);
  release(geoShapes);
}
}  // namespace ares
