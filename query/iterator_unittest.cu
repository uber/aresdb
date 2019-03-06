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

#include <thrust/execution_policy.h>
#include <thrust/fill.h>
#include <thrust/iterator/discard_iterator.h>
#include <thrust/sequence.h>
#include <thrust/sort.h>
#include <cstdint>
#include <algorithm>
#include <cfloat>
#include <iostream>
#include <iterator>
#include <tuple>
#include "gtest/gtest.h"
#include "query/unittest_utils.hpp"
#include "query/functor.hpp"
#include "query/iterator.hpp"

namespace ares {
// cppcheck-suppress *
TEST(BoolValueIteratorTest, CheckDereference) {
  uint32_t indexVectorH[8] = {0, 1, 2, 3, 4, 5, 6, 7};
  uint32_t *indexVector = allocate(&indexVectorH[0], 8);
  uint8_t nullsH[1] = {0xF0};
  uint8_t valuesH[1] = {0xF0};

  uint8_t *basePtr = allocate_column(nullptr, &nullsH[0], &valuesH[0], 0, 1, 1);

  printf("1\n");

  ColumnIterator<bool> begin = make_column_iterator<bool>(indexVector,
                                                          nullptr, 0, basePtr,
                                                          0, 8, 8, 1, 0);
  printf("2\n");
  bool expected[8] = {false, false, false, false, true, true, true, true};
  printf("3\n");
  EXPECT_TRUE(compare_value(begin, begin + 8, &expected[0]));
  printf("4\n");
  release(indexVector);
  release(basePtr);
}

// cppcheck-suppress *
TEST(BoolValueIteratorTest, CheckNullOffset) {
  uint32_t indexVectorH[4] = {0, 1, 2, 3};
  uint32_t *indexVector = allocate(&indexVectorH[0], 4);
  uint8_t nullsH[1] = {0xF0};
  uint8_t valuesH[1] = {0xF0};

  uint8_t *basePtr = allocate_column(nullptr, &nullsH[0], &valuesH[0], 0, 1, 1);

  ColumnIterator<bool> begin = make_column_iterator<bool>(indexVector,
                                                          nullptr,
                                                          0,
                                                          basePtr,
                                                          0,
                                                          8,
                                                          4,
                                                          1,
                                                          4);
  bool expected[4] = {true, true, true, true};
  EXPECT_TRUE(compare_value(begin, begin + 4, &expected[0]));
  release(basePtr);
  release(indexVector);
}

// cppcheck-suppress *
TEST(BoolValueIteratorTest, CheckMovingOperator) {
  // 7 6 5 4 3 2 1 0 | 7 6 5 4 3 2 1 0
  // 1 1 1 1 0 0 0 0 | 0 0 0 0 1 1 1 1
  uint32_t indexVectorH[16];
  thrust::sequence(&indexVectorH[0], &indexVectorH[16]);
  uint32_t *indexVector = allocate(&indexVectorH[0], 16);

  uint8_t nullsH[2] = {0xF0, 0x0F};
  uint8_t valuesH[2] = {0xF0, 0x0F};

  uint8_t *basePtr = allocate_column(nullptr, &nullsH[0], &valuesH[0], 0, 2, 2);

  ColumnIterator<bool> begin = make_column_iterator<bool>(indexVector,
                                                          nullptr, 0,
                                                          basePtr, 0,
                                                          8, 16, 1, 0);
  bool expected = false;
  EXPECT_TRUE(compare_value(begin, begin + 1, &expected));

  begin++;  // 1
  expected = false;
  EXPECT_TRUE(compare_value(begin, begin + 1, &expected));

  begin += 3;  // 4
  expected = true;
  EXPECT_TRUE(compare_value(begin, begin + 1, &expected));

  // Cross byte boundary
  begin += 8;  // 4 in byte 1
  expected = false;
  EXPECT_TRUE(compare_value(begin, begin + 1, &expected));

  begin--;  // 3 in byte 1
  expected = true;
  EXPECT_TRUE(compare_value(begin, begin + 1, &expected));

  // Go back to byte 0
  begin -= 8;  // 3 in byte 0
  expected = false;
  EXPECT_TRUE(compare_value(begin, begin + 1, &expected));

  ColumnIterator<bool>
      anotherBegin = make_column_iterator<bool>(indexVector,
                                                nullptr,
                                                0, basePtr,
                                                0, 8, 16, 1, 0);
  ColumnIterator<bool> anotherEnd = anotherBegin + 16;
  EXPECT_EQ(16, anotherEnd - anotherBegin);
  release(basePtr);
  release(indexVector);
}

// cppcheck-suppress *
TEST(ScratchSpaceIteratorTest, CheckBool) {
  uint8_t nullsH[5] = {1, 1, 1, 1, 1};
  bool valuesH[5] = {0, 1, 1, 0, 0};
  uint8_t *basePtr =
      allocate_column(nullptr, reinterpret_cast<uint8_t *>(&valuesH[0]),
                      &nullsH[0], 0, 5, 5);
  SimpleIterator<bool> begin = make_scratch_space_input_iterator<bool>(
      basePtr, 8);
  SimpleIterator<bool> end = begin + 5;
  bool expectedValues[5] = {false, true, true, false, false};
  EXPECT_TRUE(compare_value(begin, end, std::begin(expectedValues)));
  release(basePtr);
}

// cppcheck-suppress *
TEST(ScratchSpaceIteratorTest, CheckInt) {
  uint8_t nullsH[5] = {1, 1, 0, 1, 1};
  int32_t valuesH[5] = {1000000, -1, 1, -1000000, 0};
  uint8_t *basePtr =
      allocate_column(nullptr, reinterpret_cast<uint8_t *>(&valuesH[0]),
                      &nullsH[0], 0, 20, 5);
  SimpleIterator<int32_t> begin = make_scratch_space_input_iterator<int32_t>(
      basePtr, 24);
  SimpleIterator<int32_t> end = begin + 5;
  int32_t expectedValues[5] = {1000000, -1, 1, -1000000, 0};
  uint8_t expectedNulls[5] = {1, 1, 0, 1, 1};
  EXPECT_TRUE(compare_value(begin, end, std::begin(expectedValues)));
  EXPECT_TRUE(compare_null(begin, end, std::begin(expectedNulls)));
  release(basePtr);
}

// cppcheck-suppress *
TEST(BoolIteratorTest, CheckConstantIterator) {
  SimpleIterator<bool> begin = make_constant_iterator(false, true);
  SimpleIterator<bool> end = begin + 5;
  bool expectedValues[5] = {false, false, false, false, false};
  EXPECT_TRUE(compare_value(begin, end, std::begin(expectedValues)));
}

// cppcheck-suppress *
TEST(VectorPartyIteratorTest, CheckUintIterator) {
  uint32_t indexVectorH[5];
  thrust::sequence(std::begin(indexVectorH), std::end(indexVectorH));
  uint32_t *indexVector = allocate(&indexVectorH[0], 5);

  uint32_t uint32ValuesH[5] = {1000000000, 10000, 0, 10000, 1000000000};
  uint8_t nullsH[1] = {0xFF};

  uint8_t *basePtr = allocate_column(nullptr, &nullsH[0],
                                     &uint32ValuesH[0], 0, 1, 20);

  ColumnIterator<uint32_t> begin = make_column_iterator<uint32_t>(indexVector,
                                                                  nullptr,
                                                                  0,
                                                                  basePtr,
                                                                  0,
                                                                  8,
                                                                  5,
                                                                  4,
                                                                  0);

  ColumnIterator<uint32_t> end = begin + 5;
  uint32_t expectedUint32Values[5] = {1000000000, 10000, 0, 10000, 1000000000};
  EXPECT_TRUE(
      compare_value(begin, end,
                    std::begin(expectedUint32Values)));
  release(basePtr);

  int16_t uint16ValuesH[5] = {1000, 10, 0, 10, 1000};
  basePtr = allocate_column(nullptr, &nullsH[0],
                            &uint16ValuesH[0], 0, 1, 10);

  begin = make_column_iterator<uint32_t>(indexVector,
                                         nullptr, 0, basePtr, 0, 8, 5, 2, 0);
  end = begin + 5;
  uint32_t expectedValues2[5] = {1000, 10, 0, 10, 1000};
  EXPECT_TRUE(
      compare_value(begin, end,
                    std::begin(expectedValues2)));
  release(basePtr);

  int8_t uint8ValuesH[5] = {10, 1, 0, 1, 10};
  basePtr = allocate_column(nullptr, &nullsH[0],
                            &uint8ValuesH[0], 0, 1, 5);
  begin = make_column_iterator<uint32_t>(indexVector,
                                         nullptr, 0, basePtr, 0, 8, 5, 1, 0);
  end = begin + 5;
  uint32_t expectedValues3[5] = {10, 1, 0, 1, 10};
  EXPECT_TRUE(
      compare_value(begin, end, std::begin(expectedValues3)));
  release(basePtr);
  release(indexVector);
}

// cppcheck-suppress *
TEST(VectorPartyIteratorTest, CheckIntIterator) {
  uint32_t indexVectorH[5];
  thrust::sequence(std::begin(indexVectorH), std::end(indexVectorH));
  uint32_t *indexVector = allocate(&indexVectorH[0], 5);

  int32_t int32ValuesH[5] = {-1000000000, -10000, 0, 10000, 1000000000};
  uint8_t nullsH[1] = {0xFF};

  uint8_t *basePtr = allocate_column(nullptr, &nullsH[0],
                                     &int32ValuesH[0], 0, 1, 20);

  ColumnIterator<int32_t> begin = make_column_iterator<int32_t>(indexVector,
                                                                nullptr,
                                                                0,
                                                                basePtr,
                                                                0,
                                                                8,
                                                                5,
                                                                4,
                                                                0);

  ColumnIterator<int32_t> end = begin + 5;
  int32_t expectedInt32Values[5] = {-1000000000, -10000, 0, 10000, 1000000000};
  EXPECT_TRUE(
      compare_value(begin, end,
                    std::begin(expectedInt32Values)));
  release(basePtr);

  int16_t int16ValuesH[5] = {-1000, -10, 0, 10, 1000};
  basePtr = allocate_column(nullptr, &nullsH[0],
                            &int16ValuesH[0], 0, 1, 10);

  begin = make_column_iterator<int32_t>(indexVector,
                                        nullptr, 0, basePtr, 0, 8, 5, 2, 0);
  end = begin + 5;
  int expectedValues2[5] = {-1000, -10, 0, 10, 1000};
  EXPECT_TRUE(
      compare_value(begin, end,
                    std::begin(expectedValues2)));
  release(basePtr);

  int8_t int8ValuesH[5] = {-10, -1, 0, 1, 10};
  basePtr = allocate_column(nullptr, &nullsH[0],
                            &int8ValuesH[0], 0, 1, 5);
  begin = make_column_iterator<int32_t>(indexVector,
                                        nullptr, 0, basePtr, 0, 8, 5, 1, 0);
  end = begin + 5;
  int expectedValues3[5] = {-10, -1, 0, 1, 10};
  EXPECT_TRUE(
      compare_value(begin, end, std::begin(expectedValues3)));
  release(basePtr);
  release(indexVector);
}

// cppcheck-suppress *
TEST(VectorPartyIteratorTest, CheckFloatIterator) {
  uint32_t indexVectorH[5];
  thrust::sequence(std::begin(indexVectorH), std::end(indexVectorH));
  uint32_t *indexVector = allocate(&indexVectorH[0], 5);

  float_t valuesH[5] = {-10.1, -1.1, 0.0, 1.1, 10.1};
  uint8_t nullsH[1] = {0xFF};

  uint8_t *basePtr = allocate_column(nullptr, &nullsH[0],
                                     &valuesH[0], 0, 1, 20);

  ColumnIterator<float_t>
      begin = make_column_iterator<float_t>(indexVector,
                                            nullptr, 0, basePtr,
                                            0, 8, 5, 4, 0);

  ColumnIterator<float_t> end = begin + 5;
  float_t expectedValues[5] = {-10.1, -1.1, 0.0, 1.1, 10.1};
  EXPECT_TRUE(
      compare_value(begin, end,
                    std::begin(expectedValues)));
  release(basePtr);
  release(indexVector);
}

// cppcheck-suppress *
TEST(CompressedColumnTest, CheckCountPointer) {
  uint32_t indexVectorH[5];
  thrust::sequence(std::begin(indexVectorH), std::end(indexVectorH));
  uint32_t *indexVector = allocate(&indexVectorH[0], 5);

  // counts0 : 0 1 4 6 8
  uint32_t baseCountsH[5] = {0, 1, 4, 6, 8};
  // uncompressed: 2 2 2 2 3 3 3 3
  // value1: 2 3
  int valuesH[2] = {2, 3};
  // null1: 1 1 1 1 1 1 1 1
  uint8_t nullsH[1] = {0xFF};
  // count1:
  uint32_t countsH[3] = {0, 4, 8};

  uint32_t *baseCounts = allocate(&baseCountsH[0], 5);

  uint8_t *basePtr =
      allocate_column(&countsH[0], &nullsH[0], &valuesH[0], 12, 1, 8);

  ColumnIterator<int32_t>
      begin = make_column_iterator<int32_t>(indexVector,
                                            baseCounts, 0, basePtr,
                                            16, 24, 5, 4, 0);

  int32_t expectedValues[4] = {2, 2, 3, 3};
  uint8_t expectedNulls[4] = {1, 1, 1, 1};
  EXPECT_TRUE(compare_value(begin, begin + 4, std::begin(expectedValues)));
  EXPECT_TRUE(compare_null(begin, begin + 4, std::begin(expectedNulls)));

  release(basePtr);
  release(indexVector);
  release(baseCounts);
}

// cppcheck-suppress *
TEST(CompressedColumnTest, CheckStartCount) {
  uint32_t indexVectorH[4];
  thrust::sequence(std::begin(indexVectorH), std::end(indexVectorH));
  uint32_t *indexVector = allocate(&indexVectorH[0], 4);

  // uncompressed: 2 2 3 3
  // value1: 2 3
  int valuesH[2] = {2, 3};
  uint8_t nullsH[1] = {0xFF};
  // count1:
  uint32_t countsH[3] = {4, 6, 8};

  uint8_t *basePtr =
      allocate_column(&countsH[0], &nullsH[0], &valuesH[0], 12, 1, 8);

  ColumnIterator<int32_t>
      begin = make_column_iterator<int32_t>(indexVector,
                                            nullptr, 4, basePtr,
                                            16, 24, 5, 4, 0);

  int32_t expectedValues[4] = {2, 2, 3, 3};
  uint8_t expectedNulls[4] = {1, 1, 1, 1};
  EXPECT_TRUE(compare_value(begin, begin + 4, std::begin(expectedValues)));
  EXPECT_TRUE(compare_null(begin, begin + 4, std::begin(expectedNulls)));

  release(basePtr);
  release(indexVector);
}

// cppcheck-suppress *
TEST(DimensionOutputIteratorTest, CheckCopy) {
  uint16_t in1[2] = {65531, 1};
  bool in2[2] = {true, true};
  uint8_t out[8] = {0, 0, 0, 0, 0, 0, 0, 0};
  thrust::zip_iterator<thrust::tuple<Uint16Iter, BoolIter>> zipped1(
      thrust::make_tuple(std::begin(in1), std::begin(in2)));
  DimensionOutputIterator<uint16_t> dim_iter =
      make_dimension_output_iterator<uint16_t>(&out[0], &out[4]);

  thrust::copy(zipped1, zipped1 + 2, dim_iter);
  EXPECT_EQ(65531, *reinterpret_cast<uint16_t *>(out));
  EXPECT_EQ(true, *reinterpret_cast<bool *>(&out[4]));
  EXPECT_EQ(1, *reinterpret_cast<uint16_t *>(&out[2]));
  EXPECT_EQ(true, *reinterpret_cast<bool *>(&out[5]));
}

// cppcheck-suppress *
TEST(DimensionOutputIteratorTest, CheckTransform) {
  uint16_t in1[2] = {1, 2};
  bool in2[2] = {true, true};
  thrust::zip_iterator<thrust::tuple<Uint16Iter, BoolIter>> zipped1(
      thrust::make_tuple(std::begin(in1), std::begin(in2)));
  thrust::zip_iterator<thrust::tuple<Uint16Iter, BoolIter>> zipped2(
      thrust::make_tuple(std::begin(in1), std::begin(in2)));
  uint8_t out[6] = {0, 0, 0, 0, 0, 0};
  DimensionOutputIterator<uint16_t> dim_iter =
      make_dimension_output_iterator<uint16_t>(&out[0], &out[4]);
  thrust::copy(zipped1, zipped1 + 2, dim_iter);
  thrust::transform(zipped1, zipped1 + 2, zipped2, dim_iter,
                    PlusFunctor<uint16_t>());
  EXPECT_EQ(2, out[0]);
  EXPECT_EQ(true, out[4]);
  EXPECT_EQ(4, out[2]);
  EXPECT_EQ(true, out[5]);
}

// cppcheck-suppress *
TEST(MeasureIteratorTest, CheckSum) {
  uint32_t baseCounts[5] = {0, 3, 6, 9, 10};

  uint32_t indexVector[3] = {0, 2, 3};
  int output[3] = {0, 0, 0};
  MeasureOutputIterator<int> measureIter(&output[0], &baseCounts[0],
                                         &indexVector[0], AGGR_SUM_SIGNED);

  measureIter[0] = thrust::make_tuple(1, true);
  // Count = 3 - 0.
  EXPECT_EQ(output[0], 3);

  measureIter[1] = thrust::make_tuple(2, false);
  // Identity.
  EXPECT_EQ(output[1], 0);

  measureIter[2] = thrust::make_tuple(2, true);
  // Count = 10 - 9
  EXPECT_EQ(output[2], 2);

  MeasureOutputIterator<int> measureIter2(&output[0], nullptr,
                                          &indexVector[0], AGGR_SUM_SIGNED);

  // All counts are 1 if base counts is null.
  measureIter2[0] = thrust::make_tuple(1, true);
  EXPECT_EQ(output[0], 1);
  measureIter2[1] = thrust::make_tuple(2, false);
  EXPECT_EQ(output[1], 0);
  measureIter2[2] = thrust::make_tuple(2, true);
  EXPECT_EQ(output[2], 2);
}

// cppcheck-suppress *
TEST(MeasureIteratorTest, CheckMin) {
  uint32_t baseCounts[5] = {0, 3, 6, 9, 10};
  uint32_t indexVector[3] = {0, 2, 3};
  int output[3] = {0, 0, 0};
  MeasureOutputIterator<int> measureIter(&output[0], &baseCounts[0],
                                         &indexVector[0], AGGR_MIN_SIGNED);
  measureIter[0] = thrust::make_tuple(1, true);
  EXPECT_EQ(output[0], 1);
  measureIter[1] = thrust::make_tuple(2, false);
  EXPECT_EQ(output[1], INT32_MAX);

  uint32_t outputUint[3] = {0, 0, 0};
  MeasureOutputIterator<uint32_t>
      measureIterUint(&outputUint[0], &baseCounts[0],
                      &indexVector[0], AGGR_MIN_UNSIGNED);
  measureIterUint[0] = thrust::make_tuple(1, true);
  EXPECT_EQ(outputUint[0], 1);
  measureIterUint[1] = thrust::make_tuple(2, false);
  EXPECT_EQ(outputUint[1], UINT32_MAX);

  float outputFloat[3] = {0, 0, 0};
  MeasureOutputIterator<float_t>
      measureIterFloat(&outputFloat[0], &baseCounts[0],
                       &indexVector[0], AGGR_MIN_FLOAT);
  measureIterFloat[0] = thrust::make_tuple(1.0, true);
  EXPECT_EQ(outputFloat[0], 1.0);
  measureIterFloat[1] = thrust::make_tuple(2.2, false);
  EXPECT_EQ(outputFloat[1], FLT_MAX);
}

// cppcheck-suppress *
TEST(MeasureIteratorTest, CheckMax) {
  uint32_t baseCounts[5] = {0, 3, 6, 9, 10};
  uint32_t indexVector[3] = {0, 2, 3};

  int output[3] = {0, 0, 0};
  MeasureOutputIterator<int> measureIter(&output[0], baseCounts,
                                         &indexVector[0], AGGR_MAX_SIGNED);
  measureIter[0] = thrust::make_tuple(1, true);
  EXPECT_EQ(output[0], 1);
  measureIter[1] = thrust::make_tuple(2, false);
  EXPECT_EQ(output[1], INT32_MIN);

  uint32_t outputUint[3] = {0, 0, 0};
  MeasureOutputIterator<uint32_t> measureIterUint(&outputUint[0],
                                                  baseCounts,
                                                  &indexVector[0],
                                                  AGGR_MAX_UNSIGNED);
  measureIterUint[0] = thrust::make_tuple(1, true);
  EXPECT_EQ(outputUint[0], 1);
  measureIterUint[1] = thrust::make_tuple(2, false);
  EXPECT_EQ(outputUint[1], 0);

  float outputFloat[3] = {0, 0, 0};
  MeasureOutputIterator<float_t> measureIterFloat(&outputFloat[0],
                                                  baseCounts,
                                                  &indexVector[0],
                                                  AGGR_MAX_FLOAT);
  measureIterFloat[0] = thrust::make_tuple(1.0, true);
  EXPECT_EQ(outputFloat[0], 1.0);
  measureIterFloat[1] = thrust::make_tuple(2.2, false);
  EXPECT_EQ(outputFloat[1], FLT_MIN);
}

// cppcheck-suppress *
TEST(RecordIDJoinIteratorTest, CheckIterator) {
  RecordID recordIDsH[5];
  thrust::host_vector<ForeignTableIterator<int32_t>> vpItersHost(5);
  ForeignTableIterator<int32_t> *vpIters = vpItersHost.data();
  int values[5][5] = {{1, 0, 0, 0, 0},
                      {0, 2, 0, 0, 0},
                      {0, 0, 3, 0, 0},
                      {0, 0, 0, 4, 0},
                      {0, 0, 0, 0, 5}};

  // prepare record ids
  int16_t timezoneLookupH[6] = {0, 1, 2, 3, 10};
  int32_t baseBatchID = -2147483648;
  for (int i = 0; i < 5; i++) {
    RecordID recordID = {static_cast<int32_t>(baseBatchID + i),
                         static_cast<uint32_t>(i)};
    recordIDsH[i] = recordID;
  }

  RecordID* recordIDs = allocate(&recordIDsH[0], 5);
  int16_t *timezoneLookup = allocate(&timezoneLookupH[0], 5);
  // prepare batches
  uint8_t *basePtrs[5];
  for (int i = 0; i < 5; i++) {
    basePtrs[i] =
        allocate_column(nullptr, nullptr, &values[i], 0, 0, 20);
    vpItersHost[i] = ForeignTableIterator<int32_t>(VectorPartyIterator<int32_t>(
        nullptr, 0, basePtrs[i], 0, 0, 5, 4, 0));
  }

#ifdef RUN_ON_DEVICE
  thrust::device_vector<ForeignTableIterator<int32_t>>
      vpItersDevice = vpItersHost;
  vpIters = thrust::raw_pointer_cast(vpItersDevice.data());
#endif

  RecordIDJoinIterator<int32_t> joinIter(
      &recordIDs[0], 5, baseBatchID, vpIters, 5, timezoneLookup, 5);
  int32_t expectedValues[5] = {1, 2, 3, 10, 0};
  uint8_t expectedNulls[5] = {1, 1, 1, 1, 1};

  EXPECT_TRUE(compare_value(joinIter,
                            joinIter + 5,
                            std::begin(expectedValues)));
  EXPECT_TRUE(compare_null(joinIter, joinIter + 5, std::begin(expectedNulls)));

  for (int i = 0; i < 5; i++) {
    release(basePtrs[i]);
  }
}

// cppcheck-suppress *
TEST(DimensionHashIterator, CheckIterator) {
  uint8_t dimValues[20];
  uint32_t indexVector[2] = {0, 1};
  uint8_t numDimsPerDimWidth[NUM_DIM_WIDTH] = {0, 0, 1, 1, 1};
  reinterpret_cast<uint32_t *>(dimValues)[0] = 1;
  reinterpret_cast<uint32_t *>(dimValues)[1] = 1;
  reinterpret_cast<uint16_t *>(dimValues + 8)[0] = 1;
  reinterpret_cast<uint16_t *>(dimValues + 8)[1] = 1;
  (dimValues + 12)[0] = 1;
  (dimValues + 12)[1] = 1;
  (dimValues + 12)[2] = 1;
  (dimValues + 12)[3] = 1;
  (dimValues + 12)[4] = 1;
  (dimValues + 12)[5] = 1;
  (dimValues + 12)[6] = 1;
  (dimValues + 12)[7] = 1;
  DimensionHashIterator iter(dimValues, indexVector, numDimsPerDimWidth, 2);
  EXPECT_EQ(iter[0], iter[1]);
}

// cppcheck-suppress *
TEST(DimensionColumnPermutateIteratorTest, CheckIterator) {
  uint8_t valuesInH[28] = {1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                           0, 0, 1, 0, 2, 0, 0, 0, 0, 0, 1, 2, 0, 0};
  uint32_t indexVectorH[2] = {1, 0};
  uint8_t valuesOutH[28] = {0};

  uint8_t *valuesIn = allocate(valuesInH, 28);
  uint32_t *indexVector = allocate(indexVectorH, 2);
  uint8_t *valuesOut = allocate(valuesOutH, 28);

  uint8_t numDimsPerDimWidth[NUM_DIM_WIDTH] = {0, 0, 1, 1, 1};
  DimensionColumnPermutateIterator iterIn(valuesIn, indexVector, 4, 2,
                                          numDimsPerDimWidth);
  DimensionColumnOutputIterator iterOut(valuesOut, 4, 2, numDimsPerDimWidth);
#ifdef RUN_ON_DEVICE
  thrust::copy(thrust::device, iterIn, iterIn + 6, iterOut);
#else
  thrust::copy(thrust::host, iterIn, iterIn + 6, iterOut);
#endif
  uint8_t expectedOut[28] = {2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                             0, 0, 2, 0, 1, 0, 0, 0, 0, 0, 2, 1, 0, 0};
  EXPECT_TRUE(equal(valuesOut, valuesOut + 28, &expectedOut[0]));
  release(valuesIn);
  release(indexVector);
  release(valuesOut);
}

// cppcheck-suppress *
TEST(GeoBatchIntersectIteratorTest, CheckIterator) {
  // A square with (1,1), (1,-1), (-1,-1), (-1, 1) as points.
  float shapeLatsH[10] = {1, 1, -1, -1, 1, 1, 1, -1, -1, 1};
  float shapeLongsH[10] = {1, -1, -1, 1, 1, 1, -1, -1, 1, 1};
  uint8_t shapeIndexsH[10] = {0, 0, 0, 0, 0, 1, 1, 1, 1, 1};
  GeoShapeBatch geoShapeBatch =
      get_geo_shape_batch(shapeLatsH, shapeLongsH, shapeIndexsH, 2, 10);

  uint32_t indexVectorH[3] = {0, 1, 2};
  uint32_t *indexVector = allocate(indexVectorH, 3);

  // three points (0,0) (2,2), null
  GeoPointT pointsH[3] = {{0, 0}, {2, 2}, {0, 0}};
  uint8_t nullsH[1] = {0x3};

  uint32_t outputPredicateH[3] = {0, 0, 0};

  uint32_t *outputPredicate = allocate(outputPredicateH, 3);

  uint8_t
      *basePtr = allocate_column(nullptr, &nullsH[0], &pointsH[0], 0, 1, 24);

  auto columnIter = make_column_iterator<GeoPointT>(indexVector, nullptr, 0,
                                                    basePtr, 0, 8, 3, 8, 0);

  auto geoIter = make_geo_batch_intersect_iterator(columnIter, geoShapeBatch,
                                                   outputPredicate, true);

  // test moving iter.
  EXPECT_EQ(geoIter - geoIter, 0);
  EXPECT_EQ(geoIter + 1 - geoIter, 1);
  EXPECT_EQ(geoIter - 1 - geoIter, -1);
  EXPECT_EQ(geoIter + 5 - geoIter, 5);
  EXPECT_EQ(geoIter + 6 - geoIter, 6);
  EXPECT_EQ(geoIter - 6 - geoIter, -6);
  EXPECT_EQ(geoIter + 7 - geoIter, 7);
  EXPECT_EQ(geoIter + 14 - geoIter, 14);

#ifdef RUN_ON_DEVICE
  thrust::for_each(thrust::device, geoIter, geoIter + 30,
               VoidFunctor());
#else
  thrust::for_each(thrust::host, geoIter, geoIter + 30,
               VoidFunctor());
#endif
  uint32_t expectedOutputPredicate[3] = {3, 0, 0};
  EXPECT_TRUE(equal(outputPredicate,
                    outputPredicate + 3,
                    expectedOutputPredicate));

  release(outputPredicate);
  release(indexVector);
  release(basePtr);
  release(geoShapeBatch);
}

// cppcheck-suppress *
TEST(GeoPredicateIteratorTest, CheckIterator) {
  uint32_t predicate[4] = {0x00001000, 0xffffffff, 0, 0};
  int stepInWords = 2;
  GeoPredicateIterator geoIter(predicate, stepInWords);
  EXPECT_EQ(geoIter[0], 12);
  EXPECT_EQ(geoIter[1], -1);

  EXPECT_EQ(geoIter + 1 - geoIter, 1);
  EXPECT_EQ(geoIter + 2 - geoIter, 2);
}

}  // namespace ares
