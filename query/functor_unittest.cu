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

#include <thrust/host_vector.h>
#include <thrust/iterator/zip_iterator.h>
#include <thrust/transform.h>
#include <thrust/tuple.h>
#include <thrust/sort.h>
#include <thrust/execution_policy.h>
#include <algorithm>
#include <iterator>
#include <tuple>
#include "gtest/gtest.h"
#include "query/functor.h"
#include "query/iterator.h"
#include "query/unittest_utils.h"

extern uint16_t DAYS_BEFORE_MONTH_HOST[13];

namespace ares {

typedef typename thrust::host_vector<UUIDT>::iterator UUIDIter;
typedef typename thrust::host_vector<bool>::iterator BoolIter;
typedef typename thrust::host_vector<int>::iterator IntIter;
typedef typename thrust::host_vector<int16_t>::iterator Int16Iter;
typedef typename thrust::host_vector<uint32_t>::iterator Uint32Iter;

// cppcheck-suppress *
TEST(LogicalFunctorTest, TestBool) {
  bool values1[5] = {false, false, false, true, true};
  bool nulls1[5] = {false, true, true, true, true};
  bool values2[5] = {false, false, true, false, true};
  bool nulls2[5] = {false, true, true, true, true};

  // output
  bool outputValues[5];
  thrust::fill(std::begin(outputValues), std::end(outputValues), false);

  bool outputNulls[5];
  thrust::fill(std::begin(outputNulls), std::end(outputNulls), false);

  typedef thrust::zip_iterator<
      thrust::tuple<BoolIter,
                    BoolIter> > InputZipIterator;
  typedef thrust::zip_iterator<thrust::tuple<BoolIter,
                                             BoolIter> > OutputZipIterator;

  InputZipIterator
      begin1(thrust::make_tuple(std::begin(values1), std::begin(nulls1)));
  InputZipIterator
      end1(thrust::make_tuple(std::end(values1), std::end(nulls1)));

  InputZipIterator
      begin2(thrust::make_tuple(std::begin(values2), std::begin(nulls2)));

  OutputZipIterator outputBegin(
      thrust::make_tuple(std::begin(outputValues),
                         std::begin(outputNulls)));

  // Test AndFunctor
  thrust::transform(begin1, end1, begin2, outputBegin,
                    AndFunctor());

  bool expectedValues[5] = {false, false, false, false, true};
  bool expectedNulls[5] = {false, true, true, true, true};

  EXPECT_TRUE(
      thrust::equal(std::begin(outputValues), std::end(outputValues),
                    std::begin(expectedValues)));
  EXPECT_TRUE(
      thrust::equal(std::begin(outputNulls), std::end(outputNulls),
                    std::begin(expectedNulls)));

  // Test OrFunctor
  thrust::transform(begin1, end1, begin2, outputBegin, OrFunctor());

  bool expectedValues2[5] = {false, false, true, true, true};
  bool expectedNulls2[5] = {false, true, true, true, true};

  EXPECT_TRUE(
      thrust::equal(std::begin(outputValues), std::end(outputValues),
                    std::begin(expectedValues2)));
  EXPECT_TRUE(
      thrust::equal(std::begin(outputNulls), std::end(outputNulls),
                    std::begin(expectedNulls2)));

  // Test NotFunctor
  thrust::transform(begin1, end1, outputBegin, NotFunctor());

  bool expectedValues3[5] = {false, true, true, false, false};
  bool expectedNulls3[5] = {false, true, true, true, true};
  EXPECT_TRUE(
      thrust::equal(std::begin(outputValues), std::end(outputValues),
                    std::begin(expectedValues3)));
  EXPECT_TRUE(
      thrust::equal(std::begin(outputNulls), std::end(outputNulls),
                    std::begin(expectedNulls3)));
}

// cppcheck-suppress *
TEST(LogicalFunctorTest, TestInt) {
  int values1[5] = {0, 10, 0, 0, 10};
  bool nulls1[5] = {false, true, true, true, true};

  int values2[5] = {0, 0, 10, 0, 10};
  bool nulls2[5] = {false, true, true, true, true};

  // output
  bool outputValues[5];
  thrust::fill(std::begin(outputValues), std::end(outputValues), false);

  bool outputNulls[5];
  thrust::fill(std::begin(outputNulls), std::end(outputNulls), false);

  int *valuesBegin1 = &values1[0];
  bool *nullsBegin1 = &nulls1[0];

  int *valuesBegin2 = &values2[0];
  bool *nullsBegin2 = &nulls2[0];

  typedef thrust::zip_iterator<
      thrust::tuple<IntIter, BoolIter> > InputZipIterator;
  typedef thrust::zip_iterator<thrust::tuple<BoolIter,
                                             BoolIter> > OutputZipIterator;

  InputZipIterator begin1(thrust::make_tuple(valuesBegin1, nullsBegin1));
  InputZipIterator end1(
      thrust::make_tuple(valuesBegin1 + 5, nullsBegin1 + 5));

  InputZipIterator begin2(thrust::make_tuple(valuesBegin2, nullsBegin2));

  OutputZipIterator outputBegin(
      thrust::make_tuple(std::begin(outputValues),
                         std::begin(outputNulls)));

  thrust::transform(begin1, end1, begin2, outputBegin, AndFunctor());

  bool expectedValues[5] = {false, false, false, false, true};
  bool expectedNulls[5] = {false, true, true, true, true};
  EXPECT_TRUE(
      thrust::equal(std::begin(outputValues), std::end(outputValues),
                    std::begin(expectedValues)));
  EXPECT_TRUE(
      thrust::equal(std::begin(outputNulls), std::end(outputNulls),
                    std::begin(expectedNulls)));

  // Test OrFunctor
  thrust::transform(begin1, end1, begin2, outputBegin, OrFunctor());

  bool expectedValues2[5] = {false, true, true, false, true};
  bool expectedNulls2[5] = {false, true, true, true, true};

  EXPECT_TRUE(
      thrust::equal(std::begin(outputValues), std::end(outputValues),
                    std::begin(expectedValues2)));
  EXPECT_TRUE(
      thrust::equal(std::begin(outputNulls), std::end(outputNulls),
                    std::begin(expectedNulls2)));

  // Test NotFunctor
  thrust::transform(begin1, end1, outputBegin, NotFunctor());

  bool expectedValues3[5] = {false, false, true, true, false};
  bool expectedNulls3[5] = {false, true, true, true, true};
  EXPECT_TRUE(
      thrust::equal(std::begin(outputValues), std::end(outputValues),
                    std::begin(expectedValues3)));
  EXPECT_TRUE(
      thrust::equal(std::begin(outputNulls), std::end(outputNulls),
                    std::begin(expectedNulls3)));
}

// cppcheck-suppress *
TEST(LogicalFunctorTest, TestOrFunctor) {
  OrFunctor f;
  thrust::tuple<bool, bool> res = f(thrust::make_tuple(true, true),
                                    thrust::make_tuple(true, false));
  EXPECT_EQ(thrust::get<0>(res), true);
  EXPECT_EQ(thrust::get<1>(res), true);

  res = f(thrust::make_tuple(false, true),
          thrust::make_tuple(true, false));
  EXPECT_EQ(thrust::get<0>(res), false);
  EXPECT_EQ(thrust::get<1>(res), false);

  res = f(thrust::make_tuple(false, true),
          thrust::make_tuple(false, true));
  EXPECT_EQ(thrust::get<0>(res), false);
  EXPECT_EQ(thrust::get<1>(res), true);
}

// cppcheck-suppress *
TEST(ComparisonFunctorTest, TestInt) {
  int values1[5] = {0, 10, 0, 0, 10};
  bool nulls1[5] = {false, true, true, true, true};

  int values2[5] = {0, 0, 10, 0, 10};
  bool nulls2[5] = {false, true, true, true, true};

  // output
  bool outputValues[5];
  thrust::fill(std::begin(outputValues), std::end(outputValues), false);

  bool outputNulls[5];
  thrust::fill(std::begin(outputNulls), std::end(outputNulls), false);

  int *valuesBegin1 = &values1[0];
  bool *nullsBegin1 = &nulls1[0];

  int *valuesBegin2 = &values2[0];
  bool *nullsBegin2 = &nulls2[0];

  typedef thrust::zip_iterator<
      thrust::tuple<IntIter, BoolIter> > InputZipIterator;
  typedef thrust::zip_iterator<thrust::tuple<BoolIter,
                                             BoolIter> > OutputZipIterator;

  InputZipIterator begin1(thrust::make_tuple(valuesBegin1, nullsBegin1));
  InputZipIterator end1(
      thrust::make_tuple(valuesBegin1 + 5, nullsBegin1 + 5));

  InputZipIterator begin2(thrust::make_tuple(valuesBegin2, nullsBegin2));

  OutputZipIterator outputBegin(
      thrust::make_tuple(std::begin(outputValues),
                         std::begin(outputNulls)));

  // Test EqualFunctor
  thrust::transform(begin1, end1, begin2, outputBegin,
                    EqualFunctor<int>());

  bool expectedValues[5] = {false, false, false, true, true};
  bool expectedNulls[5] = {false, true, true, true, true};
  EXPECT_TRUE(
      thrust::equal(std::begin(outputValues), std::end(outputValues),
                    std::begin(expectedValues)));
  EXPECT_TRUE(
      thrust::equal(std::begin(outputNulls), std::end(outputNulls),
                    std::begin(expectedNulls)));

  // Test NotEqualFunctor
  thrust::transform(begin1, end1, begin2, outputBegin,
                    NotEqualFunctor<int>());

  bool expectedValues2[5] = {false, true, true, false, false};
  bool expectedNulls2[5] = {false, true, true, true, true};

  EXPECT_TRUE(
      thrust::equal(std::begin(outputValues), std::end(outputValues),
                    std::begin(expectedValues2)));
  EXPECT_TRUE(
      thrust::equal(std::begin(outputNulls), std::end(outputNulls),
                    std::begin(expectedNulls2)));

  // Test LessThanFunctor
  thrust::transform(begin1, end1, begin2, outputBegin,
                    LessThanFunctor<int>());
  bool expectedValues3[5] = {false, false, true, false, false};
  bool expectedNulls3[5] = {false, true, true, true, true};
  EXPECT_TRUE(
      thrust::equal(std::begin(outputValues), std::end(outputValues),
                    std::begin(expectedValues3)));
  EXPECT_TRUE(
      thrust::equal(std::begin(outputNulls), std::end(outputNulls),
                    std::begin(expectedNulls3)));

  // Test LessThanOrEqualFunctor
  thrust::transform(begin1, end1, begin2, outputBegin,
                    LessThanOrEqualFunctor<int>());

  bool expectedValues4[5] = {false, false, true, true, true};
  bool expectedNulls4[5] = {false, true, true, true, true};
  EXPECT_TRUE(
      thrust::equal(std::begin(outputValues), std::end(outputValues),
                    std::begin(expectedValues4)));
  EXPECT_TRUE(
      thrust::equal(std::begin(outputNulls), std::end(outputNulls),
                    std::begin(expectedNulls4)));

  // Test GreaterThanFunctor
  thrust::transform(begin1, end1, begin2, outputBegin,
                    GreaterThanFunctor<int>());

  bool expectedValues5[5] = {false, true, false, false, false};
  bool expectedNulls5[5] = {false, true, true, true, true};
  EXPECT_TRUE(
      thrust::equal(std::begin(outputValues), std::end(outputValues),
                    std::begin(expectedValues5)));
  EXPECT_TRUE(
      thrust::equal(std::begin(outputNulls), std::end(outputNulls),
                    std::begin(expectedNulls5)));

  // Test GreaterThanOrEqualFunctor
  thrust::transform(begin1, end1, begin2, outputBegin,
                    GreaterThanOrEqualFunctor<int>());

  bool expectedValues6[5] = {false, true, false, true, true};
  bool expectedNulls6[5] = {false, true, true, true, true};
  EXPECT_TRUE(
      thrust::equal(std::begin(outputValues), std::end(outputValues),
                    std::begin(expectedValues6)));
  EXPECT_TRUE(
      thrust::equal(std::begin(outputNulls), std::end(outputNulls),
                    std::begin(expectedNulls6)));
}

// cppcheck-suppress *
TEST(ComparisonFunctorTest, TestUpperCast) {
  int values1[5] = {0, 10, 0, 0x10, 10};
  bool nulls1[5] = {false, true, true, true, true};

  int16_t values2[5] = {0, 0, 10, 0, 10};
  bool nulls2[5] = {false, true, true, true, true};

  // output
  bool outputValues[5];
  thrust::fill(std::begin(outputValues), std::end(outputValues), false);

  bool outputNulls[5];
  thrust::fill(std::begin(outputNulls), std::end(outputNulls), false);

  int *valuesBegin1 = &values1[0];
  bool *nullsBegin1 = &nulls1[0];

  int16_t *valuesBegin2 = &values2[0];
  bool *nullsBegin2 = &nulls2[0];

  typedef thrust::zip_iterator<
      thrust::tuple<IntIter, BoolIter> > InputZipIterator;
  typedef thrust::zip_iterator<
      thrust::tuple<Int16Iter,
                    BoolIter> > InputZipIterator2;
  typedef thrust::zip_iterator<thrust::tuple<BoolIter,
                                             BoolIter> > OutputZipIterator;

  InputZipIterator begin1(thrust::make_tuple(valuesBegin1, nullsBegin1));
  InputZipIterator end1(
      thrust::make_tuple(valuesBegin1 + 5, nullsBegin1 + 5));

  InputZipIterator2 begin2(thrust::make_tuple(valuesBegin2, nullsBegin2));

  OutputZipIterator outputBegin(
      thrust::make_tuple(std::begin(outputValues),
                         std::begin(outputNulls)));

  thrust::transform(begin1, end1, begin2, outputBegin,
                    GreaterThanFunctor<int>());

  bool expectedValues[5] = {false, true, false, true, false};
  bool expectedNulls[5] = {false, true, true, true, true};
  EXPECT_TRUE(
      thrust::equal(std::begin(outputValues), std::end(outputValues),
                    std::begin(expectedValues)));
  EXPECT_TRUE(
      thrust::equal(std::begin(outputNulls), std::end(outputNulls),
                    std::begin(expectedNulls)));
}

// cppcheck-suppress *
TEST(ArithmeticFunctorTest, TestInt) {
  int values1[5] = {0, 10, 0, 0, 10};
  bool nulls1[5] = {false, true, true, true, true};

  int values2[5] = {0, 1, 10, 1, 10};
  bool nulls2[5] = {false, true, true, true, true};
  // output
  int outputValues[5];
  thrust::fill(std::begin(outputValues), std::end(outputValues), 0);

  bool outputNulls[5];
  thrust::fill(std::begin(outputNulls), std::end(outputNulls), false);

  int *valuesBegin1 = &values1[0];
  bool *nullsBegin1 = &nulls1[0];

  int *valuesBegin2 = &values2[0];
  bool *nullsBegin2 = &nulls2[0];

  typedef thrust::zip_iterator<
      thrust::tuple<IntIter, BoolIter> > InputZipIterator;
  typedef thrust::zip_iterator<thrust::tuple<IntIter,
                                             BoolIter> > OutputZipIterator;

  InputZipIterator begin1(thrust::make_tuple(valuesBegin1, nullsBegin1));
  InputZipIterator end1(
      thrust::make_tuple(valuesBegin1 + 5, nullsBegin1 + 5));

  InputZipIterator begin2(thrust::make_tuple(valuesBegin2, nullsBegin2));

  OutputZipIterator outputBegin(
      thrust::make_tuple(std::begin(outputValues),
                         std::begin(outputNulls)));

  // Test PlusFunctor
  thrust::transform(begin1, end1, begin2, outputBegin,
                    PlusFunctor<int>());

  int expectedValues[5] = {0, 11, 10, 1, 20};
  bool expectedNulls[5] = {false, true, true, true, true};
  EXPECT_TRUE(
      thrust::equal(std::begin(outputValues), std::end(outputValues),
                    std::begin(expectedValues)));
  EXPECT_TRUE(
      thrust::equal(std::begin(outputNulls), std::end(outputNulls),
                    std::begin(expectedNulls)));

  // Test MinusFunctor
  thrust::transform(begin1, end1, begin2, outputBegin,
                    MinusFunctor<int>());

  int expectedValues2[5] = {0, 9, -10, -1, 0};
  bool expectedNulls2[5] = {false, true, true, true, true};

  EXPECT_TRUE(
      thrust::equal(std::begin(outputValues), std::end(outputValues),
                    std::begin(expectedValues2)));
  EXPECT_TRUE(
      thrust::equal(std::begin(outputNulls), std::end(outputNulls),
                    std::begin(expectedNulls2)));

  // Test MutiplyFunctor
  thrust::transform(begin1, end1, begin2, outputBegin,
                    MultiplyFunctor<int>());
  int expectedValues3[5] = {0, 10, 0, 0, 100};
  bool expectedNulls3[5] = {false, true, true, true, true};
  EXPECT_TRUE(
      thrust::equal(std::begin(outputValues), std::end(outputValues),
                    std::begin(expectedValues3)));
  EXPECT_TRUE(
      thrust::equal(std::begin(outputNulls), std::end(outputNulls),
                    std::begin(expectedNulls3)));

  // Test DivideFunctor
  thrust::transform(begin1, end1, begin2, outputBegin,
                    DivideFunctor<int>());
  int expectedValues4[5] = {0, 10, 0, 0, 1};
  bool expectedNulls4[5] = {false, true, true, true, true};
  EXPECT_TRUE(
      thrust::equal(std::begin(outputValues), std::end(outputValues),
                    std::begin(expectedValues4)));
  EXPECT_TRUE(
      thrust::equal(std::begin(outputNulls), std::end(outputNulls),
                    std::begin(expectedNulls4)));

  // Test ModFunctor
  thrust::transform(begin1, end1, begin2, outputBegin, ModFunctor<int>());
  int expectedValues5[5] = {0, 0, 0, 0, 0};
  bool expectedNulls5[5] = {false, true, true, true, true};
  EXPECT_TRUE(
      thrust::equal(std::begin(outputValues), std::end(outputValues),
                    std::begin(expectedValues5)));
  EXPECT_TRUE(
      thrust::equal(std::begin(outputNulls), std::end(outputNulls),
                    std::begin(expectedNulls5)));

  // Test NegateFunctor
  thrust::transform(begin1, end1, outputBegin, NegateFunctor<int>());
  int expectedValues6[5] = {0, -10, 0, 0, -10};
  bool expectedNulls6[5] = {false, true, true, true, true};
  EXPECT_TRUE(
      thrust::equal(std::begin(outputValues), std::end(outputValues),
                    std::begin(expectedValues6)));
  EXPECT_TRUE(
      thrust::equal(std::begin(outputNulls), std::end(outputNulls),
                    std::begin(expectedNulls6)));

  // Test FloorFunctor
  thrust::transform(begin1, end1, begin2, outputBegin, FloorFunctor<int>());
  int expectedValues7[5] = {0, 10, 0, 0, 10};
  bool expectedNulls7[5] = {false, true, true, true, true};
  EXPECT_TRUE(
      thrust::equal(std::begin(outputValues), std::end(outputValues),
                    std::begin(expectedValues7)));
  EXPECT_TRUE(
      thrust::equal(std::begin(outputNulls), std::end(outputNulls),
                    std::begin(expectedNulls7)));
}

// cppcheck-suppress *
TEST(BitwiseFunctorTest, TestInt) {
  int values1[5] = {0, 0xF0, 0x0F, 0x00, 0x00};
  bool nulls1[5] = {false, true, true, true, true};

  int values2[5] = {0, 0x00, 0x0F, 0xF0, 0x00};
  bool nulls2[5] = {false, true, true, true, true};

  // output
  int outputValues[5];
  thrust::fill(std::begin(outputValues), std::end(outputValues), 0);

  bool outputNulls[5];
  thrust::fill(std::begin(outputNulls), std::end(outputNulls), false);

  int *valuesBegin1 = &values1[0];
  bool *nullsBegin1 = &nulls1[0];

  int *valuesBegin2 = &values2[0];
  bool *nullsBegin2 = &nulls2[0];

  typedef thrust::zip_iterator<
      thrust::tuple<IntIter, BoolIter> > InputZipIterator;
  typedef thrust::zip_iterator<thrust::tuple<IntIter,
                                             BoolIter> > OutputZipIterator;

  InputZipIterator begin1(thrust::make_tuple(valuesBegin1, nullsBegin1));
  InputZipIterator end1(
      thrust::make_tuple(valuesBegin1 + 5, nullsBegin1 + 5));

  InputZipIterator begin2(thrust::make_tuple(valuesBegin2, nullsBegin2));

  OutputZipIterator outputBegin(
      thrust::make_tuple(std::begin(outputValues),
                         std::begin(outputNulls)));

  // Test BitwiseAndFunctor
  thrust::transform(begin1, end1, begin2, outputBegin,
                    BitwiseAndFunctor<int>());

  int expectedValues[5] = {0, 0x00, 0x0F, 0x00, 0x00};
  bool expectedNulls[5] = {false, true, true, true, true};
  EXPECT_TRUE(
      thrust::equal(std::begin(outputValues), std::end(outputValues),
                    std::begin(expectedValues)));
  EXPECT_TRUE(
      thrust::equal(std::begin(outputNulls), std::end(outputNulls),
                    std::begin(expectedNulls)));

  // Test BitwiseOrFunctor
  thrust::transform(begin1, end1, begin2, outputBegin,
                    BitwiseOrFunctor<int>());

  int expectedValues2[5] = {0, 0xF0, 0x0F, 0xF0, 0x00};
  bool expectedNulls2[5] = {false, true, true, true, true};
  EXPECT_TRUE(
      thrust::equal(std::begin(outputValues), std::end(outputValues),
                    std::begin(expectedValues2)));
  EXPECT_TRUE(
      thrust::equal(std::begin(outputNulls), std::end(outputNulls),
                    std::begin(expectedNulls2)));

  // Test BitwiseXorFunctor
  thrust::transform(begin1, end1, begin2, outputBegin,
                    BitwiseXorFunctor<int>());

  int expectedValues3[5] = {0, 0xF0, 0x00, 0xF0, 0x00};
  bool expectedNulls3[5] = {false, true, true, true, true};
  EXPECT_TRUE(
      thrust::equal(std::begin(outputValues), std::end(outputValues),
                    std::begin(expectedValues3)));
  EXPECT_TRUE(
      thrust::equal(std::begin(outputNulls), std::end(outputNulls),
                    std::begin(expectedNulls3)));

  // Test BitwiseNotFunctor
  thrust::transform(begin1, end1, outputBegin, BitwiseNotFunctor<int>());

  int expectedValues4[5] = {0, static_cast<int>(0xFFFFFF0F),
                            static_cast<int>(0xFFFFFFF0),
                            static_cast<int>(0xFFFFFFFF),
                            static_cast<int>(0xFFFFFFFF)};
  bool expectedNulls4[5] = {false, true, true, true, true};
  EXPECT_TRUE(
      thrust::equal(std::begin(outputValues), std::end(outputValues),
                    std::begin(expectedValues4)));
  EXPECT_TRUE(
      thrust::equal(std::begin(outputNulls), std::end(outputNulls),
                    std::begin(expectedNulls4)));
}

// cppcheck-suppress *
TEST(MiscFunctorTest, TestInt) {
  int values1[5] = {0, 0, 0, 0, 0};
  bool nulls1[5] = {true, true, true, true, false};

  // output
  int outputValues[5];
  thrust::fill(std::begin(outputValues), std::end(outputValues), 0);

  bool outputNulls[5];
  thrust::fill(std::begin(outputNulls), std::end(outputNulls), false);

  int *valuesBegin1 = &values1[0];
  bool *nullsBegin1 = &nulls1[0];

  typedef thrust::zip_iterator<
      thrust::tuple<IntIter, BoolIter> > InputZipIterator;
  typedef thrust::zip_iterator<thrust::tuple<IntIter,
                                             BoolIter> > OutputZipIterator;

  InputZipIterator begin1(thrust::make_tuple(valuesBegin1, nullsBegin1));
  InputZipIterator end1(
      thrust::make_tuple(valuesBegin1 + 5, nullsBegin1 + 5));

  OutputZipIterator outputBegin(
      thrust::make_tuple(std::begin(outputValues),
                         std::begin(outputNulls)));

  // Test IsNullFunctor
  thrust::transform(begin1, end1, outputBegin, IsNullFunctor());

  int expectedValues[5] = {0, 0, 0, 0, 1};
  bool expectedNulls[5] = {true, true, true, true, true};
  EXPECT_TRUE(
      thrust::equal(std::begin(outputValues), std::end(outputValues),
                    std::begin(expectedValues)));
  EXPECT_TRUE(
      thrust::equal(std::begin(outputNulls), std::end(outputNulls),
                    std::begin(expectedNulls)));

  // Test IsNotNullFunctor
  thrust::transform(begin1, end1, outputBegin, IsNotNullFunctor());

  int expectedValues2[5] = {1, 1, 1, 1, 0};
  bool expectedNulls2[5] = {true, true, true, true, true};
  EXPECT_TRUE(
      thrust::equal(std::begin(outputValues), std::end(outputValues),
                    std::begin(expectedValues2)));
  EXPECT_TRUE(
      thrust::equal(std::begin(outputNulls), std::end(outputNulls),
                    std::begin(expectedNulls2)));

  // Test NoopFunctor
  thrust::transform(begin1, end1, outputBegin, NoopFunctor<int>());
  int expectedValues3[5] = {0, 0, 0, 0, 0};
  bool expectedNulls3[5] = {true, true, true, true, false};

  EXPECT_TRUE(
      thrust::equal(std::begin(outputValues), std::end(outputValues),
                    std::begin(expectedValues3)));
  EXPECT_TRUE(
      thrust::equal(std::begin(outputNulls), std::end(outputNulls),
                    std::begin(expectedNulls3)));
}

// cppcheck-suppress *
TEST(RemoveFilterTest, CheckRemoveFilter) {
  uint8_t predicates[5] = {1, 1, 1, 1, 0};
  RemoveFilter<thrust::tuple<uint32_t, uint32_t>, uint8_t> f(&predicates[0]);

  EXPECT_FALSE(f(thrust::make_tuple(0, 0)));
  EXPECT_FALSE(f(thrust::make_tuple(1, 0)));
  EXPECT_FALSE(f(thrust::make_tuple(2, 0)));
  EXPECT_FALSE(f(thrust::make_tuple(3, 0)));
  EXPECT_TRUE(f(thrust::make_tuple(4, 0)));
}

// cppcheck-suppress *
TEST(UnaryFunctorTest, CheckUnaryFunctor) {
  int values1[5] = {0, 0, 0, 0, 0};
  bool nulls1[5] = {true, true, true, true, false};

  // output
  int outputValues[5];
  thrust::fill(std::begin(outputValues), std::end(outputValues), 0);

  bool outputNulls[5];
  thrust::fill(std::begin(outputNulls), std::end(outputNulls), false);

  int *valuesBegin1 = &values1[0];
  bool *nullsBegin1 = &nulls1[0];

  typedef thrust::zip_iterator<
      thrust::tuple<IntIter, BoolIter> > InputZipIterator;
  typedef thrust::zip_iterator<
      thrust::tuple<IntIter, BoolIter> > OutputZipIterator;

  InputZipIterator begin1(thrust::make_tuple(valuesBegin1, nullsBegin1));
  OutputZipIterator outputBegin(
      thrust::make_tuple(std::begin(outputValues),
                         std::begin(outputNulls)));

  thrust::transform(begin1, begin1 + 5, outputBegin,
                    UnaryFunctor<int, int>(IsNull));

  int expectedValues[5] = {0, 0, 0, 0, 1};
  bool expectedNulls[5] = {true, true, true, true, true};
  EXPECT_TRUE(
      thrust::equal(std::begin(outputValues), std::end(outputValues),
                    std::begin(expectedValues)));
  EXPECT_TRUE(
      thrust::equal(std::begin(outputNulls), std::end(outputNulls),
                    std::begin(expectedNulls)));
}

// cppcheck-suppress *
TEST(BinaryFunctorTest, CheckBinaryFunctor) {
  int values1[5] = {0, 0xF0, 0x0F, 0x00, 0x00};
  bool nulls1[5] = {false, true, true, true, true};

  int values2[5] = {0, 0x00, 0x0F, 0xF0, 0x00};
  bool nulls2[5] = {false, true, true, true, true};

  // output
  int outputValues[5];
  thrust::fill(std::begin(outputValues), std::end(outputValues), 0);

  bool outputNulls[5];
  thrust::fill(std::begin(outputNulls), std::end(outputNulls), false);

  int *valuesBegin1 = &values1[0];
  bool *nullsBegin1 = &nulls1[0];

  int *valuesBegin2 = &values2[0];
  bool *nullsBegin2 = &nulls2[0];

  typedef thrust::zip_iterator<
      thrust::tuple<IntIter, BoolIter> > InputZipIterator;
  typedef thrust::zip_iterator<
      thrust::tuple<IntIter, BoolIter> > OutputZipIterator;

  InputZipIterator begin1(thrust::make_tuple(valuesBegin1, nullsBegin1));
  InputZipIterator begin2(thrust::make_tuple(valuesBegin2, nullsBegin2));

  OutputZipIterator outputBegin(
      thrust::make_tuple(std::begin(outputValues),
                         std::begin(outputNulls)));

  // Test BitwiseAndFunctor
  thrust::transform(begin1, begin1 + 5, begin2, outputBegin,
                    BinaryFunctor<int, int>(BitwiseAnd));

  int expectedValues[5] = {0, 0x00, 0x0F, 0x00, 0x00};
  bool expectedNulls[5] = {false, true, true, true, true};
  EXPECT_TRUE(
      thrust::equal(std::begin(outputValues), std::end(outputValues),
                    std::begin(expectedValues)));
  EXPECT_TRUE(
      thrust::equal(std::begin(outputNulls), std::end(outputNulls),
                    std::begin(expectedNulls)));
}

// cppcheck-suppress *
TEST(BinaryPredicateFunctorTest, CheckBinaryTranformFunctor) {
  int values1[5] = {100, 1, 200, 0, 1};
  bool nulls1[5] = {false, true, true, true, true};

  int values2[5] = {0, 1, 1, 0, 100};
  bool nulls2[5] = {false, true, true, true, true};

  // output
  bool outputValues[5];
  thrust::fill(std::begin(outputValues), std::end(outputValues), 0);

  int *valuesBegin1 = &values1[0];
  bool *nullsBegin1 = &nulls1[0];

  int *valuesBegin2 = &values2[0];
  bool *nullsBegin2 = &nulls2[0];

  typedef thrust::zip_iterator<
      thrust::tuple<IntIter, BoolIter> > InputZipIterator;
  typedef thrust::zip_iterator<
      thrust::tuple<InputZipIterator,
                    InputZipIterator> > ZipOfInputZipIterator;
  typedef thrust::zip_iterator<thrust::tuple<IntIter,
                                             BoolIter> > OutputZipIterator;

  InputZipIterator begin1(thrust::make_tuple(valuesBegin1, nullsBegin1));
  InputZipIterator begin2(thrust::make_tuple(valuesBegin2, nullsBegin2));

  // Test BitwiseAndFunctor
  thrust::transform(begin1, begin1 + 5, begin2, &outputValues[0],
                    BinaryPredicateFunctor<bool, int>(And));

  bool expectedValues[5] = {0, 1, 1, 0, 1};
  EXPECT_TRUE(
      thrust::equal(std::begin(outputValues), std::end(outputValues),
                    std::begin(expectedValues)));
}

TEST(HashLookupFunctorTest, CheckHashLookupFunctor) {
  // hash index created in golang cuckoo_hash_index
  uint32_t seeds[4] = {2596996162, 4039455774, 2854263694, 1879968118};
  uint8_t buckets[312] = {
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
  int keyBytes = 4;
  int numHashes = 4;
  int numBuckets = 2;

  uint32_t keys[18] = {0, 1, 2, 3, 4, 5, 6, 7, 8,
                       9, 10, 11, 12, 13, 14, 15, 16, 17};
  bool nulls[18];
  thrust::fill(std::begin(nulls), std::end(nulls), true);
  bool *nullsBegin = &nulls[0];

  HashLookupFunctor<uint32_t> lookupFunctor(buckets, seeds, keyBytes, numHashes,
                                            numBuckets);
  RecordID outputValues[18];
  typedef typename thrust::host_vector<RecordID>::iterator RecordIDIter;
  typedef thrust::zip_iterator<thrust::tuple<Uint32Iter, BoolIter> >
      InputZipIterator;

  InputZipIterator inputBegin(thrust::make_tuple(std::begin(keys), nullsBegin));
  thrust::transform(inputBegin, inputBegin + 18, std::begin(outputValues),
                    lookupFunctor);

  for (int i = 0; i < 18; i++) {
    EXPECT_EQ(outputValues[i].batchID, 0);
    EXPECT_EQ(outputValues[i].index, i);
  }
}

// cppcheck-suppress *
TEST(ResolveTimeBucketizerTest, CheckTimeSeriesBucketizer) {
  // works for epoch time.
  uint32_t ts = get_ts(1970, 1, 1);
  uint32_t yearStart = resolveTimeBucketizer(ts, YEAR, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(yearStart, get_ts(1970, 1, 1));

  uint32_t
      quarterStart = resolveTimeBucketizer(ts, QUATER, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(quarterStart, get_ts(1970, 1, 1));

  uint32_t
      monthStart = resolveTimeBucketizer(ts, MONTH, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(monthStart, get_ts(1970, 1, 1));

  /* 1970-01-31 */
  ts = get_ts(1970, 1, 31);
  yearStart = resolveTimeBucketizer(ts, YEAR, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(yearStart, get_ts(1970, 1, 1));

  quarterStart = resolveTimeBucketizer(ts, QUATER, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(quarterStart, get_ts(1970, 1, 1));

  monthStart = resolveTimeBucketizer(ts, MONTH, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(monthStart, get_ts(1970, 1, 1));

  /* 1970-01-30 */
  ts = get_ts(1970, 1, 30);
  yearStart = resolveTimeBucketizer(ts, YEAR, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(yearStart, get_ts(1970, 1, 1));

  quarterStart = resolveTimeBucketizer(ts, QUATER, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(quarterStart, get_ts(1970, 1, 1));

  monthStart = resolveTimeBucketizer(ts, MONTH, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(monthStart, get_ts(1970, 1, 1));

  /* 1970-02-01 */
  ts = get_ts(1970, 2, 1);
  yearStart = resolveTimeBucketizer(ts, YEAR, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(yearStart, get_ts(1970, 1, 1));

  quarterStart = resolveTimeBucketizer(ts, QUATER, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(quarterStart, get_ts(1970, 1, 1));

  monthStart = resolveTimeBucketizer(ts, MONTH, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(monthStart, get_ts(1970, 2, 1));

  /* 1970-02-28 */
  ts = get_ts(1970, 2, 28);
  yearStart = resolveTimeBucketizer(ts, YEAR, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(yearStart, get_ts(1970, 1, 1));

  quarterStart = resolveTimeBucketizer(ts, QUATER, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(quarterStart, get_ts(1970, 1, 1));

  monthStart = resolveTimeBucketizer(ts, MONTH, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(monthStart, get_ts(1970, 2, 1));

  /* 1970-03-01 */
  ts = get_ts(1970, 3, 1);
  yearStart = resolveTimeBucketizer(ts, YEAR, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(yearStart, get_ts(1970, 1, 1));

  quarterStart = resolveTimeBucketizer(ts, QUATER, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(quarterStart, get_ts(1970, 1, 1));

  monthStart = resolveTimeBucketizer(ts, MONTH, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(monthStart, get_ts(1970, 3, 1));

  /* 1970-04-01 */
  ts = get_ts(1970, 4, 1);
  yearStart = resolveTimeBucketizer(ts, YEAR, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(yearStart, get_ts(1970, 1, 1));

  quarterStart = resolveTimeBucketizer(ts, QUATER, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(quarterStart, get_ts(1970, 4, 1));

  monthStart = resolveTimeBucketizer(ts, MONTH, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(monthStart, get_ts(1970, 4, 1));

  /* 1970-05-01 */
  ts = get_ts(1970, 5, 1);
  yearStart = resolveTimeBucketizer(ts, YEAR, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(yearStart, get_ts(1970, 1, 1));

  quarterStart = resolveTimeBucketizer(ts, QUATER, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(quarterStart, get_ts(1970, 4, 1));

  monthStart = resolveTimeBucketizer(ts, MONTH, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(monthStart, get_ts(1970, 5, 1));

  /* 1970-09-01 */
  ts = get_ts(1970, 9, 1);
  yearStart = resolveTimeBucketizer(ts, YEAR, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(yearStart, get_ts(1970, 1, 1));

  quarterStart = resolveTimeBucketizer(ts, QUATER, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(quarterStart, get_ts(1970, 7, 1));

  monthStart = resolveTimeBucketizer(ts, MONTH, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(monthStart, get_ts(1970, 9, 1));

  /* 1970-12-31 */
  ts = get_ts(1970, 12, 31);
  yearStart = resolveTimeBucketizer(ts, YEAR, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(yearStart, get_ts(1970, 1, 1));

  quarterStart = resolveTimeBucketizer(ts, QUATER, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(quarterStart, get_ts(1970, 10, 1));

  monthStart = resolveTimeBucketizer(ts, MONTH, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(monthStart, get_ts(1970, 12, 1));

  /* 1971-01-01 */
  ts = get_ts(1971, 1, 1);
  yearStart = resolveTimeBucketizer(ts, YEAR, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(yearStart, get_ts(1971, 1, 1));

  quarterStart = resolveTimeBucketizer(ts, QUATER, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(quarterStart, get_ts(1971, 1, 1));

  monthStart = resolveTimeBucketizer(ts, MONTH, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(monthStart, get_ts(1971, 1, 1));

  /* 1971-06-01 */
  ts = get_ts(1971, 6, 1);
  yearStart = resolveTimeBucketizer(ts, YEAR, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(yearStart, get_ts(1971, 1, 1));

  quarterStart = resolveTimeBucketizer(ts, QUATER, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(quarterStart, get_ts(1971, 4, 1));

  monthStart = resolveTimeBucketizer(ts, MONTH, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(monthStart, get_ts(1971, 6, 1));

  // leap year.
  /* 1972-01-01 */
  ts = get_ts(1972, 1, 1);
  yearStart = resolveTimeBucketizer(ts, YEAR, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(yearStart, get_ts(1972, 1, 1));

  quarterStart = resolveTimeBucketizer(ts, QUATER, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(quarterStart, get_ts(1972, 1, 1));

  monthStart = resolveTimeBucketizer(ts, MONTH, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(monthStart, get_ts(1972, 1, 1));

  /* 1972-02-29 */
  ts = get_ts(1972, 2, 29);
  yearStart = resolveTimeBucketizer(ts, YEAR, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(yearStart, get_ts(1972, 1, 1));

  quarterStart = resolveTimeBucketizer(ts, QUATER, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(quarterStart, get_ts(1972, 1, 1));

  monthStart = resolveTimeBucketizer(ts, MONTH, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(monthStart, get_ts(1972, 2, 1));

  /* 1972-03-01 */
  ts = get_ts(1972, 3, 1);
  yearStart = resolveTimeBucketizer(ts, YEAR, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(yearStart, get_ts(1972, 1, 1));

  quarterStart = resolveTimeBucketizer(ts, QUATER, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(quarterStart, get_ts(1972, 1, 1));

  monthStart = resolveTimeBucketizer(ts, MONTH, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(monthStart, get_ts(1972, 3, 1));

  /* 1972-03-31 */
  ts = get_ts(1972, 3, 31);
  yearStart = resolveTimeBucketizer(ts, YEAR, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(yearStart, get_ts(1972, 1, 1));

  quarterStart = resolveTimeBucketizer(ts, QUATER, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(quarterStart, get_ts(1972, 1, 1));

  monthStart = resolveTimeBucketizer(ts, MONTH, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(monthStart, get_ts(1972, 3, 1));

  /* 1972-04-01 */
  ts = get_ts(1972, 4, 1);
  yearStart = resolveTimeBucketizer(ts, YEAR, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(yearStart, get_ts(1972, 1, 1));

  quarterStart = resolveTimeBucketizer(ts, QUATER, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(quarterStart, get_ts(1972, 4, 1));

  monthStart = resolveTimeBucketizer(ts, MONTH, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(monthStart, get_ts(1972, 4, 1));

  // leap year if year % 400 == 0
  /* 2000-03-01 */
  ts = get_ts(2000, 3, 1);
  yearStart = resolveTimeBucketizer(ts, YEAR, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(yearStart, get_ts(2000, 1, 1));

  quarterStart = resolveTimeBucketizer(ts, QUATER, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(quarterStart, get_ts(2000, 1, 1));

  monthStart = resolveTimeBucketizer(ts, MONTH, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(monthStart, get_ts(2000, 3, 1));

  /* 2018-06-11 */
  ts = get_ts(2018, 6, 11);
  yearStart = resolveTimeBucketizer(ts, YEAR, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(yearStart, get_ts(2018, 1, 1));

  quarterStart = resolveTimeBucketizer(ts, QUATER, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(quarterStart, get_ts(2018, 4, 1));

  monthStart = resolveTimeBucketizer(ts, MONTH, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(monthStart, get_ts(2018, 6, 1));
}

TEST(ResolveTimeBucketizerTest, CheckRecurringTimeBucketizer) {
  // works for epoch time.
  uint32_t ts = get_ts(1970, 1, 1);
  uint32_t dayOfYear =
      resolveTimeBucketizer(ts, DAY_OF_YEAR, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(dayOfYear, 0);

  uint32_t dayOfMonth =
      resolveTimeBucketizer(ts, DAY_OF_MONTH, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(dayOfMonth, 0);

  uint32_t monthOfYear =
      resolveTimeBucketizer(ts, MONTH_OF_YEAR, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(monthOfYear, 0);

  uint32_t quarterOfMonth =
      resolveTimeBucketizer(ts, QUARTER_OF_YEAR, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(quarterOfMonth, 0);

  ts = get_ts(1972, 2, 29);
  dayOfYear =
      resolveTimeBucketizer(ts, DAY_OF_YEAR, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(dayOfYear, 59);

  dayOfMonth =
      resolveTimeBucketizer(ts, DAY_OF_MONTH, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(dayOfMonth, 28);

  monthOfYear =
      resolveTimeBucketizer(ts, MONTH_OF_YEAR, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(monthOfYear, 1);

  quarterOfMonth =
      resolveTimeBucketizer(ts, QUARTER_OF_YEAR, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(quarterOfMonth, 0);

  ts = get_ts(1972, 3, 1);
  dayOfYear =
      resolveTimeBucketizer(ts, DAY_OF_YEAR, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(dayOfYear, 60);

  dayOfMonth =
      resolveTimeBucketizer(ts, DAY_OF_MONTH, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(dayOfMonth, 0);

  monthOfYear =
      resolveTimeBucketizer(ts, MONTH_OF_YEAR, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(monthOfYear, 2);

  quarterOfMonth =
      resolveTimeBucketizer(ts, QUARTER_OF_YEAR, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(quarterOfMonth, 0);

  ts = get_ts(2018, 6, 11);
  dayOfYear =
      resolveTimeBucketizer(ts, DAY_OF_YEAR, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(dayOfYear, 161);

  dayOfMonth =
      resolveTimeBucketizer(ts, DAY_OF_MONTH, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(dayOfMonth, 10);

  monthOfYear =
      resolveTimeBucketizer(ts, MONTH_OF_YEAR, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(monthOfYear, 5);

  quarterOfMonth =
      resolveTimeBucketizer(ts, QUARTER_OF_YEAR, DAYS_BEFORE_MONTH_HOST);
  EXPECT_EQ(quarterOfMonth, 1);
}

TEST(getWeekStartTimestamp, CheckWeeklyBucketizer) {
    uint32_t ts = get_ts(1970, 1, 3);
    uint32_t weekts = getWeekStartTimestamp(ts);
    EXPECT_EQ(weekts, 0);

    ts = get_ts(1970, 1, 6);
    weekts = getWeekStartTimestamp(ts);
    EXPECT_EQ(weekts, 345600);

    ts = 1533081655;
    weekts = getWeekStartTimestamp(ts);
    EXPECT_EQ(weekts, 1532908800);

    ts = 1534520171;
    weekts = getWeekStartTimestamp(ts);
    EXPECT_EQ(weekts, 1534118400);

    ts = 1528675200;
    weekts = getWeekStartTimestamp(ts);
    EXPECT_EQ(weekts, 1528675200);
}

TEST(CalculateHLLHashTest, CheckUUIDT) {
  UUIDT uuidT = {0x0000483EC1324C38, 0xBE372EB5A01BBB30};
  UUIDT uuidTs[2] = {uuidT, uuidT};
  bool nulls[2] = {true, false};
  uint32_t expectedValues[2] = {14088, 0};
  bool expectedNulls[2] = {true, false};
  uint32_t outputValues[2] = {0};
  bool outputNulls[2] = {0};

  typedef thrust::zip_iterator<thrust::tuple<UUIDIter, BoolIter> >
      InputZipIterator;
  typedef thrust::zip_iterator<thrust::tuple<Uint32Iter, BoolIter> >
      OutputZipIterator;

  InputZipIterator inputBegin(
      thrust::make_tuple(std::begin(uuidTs), std::begin(nulls)));
  OutputZipIterator outputBegin(
      thrust::make_tuple(std::begin(outputValues), std::begin(outputNulls)));

  thrust::transform(inputBegin, inputBegin + 2, outputBegin,
                    GetHLLValueFunctor<UUIDT>());

  EXPECT_TRUE(thrust::equal(std::begin(outputValues), std::end(outputValues),
                            std::begin(expectedValues)));
  EXPECT_TRUE(thrust::equal(std::begin(outputNulls), std::end(outputNulls),
                            std::begin(expectedNulls)));
}

}  // namespace ares
