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


#ifndef QUERY_FUNCTOR_HPP_
#define QUERY_FUNCTOR_HPP_
#include <cuda_runtime.h>
#include <thrust/tuple.h>
#include <iostream>
#include <tuple>
#include "query/iterator.hpp"
#include "query/time_series_aggregate.h"
#include "query/utils.hpp"

namespace ares {

// logical operators.

struct AndFunctor {
  __host__ __device__
  thrust::tuple<bool, bool> operator()(
      const thrust::tuple<bool, bool> t1,
      const thrust::tuple<bool, bool> t2) const {
    // if one of them is null, the result is null.
    if (!thrust::get<1>(t1) || !thrust::get<1>(t2)) {
      return thrust::make_tuple(false, false);
    }
    return thrust::make_tuple(thrust::get<0>(t1) && thrust::get<0>(t2) != 0,
                              true);
  }
};

struct OrFunctor {
  __host__ __device__
  thrust::tuple<bool, bool> operator()(
      const thrust::tuple<bool, bool> t1,
      const thrust::tuple<bool, bool> t2) const {
    bool value1 = thrust::get<0>(t1);
    bool valid1 = thrust::get<1>(t1);
    bool value2 = thrust::get<0>(t2);
    bool valid2 = thrust::get<1>(t2);

    // If one of them is true, the result is true.
    if ((value1 && valid1) || (value2 && valid2)) {
      return thrust::make_tuple(true, true);
    }

    // Otherwise if one of them is null, the result is null.
    if (!valid1 || !valid2) {
      return thrust::make_tuple(false, false);
    }

    return thrust::make_tuple(false, true);
  }
};

struct NotFunctor {
  __host__ __device__
  thrust::tuple<bool, bool> operator()(
      const thrust::tuple<bool, bool> t) const {
    // Not null is null.
    if (!thrust::get<1>(t)) {
      return thrust::make_tuple(false, false);
    }
    return thrust::make_tuple(!thrust::get<0>(t), true);
  }
};

// comparison operators

template<typename T>
struct EqualFunctor {
  __host__ __device__
  thrust::tuple<bool, bool> operator()(const thrust::tuple<T, bool> t1,
                                       const thrust::tuple<T, bool> t2) const {
    // if one of them is null, the result is null.
    if (!thrust::get<1>(t1) || !thrust::get<1>(t2)) {
      return thrust::make_tuple(false, false);
    }

    return thrust::make_tuple(thrust::get<0>(t1) == thrust::get<0>(t2), true);
  }
};

// Equal functor for GeoPointT.
template<>
struct EqualFunctor<GeoPointT> {
  __host__ __device__
  thrust::tuple<bool, bool> operator()(
      const thrust::tuple<const GeoPointT, bool> t1,
      const thrust::tuple<const GeoPointT, bool> t2) const {
    // if one of them is null, the result is null.
    if (!thrust::get<1>(t1) || !thrust::get<1>(t2)) {
      return thrust::make_tuple(false, false);
    }
    return thrust::make_tuple(
        thrust::get<0>(t1).Lat == thrust::get<0>(t2).Lat &&
            thrust::get<0>(t1).Long == thrust::get<0>(t2).Long,
        true);
  }
};

template<typename T>
struct NotEqualFunctor {
  __host__ __device__
  thrust::tuple<bool, bool> operator()(const thrust::tuple<T, bool> t1,
                                       const thrust::tuple<T, bool> t2) const {
    // if one of them is null, the result is null.
    if (!thrust::get<1>(t1) || !thrust::get<1>(t2)) {
      return thrust::make_tuple(false, false);
    }

    return thrust::make_tuple(thrust::get<0>(t1) != thrust::get<0>(t2), true);
  }
};

template<typename T>
struct LessThanFunctor {
  __host__ __device__
  thrust::tuple<bool, bool> operator()(const thrust::tuple<T, bool> t1,
                                       const thrust::tuple<T, bool> t2) const {
    // if one of them is null, the result is null.
    if (!thrust::get<1>(t1) || !thrust::get<1>(t2)) {
      return thrust::make_tuple(false, false);
    }

    return thrust::make_tuple(thrust::get<0>(t1) < thrust::get<0>(t2), true);
  }
};

template<typename T>
struct LessThanOrEqualFunctor {
  __host__ __device__
  thrust::tuple<bool, bool> operator()(const thrust::tuple<T, bool> t1,
                                       const thrust::tuple<T, bool> t2) const {
    // if one of them is null, the result is null.
    if (!thrust::get<1>(t1) || !thrust::get<1>(t2)) {
      return thrust::make_tuple(false, false);
    }

    return thrust::make_tuple(thrust::get<0>(t1) <= thrust::get<0>(t2), true);
  }
};

template<typename T>
struct GreaterThanFunctor {
  __host__ __device__
  thrust::tuple<bool, bool> operator()(const thrust::tuple<T, bool> t1,
                                       const thrust::tuple<T, bool> t2) const {
    // if one of them is null, the result is null.
    if (!thrust::get<1>(t1) || !thrust::get<1>(t2)) {
      return thrust::make_tuple(false, false);
    }

    return thrust::make_tuple(thrust::get<0>(t1) > thrust::get<0>(t2), true);
  }
};

template<typename T>
struct GreaterThanOrEqualFunctor {
  __host__ __device__
  thrust::tuple<bool, bool> operator()(const thrust::tuple<T, bool> t1,
                                       const thrust::tuple<T, bool> t2) const {
    // if one of them is null, the result is null.
    if (!thrust::get<1>(t1) || !thrust::get<1>(t2)) {
      return thrust::make_tuple(false, false);
    }

    return thrust::make_tuple(thrust::get<0>(t1) >= thrust::get<0>(t2), true);
  }
};

// arithmetic operators

template<typename T>
struct PlusFunctor {
  __host__ __device__
  thrust::tuple<T, bool> operator()(const thrust::tuple<T, bool> t1,
                                    const thrust::tuple<T, bool> t2) const {
    // if one of them is null, the result is null.
    if (!thrust::get<1>(t1) || !thrust::get<1>(t2)) {
      return thrust::make_tuple(0, false);
    }
    return thrust::make_tuple(thrust::get<0>(t1) + thrust::get<0>(t2), true);
  }
};

template<typename T>
struct MinusFunctor {
  __host__ __device__
  thrust::tuple<T, bool> operator()(const thrust::tuple<T, bool> t1,
                                    const thrust::tuple<T, bool> t2) const {
    // if one of them is null, the result is null.
    if (!thrust::get<1>(t1) || !thrust::get<1>(t2)) {
      return thrust::make_tuple(0, false);
    }

    return thrust::make_tuple(thrust::get<0>(t1) - thrust::get<0>(t2), true);
  }
};

template<typename T>
struct MultiplyFunctor {
  __host__ __device__
  thrust::tuple<T, bool> operator()(const thrust::tuple<T, bool> t1,
                                    const thrust::tuple<T, bool> t2) const {
    // if one of them is null, the result is null.
    if (!thrust::get<1>(t1) || !thrust::get<1>(t2)) {
      return thrust::make_tuple(0, false);
    }

    return thrust::make_tuple(thrust::get<0>(t1) * thrust::get<0>(t2), true);
  }
};

template<typename T>
struct DivideFunctor {
  __host__ __device__
  thrust::tuple<T, bool> operator()(const thrust::tuple<T, bool> t1,
                                    const thrust::tuple<T, bool> t2) const {
    // if one of them is null, the result is null.
    if (!thrust::get<1>(t1) || !thrust::get<1>(t2)) {
      return thrust::make_tuple(0, false);
    }

    return thrust::make_tuple(thrust::get<0>(t1) / thrust::get<0>(t2), true);
  }
};

template<typename T>
struct ModFunctor {
  __host__ __device__
  thrust::tuple<T, bool> operator()(const thrust::tuple<T, bool> t1,
                                    const thrust::tuple<T, bool> t2) const {
    // if one of them is null, the result is null.
    if (!thrust::get<1>(t1) || !thrust::get<1>(t2)) {
      return thrust::make_tuple(0, false);
    }

    return thrust::make_tuple(thrust::get<0>(t1) % thrust::get<0>(t2), true);
  }
};

template<typename T>
struct NegateFunctor {
  __host__ __device__
  thrust::tuple<T, bool> operator()(const thrust::tuple<T, bool> t) const {
    bool valid = thrust::get<1>(t);
    if (!valid) {
      return thrust::make_tuple(0, valid);
    }
    return thrust::make_tuple(-thrust::get<0>(t), valid);
  }
};

// bitwise operators

template<typename T>
struct BitwiseAndFunctor {
  __host__ __device__
  thrust::tuple<T, bool> operator()(const thrust::tuple<T, bool> t1,
                                    const thrust::tuple<T, bool> t2) const {
    // if one of them is null, the result is null.
    if (!thrust::get<1>(t1) || !thrust::get<1>(t2)) {
      return thrust::make_tuple(0, false);
    }
    return thrust::make_tuple(thrust::get<0>(t1) & thrust::get<0>(t2), true);
  }
};

template<typename T>
struct BitwiseOrFunctor {
  __host__ __device__
  thrust::tuple<T, bool> operator()(const thrust::tuple<T, bool> t1,
                                    const thrust::tuple<T, bool> t2) const {
    // if one of them is null, the result is null.
    if (!thrust::get<1>(t1) || !thrust::get<1>(t2)) {
      return thrust::make_tuple(0, false);
    }
    return thrust::make_tuple(thrust::get<0>(t1) | thrust::get<0>(t2), true);
  }
};

template<typename T>
struct BitwiseXorFunctor {
  __host__ __device__
  thrust::tuple<T, bool> operator()(const thrust::tuple<T, bool> t1,
                                    const thrust::tuple<T, bool> t2) const {
    // if one of them is null, the result is null.
    if (!thrust::get<1>(t1) || !thrust::get<1>(t2)) {
      return thrust::make_tuple(0, false);
    }
    return thrust::make_tuple(thrust::get<0>(t1) ^ thrust::get<0>(t2), true);
  }
};

template<typename T>
struct BitwiseNotFunctor {
  __host__ __device__
  thrust::tuple<T, bool> operator()(const thrust::tuple<T, bool> t) const {
    if (!thrust::get<1>(t)) {
      return thrust::make_tuple(0, false);
    }
    return thrust::make_tuple(~thrust::get<0>(t), true);
  }
};

template<typename T>
struct FloorFunctor {
  __host__ __device__
  thrust::tuple<T, bool> operator()(const thrust::tuple<T, bool> t1,
                                    const thrust::tuple<T, bool> t2) const {
    // if one of them is null, the result is null.
    if (!thrust::get<1>(t1) || !thrust::get<1>(t2)) {
      return thrust::make_tuple(0, false);
    }

    return thrust::make_tuple(
        thrust::get<0>(t1) - thrust::get<0>(t1) % thrust::get<0>(t2),
        true);
  }
};


// misc operators

struct IsNullFunctor {
  __host__ __device__
  thrust::tuple<bool, bool> operator()(
      const thrust::tuple<bool, bool> t) const {
    return thrust::make_tuple(!thrust::get<1>(t), true);
  }
};

struct IsNotNullFunctor {
  __host__ __device__
  thrust::tuple<bool, bool> operator()(
      const thrust::tuple<bool, bool> t) const {
    return thrust::make_tuple(thrust::get<1>(t), true);
  }
};

// Functor used for filtering based on a single column directly.
// It just return the argument as it is.
template<typename T>
struct NoopFunctor {
  __host__ __device__
  thrust::tuple<T, bool> operator()(const thrust::tuple<T, bool> t) const {
    return t;
  }
};

// date operators.
struct GetWeekStartFunctor {
  __host__ __device__
  thrust::tuple<uint32_t, bool> operator()(
      const thrust::tuple<uint32_t, bool> t) const;
};

struct GetMonthStartFunctor {
  __host__ __device__
  thrust::tuple<uint32_t, bool> operator()(
      const thrust::tuple<uint32_t, bool> t) const;
};

struct GetQuarterStartFunctor {
  __host__ __device__
  thrust::tuple<uint32_t, bool> operator()(
      const thrust::tuple<uint32_t, bool> t) const;
};

struct GetYearStartFunctor {
  __host__ __device__
  thrust::tuple<uint32_t, bool> operator()(
      const thrust::tuple<uint32_t, bool> t) const;
};

struct GetDayOfMonthFunctor {
  __host__ __device__
  thrust::tuple<uint32_t, bool> operator()(
      const thrust::tuple<uint32_t, bool> t) const;
};

struct GetDayOfYearFunctor {
  __host__ __device__
  thrust::tuple<uint32_t, bool> operator()(
      const thrust::tuple<uint32_t, bool> t) const;
};

struct GetMonthOfYearFunctor {
  __host__ __device__
  thrust::tuple<uint32_t, bool> operator()(
      const thrust::tuple<uint32_t, bool> t) const;
};

struct GetQuarterOfYearFunctor {
  __host__ __device__
  thrust::tuple<uint32_t, bool> operator()(
      const thrust::tuple<uint32_t, bool> t) const;
};

template <typename I>
inline __host__ __device__ uint64_t hll_hash(I value) {
  uint64_t hashedOutput[2];
  murmur3sum128(reinterpret_cast<uint8_t *>(&value), sizeof(value), 0,
                hashedOutput);
  return hashedOutput[0];
}

template <>
inline __host__ __device__ uint64_t hll_hash(UUIDT uuid) {
  return uuid.p1 ^ uuid.p2;
}

// GetHLLValueFunctor calcuates the register
template <typename I>
struct GetHLLValueFunctor {
  __host__ __device__ thrust::tuple<uint32_t, bool> operator()(
      thrust::tuple<I, bool> input) const {
    if (!thrust::get<1>(input)) {
      return thrust::make_tuple<uint32_t>(0, false);
    }
    I value = thrust::get<0>(input);
    uint64_t hashed = hll_hash(value);
    uint32_t group = static_cast<uint32_t>(hashed & ((1 << HLL_BITS) - 1));
    uint32_t rho = 0;
    while (true) {
      uint32_t h = hashed & (1 << (rho + HLL_BITS));
      if (rho + HLL_BITS < 64 && h == 0) {
        rho++;
      } else {
        break;
      }
    }
    return thrust::make_tuple(rho << 16 | group, true);
  }
};

// functor to calculate array length
template <typename I, typename O>
struct ArrayLengthFunctor {
  typedef typename thrust::tuple<I, bool> argument_type;
  typedef typename thrust::tuple<O, bool> result_type;

  __host__ __device__
  result_type operator()(argument_type arrVal) const {
    // should never come here
    O o;
    return thrust::make_tuple<O, bool>(o, false);
  }
};

template <typename I>
struct ArrayLengthFunctor<I, uint32_t> {
  typedef typename thrust::tuple<I, bool> argument_type;
  typedef typename thrust::tuple<uint32_t, bool> result_type;

  __host__ __device__
  result_type operator()(argument_type arrVal) const {
    auto valid = thrust::get<1>(arrVal);
    if (!valid) {
      return thrust::make_tuple<uint32_t, bool>(0, false);
    }
    uint32_t *valP = reinterpret_cast<uint32_t *>(thrust::get<0>(arrVal));
    if (valP == nullptr) {
      return thrust::make_tuple<uint32_t, bool>(0, true);
    }
    return thrust::make_tuple<uint32_t, bool>(*valP, true);
  }
};

// functor to get index-th element from array value
template <typename O, typename I1, typename I2, typename Enabled = void>
struct ArrayElementAtFunctor {
  typedef typename thrust::tuple<I1, bool> argument_type_1;
  typedef typename thrust::tuple<I2, bool> argument_type_2;
  typedef typename thrust::tuple<O, bool> result_type;

  __host__ __device__
  result_type operator()(argument_type_1 arg1, argument_type_2 arg2) const {
    O o;
    return thrust::make_tuple<O, bool>(o, false);
  }
};

// specialized ArrayElementAtFunctor which the 2nd paramenter is int32_t
// for position index, the Input and Output may have different data type
// as the aql compiler will convert NumberLitereral to uint32_t or float_t
// for output data type
template <typename O, typename I>
struct ArrayElementAtFunctor<O, I, int32_t> {
  typedef typename thrust::tuple<I, bool> argument_type_1;
  typedef typename thrust::tuple<int32_t, bool> argument_type_2;
  typedef typename thrust::tuple<O, bool> result_type;
  typedef typename std::remove_pointer<I>::type input_type;

  __host__ __device__
  result_type operator()(argument_type_1 arrVal, argument_type_2 indexT) const {
    auto valid = thrust::get<1>(arrVal);
    if (!valid) {
      O v;
      return thrust::make_tuple<O, bool>(v, false);
    }
    uint32_t *lenP = reinterpret_cast<uint32_t *>(thrust::get<0>(arrVal));
    int index = (int)thrust::get<0>(indexT);
    if (lenP == nullptr || (index >= 0 && *lenP <= index) ||
       (index < 0 && *lenP < -index)) {
      O v;
      return thrust::make_tuple<O, bool>(v, false);
    }
    auto valP = reinterpret_cast<I>(
        reinterpret_cast<uint8_t*>(thrust::get<0>(arrVal)) + 4);
    int len = static_cast<int>(*lenP);
    if (index < 0) {
      index = len + index;
    }
    if (len == 0 || index >= len || index < 0) {
        O v;
        return thrust::make_tuple<O, bool>(v, false);
    }
    uint8_t * elemValidP = reinterpret_cast<uint8_t*>(valP) +
        (sizeof(input_type)*8*len + 7) / 8 + index / 8;
    bool elemValid = (*elemValidP & (0x1 << (index%8))) != 0x0;
    if (elemValid) {
        return thrust::make_tuple<O, bool>(
            static_cast<O>(*(valP + index)), true);
    }
    O v;
    return thrust::make_tuple<O, bool>(v, false);
  }
};

// functor to check if specified element value exists in array
template <typename O, typename I1, typename I2, typename Enabled = void>
struct ArrayContainsFunctor {
  typedef typename thrust::tuple<I1, bool> argument_type_1;
  typedef typename thrust::tuple<I2, bool> argument_type_2;
  typedef typename thrust::tuple<O, bool> result_type;

  __host__ __device__
  result_type operator() (argument_type_1 arrVal,
                          argument_type_2 constVal) const {
    O o;
    return thrust::make_tuple<O, bool>(o, false);
  }
};

// specialized ArrayContainsFunctor while the return type is boolean
// the Input and Output type may not always be the same
// the aql compiler will convert NumberLitereral to uint32_t or float_t
// for 2nd argument
template <typename I1, typename I2>
struct ArrayContainsFunctor<bool, I1, I2> {
  typedef typename thrust::tuple<I1, bool> argument_type_1;
  typedef typename thrust::tuple<I2, bool> argument_type_2;
  typedef typename thrust::tuple<bool, bool> result_type;
  typedef typename std::remove_pointer<I1>::type input_type;

  __host__ __device__
  result_type operator() (argument_type_1 arrVal,
                          argument_type_2 constVal) const {
    auto valid = thrust::get<1>(arrVal);
    if (!valid) {
       return thrust::make_tuple<bool, bool>(false, false);
    }
    uint32_t *lenP = reinterpret_cast<uint32_t *>(thrust::get<0>(arrVal));
    if (lenP == nullptr) {
       return thrust::make_tuple<bool, bool>(false, true);
    }
    auto valP = reinterpret_cast<I1>(
        reinterpret_cast<uint8_t*>(thrust::get<0>(arrVal)) + 4);
    int len = static_cast<int>(*lenP);
    if (len <= 0) {
        return thrust::make_tuple<bool, bool>(false, true);
    }
    input_type val = static_cast<input_type>(thrust::get<0>(constVal));
    uint8_t * validStartP = reinterpret_cast<uint8_t*>(valP) +
                            (sizeof(input_type)*8*len + 7) / 8;
    for (int i = 0; i < len; i++) {
      bool elemValid = (*(validStartP + i / 8) & (0x1 << (i%8))) != 0x0;
      if (elemValid && val == *(valP + i)) {
        return thrust::make_tuple<bool, bool>(true, true);
      }
    }
    return thrust::make_tuple<bool, bool>(false, true);
  }
};

// We combine all unary functors into a single functor class and use the
// UnaryFunctorType enum to do RTTI function call. Thereby we reduce number
// of class bindings for thrust template functions.
template<typename O, typename I, typename Enable = void>
struct UnaryFunctor {
  typedef thrust::tuple<I, bool> argument_type;
  typedef thrust::tuple<O, bool> result_type;

  explicit UnaryFunctor(UnaryFunctorType functorType)
      : functorType(functorType) {
  }

  UnaryFunctorType functorType;

  __host__ __device__
  result_type operator()(const argument_type t) const {
    switch (functorType) {
      case Not:return NotFunctor()(t);
      case IsNull:return IsNullFunctor()(t);
      case IsNotNull:return IsNotNullFunctor()(t);
      case Negate:return NegateFunctor<I>()(t);
      case BitwiseNot:return BitwiseNotFunctor<I>()(t);
      case Noop:return NoopFunctor<I>()(t);
      case GetWeekStart: return GetWeekStartFunctor()(t);
      case GetMonthStart: return GetMonthStartFunctor()(t);
      case GetQuarterStart: return GetQuarterStartFunctor()(t);
      case GetYearStart: return GetYearStartFunctor()(t);
      case GetDayOfMonth: return GetDayOfMonthFunctor()(t);
      case GetDayOfYear: return GetDayOfYearFunctor()(t);
      case GetMonthOfYear: return GetMonthOfYearFunctor()(t);
      case GetQuarterOfYear: return GetQuarterOfYearFunctor()(t);
      case GetHLLValue: return GetHLLValueFunctor<I>()(t);
      default:
        // We will not handle uncaught enum here since the AQL compiler
        // should ensure that.
        return t;
    }
  }
};

// Unary functor to support Array type transformation
template<typename O, typename I>
struct UnaryFunctor<O, I,
    typename std::enable_if<std::is_pointer<I>::value>::type> {
  typedef thrust::tuple<I, bool> argument_type;
  typedef thrust::tuple<O, bool> result_type;
  typedef typename std::remove_pointer<I>::type value_type;

  explicit UnaryFunctor(UnaryFunctorType functorType)
      : functorType(functorType) {
  }

  UnaryFunctorType functorType;

  __host__ __device__
  result_type operator()(const argument_type t) const {
    switch (functorType) {
      case ArrayLength: return ArrayLengthFunctor<I, O>()(t);
      default:
        O o;
        return thrust::make_tuple<O, bool>(o, false);
    }
  }
};

// disable unary transformation from any type to UUIDT
template <typename I>
struct UnaryFunctor<UUIDT, I,
    typename std::enable_if<!std::is_pointer<I>::value>::type> {
  typedef thrust::tuple<I, bool> argument_type;
  typedef thrust::tuple<UUIDT, bool> result_type;

  explicit UnaryFunctor(UnaryFunctorType functorType)
      : functorType(functorType) {}

  UnaryFunctorType functorType;

  __host__ __device__ result_type operator()(const argument_type t) const {
    UUIDT uuid = {0, 0};
    return thrust::make_tuple<UUIDT, bool>(uuid, false);
  }
};

// Specialization with float type to avoid illegal functor type template
// generation.
template <typename O>
struct UnaryFunctor<
    O, float_t, typename std::enable_if<!std::is_same<O, UUIDT>::value>::type> {
  typedef thrust::tuple<float_t, bool> argument_type;
  typedef thrust::tuple<O, bool> result_type;

  explicit UnaryFunctor(UnaryFunctorType functorType)
      : functorType(functorType) {}

  UnaryFunctorType functorType;

  __host__ __device__ result_type operator()(const argument_type t) const {
    switch (functorType) {
      case Not:
        return NotFunctor()(t);
      case IsNull:
        return IsNullFunctor()(t);
      case IsNotNull:
        return IsNotNullFunctor()(t);
      case Negate:
        return NegateFunctor<float_t>()(t);
      case Noop:
        return NoopFunctor<float_t>()(t);
      default:
        // We will not handle uncaught enum here since the AQL compiler
        // should ensure that.
        return t;
    }
  }
};

// specialize UnaryFunctor for UUIDT input input type to types other than UUIDT
template <typename O>
struct UnaryFunctor<
    O,
    UUIDT,
    typename std::enable_if<!std::is_same<O, GeoPointT>::value>::type
  > {
  typedef thrust::tuple<UUIDT, bool> argument_type;
  typedef thrust::tuple<O, bool> result_type;

  explicit UnaryFunctor(UnaryFunctorType functorType)
      : functorType(functorType) {}

  UnaryFunctorType functorType;

  __host__ __device__ result_type operator()(const argument_type t) const {
    switch (functorType) {
      case GetHLLValue:
        return GetHLLValueFunctor<UUIDT>()(t);
      default:
        O o;
        return thrust::make_tuple<O, bool>(o, false);
    }
  }
};

// Specialize unary transformation from UUIDT to UUIDT
template <>
struct UnaryFunctor<UUIDT, UUIDT> {
  typedef thrust::tuple<UUIDT, bool> argument_type;
  typedef thrust::tuple<UUIDT, bool> result_type;

  explicit UnaryFunctor(UnaryFunctorType functorType)
      : functorType(functorType) {}

  UnaryFunctorType functorType;

  __host__ __device__ result_type operator()(const argument_type t) const {
    return NoopFunctor<UUIDT>()(t);
  }
};

// Specialize UnaryFunctor for GeoPointT input type to types other than
// GeoPointT
template <typename O>
struct UnaryFunctor<
    O,
    GeoPointT,
    typename std::enable_if<
        !std::is_same<O, GeoPointT>::value && !std::is_same<O, UUIDT>::value
    >::type
  >{
  typedef thrust::tuple<GeoPointT, bool> argument_type;
  typedef thrust::tuple<O, bool> result_type;

  explicit UnaryFunctor(UnaryFunctorType functorType)
      : functorType(functorType) {}

  UnaryFunctorType functorType;

  __host__ __device__ result_type operator()(const argument_type t) const {
       O o;
       return thrust::make_tuple<O, bool>(o, false);
  }
};

// Specialize UnaryFunctor for input type other than GeoPointT to
// GeoPointT output type
template <typename I>
struct UnaryFunctor<
    GeoPointT,
    I,
    typename std::enable_if<!std::is_same<I,
        GeoPointT>::value && !std::is_pointer<I>::value>::type
  > {
  typedef thrust::tuple<I, bool> argument_type;
  typedef thrust::tuple<GeoPointT, bool> result_type;

  explicit UnaryFunctor(UnaryFunctorType functorType)
      : functorType(functorType) {}

  UnaryFunctorType functorType;

  __host__ __device__ result_type operator()(const argument_type t) const {
       GeoPointT o;
       return thrust::make_tuple<GeoPointT, bool>(o, false);
  }
};

// Specialize from GeoPointT to GeoPointT
template <>
struct UnaryFunctor<GeoPointT, GeoPointT> {
  typedef thrust::tuple<GeoPointT, bool> argument_type;
  typedef thrust::tuple<GeoPointT, bool> result_type;

  explicit UnaryFunctor(UnaryFunctorType functorType)
      : functorType(functorType) {}

  UnaryFunctorType functorType;

  __host__ __device__ result_type operator()(const argument_type t) const {
    return NoopFunctor<GeoPointT>()(t);
  }
};


// Specialize from GeoPointT to float_t(to resolve partial specialization tie)
template <>
struct UnaryFunctor<GeoPointT, float_t> {
  typedef thrust::tuple<float_t, bool> argument_type;
  typedef thrust::tuple<GeoPointT, bool> result_type;

  explicit UnaryFunctor(UnaryFunctorType functorType)
      : functorType(functorType) {}

  UnaryFunctorType functorType;

  __host__ __device__ result_type operator()(const argument_type t) const {
     GeoPointT g;
     return thrust::make_tuple<GeoPointT, bool>(g, false);
  }
};

// UnaryPredicateFunctor simply applies the UnaryFunctor f on the argument
// and extract the 1st element of the result tuple which should usually
// be a boolean value.
template<typename O, typename I>
struct UnaryPredicateFunctor {
  explicit UnaryPredicateFunctor(UnaryFunctorType functorType)
      : f(UnaryFunctor<O, I>(functorType)) {
  }

  typedef typename UnaryFunctor<O, I>::argument_type argument_type;

  UnaryFunctor<O, I> f;

  __host__ __device__
  bool operator()(const argument_type t) {
    return thrust::get<0>(f(t));
  }
};

template <typename O, typename I1, typename I2, typename Enable = void>
struct BinaryFunctor {
};

// Same as the single UnaryFunctor class to avoid generating too many class
// bindings for thrust template functions.
template <typename O, typename I1, typename I2>
struct BinaryFunctor<O, I1, I2,
        typename std::enable_if<std::is_same<I1, I2>::value &&
            !std::is_same<I1, float_t>::value &&
            !std::is_same<I1, GeoPointT>::value>::type > {
  typedef thrust::tuple<I1, bool> argument_type_1;
  typedef thrust::tuple<I2, bool> argument_type_2;

  typedef thrust::tuple<O, bool> result_type;

  explicit BinaryFunctor(BinaryFunctorType functorType)
      : functorType(functorType) {
  }

  BinaryFunctorType functorType;

  __host__ __device__
  result_type operator()(const argument_type_1 t1,
        const argument_type_2 t2) const {
    switch (functorType) {
      case And:return AndFunctor()(t1, t2);
      case Or:return OrFunctor()(t1, t2);
      case Equal:return EqualFunctor<I1>()(t1, t2);
      case NotEqual:return NotEqualFunctor<I1>()(t1, t2);
      case LessThan:return LessThanFunctor<I1>()(t1, t2);
      case LessThanOrEqual:return LessThanOrEqualFunctor<I1>()(t1, t2);
      case GreaterThan:return GreaterThanFunctor<I1>()(t1, t2);
      case GreaterThanOrEqual:return GreaterThanOrEqualFunctor<I1>()(t1, t2);
      case Plus:return PlusFunctor<I1>()(t1, t2);
      case Minus:return MinusFunctor<I1>()(t1, t2);
      case Multiply:return MultiplyFunctor<I1>()(t1, t2);
      case Divide:return DivideFunctor<I1>()(t1, t2);
      case Mod:return ModFunctor<I1>()(t1, t2);
      case BitwiseAnd:return BitwiseAndFunctor<I1>()(t1, t2);
      case BitwiseOr:return BitwiseOrFunctor<I1>()(t1, t2);
      case BitwiseXor:return BitwiseXorFunctor<I1>()(t1, t2);
      case Floor:return FloorFunctor<I1>()(t1, t2);
      default:
        // We will not handle uncaught enum here since the AQL compiler
        // should ensure that.
        return t1;
    }
  }
};

// binary functor to support array type transformation
template <typename O, typename I1, typename I2>
struct BinaryFunctor<O, I1, I2,
    typename std::enable_if<std::is_pointer<I1>::value>::type> {
  typedef thrust::tuple<I1, bool> argument_type_1;
  typedef thrust::tuple<I2, bool> argument_type_2;

  typedef thrust::tuple<O, bool> result_type;

  explicit BinaryFunctor(BinaryFunctorType functorType)
      : functorType(functorType) {
  }
  BinaryFunctorType functorType;

  __host__ __device__
  result_type operator()(const argument_type_1 t1,
        const argument_type_2 t2) const {
    switch (functorType) {
      case ArrayContains: return ArrayContainsFunctor<O, I1, I2>()(t1, t2);
      case ArrayElementAt: return ArrayElementAtFunctor<O, I1, I2>()(t1, t2);
      default:
        O o;
        return thrust::make_tuple<O, bool>(o, false);
    }
  }
};

template <typename I>
struct BinaryFunctor<UUIDT, I, I> {
  typedef thrust::tuple<I, bool> argument_type_1;
  typedef thrust::tuple<I, bool> argument_type_2;
  typedef thrust::tuple<UUIDT, bool> result_type;

  explicit BinaryFunctor(BinaryFunctorType functorType)
      : functorType(functorType) {}

  BinaryFunctorType functorType;

  __host__ __device__ result_type operator()(const argument_type_1 t1,
                                             const argument_type_2 t2) const {
    UUIDT uuid = {0, 0};
    return thrust::make_tuple(uuid, false);
  }
};

// Specialization with float type to avoid illegal functor type template
// generation.
template <typename O>
struct BinaryFunctor<
    O, float_t, float_t,
        typename std::enable_if<!std::is_same<O, UUIDT>::value>::type> {
  typedef thrust::tuple<float_t, bool> argument_type_1;
  typedef thrust::tuple<float_t, bool> argument_type_2;
  typedef thrust::tuple<O, bool> result_type;

  explicit BinaryFunctor(BinaryFunctorType functorType)
      : functorType(functorType) {
  }

  BinaryFunctorType functorType;

  __host__ __device__
  result_type operator()(const argument_type_1 t1,
        const argument_type_2 t2) const {
    switch (functorType) {
      case And:return AndFunctor()(t1, t2);
      case Or:return OrFunctor()(t1, t2);
      case Equal:return EqualFunctor<float_t>()(t1, t2);
      case NotEqual:return NotEqualFunctor<float_t>()(t1, t2);
      case LessThan:return LessThanFunctor<float_t>()(t1, t2);
      case LessThanOrEqual:return LessThanOrEqualFunctor<float_t>()(t1, t2);
      case GreaterThan:return GreaterThanFunctor<float_t>()(t1, t2);
      case GreaterThanOrEqual:
        return GreaterThanOrEqualFunctor<float_t>()(t1,
                                                    t2);
      case Plus:return PlusFunctor<float_t>()(t1, t2);
      case Minus:return MinusFunctor<float_t>()(t1, t2);
      case Multiply:return MultiplyFunctor<float_t>()(t1, t2);
      case Divide:return DivideFunctor<float_t>()(t1, t2);
      default:
        // We will not handle uncaught enum here since the AQL compiler
        // should ensure that.
        return t1;
    }
  }
};

// Specialization with GeoPointT type to avoid illegal functor type template
// generation.
template <typename O>
struct BinaryFunctor<
    O, GeoPointT, GeoPointT,
    typename std::enable_if<!std::is_same<O, UUIDT>::value>::type> {
  typedef thrust::tuple<GeoPointT, bool> argument_type_1;
  typedef thrust::tuple<GeoPointT, bool> argument_type_2;
  typedef thrust::tuple<O, bool> result_type;

  explicit BinaryFunctor(BinaryFunctorType functorType)
      : functorType(functorType) {}

  BinaryFunctorType functorType;

  __host__ __device__ result_type operator()(const argument_type_1 t1,
                                             const argument_type_2 t2) const {
    switch (functorType) {
      case Equal:
        return EqualFunctor<GeoPointT>()(t1, t2);
      default:
        // should not came here, GeoPoint only support equal function
        return false;
    }
  }
};

// BinaryPredicateFunctor simply applies the BinaryFunctor f on <lhs, rhs>
// and extract the 1st element of the result tuple which should usually
// be a boolean value.
template<typename O, typename I1, typename I2>
struct BinaryPredicateFunctor {
  explicit BinaryPredicateFunctor(BinaryFunctorType functorType)
      : f(BinaryFunctor<O, I1, I2>(functorType)) {
  }

  typedef typename BinaryFunctor<O, I1, I2>::argument_type_1 argument_type_1;
  typedef typename BinaryFunctor<O, I1, I2>::argument_type_2 argument_type_2;

  BinaryFunctor<O, I1, I2> f;

  __host__ __device__
  bool operator()(const argument_type_1 t1, const argument_type_2 t2) {
    return thrust::get<0>(f(t1, t2));
  }
};

// RemoveFilter is a functor to tell whether we need to remove an index
// from index vector given a pre-computed predicate vector. Note the
// predicate vector tells us whether we need to "keep" the row. So we
// need to negate the predicate value.The argumenttype is a tuple of
// <index seq, index value> where we will only use the 1st element of
// the tuple to indexing the predicate vector.
template<typename Value, typename Predicate>
struct RemoveFilter {
  explicit RemoveFilter(Predicate *predicates) : predicates(predicates) {}

  Predicate *predicates;

  __host__ __device__
  bool operator()(const Value &index) {
    return !predicates[thrust::get<0>(index)];
  }
};

// This functor lookup a value in a hash table
template<typename I>
struct HashLookupFunctor {
  uint8_t *buckets;
  uint8_t *stash;
  uint32_t seeds[4] = {0};

  int keyBytes;
  int bucketBytes;
  // numHashes might be less then 4, but never more than 4
  int numHashes;
  int numBuckets;

  int offsetToSignature;
  int offsetToKey;

  typedef thrust::tuple<I, bool> argument_type;

  explicit HashLookupFunctor(uint8_t *_buckets, uint32_t *_seeds, int _keyBytes,
                             int _numHashes, int _numBuckets) {
    buckets = _buckets;
    keyBytes = _keyBytes;
    numHashes = _numHashes;
    numBuckets = _numBuckets;

    // recordIDBytes + keyBytes + signatureByte
    // No event time here for dimension table join
    int cellBytes = 8 + keyBytes + 1;
    bucketBytes = HASH_BUCKET_SIZE * cellBytes;
    int totalBucketBytes = bucketBytes * numBuckets;
    stash = buckets + totalBucketBytes;
    for (int i = 0; i < _numHashes; i++) {
      seeds[i] = _seeds[i];
    }

    offsetToSignature = HASH_BUCKET_SIZE * 8;
    offsetToKey = offsetToSignature + HASH_BUCKET_SIZE * 1;
  }

  __host__ __device__
  uint8_t getSignature(uint8_t *bucket, int index) const {
    return bucket[offsetToSignature + index];
  }

  __host__ __device__
  uint8_t *getKey(uint8_t *bucket, int index) const {
    return bucket + (offsetToKey + index * keyBytes);
  }

  __host__ __device__
  RecordID getRecordID(uint8_t *bucket,
                       int index) const {
    return reinterpret_cast<RecordID *>(bucket)[index];
  }

  // Note: RecordID{0,0} is used to represent unfound record,
  // for all live batch ids are larger than 0
  // all archive batch ids (epoch date) are larger than 0
  __host__ __device__
  RecordID operator()(const argument_type t) const {
    if (!thrust::get<1>(t)) {
      RecordID recordID = {0, 0};
      return recordID;
    }

    I v = thrust::get<0>(t);
    uint8_t *key = reinterpret_cast<uint8_t *>(&v);
    for (int i = 0; i < numHashes; i++) {
      uint32_t hashValue = murmur3sum32(key, keyBytes, seeds[i]);
      int bucketIndex = hashValue % numBuckets;
      uint8_t *bucket = buckets + bucketIndex * bucketBytes;
      uint8_t signature = (uint8_t)(hashValue >> 24);
      if (signature < 1) {
        signature = 1;
      }

      for (int j = 0; j < HASH_BUCKET_SIZE; j++) {
        if (signature == getSignature(bucket, j) &&
            memequal(getKey(bucket, j), key, keyBytes)) {
          return getRecordID(bucket, j);
        }
      }
    }

    for (int j = 0; j < HASH_STASH_SIZE; j++) {
      if (getSignature(stash, j) != 0 &&
          memequal(getKey(stash, j), key, keyBytes)) {
        return getRecordID(stash, j);
      }
    }

    RecordID recordID = {0, 0};
    return recordID;
  }
};

// ReduceByHashFunctor is the binaryOp for reduce dimIndexVector and values
// together, according to hash vector as key for reduction AssociativeOperator
// is the binary operator for reducing values eg. we have:
//  hashVector:     [1,1,2,2]
//  dimIndexVector: [3,1,2,0]
//  valueVector:    [1.0,2.0,3.0,4.0]
// if we have AssociativeOperator of thrust::plus<uint32_t>
// then we have value_type of uint32_t
// the result of dimIndexVector is [3,2] and
// the result of valueVector is    [3.0,7.0]
template<typename AssociativeOperator>
struct ReduceByHashFunctor {
  typedef typename AssociativeOperator::first_argument_type value_type;
  AssociativeOperator binaryOp;

  __host__ __device__
  explicit ReduceByHashFunctor(AssociativeOperator binaryOp)
      : binaryOp(binaryOp) {}

  __host__ __device__
  thrust::tuple<uint32_t, value_type> operator()(
      const thrust::tuple<uint32_t, value_type> t1,
      const thrust::tuple<uint32_t, value_type> t2) const {
    value_type ret = binaryOp(thrust::get<1>(t1), thrust::get<1>(t2));
    return thrust::make_tuple(thrust::get<0>(t1), ret);
  }
};

// HLLHashFunctor calculate hash value combining dimension 64bit hash
// with reg_id of hll value The final 64bit will use higher 48bit of the
// dimension hash and lower 16bit of hll value
struct HLLHashFunctor {
  __host__ __device__
  uint64_t operator()(uint64_t hash,
                      uint32_t hllValue) const {
    return (hash & 0xFFFFFFFFFFFF0000) | (hllValue & 0x3FFF);
  }
};

// HLLDimNotEqualFunctor only compares the first 48bit
// of the hash to determine whether two hashes represent the same dimension
struct HLLDimNotEqualFunctor {
  __host__ __device__
  bool operator()(uint64_t v1, uint64_t v2) const {
    return (v1 >> 16) ^ (v2 >> 16);
  }
};

// HLLMergeComparator
// first sort by hash value, then break tie by hll value
struct HLLMergeComparator {
  __host__ __device__
  bool operator()(
      thrust::tuple<uint64_t, uint32_t> t1,
      thrust::tuple<uint64_t, uint32_t> t2) const {
    if (thrust::get<0>(t1) == thrust::get<0>(t2)) {
      return thrust::get<1>(t1) > thrust::get<1>(t2);
    }
    return thrust::get<0>(t1) < thrust::get<0>(t2);
  }
};

// HLLDimByteCountFunctor
struct HLLDimByteCountFunctor {
  __host__ __device__
  uint64_t operator()(uint16_t regCount) const {
    if (regCount < HLL_DENSE_THRESHOLD) {
      return (uint64_t) regCount * 4;
    } else {
      return HLL_DENSE_SIZE;
    }
  }
};

template<typename I, typename O>
struct CastFunctor {
  __host__ __device__
  O operator()(I in) const {
    return static_cast<O>(in);
  }
};

// CopyHLLFunctor
struct CopyHLLFunctor {
  uint8_t *hllVector;
  __host__ __device__
  explicit CopyHLLFunctor(uint8_t *hllVector)
      : hllVector(hllVector) {}

  __host__ __device__
  int operator()(
      thrust::tuple<uint64_t, uint32_t, int> t) const {
    uint64_t offset = thrust::get<0>(t);
    uint32_t value = thrust::get<1>(t);
    uint16_t regID = static_cast<uint16_t>(value & 0x3FFF);
    // rho must plus 1
    uint8_t rho = static_cast<uint8_t>((value >> 16) & 0xFF) + 1;
    int numBytes = thrust::get<2>(t);
    if (numBytes == 4) {
      *reinterpret_cast<uint32_t *>(hllVector + offset) =
          static_cast<uint32_t>(rho << 16 | regID);
    } else {
      *reinterpret_cast<uint8_t *>(hllVector + offset) = rho;
    }
    return 0;
  }
};

// dateutils

enum TimeBucketizer {
  YEAR,
  QUATER,
  MONTH,
  DAY_OF_MONTH,
  DAY_OF_YEAR,
  MONTH_OF_YEAR,
  QUARTER_OF_YEAR,
};

// resolveTimeBucketizer returns the start timestamp of a time that ts
// represents if the timeBucketizer is a time series bucketizer (
// YEAR/QUARTER/MONTH). If the time bucketizer is a recurring time bucketizer(
// DAY_OF_MONTH/DAY_OF_YEAR/MONTH_OF_YEAR/QUARTER_OF_YEAR), it will return the
// number of units of the bucketizer. Note daysBeforeMonth need to be passed by
// caller so that the device/host logic is determined outside of this function.
__host__ __device__
uint32_t
resolveTimeBucketizer(int64_t ts, enum TimeBucketizer timeBucketizer,
                      const uint16_t *daysBeforeMonth);

__host__ __device__
uint32_t
getWeekStartTimestamp(uint32_t ts);

// Forward declaration of the struct.
struct EmptyStruct;

// VoidFunctor is the functor to take empty struct as argument
// and produce no result. Notice it does not occupy any memory space.
struct VoidFunctor {
  __host__ __device__
  void operator()(EmptyStruct) const {
  }
};

struct RollingAvgFunctor {
  typedef uint64_t first_argument_type;
  typedef uint64_t second_argument_type;
  typedef uint64_t result_type;

  __host__  __device__ uint64_t operator()(
      uint64_t lhs, uint64_t rhs) const {
    uint32_t lCount = lhs >> 32;
    uint32_t rCount = rhs >> 32;
    uint32_t totalCount = lCount + rCount;
    if (totalCount == 0) {
      return 0;
    }

    uint64_t res = 0;
    *(reinterpret_cast<uint32_t *>(&res) + 1) = totalCount;
    // do division first to avoid overflow.
    *reinterpret_cast<float_t*>(&res) =
        *reinterpret_cast<float_t*>(&lhs) / totalCount * lCount +
        *reinterpret_cast<float_t*>(&rhs) / totalCount * rCount;
    return res;
  }
};

}  // namespace ares
#endif  // QUERY_FUNCTOR_HPP_
