// Modifications Copyright (c) 2018 Uber Technologies, Inc.
// Copyright (c) 2009 The Go Authors. All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//    * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//    * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//    * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#include "query/functor.h"
#include <tuple>

extern __constant__  uint16_t DAYS_BEFORE_MONTH_DEVICE[13];

namespace ares {

const int SECONDS_PER_HOUR = 60 * 60;
const int SECONDS_PER_DAY = 24 * SECONDS_PER_HOUR;
const int DAYS_PER_400_YEARS = 365 * 400 + 97;
const int DAYS_PER_100_YEARS = 365 * 100 + 24;
const int DAYS_PER_4_YEARS = 365 * 4 + 1;
const int SECONDS_PER_FOUR_DAYS = 4 * SECONDS_PER_DAY;
const int SECONDS_PER_WEEK = 7 * SECONDS_PER_DAY;

// The unsigned zero year for internal calculations.
// Must be 1 mod 400, and times before it will not compute correctly,
// but otherwise can be changed at will.
const int64_t ABSOLUTE_ZERO_TS = -62135596800;

inline __host__ __device__ bool isLeapYear(uint16_t year) {
  return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
}

// For host, daysBeforeMonth will be DAYS_BEFORE_MONTH_HOST, otherwise
// it should be DAYS_BEFORE_MONTH_DEVICE.
inline __host__ __device__ uint16_t
getDaysBeforeMonth(uint8_t month,
                   bool isLeapYear,
                   const uint16_t *daysBeforeMonth) {
  uint16_t days = daysBeforeMonth[month];
  if (isLeapYear && month >= 2) {
    days++;
  }
  return days;
}

// All following code refers to https://golang.org/src/time/time.go
// Line 940. To save space, we only allowed ts >= 0, so time before 1970-01-01
// is not supported.
__host__ __device__
uint32_t resolveTimeBucketizer(int64_t ts,
                               enum TimeBucketizer timeBucketizer,
                               const uint16_t *daysBeforeMonth) {
  // Adjust the timestamp to zero year for calculation.
  ts -= ABSOLUTE_ZERO_TS;
  uint32_t days = ts / SECONDS_PER_DAY;

  // 400 years cycle.
  int64_t n = days / DAYS_PER_400_YEARS;
  uint16_t year = 400 * n;
  int64_t start = n * DAYS_PER_400_YEARS * SECONDS_PER_DAY;
  days -= DAYS_PER_400_YEARS * n;

  // Cut off 100-year cycles.
  // The last cycle has one extra leap year, so on the last day
  // of that year, day / daysPer100Years will be 4 instead of 3.
  // Cut it back down to 3 by subtracting n>>2.
  n = days / DAYS_PER_100_YEARS;
  n -= n >> 2;
  year += 100 * n;
  start += n * DAYS_PER_100_YEARS * SECONDS_PER_DAY;
  days -= DAYS_PER_100_YEARS * n;

  // Cut off 4-year cycles.
  // The last cycle has a missing leap year, which does not
  // affect the computation.
  n = days / DAYS_PER_4_YEARS;
  year += 4 * n;
  start += n * DAYS_PER_4_YEARS * SECONDS_PER_DAY;
  days -= DAYS_PER_4_YEARS * n;


  // Cut off years within a 4-year cycle.
  // The last year is a leap year, so on the last day of that year,
  // day / 365 will be 4 instead of 3. Cut it back down to 3
  // by subtracting n>>2.
  n = days / 365;
  n -= n >> 2;
  year += n;
  days -= 365 * n;
  start += n * 365 * SECONDS_PER_DAY;

  // Adjust back;
  start += ABSOLUTE_ZERO_TS;

  // get day of the year.
  if (timeBucketizer == YEAR) {
    return start;
  }

  // day of the year.
  if (timeBucketizer == DAY_OF_YEAR) {
    return days;
  }

  bool leapYear = isLeapYear(year + 1);
  // Estimate month on assumption that every month has 31 days.
  // The estimate may be too low by at most one month, so adjust.
  uint8_t month = days / 31;
  uint16_t monthEnd = getDaysBeforeMonth(month + 1, leapYear, daysBeforeMonth);
  if (days >= monthEnd) {
    month++;
  }

  if (timeBucketizer == MONTH || timeBucketizer == DAY_OF_MONTH) {
    uint32_t
        dayBeforeMonth = getDaysBeforeMonth(month, leapYear, daysBeforeMonth);
    // month start.
    if (timeBucketizer == MONTH) {
      return start + dayBeforeMonth * SECONDS_PER_DAY;
    }
    // day of month.
    return days - dayBeforeMonth;
  }

  // month of year.
  if (timeBucketizer == MONTH_OF_YEAR) {
    return month;
  }

  int quarter = month / 3;

  // quarter of year.
  if (timeBucketizer == QUARTER_OF_YEAR) {
    return quarter;
  }

  // quarter start.
  return start + getDaysBeforeMonth(quarter * 3, leapYear, daysBeforeMonth)
      * SECONDS_PER_DAY;
}

// The function overload for resolveTimeBucketizer with daysBeforeMonth provided
// according to the running environment.
__host__ __device__
thrust::tuple<uint32_t, bool> resolveTimeBucketizer(
    const thrust::tuple<uint32_t, bool> t,
    enum TimeBucketizer timeBucketizer) {
  if (!thrust::get<1>(t)) {
    return thrust::make_tuple(0, false);
  }

  // Choose daysBeforeMonth based on whether it's running on device.
  const uint16_t *daysBeforeMonth;
#ifdef RUN_ON_DEVICE
  daysBeforeMonth = DAYS_BEFORE_MONTH_DEVICE;
#else
  // Have to allocate this array and assign values in stack as even we
  // compile in host mode, nvcc still thinks it's a device function as we
  // annotate it with __device__.
  uint16_t daysBeforeMonthHost[13] = {
      0,
      31,
      31 + 28,
      31 + 28 + 31,
      31 + 28 + 31 + 30,
      31 + 28 + 31 + 30 + 31,
      31 + 28 + 31 + 30 + 31 + 30,
      31 + 28 + 31 + 30 + 31 + 30 + 31,
      31 + 28 + 31 + 30 + 31 + 30 + 31 + 31,
      31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30,
      31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31,
      31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31 + 30,
      31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31 + 30 + 31,
  };
  daysBeforeMonth = daysBeforeMonthHost;
#endif

  return thrust::make_tuple(
      resolveTimeBucketizer(
          thrust::get<0>(t), timeBucketizer, daysBeforeMonth), true);
}

// The function is used to get the start of week timestamp based on
// the given timestamp, will return 0 if ts is before 1970/1/1
__host__ __device__
uint32_t getWeekStartTimestamp(uint32_t ts) {
    if (ts < SECONDS_PER_FOUR_DAYS) {
        return 0;
    }
    return ts - (ts - SECONDS_PER_FOUR_DAYS) % SECONDS_PER_WEEK;
}

__host__ __device__
thrust::tuple<uint32_t, bool> GetWeekStartFunctor::operator()(
    const thrust::tuple<uint32_t, bool> t) const {
    if (!thrust::get<1>(t)) {
      return thrust::make_tuple(0, false);
    }
    return thrust::make_tuple(getWeekStartTimestamp(thrust::get<0>(t)), true);
}

__host__ __device__
thrust::tuple<uint32_t, bool> GetMonthStartFunctor::operator()(
    const thrust::tuple<uint32_t, bool> t) const {
  return resolveTimeBucketizer(t, MONTH);
}

__host__ __device__
thrust::tuple<uint32_t, bool> GetQuarterStartFunctor::operator()(
    const thrust::tuple<uint32_t, bool> t) const {
  return resolveTimeBucketizer(t, QUATER);
}

__host__ __device__
thrust::tuple<uint32_t, bool> GetYearStartFunctor::operator()(
    const thrust::tuple<uint32_t, bool> t) const {
  return resolveTimeBucketizer(t, YEAR);
}

__host__ __device__
thrust::tuple<uint32_t, bool> GetDayOfMonthFunctor::operator()(
    const thrust::tuple<uint32_t, bool> t) const {
  return resolveTimeBucketizer(t, DAY_OF_MONTH);
}

__host__ __device__
thrust::tuple<uint32_t, bool> GetDayOfYearFunctor::operator()(
    const thrust::tuple<uint32_t, bool> t) const {
  return resolveTimeBucketizer(t, DAY_OF_YEAR);
}

__host__ __device__
thrust::tuple<uint32_t, bool> GetMonthOfYearFunctor::operator()(
    const thrust::tuple<uint32_t, bool> t) const {
  return resolveTimeBucketizer(t, MONTH_OF_YEAR);
}

__host__ __device__
thrust::tuple<uint32_t, bool> GetQuarterOfYearFunctor::operator()(
    const thrust::tuple<uint32_t, bool> t) const {
  return resolveTimeBucketizer(t, QUARTER_OF_YEAR);
}
}  // namespace ares
