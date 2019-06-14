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

#ifndef QUERY_UNITTEST_UTILS_HPP_
#define QUERY_UNITTEST_UTILS_HPP_

#include <thrust/device_vector.h>
#include <thrust/equal.h>
#include <thrust/execution_policy.h>
#include <thrust/host_vector.h>
#include <thrust/transform.h>
#include <algorithm>
#include <cmath>
#include <functional>
#include <tuple>
#include "memory.hpp"
#include "utils.hpp"

typedef typename thrust::host_vector<unsigned char>::iterator charIter;
typedef typename thrust::host_vector<uint32_t>::iterator UInt32Iter;
typedef typename thrust::host_vector<int>::iterator IntIter;
typedef typename thrust::host_vector<bool>::iterator BoolIter;
typedef typename thrust::host_vector<uint8_t>::iterator Uint8Iter;
typedef typename thrust::host_vector<uint16_t>::iterator Uint16Iter;

struct float_compare_func {
  __host__ __device__
  bool operator()(float x, float y) const {
    return abs(x - y) < 0.0001;
  }
};

// Functor to compare the value element of a tuple against an expected value.
template<int N>
struct tuple_compare_func {
  template<typename Value>
  __host__ __device__
  bool operator()(thrust::tuple<Value, bool> t, Value v) const {
    return thrust::get<N>(t) == v;
  }

  __host__ __device__
  bool operator()(thrust::tuple<float_t, bool> t, float_t v) const {
    return float_compare_func()(thrust::get<N>(t), v);
  }
};

// compare_value extracts element from tuples returned by Iterator1 and compare
// with Iterator2.
template<typename Iterator1, typename Iterator2, int N>
inline bool compare_tuple(Iterator1 begin,
                          Iterator1 end,
                          Iterator2 expectedBegin) {
#ifdef RUN_ON_DEVICE
  int size = end - begin;
  typedef typename thrust::iterator_traits<Iterator1>::value_type V;
  ares::device_vector<V> actualD(size);
  thrust::copy(thrust::device, begin, end, actualD.begin());
  thrust::host_vector<V> actualH(size);
  cudaMemcpy(actualH.data(), thrust::raw_pointer_cast(actualD.data()),
      sizeof(V) * size, cudaMemcpyDeviceToHost);
  CheckCUDAError("cudaMemcpy");
  return std::equal(actualH.begin(), actualH.end(), expectedBegin,
                    tuple_compare_func<N>());
#else
  return std::equal(begin, end, expectedBegin, tuple_compare_func<N>());
#endif
}

template<typename Iterator1, typename Iterator2>
inline bool compare_value(Iterator1 begin, Iterator1 end, Iterator2 expected) {
  return compare_tuple<Iterator1, Iterator2, 0>(begin, end, expected);
}

template<typename Iterator1, typename Iterator2>
inline bool compare_null(Iterator1 begin, Iterator1 end, Iterator2 expected) {
  return compare_tuple<Iterator1, Iterator2, 1>(begin, end, expected);
}

// Pointer returned by this function must be released by caller.
template<typename V>
inline V *allocate(V *input, int size) {
  size_t totalSize = sizeof(V) * size;
  V *ptr;
#ifdef RUN_ON_DEVICE
  ares::deviceMalloc(reinterpret_cast<void **>(&ptr), totalSize);
  cudaMemcpy(ptr, input, totalSize, cudaMemcpyHostToDevice);
  CheckCUDAError("cudaMemcpy");
#else
  ptr = reinterpret_cast<V *>(malloc(totalSize));
  memcpy(reinterpret_cast<void *>(ptr),
         reinterpret_cast<void *>(input),
         totalSize);
#endif
  return ptr;
}

inline int align_offset(int offset, int alignment) {
  return (offset + alignment - 1) / alignment * alignment;
}

// Pointer returned by this function must be released by caller. Pointers
// will be aligned by 8 bytes. Note if it's scratch space, values comes
// before nulls. So 5th parameter should be values bytes and 6th parameter
// is nullsBytes.
template<typename Value>
inline uint8_t *
allocate_column(uint32_t *counts,
                uint8_t *nulls,
                Value *values,
                int countsBytes,
                int nullsBytes, int valuesBytes) {
  uint8_t *ptr;
  int alignedCountsBytes = align_offset(countsBytes, 8);
  int alignedNullsBytes = align_offset(nullsBytes, 8);
  int alignedValuesBytes = align_offset(valuesBytes, 8);
  int totalBytes = alignedCountsBytes + alignedNullsBytes + alignedValuesBytes;
#ifdef RUN_ON_DEVICE
  ares::deviceMalloc(reinterpret_cast<void **>(&ptr), totalBytes);
  if (counts != nullptr) {
    cudaMemcpy(ptr, counts, countsBytes, cudaMemcpyHostToDevice);
    CheckCUDAError("cudaMemcpy counts");
  }

  if (nulls != nullptr) {
    cudaMemcpy(ptr + alignedCountsBytes, nulls, nullsBytes,
               cudaMemcpyHostToDevice);
    CheckCUDAError("cudaMemcpy nulls");
  }
  cudaMemcpy(ptr + alignedCountsBytes + alignedNullsBytes,
             reinterpret_cast<void *>(values),
             valuesBytes,
             cudaMemcpyHostToDevice);
  CheckCUDAError("cudaMemcpy values");
#else
  ptr = reinterpret_cast<uint8_t *>(malloc(totalBytes));
  if (counts != nullptr) {
    memcpy(ptr, counts, countsBytes);
  }

  if (nulls != nullptr) {
    memcpy(ptr + alignedCountsBytes, nulls, nullsBytes);
  }
  memcpy(ptr + alignedCountsBytes + alignedNullsBytes,
         reinterpret_cast<void *>(values),
         valuesBytes);
#endif
  return ptr;
}

template<typename V, typename CmpFunc>
inline bool equal(V *resBegin, V *resEnd, V *expectedBegin, CmpFunc f) {
#ifdef RUN_ON_DEVICE
  int size = resEnd - resBegin;
  thrust::host_vector<V> expectedH(expectedBegin, expectedBegin + size);
  thrust::device_vector<V> expectedV = expectedH;
  return thrust::equal(thrust::device, resBegin, resEnd, expectedV.begin(), f);
#else
  return thrust::equal(resBegin, resEnd, expectedBegin, f);
#endif
}

template<typename V>
inline bool equal(V *resBegin, V *resEnd, V *expectedBegin) {
  return equal(resBegin, resEnd, expectedBegin, thrust::equal_to<V>());
}

inline bool equal(float *resBegin, float *resEnd, float *expectedBegin) {
  return equal(resBegin, resEnd, expectedBegin, float_compare_func());
}

// equal_print prints the result if it's running on host and returns whether
// the result is as expected.
template<typename V, typename CmpFunc>
inline bool equal_print(V *resBegin, V *resEnd, V *expectedBegin, CmpFunc f) {
  int size = resEnd - resBegin;
#ifdef RUN_ON_DEVICE
  thrust::host_vector<V> expectedH(expectedBegin, expectedBegin + size);
  thrust::device_vector<V> expectedV = expectedH;
  thrust::device_vector<V> actualD(resBegin, resEnd);
  thrust::device_vector<V> actualH = actualD;
  std::cout << "result:" << std::endl;
  std::ostream_iterator<int > out_it(std::cout, ", ");
  std::copy(actualH.begin(), actualH.end(), out_it);
  std::cout << std::endl;
  std::cout << "expected:" << std::endl;
  std::copy(expectedH.begin(), expectedH.end(), out_it);
  std::cout << std::endl;
  return thrust::equal(thrust::device, resBegin, resEnd, expectedV.begin(), f);
#else
  std::cout << "result:" << std::endl;
  std::ostream_iterator<int > out_it(std::cout, ", ");
  std::copy(resBegin, resEnd, out_it);
  std::cout << std::endl;
  std::cout << "expected:" << std::endl;
  std::copy(expectedBegin, expectedBegin + size, out_it);
  std::cout << std::endl;
  return thrust::equal(resBegin, resEnd, expectedBegin, f);
#endif
}

template<typename V>
inline bool equal_print(V *resBegin, V *resEnd, V *expectedBegin) {
  return equal_print(resBegin, resEnd, expectedBegin, thrust::equal_to<V>());
}

inline bool equal_print(float *resBegin, float *resEnd, float *expectedBegin) {
  return equal_print(resBegin, resEnd, expectedBegin, float_compare_func());
}

inline uint32_t get_ts(int year, int month, int day) {
  std::tm tm;
  tm.tm_hour = 0;
  tm.tm_min = 0;
  tm.tm_sec = 0;
  tm.tm_mday = day;
  tm.tm_mon = month - 1;
  tm.tm_year = year - 1900;
  return static_cast<uint32_t >(timegm(&tm));
}

inline GeoShapeBatch get_geo_shape_batch(const float *shapeLatsH,
                                         const float *shapeLongsH,
                                         const uint8_t *shapeIndexsH,
                                         uint8_t numShapes,
                                         int32_t totalNumPoints) {
  uint8_t *shapeLatLongsH = reinterpret_cast<uint8_t *>(
      malloc(totalNumPoints * 4 * 2 + totalNumPoints));
  for (int i = 0; i < totalNumPoints; i++) {
    reinterpret_cast<float *>(shapeLatLongsH)[i] = shapeLatsH[i];
  }
  for (int i = 0; i < totalNumPoints; i++) {
    reinterpret_cast<float *>(shapeLatLongsH)[totalNumPoints + i] =
        shapeLongsH[i];
  }
  for (int i = 0; i < totalNumPoints; i++) {
    shapeLatLongsH[totalNumPoints * 8 + i] = shapeIndexsH[i];
  }
  uint8_t *shapeLatLongs =
      allocate(shapeLatLongsH, totalNumPoints * 4 * 2 + totalNumPoints);
  uint8_t totalWords = (numShapes + 31) / 32;
  GeoShapeBatch geoShapeBatch = {shapeLatLongs, totalNumPoints, totalWords};
  free(shapeLatLongsH);
  return geoShapeBatch;
}

inline GeoShape get_geo_shape(const float *shapeLatH,
                              const float *shapeLongH, uint16_t numPoints) {
  float *shapeLat = allocate(const_cast<float *>(shapeLatH), numPoints);
  float *shapeLong = allocate(const_cast<float *>(shapeLongH), numPoints);
  GeoShape geoShape = {shapeLat, shapeLong, numPoints};
  return geoShape;
}

template<typename V>
inline void release(V* devPtr) {
  ares::deviceFree(devPtr);
}

template<typename V>
inline void copy_device_to_host(V* dst, V* src, size_t size) {
  size_t totalSize = size * sizeof(V);
  ares::asyncCopyDeviceToHost(reinterpret_cast<void *>(dst),
                              reinterpret_cast<void *>(src),
                              totalSize,
                              0);
  ares::waitForCudaStream(0);
}

inline void release(GeoShapeBatch shapes) {
  ares::deviceFree(shapes.LatLongs);
}

inline void release(GeoShape shape) {
  ares::deviceFree(shape.Lats);
  ares::deviceFree(shape.Longs);
}

#endif  // QUERY_UNITTEST_UTILS_HPP_
