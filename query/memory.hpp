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

#ifndef QUERY_MEMORY_HPP_
#define QUERY_MEMORY_HPP_

#include <thrust/device_vector.h>
#include <thrust/host_vector.h>
#include <memory>
#include <utility>
#include "utils.hpp"
#ifdef USE_RMM
#include <rmm/thrust_rmm_allocator.h>
#endif

namespace ares {
template<typename V>
#ifdef RUN_ON_DEVICE
# ifdef USE_RMM
using device_vector = rmm::device_vector<V>;
# else
using device_vector = thrust::device_vector<V>;
# endif
#else
using device_vector = thrust::host_vector<V>;
#endif

// Wrappers over cgoutils/memory.h but will raise exceptions if error happens.
void deviceMalloc(void **devPtr, size_t size);
void deviceFree(void *devPtr);
void deviceMemset(void *devPtr, int value, size_t count);
void asyncCopyHostToDevice(void* dst, const void* src, size_t count,
    cudaStream_t stream);
void asyncCopyDeviceToHost(void* dst, const void* src, size_t count,
    cudaStream_t stream);
void waitForCudaStream(void *stream);

// deleter called from unique_ptr.
template <typename T>
struct device_deleter{
  void operator()(T* p) {
    deviceFree(p);
  }
};

template <typename T>
using device_unique_ptr = std::unique_ptr<T, device_deleter<T> >;

template <typename T>
using host_unique_ptr = std::unique_ptr<T>;

// This should only be used for POD type without custom constructor and
// destructor, e.g. primitive types or plain structs.
template<class T, class... Args>
device_unique_ptr<T> make_device_unique(cudaStream_t cudaStream,
                                        Args &&... args) {
  T tHost(std::forward<Args>(args)...);
  T *tDevice;
  deviceMalloc(reinterpret_cast<void **>(&tDevice), sizeof(T));
  deviceMemset(tDevice, 0, sizeof(T));
  ares::asyncCopyHostToDevice(
      reinterpret_cast<void *>(tDevice),
      reinterpret_cast<void *>(&tHost),
      sizeof(T), cudaStream);
  return device_unique_ptr<T>(tDevice);
}

// Resource management is done by host unique_ptr. This is unique_ptr is only
// responsible for device memory allocated to hold this object. Other resource
// is managed by hostPtr.
template<class T>
device_unique_ptr<T> make_device_unique(cudaStream_t cudaStream,
                                        T* hostPtr) {
  T *tDevice;
  deviceMalloc(reinterpret_cast<void **>(&tDevice), sizeof(T));
  deviceMemset(tDevice, 0, sizeof(T));
  ares::asyncCopyHostToDevice(
      reinterpret_cast<void *>(tDevice),
      reinterpret_cast<void *>(hostPtr),
      sizeof(T), cudaStream);
  return device_unique_ptr<T>(tDevice);
}

template<class T, class... Args>
host_unique_ptr<T> make_host_unique(Args &&... args) {
  return host_unique_ptr<T>(new T(std::forward<Args>(args)...));
}

}  // namespace ares
#endif  // QUERY_MEMORY_HPP_
