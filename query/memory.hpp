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

#ifndef QUERY_MEMORY_HPP
#define QUERY_MEMORY_HPP

#include <thrust/device_vector.h>
#include <thrust/host_vector.h>
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

// Wrappers over memutils/memory.h but will raise exceptions if error happens.
void deviceMalloc(void **devPtr, size_t size);
void deviceFree(void *devPtr);
void deviceMemset(void *devPtr, int value, size_t count);
void memcpyAsyncHostToDevice(void* dst, const void* src, size_t count,
    cudaStream_t stream);
}  // namespace ares
#endif  // QUERY_MEMORY_HPP
