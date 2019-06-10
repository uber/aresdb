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

#include "query/memory.hpp"
#include <cuda_runtime.h>
#include <cstdlib>
#include <cstring>
#include <string>
#include "cgoutils/memory.h"
#include "query/utils.hpp"

// MemoryError represents a memory error.
class MemoryError : public AlgorithmError{
 public:
  explicit MemoryError(const std::string &message) : AlgorithmError(message) {
  }
};

void checkMemoryError(CGoCallResHandle res) {
  if (res.pStrErr != nullptr) {
    throw MemoryError(res.pStrErr);
  }
}

namespace ares {
void deviceMalloc(void **devPtr, size_t size) {
  checkMemoryError(::deviceMalloc(devPtr, size));
}

void deviceFree(void *devPtr) {
  checkMemoryError(::deviceFree(devPtr));
}

void deviceMemset(void *devPtr, int value, size_t count) {
  checkMemoryError(::deviceMemset(devPtr, value, count));
}

void asyncCopyHostToDevice(void* dst, const void* src, size_t count,
    cudaStream_t stream) {
  checkMemoryError(::asyncCopyHostToDevice(dst, src, count, stream));
}

}  // namespace ares
#endif  // QUERY_MEMORY_HPP
