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

#ifndef CGOUTILS_MEMORY_H_
#define CGOUTILS_MEMORY_H_

#include <stddef.h>
#include <stdint.h>
#include "utils.h"

#ifdef __cplusplus
extern "C" {
#endif

enum {
  DEVICE_MEMORY_IMPLEMENTATION_FLAG = 1,
  POOLED_MEMORY_FLAG = 1 << 1
  HASH_REDUCTION_SUPPORT = 1 << 2;
};

char NOT_SUPPORTED_ERR_MSG[] = "Not supported";

// device_memory_flags_t & 0x1: HOST(0) or DEVICE(1) implementation
// device_memory_flags_t & 0x2: USE POOLED MEMORY MANAGEMENT OR NOT
// device_memory_flags_t & 0x4: SUPPORT HASH REDUCTION OR NOT
typedef uint32_t DeviceMemoryFlags;

// We don't wrap the result with CGoCallResHandle as we know
// GetFlags is a safe call that will not throw any exceptions.
// Change to CGoCallResHandle when future it will throw any.
DeviceMemoryFlags GetFlags();

CGoCallResHandle HostAlloc(size_t bytes);

CGoCallResHandle HostFree(void *p);

CGoCallResHandle CreateCudaStream(int device);

CGoCallResHandle WaitForCudaStream(void *s, int device);

CGoCallResHandle DestroyCudaStream(void *s, int device);

CGoCallResHandle DeviceAllocate(size_t bytes, int device);

CGoCallResHandle DeviceFree(void *p, int device);

CGoCallResHandle AsyncCopyHostToDevice(
    void *dst, void *src, size_t bytes, void *stream, int device);

CGoCallResHandle AsyncCopyDeviceToDevice(
    void *dst, void *src, size_t bytes, void *stream, int device);

CGoCallResHandle AsyncCopyDeviceToHost(
    void *dst, void *src, size_t bytes, void *stream, int device);

CGoCallResHandle GetDeviceCount();

CGoCallResHandle GetDeviceGlobalMemoryInMB(int device);

CGoCallResHandle CudaProfilerStart();

CGoCallResHandle CudaProfilerStop();

CGoCallResHandle GetDeviceMemoryInfo(size_t *freeSize, size_t *totalSize,
    int device);

// All following functions are called by libalgorithm.so only, not intended
// to be exposed to go. Caller need to call cudaSetDevice before invocation.
CGoCallResHandle deviceMalloc(void **devPtr, size_t size);
CGoCallResHandle deviceFree(void *devPtr);
CGoCallResHandle deviceMemset(void *devPtr, int value, size_t count);
CGoCallResHandle asyncCopyHostToDevice(void *dst, const void *src,
                                       size_t count, void *stream);
CGoCallResHandle asyncCopyDeviceToHost(void *dst, const void *src,
                                       size_t count, void *stream);

CGoCallResHandle waitForCudaStream(void *stream);
#ifdef __cplusplus
}
#endif

#endif  // CGOUTILS_MEMORY_H_
