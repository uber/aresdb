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

#ifndef MEMUTILS_MEMORY_H_
#define MEMUTILS_MEMORY_H_

#include <stddef.h>
#include "../cgoutils/utils.h"

#ifdef __cplusplus
extern "C" {
#endif

CGoCallResHandle HostAlloc(size_t bytes);

CGoCallResHandle HostFree(void* p);

CGoCallResHandle CreateCudaStream(int device);

CGoCallResHandle WaitForCudaStream(void* s, int device);

CGoCallResHandle DestroyCudaStream(void* s, int device);

CGoCallResHandle DeviceAllocate(size_t bytes, int device);

CGoCallResHandle DeviceFree(void* p, int device);

CGoCallResHandle AsyncCopyHostToDevice(
    void* dst, void* src, size_t bytes, void* stream, int device);

CGoCallResHandle AsyncCopyDeviceToDevice(
    void* dst, void* src, size_t bytes, void* stream, int device);

CGoCallResHandle AsyncCopyDeviceToHost(
    void* dst, void* src, size_t bytes, void* stream, int device);

CGoCallResHandle GetDeviceCount();

CGoCallResHandle GetDeviceGlobalMemoryInMB(int device);

CGoCallResHandle CudaProfilerStart();
CGoCallResHandle CudaProfilerStop();


#ifdef __cplusplus
}
#endif

#endif  // MEMUTILS_MEMORY_H_
