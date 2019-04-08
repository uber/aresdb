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

#include <cuda_runtime.h>
#include <cuda_profiler_api.h>
#include <rmm/rmm.h>
#include <rmm/rmm_api.h>
#include <cstdio>
#include <cstring>

#include "../memory.h"

const int MAX_ERROR_LEN = 100;

// checkCUDAError checks the cuda error of last runtime calls and returns the
// pointer to the buffer of error message. This buffer needs to be released
// by caller or upper callers.
char *checkCUDAError(const char *message) {
  cudaError_t error = cudaGetLastError();
  if (error != cudaSuccess) {
    char *buffer = reinterpret_cast<char *>(malloc(MAX_ERROR_LEN));
    snprintf(buffer, MAX_ERROR_LEN,
             "ERROR when calling CUDA functions from host: %s: %s\n",
             message, cudaGetErrorString(error));
    return buffer;
  }
  return NULL;
}

char *checkRMMError(rmmError_t rmmError, const char* message) {
  if (rmmError != RMM_SUCCESS) {
    char *buffer = reinterpret_cast<char *>(malloc(MAX_ERROR_LEN));
    snprintf(buffer, MAX_ERROR_LEN,
             "ERROR when calling RMM functions: %s: %s\n",
             message, rmmGetErrorString(rmmError));
    return buffer;
  }
  return NULL;
}

DeviceMemoryFlags GetFlags() {
  return DEVICE_MEMORY_IMPLEMENTATION_FLAG & POOLED_MEMORY_FLAG;
}

CGoCallResHandle Init() {
  CGoCallResHandle resHandle = GetDeviceCount();
  if (resHandle.pStrErr != nullptr) {
    return resHandle;
  }

  size_t deviceCount = reinterpret_cast<size_t>(resHandle.res);
  for (size_t device = 0; device < deviceCount; device++) {
    cudaSetDevice(device);
    rmmOptions_t options = {
        PoolAllocation,
        0,  // Default to half ot total memory
        false  // Disable logging.
    };
    resHandle.pStrErr = checkRMMError(rmmInitialize(&options), "rmmInitialize");
    if (resHandle.pStrErr != nullptr) {
      return resHandle;
    }
  }
  return resHandle;
}

CGoCallResHandle DeviceAllocate(size_t bytes, int device) {
  CGoCallResHandle resHandle = {NULL, NULL};
  cudaSetDevice(device);
  // For now use default stream to avoid changing the memory allocation
  // interface.
  // TODO(lucafuji): use the stream of current execution pipeline for
  // allocation and free.
  resHandle.pStrErr = checkRMMError(RMM_ALLOC(&resHandle.res, bytes, 0),
      "DeviceAllocate");
  if (resHandle.pStrErr == nullptr) {
    cudaMemset(resHandle.res, 0, bytes);
    resHandle.pStrErr = checkCUDAError("DeviceAllocate");
  }
  return resHandle;
}

CGoCallResHandle DeviceFree(void *p, int device) {
  CGoCallResHandle resHandle = {NULL, NULL};
  cudaSetDevice(device);
  // For now use default stream to avoid changing the memory allocation
  // interface.
  // TODO(lucafuji): use the stream of current execution pipeline for
  // allocation and free.
  resHandle.pStrErr = checkRMMError(RMM_FREE(p, 0),
      "DeviceFree");
  return resHandle;
}

// All following function implementation is the same as cuda_malloc.cu.
// We might remove cuda_malloc.cu file after RMM is proven to be working
// in production environment

CGoCallResHandle HostAlloc(size_t bytes) {
  CGoCallResHandle resHandle = {NULL, NULL};
  // cudaHostAllocPortable makes sure that the allocation is associated with all
  // devices, not just the current device.
  cudaHostAlloc(&resHandle.res, bytes, cudaHostAllocPortable);
  memset(resHandle.res, 0, bytes);
  resHandle.pStrErr = checkCUDAError("Allocate");
  return resHandle;
}

CGoCallResHandle HostFree(void *p) {
  CGoCallResHandle resHandle = {NULL, NULL};
  cudaFreeHost(p);
  resHandle.pStrErr = checkCUDAError("Free");
  return resHandle;
}

CGoCallResHandle CreateCudaStream(int device) {
  CGoCallResHandle resHandle = {NULL, NULL};
  cudaSetDevice(device);
  cudaStream_t s = NULL;
  cudaStreamCreate(&s);
  resHandle.res = reinterpret_cast<void *>(s);
  resHandle.pStrErr = checkCUDAError("CreateCudaStream");
  return resHandle;
}

CGoCallResHandle WaitForCudaStream(void *s, int device) {
  CGoCallResHandle resHandle = {NULL, NULL};
  cudaSetDevice(device);
  cudaStreamSynchronize((cudaStream_t) s);
  resHandle.pStrErr = checkCUDAError("WaitForCudaStream");
  return resHandle;
}

CGoCallResHandle DestroyCudaStream(void *s, int device) {
  CGoCallResHandle resHandle = {NULL, NULL};
  cudaSetDevice(device);
  cudaStreamDestroy((cudaStream_t) s);
  resHandle.pStrErr = checkCUDAError("DestroyCudaStream");
  return resHandle;
}

CGoCallResHandle AsyncCopyHostToDevice(
    void *dst, void *src, size_t bytes, void *stream, int device) {
  CGoCallResHandle resHandle = {NULL, NULL};
  cudaSetDevice(device);
  cudaMemcpyAsync(dst, src, bytes,
                  cudaMemcpyHostToDevice, (cudaStream_t) stream);
  resHandle.pStrErr = checkCUDAError("AsyncCopyHostToDevice");
  return resHandle;
}

CGoCallResHandle AsyncCopyDeviceToDevice(
    void *dst, void *src, size_t bytes, void *stream, int device) {
  CGoCallResHandle resHandle = {NULL, NULL};
  cudaSetDevice(device);
  cudaMemcpyAsync(dst, src, bytes,
                  cudaMemcpyDeviceToDevice, (cudaStream_t) stream);
  resHandle.pStrErr = checkCUDAError("AsyncCopyDeviceToDevice");
  return resHandle;
}

CGoCallResHandle AsyncCopyDeviceToHost(
    void *dst, void *src, size_t bytes, void *stream, int device) {
  CGoCallResHandle resHandle = {NULL, NULL};
  cudaSetDevice(device);
  cudaMemcpyAsync(dst, src, bytes,
                  cudaMemcpyDeviceToHost, (cudaStream_t) stream);
  resHandle.pStrErr = checkCUDAError("AsyncCopyDeviceToHost");
  return resHandle;
}

CGoCallResHandle GetDeviceCount() {
  CGoCallResHandle resHandle = {NULL, NULL};
  cudaGetDeviceCount(reinterpret_cast<int *>(&resHandle.res));
  resHandle.pStrErr = checkCUDAError("GetDeviceCount");
  return resHandle;
}

CGoCallResHandle GetDeviceGlobalMemoryInMB(int device) {
  CGoCallResHandle resHandle = {NULL, NULL};
  cudaDeviceProp prop;
  cudaGetDeviceProperties(&prop, device);
  resHandle.res = reinterpret_cast<void *>(prop.totalGlobalMem / (1024 * 1024));
  resHandle.pStrErr = checkCUDAError("GetDeviceGlobalMemoryInMB");
  return resHandle;
}

CGoCallResHandle CudaProfilerStart() {
  CGoCallResHandle resHandle = {NULL, NULL};
  cudaProfilerStart();
  resHandle.pStrErr = checkCUDAError("cudaProfilerStart");
  return resHandle;
}

CGoCallResHandle CudaProfilerStop() {
  CGoCallResHandle resHandle = {NULL, NULL};
  cudaDeviceSynchronize();
  cudaProfilerStop();
  resHandle.pStrErr = checkCUDAError("cudaProfilerStop");
  return resHandle;
}

CGoCallResHandle GetDeviceMemoryInfo(size_t *freeSize, size_t *totalSize,
                                     int device) {
  CGoCallResHandle resHandle = {NULL, NULL};
  cudaSetDevice(device);
  resHandle.pStrErr = checkRMMError(rmmGetInfo(freeSize, totalSize, 0),
      "GetDeviceMemoryInfo");
  return resHandle;
}

CGoCallResHandle deviceMalloc(void **devPtr, size_t size) {
  CGoCallResHandle resHandle = {NULL, NULL};
  // For now use default stream to avoid changing the memory allocation
  // interface.
  // TODO(lucafuji): use the stream of current execution pipeline for
  // allocation and free.
  resHandle.pStrErr = checkRMMError(RMM_ALLOC(devPtr, size, 0),
      "deviceMalloc");
  return resHandle;
}

CGoCallResHandle deviceFree(void *devPtr) {
  CGoCallResHandle resHandle = {NULL, NULL};
  // For now use default stream to avoid changing the memory allocation
  // interface.
  // TODO(lucafuji): use the stream of current execution pipeline for
  // allocation and free.
  resHandle.pStrErr = checkRMMError(RMM_FREE(devPtr, 0), "deviceFree");
  return resHandle;
}

CGoCallResHandle deviceMemset(void *devPtr, int value, size_t count) {
  CGoCallResHandle resHandle = {NULL, NULL};
  cudaMemset(devPtr, value, count);
  resHandle.pStrErr = checkCUDAError("deviceMemset");
  return resHandle;
}

CGoCallResHandle asyncCopyHostToDevice(void* dst, const void* src,
    size_t count, void* stream) {
  CGoCallResHandle resHandle = {NULL, NULL};
  cudaMemcpyAsync(dst, src, count,
                  cudaMemcpyHostToDevice, (cudaStream_t) stream);
  resHandle.pStrErr = checkCUDAError("asyncCopyHostToDevice");
  return resHandle;
}
