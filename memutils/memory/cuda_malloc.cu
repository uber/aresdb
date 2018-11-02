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

#include <cstdio>
#include <cstring>
#include <cuda_runtime.h>
#include <cuda_profiler_api.h>
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

extern "C" CGoCallResHandle HostAlloc(size_t bytes) {
  CGoCallResHandle resHandle = {NULL, NULL};
  // cudaHostAllocPortable makes sure that the allocation is associated with all
  // devices, not just the current device.
  cudaHostAlloc(&resHandle.res, bytes, cudaHostAllocPortable);
  memset(resHandle.res, 0, bytes);
  resHandle.pStrErr = checkCUDAError("Allocate");
  return resHandle;
}

extern "C" CGoCallResHandle HostFree(void *p) {
  CGoCallResHandle resHandle = {NULL, NULL};
  cudaFreeHost(p);
  resHandle.pStrErr = checkCUDAError("Free");
  return resHandle;
}

extern "C" CGoCallResHandle CreateCudaStream(int device) {
  CGoCallResHandle resHandle = {NULL, NULL};
  cudaSetDevice(device);
  cudaStream_t s = NULL;
  cudaStreamCreate(&s);
  resHandle.res = reinterpret_cast<void *>(s);
  resHandle.pStrErr = checkCUDAError("CreateCudaStream");
  return resHandle;
}

extern "C" CGoCallResHandle WaitForCudaStream(void *s, int device) {
  CGoCallResHandle resHandle = {NULL, NULL};
  cudaSetDevice(device);
  cudaStreamSynchronize((cudaStream_t) s);
  resHandle.pStrErr = checkCUDAError("WaitForCudaStream");
  return resHandle;
}

extern "C" CGoCallResHandle DestroyCudaStream(void *s, int device) {
  CGoCallResHandle resHandle = {NULL, NULL};
  cudaSetDevice(device);
  cudaStreamDestroy((cudaStream_t) s);
  resHandle.pStrErr = checkCUDAError("DestroyCudaStream");
  return resHandle;
}

extern "C" CGoCallResHandle DeviceAllocate(size_t bytes, int device) {
  CGoCallResHandle resHandle = {NULL, NULL};
  cudaSetDevice(device);
  cudaMalloc(&resHandle.res, bytes);
  cudaMemset(resHandle.res, 0, bytes);
  resHandle.pStrErr = checkCUDAError("DeviceAllocate");
  return resHandle;
}

extern "C" CGoCallResHandle DeviceFree(void *p, int device) {
  CGoCallResHandle resHandle = {NULL, NULL};
  cudaSetDevice(device);
  cudaFree(p);
  resHandle.pStrErr = checkCUDAError("DeviceFree");
  return resHandle;
}

extern "C" CGoCallResHandle AsyncCopyHostToDevice(
    void *dst, void *src, size_t bytes, void *stream, int device) {
  CGoCallResHandle resHandle = {NULL, NULL};
  cudaSetDevice(device);
  cudaMemcpyAsync(dst, src, bytes,
                  cudaMemcpyHostToDevice, (cudaStream_t) stream);
  resHandle.pStrErr = checkCUDAError("AsyncCopyHostToDevice");
  return resHandle;
}

extern "C" CGoCallResHandle AsyncCopyDeviceToDevice(
    void *dst, void *src, size_t bytes, void *stream, int device) {
  CGoCallResHandle resHandle = {NULL, NULL};
  cudaSetDevice(device);
  cudaMemcpyAsync(dst, src, bytes,
                  cudaMemcpyDeviceToDevice, (cudaStream_t) stream);
  resHandle.pStrErr = checkCUDAError("AsyncCopyDeviceToDevice");
  return resHandle;
}

extern "C" CGoCallResHandle AsyncCopyDeviceToHost(
    void *dst, void *src, size_t bytes, void *stream, int device) {
  CGoCallResHandle resHandle = {NULL, NULL};
  cudaSetDevice(device);
  cudaMemcpyAsync(dst, src, bytes,
                  cudaMemcpyDeviceToHost, (cudaStream_t) stream);
  resHandle.pStrErr = checkCUDAError("AsyncCopyDeviceToHost");
  return resHandle;
}

extern "C" CGoCallResHandle GetDeviceCount() {
  CGoCallResHandle resHandle = {NULL, NULL};
  cudaGetDeviceCount(reinterpret_cast<int *>(&resHandle.res));
  resHandle.pStrErr = checkCUDAError("GetDeviceCount");
  return resHandle;
}

extern "C" CGoCallResHandle GetDeviceGlobalMemoryInMB(int device) {
  CGoCallResHandle resHandle = {NULL, NULL};
  cudaDeviceProp prop;
  cudaGetDeviceProperties(&prop, device);
  resHandle.res = reinterpret_cast<void *>(prop.totalGlobalMem / (1024 * 1024));
  resHandle.pStrErr = checkCUDAError("GetDeviceGlobalMemoryInMB");
  return resHandle;
}

extern "C" CGoCallResHandle CudaProfilerStart() {
  CGoCallResHandle resHandle = {NULL, NULL};
  cudaProfilerStart();
  resHandle.pStrErr = checkCUDAError("cudaProfilerStart");
  return resHandle;
}

extern "C" CGoCallResHandle CudaProfilerStop() {
  CGoCallResHandle resHandle = {NULL, NULL};
  cudaDeviceSynchronize();
  cudaProfilerStop();
  resHandle.pStrErr = checkCUDAError("cudaProfilerStop");
  return resHandle;
}
