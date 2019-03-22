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

#include <stdlib.h>
#include <string.h>

#include "../memory.h"

DeviceMemoryFlags GetFlags(){
  return 0x0;
}

CGoCallResHandle Init(){
  CGoCallResHandle resHandle = {NULL, NULL};
  return resHandle;
}

CGoCallResHandle HostAlloc(size_t bytes) {
  CGoCallResHandle resHandle = {NULL, NULL};
  resHandle.res = malloc(bytes);
  memset(resHandle.res, 0, bytes);
  return resHandle;
}

CGoCallResHandle HostFree(void *p) {
  CGoCallResHandle resHandle = {NULL, NULL};
  free(p);
  return resHandle;
}

// Dummy implementations.
CGoCallResHandle CreateCudaStream(int device) {
  CGoCallResHandle resHandle = {NULL, NULL};
  return resHandle;
}
CGoCallResHandle WaitForCudaStream(void *s, int device) {
  CGoCallResHandle resHandle = {NULL, NULL};
  return resHandle;
}

CGoCallResHandle DestroyCudaStream(void *s, int device) {
  CGoCallResHandle resHandle = {NULL, NULL};
  return resHandle;
}

// Simulate on host side.
CGoCallResHandle DeviceAllocate(size_t bytes, int device) {
  return HostAlloc(bytes);
}

CGoCallResHandle DeviceFree(void *p, int device) {
  return HostFree(p);
}

// Simulate on host side.
CGoCallResHandle AsyncCopyHostToDevice(void *dst, void *src, size_t bytes,
                                       void *stream, int device) {
  memcpy(dst, src, bytes);
  CGoCallResHandle resHandle = {NULL, NULL};
  return resHandle;
}

CGoCallResHandle AsyncCopyDeviceToDevice(void *dst,
                                                void *src,
                                                size_t bytes,
                                                void *stream,
                                                int device) {
  memcpy(dst, src, bytes);
  CGoCallResHandle resHandle = {NULL, NULL};
  return resHandle;
}

CGoCallResHandle AsyncCopyDeviceToHost(void *dst,
                                              void *src,
                                              size_t bytes,
                                              void *stream,
                                              int device) {
  memcpy(dst, src, bytes);
  CGoCallResHandle resHandle = {NULL, NULL};
  return resHandle;
}

// Simulate on host side.
CGoCallResHandle GetDeviceCount() {
  CGoCallResHandle resHandle = {(void *)1, NULL};
  return resHandle;
}

CGoCallResHandle GetDeviceGlobalMemoryInMB(int device) {
  // 24 GB
  CGoCallResHandle resHandle = {(void *)24392, NULL};
  return resHandle;
}

CGoCallResHandle CudaProfilerStart() {
  CGoCallResHandle resHandle = {NULL, NULL};
  return resHandle;
}
CGoCallResHandle CudaProfilerStop() {
  CGoCallResHandle resHandle = {NULL, NULL};
  return resHandle;
}

CGoCallResHandle GetDeviceMemoryInfo(size_t *freeSize, size_t *totalSize,
    int device){
  CGoCallResHandle resHandle = {"Not supported", NULL};
  return resHandle;
}

CGoCallResHandle deviceMalloc(void **devPtr, size_t size){
  CGoCallResHandle resHandle = {NULL, NULL};
  *devPtr = malloc(size);
  return resHandle;
}

CGoCallResHandle deviceFree(void *devPtr){
  CGoCallResHandle resHandle = {NULL, NULL};
  free(devPtr);
  return resHandle;
}

CGoCallResHandle deviceMemset(void *devPtr, int value, size_t count) {
  CGoCallResHandle resHandle = {NULL, NULL};
  memset(devPtr, value, count);
  return resHandle;
}

CGoCallResHandle memcpyAsyncHostToDevice( void* dst, const void* src,
    size_t count, void* stream){
  CGoCallResHandle resHandle = {NULL, NULL};
  memcpy(dst, src, count);
  return resHandle;
}