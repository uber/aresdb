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

#include "query/utils.h"
#include <cuda_runtime.h>
#include <iostream>
#include <exception>
#include <string>

const int MAX_CUDA_ERROR_LEN = 80;

uint16_t DAYS_BEFORE_MONTH_HOST[13] = {
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

__constant__ uint16_t DAYS_BEFORE_MONTH_DEVICE[13];

// CUDAError represents a exception class that contains a error message.
class CUDAError : public std::exception {
 private:
  std::string message_;
 public:
  explicit CUDAError(const std::string &message) : message_(message) {
  }
  virtual const char *what() const throw() {
    return message_.c_str();
  }
};

// CheckCUDAError implementation. Notes for host we don't throw the exception
// on purpose since we will always receive error messages like "insufficient
// driver version".
void CheckCUDAError(const char *message) {
  cudaError_t error = cudaGetLastError();
  if (error != cudaSuccess) {
    char buf[MAX_CUDA_ERROR_LEN];
    snprintf(buf,
             sizeof(buf),
             "ERROR: %s: %s",
             message,
             cudaGetErrorString(error));
#ifdef RUN_ON_DEVICE
    throw CUDAError(buf);
#else
    printf("%s\n", buf);
#endif
  }
}

// BootstrapDevice implementation. It should be only called by unit tests
// and golang code.
CGoCallResHandle BootstrapDevice() {
  CGoCallResHandle resHandle = {nullptr, nullptr};
  try {
#ifdef RUN_ON_DEVICE
    int deviceCount;
    cudaGetDeviceCount(&deviceCount);
    CheckCUDAError("cudaGetDeviceCount");
    for (int device = 0; device < deviceCount; device++) {
      cudaSetDevice(device);
      CheckCUDAError("cudaSetDevice");
      cudaMemcpyToSymbol(DAYS_BEFORE_MONTH_DEVICE, DAYS_BEFORE_MONTH_HOST,
                         sizeof(DAYS_BEFORE_MONTH_HOST));
      CheckCUDAError("cudaMemcpyToSymbol");
    }
#endif
  }
  catch (std::exception &e) {
    std::cerr << "Exception happened when bootstraping device:" << e.what()
              << std::endl;
    resHandle.pStrErr = strdup(e.what());
  }
  return resHandle;
}

namespace ares {

__host__ __device__ uint64_t rotl64(uint64_t x, int8_t r) {
  return (x << r) | (x >> (64 - r));
}

__host__ __device__ uint64_t fmix64(uint64_t k) {
  k ^= k >> 33;
  k *= 0xff51afd7ed558ccdLLU;
  k ^= k >> 33;
  k *= 0xc4ceb9fe1a85ec53LLU;
  k ^= k >> 33;
  return k;
}

// Murmur3Sum32 implements Murmur3Sum32 hash algorithm.
__host__ __device__ uint32_t murmur3sum32(const uint8_t *key, int bytes,
                                          uint32_t seed) {
  uint32_t h1 = seed;
  int nBlocks = bytes / 4;
  const uint8_t *p = key;
  const uint8_t *p1 = p + 4 * nBlocks;

  for (; p < p1; p += 4) {
    uint32_t k1 = *reinterpret_cast<const uint32_t *>(p);
    k1 *= 0xcc9e2d51;
    k1 = (k1 << 15) | (k1 >> 17);
    k1 *= 0x1b873593;

    h1 ^= k1;
    h1 = (h1 << 13) | (h1 >> 19);
    h1 = h1 * 5 + 0xe6546b64;
  }

  int tailBytes = bytes - nBlocks * 4;
  const uint8_t *tail = p1;

  uint32_t k1 = 0;
  switch (tailBytes & 3) {
    case 3:k1 ^= (uint32_t) tail[2] << 16;
    case 2:k1 ^= (uint32_t) tail[1] << 8;
    case 1:k1 ^= (uint32_t) tail[0];
      k1 *= 0xcc9e2d51;
      k1 = (k1 << 15) | (k1 >> 17);
      k1 *= 0x1b873593;
      h1 ^= k1;
      break;
  }

  h1 ^= bytes;
  h1 ^= h1 >> 16;
  h1 *= 0x85ebca6b;
  h1 ^= h1 >> 13;
  h1 *= 0xc2b2ae35;
  h1 ^= h1 >> 16;

  return h1;
}

__host__ __device__ void murmur3sum128(const uint8_t *key, int len,
                                       uint32_t seed, uint64_t *out) {
  const uint8_t *data = key;
  const int nblocks = len / 16;
  int i;

  uint64_t h1 = seed;
  uint64_t h2 = seed;

  uint64_t c1 = 0x87c37b91114253d5LLU;
  uint64_t c2 = 0x4cf5ad432745937fLLU;

  const uint64_t *blocks = reinterpret_cast<const uint64_t *>(data);

  for (i = 0; i < nblocks; i++) {
    uint64_t k1 = blocks[i * 2];
    uint64_t k2 = blocks[i * 2 + 1];

    k1 *= c1;
    k1 = rotl64(k1, 31);
    k1 *= c2;
    h1 ^= k1;

    h1 = rotl64(h1, 27);
    h1 += h2;
    h1 = h1 * 5 + 0x52dce729;

    k2 *= c2;
    k2 = rotl64(k2, 33);
    k2 *= c1;
    h2 ^= k2;

    h2 = rotl64(h2, 31);
    h2 += h1;
    h2 = h2 * 5 + 0x38495ab5;
  }

  const uint8_t *tail = reinterpret_cast<const uint8_t *>(data + nblocks * 16);

  uint64_t k1 = 0;
  uint64_t k2 = 0;

  switch (len & 15) {
    case 15:k2 ^= (uint64_t) (tail[14]) << 48;
    case 14:k2 ^= (uint64_t) (tail[13]) << 40;
    case 13:k2 ^= (uint64_t) (tail[12]) << 32;
    case 12:k2 ^= (uint64_t) (tail[11]) << 24;
    case 11:k2 ^= (uint64_t) (tail[10]) << 16;
    case 10:k2 ^= (uint64_t) (tail[9]) << 8;
    case 9:k2 ^= (uint64_t) (tail[8]) << 0;
      k2 *= c2;
      k2 = rotl64(k2, 33);
      k2 *= c1;
      h2 ^= k2;

    case 8:k1 ^= (uint64_t) (tail[7]) << 56;
    case 7:k1 ^= (uint64_t) (tail[6]) << 48;
    case 6:k1 ^= (uint64_t) (tail[5]) << 40;
    case 5:k1 ^= (uint64_t) (tail[4]) << 32;
    case 4:k1 ^= (uint64_t) (tail[3]) << 24;
    case 3:k1 ^= (uint64_t) (tail[2]) << 16;
    case 2:k1 ^= (uint64_t) (tail[1]) << 8;
    case 1:k1 ^= (uint64_t) (tail[0]) << 0;
      k1 *= c1;
      k1 = rotl64(k1, 31);
      k1 *= c2;
      h1 ^= k1;
  }

  h1 ^= len;
  h2 ^= len;

  h1 += h2;
  h2 += h1;

  h1 = fmix64(h1);
  h2 = fmix64(h2);

  h1 += h2;
  h2 += h1;

  out[0] = h1;
  out[1] = h2;
}

}  // namespace ares
