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

#include <thrust/transform.h>
#include <algorithm>
#include <exception>
#include "query/algorithm.hpp"
#include "query/iterator.hpp"

CGoCallResHandle InitIndexVector(uint32_t *indexVector, uint32_t start,
                     int indexVectorLength, void *cudaStream, int device) {
  CGoCallResHandle resHandle = {nullptr, nullptr};
  try {
#ifdef RUN_ON_DEVICE
    cudaSetDevice(device);
    thrust::sequence(
        thrust::cuda::par.on(reinterpret_cast<cudaStream_t>(cudaStream)),
        indexVector, indexVector + indexVectorLength, start);
#else
    thrust::sequence(thrust::host,
                     indexVector,
                     indexVector + indexVectorLength,
                     start);
#endif
    CheckCUDAError("InitIndexVector");
  }
  catch (std::exception &e) {
    std::cerr << "Exception happend when doing InitIndexVector:" << e.what()
              << std::endl;
    resHandle.pStrErr = strdup(e.what());
  }
  return resHandle;
}
