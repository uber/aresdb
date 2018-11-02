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

#include <cstring>
#include <algorithm>
#include <exception>
#include <vector>
#include "query/transform.h"

CGoCallResHandle UnaryTransform(InputVector input,
                                OutputVector output,
                                uint32_t *indexVector,
                                int indexVectorLength,
                                uint32_t *baseCounts,
                                uint32_t startCount,
                                UnaryFunctorType functorType,
                                void *cudaStream,
                                int device) {
  CGoCallResHandle resHandle = {nullptr, nullptr};
  try {
#ifdef RUN_ON_DEVICE
    cudaSetDevice(device);
#endif
    std::vector<InputVector> inputVectors = {input};
    ares::OutputVectorBinder<1, UnaryFunctorType> outputVectorBinder(output,
                                                           inputVectors,
                                                           indexVector,
                                                           indexVectorLength,
                                                           baseCounts,
                                                           startCount,
                                                           functorType,
                                                           cudaStream);
    resHandle.res = reinterpret_cast<void *>(outputVectorBinder.bind());
    CheckCUDAError("UnaryTransform");
  } catch (std::exception &e) {
    std::cerr << "Exception happend when doing UnaryTransform:" << e.what()
              << std::endl;
    resHandle.pStrErr = strdup(e.what());
  }
  return resHandle;
}

CGoCallResHandle BinaryTransform(InputVector lhs,
                                 InputVector rhs,
                                 OutputVector output,
                                 uint32_t *indexVector,
                                 int indexVectorLength,
                                 uint32_t *baseCounts,
                                 uint32_t startCount,
                                 BinaryFunctorType functorType,
                                 void *cudaStream,
                                 int device) {
  CGoCallResHandle resHandle = {nullptr, nullptr};
  try {
#ifdef RUN_ON_DEVICE
    cudaSetDevice(device);
#endif
    std::vector<InputVector> inputVectors = {lhs, rhs};
    ares::OutputVectorBinder<2, BinaryFunctorType> outputVectorBinder(output,
                                                            inputVectors,
                                                            indexVector,
                                                            indexVectorLength,
                                                            baseCounts,
                                                            startCount,
                                                            functorType,
                                                            cudaStream);
    resHandle.res = reinterpret_cast<void *>(outputVectorBinder.bind());
    CheckCUDAError("BinaryTransform");
  } catch (std::exception &e) {
    std::cerr << "Exception happend when doing BinaryTransform:" << e.what()
              << std::endl;
    resHandle.pStrErr = strdup(e.what());
  }
  return resHandle;
}
