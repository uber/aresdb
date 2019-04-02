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
#include "query/transform.hpp"
#include "query/binder.hpp"

namespace ares {

class HashLookupContext {
 public:
  HashLookupContext(int indexVectorLength, CuckooHashIndex hashIndex,
                    RecordID* recordIDVector,
                    void *cudaStream)
      : indexVectorLength(indexVectorLength),
        hashIndex(hashIndex),
        recordIDVector(recordIDVector),
        cudaStream(reinterpret_cast<cudaStream_t>(cudaStream)) {}

  template<typename InputIterator>
  int run(uint32_t *indexVector, InputIterator inputIterator);

  cudaStream_t getStream() const {
    return cudaStream;
  }

 private:
  int indexVectorLength;
  CuckooHashIndex hashIndex;
  RecordID* recordIDVector;
  cudaStream_t cudaStream;
};

// Specialized for HashLookupContext.
template <>
class InputVectorBinder<HashLookupContext, 1> : public InputVectorBinderBase<
    HashLookupContext, 1, 1> {
  typedef InputVectorBinderBase<HashLookupContext, 1, 1> super_t;
 public:
  explicit InputVectorBinder(HashLookupContext context,
                             std::vector<InputVector> inputVectors,
                             uint32_t *indexVector, uint32_t *baseCounts,
                             uint32_t startCount) : super_t(context,
                                                            inputVectors,
                                                            indexVector,
                                                            baseCounts,
                                                            startCount) {
  }
 public:
  template<typename ...InputIterators>
  int bind(InputIterators... boundInputIterators);
};

}  // namespace ares

CGoCallResHandle HashLookup(InputVector input, RecordID *output,
                            uint32_t *indexVector, int indexVectorLength,
                            uint32_t *baseCounts, uint32_t startCount,
                            CuckooHashIndex hashIndex, void *cudaStream,
                            int device) {
  CGoCallResHandle resHandle = {nullptr, nullptr};
  try {
#ifdef RUN_ON_DEVICE
    cudaSetDevice(device);
#endif
    ares::HashLookupContext ctx(indexVectorLength,
                                hashIndex, output, cudaStream);
    std::vector<InputVector> inputVectors = {input};
    ares::InputVectorBinder<ares::HashLookupContext, 1>
        binder(ctx, inputVectors, indexVector, baseCounts, startCount);
    resHandle.res =
        reinterpret_cast<void *>(binder.bind());
    CheckCUDAError("HashLookup");
  } catch (std::exception &e) {
    std::cerr << "Exception happened when doing HashLookup:" << e.what()
              << std::endl;
    resHandle.pStrErr = strdup(e.what());
  }
  return resHandle;
}


namespace ares {

// Specialized version for hash lookup to support
// UUID.
template<typename ...InputIterators>
int InputVectorBinder<HashLookupContext, 1>::bind(
    InputIterators... boundInputIterators) {
  InputVector input = super_t::inputVectors[0];
  uint32_t *indexVector = super_t::indexVector;
  uint32_t *baseCounts = super_t::baseCounts;
  uint32_t startCount = super_t::startCount;

  if (input.Type == VectorPartyInput) {
    InputVectorBinderBase<HashLookupContext, 1, 0>
        nextBinder(context, inputVectors, indexVector, baseCounts, startCount);
    VectorPartySlice inputVP = input.Vector.VP;
    if (inputVP.DataType == UUID) {
      uint8_t *basePtr = inputVP.BasePtr;
      // Treat mode 0 as constant vector.
      if (basePtr == nullptr) {
        bool hasDefault = inputVP.DefaultValue.HasDefault;
        DefaultValue defaultValue = inputVP.DefaultValue;
        return nextBinder.bind(boundInputIterators...,
                               thrust::make_constant_iterator(
                                   thrust::make_tuple<
                                       UUIDT,
                                       bool>(
                                       defaultValue.Value.UUIDVal,
                                       hasDefault)));
      }

      uint32_t nullsOffset = inputVP.NullsOffset;
      uint32_t valuesOffset = inputVP.ValuesOffset;
      uint8_t startingIndex = inputVP.StartingIndex;
      uint8_t stepInBytes = getStepInBytes(inputVP.DataType);
      uint32_t length = inputVP.Length;
      return nextBinder.bind(boundInputIterators...,
                                    make_column_iterator<UUIDT>(indexVector,
                                                                baseCounts,
                                                                startCount,
                                                                basePtr,
                                                                nullsOffset,
                                                                valuesOffset,
                                                                length,
                                                                stepInBytes,
                                                                startingIndex));
    }
  }
  return super_t::bind(boundInputIterators...);
}

template<typename InputIterator>
int HashLookupContext::run(uint32_t *indexVector, InputIterator inputIter) {
  typedef typename InputIterator::value_type::head_type InputValueType;
  HashLookupFunctor<InputValueType> f(hashIndex.buckets, hashIndex.seeds,
                                      hashIndex.keyBytes, hashIndex.numHashes,
                                      hashIndex.numBuckets);
  return thrust::transform(GET_EXECUTION_POLICY(cudaStream), inputIter,
      inputIter + indexVectorLength, recordIDVector, f) -
  recordIDVector;
}

}  // namespace ares
