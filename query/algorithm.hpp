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

#ifndef QUERY_ALGORITHM_HPP_
#define QUERY_ALGORITHM_HPP_
#include <cuda_runtime.h>
#include <thrust/device_vector.h>
#include <thrust/execution_policy.h>
#include <thrust/host_vector.h>
#include <thrust/system/cuda/execution_policy.h>
#include <cfloat>
#include <cstdint>
#include <algorithm>
#include <type_traits>
#include "query/functor.hpp"
#include "query/iterator.hpp"
#include "query/time_series_aggregate.h"
#include "query/utils.hpp"

void CheckCUDAError(const char *message);

namespace ares {

class HashLookupContext;

int hashLookup(HashLookupContext ctx, RecordID *output,
               InputVector input, uint32_t *indexVector,
               uint32_t *baseCounts, uint32_t startCount);

// bind to ctx's bindNext function for further class bindings.
template<typename TransformContext>
int transform(TransformContext ctx, OutputVector output,
              InputVector input, uint32_t *indexVector,
              uint32_t *baseCounts, uint32_t startCount);

// filter simply binds uint8_t* as the output iterator type.
template<typename TransformContext>
int filter(TransformContext ctx, InputVector input,
           uint32_t *indexVector, RecordID **foreignTableRecordIDVectors,
           uint8_t *boolVector, uint32_t *baseCounts, uint32_t startCount);

// reduce binds aggregate function type and data type from
// aggFunc.
int reduce(DimensionColumnVector inputKeys, uint8_t *inputValues,
           DimensionColumnVector outputKeys, uint8_t *outputValues,
           int valueBytes, int length, AggregateFunction aggFunc,
           cudaStream_t cudaStream);

// sort binds KeyIter type from keys.
void sort(DimensionColumnVector keys, int length, cudaStream_t cudaStream);

// expand function is used to uncompress the compressed dimension keys and
// append to outputKeys.
int expand(DimensionColumnVector inputKeys,
           DimensionColumnVector outputKeys,
           uint32_t *baseCounts,
           uint32_t *indexVector,
           int indexVectorLen,
           int outputOccupiedLen,
           cudaStream_t cudaStream);

// hyperloglog.
int hyperloglog(DimensionColumnVector prevDimOut,
                DimensionColumnVector curDimOut, uint32_t *prevValuesOut,
                uint32_t *curValuesOut, int prevResultSize, int curBatchSize,
                bool isLastBatch, uint8_t **hllVectorPtr,
                size_t *hllVectorSizePtr, uint16_t **hllDimRegIDCountPtr,
                cudaStream_t cudaStream);

// write geo shape index to dim output.
void write_geo_shape_dim(int shapeTotalWords,
    DimensionOutputVector dimOut, int indexVectorLength,
    uint32_t *outputPredicate, cudaStream_t cudaStream);

}  // namespace ares

#endif  // QUERY_ALGORITHM_HPP_
