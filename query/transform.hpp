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

#ifndef QUERY_TRANSFORM_HPP_
#define QUERY_TRANSFORM_HPP_

#include <cuda_runtime.h>
#include <thrust/device_vector.h>
#include <thrust/execution_policy.h>
#include <thrust/host_vector.h>
#include <thrust/system/cuda/execution_policy.h>
#include <algorithm>
#include <cfloat>
#include <cstdint>
#include <vector>
#include "query/algorithm.hpp"
#include "query/binder.hpp"
#include "query/functor.hpp"
#include "query/iterator.hpp"
#include "query/time_series_aggregate.h"
#include "query/utils.hpp"

void CheckCUDAError(const char *message);

namespace ares {

template<typename OutputIterator, typename FunctorType>
class TransformContext {
 public:
  TransformContext(
      OutputIterator outputIter,
      int indexVectorLength,
      FunctorType functorType,
      void *cudaStream)
      : outputIter(outputIter),
        indexVectorLength(indexVectorLength),
        functorType(functorType),
        cudaStream(reinterpret_cast<cudaStream_t>(cudaStream)) {}

 public:
  cudaStream_t getStream() const {
    return cudaStream;
  }

  template<typename InputIterator>
  int run(uint32_t *indexVector, InputIterator inputIter) {
    typedef typename InputIterator::value_type::head_type InputValueType;
    typedef typename OutputIterator::value_type::head_type OutputValueType;

    UnaryFunctor<OutputValueType, InputValueType> f(functorType);
    return thrust::transform(GET_EXECUTION_POLICY(cudaStream), inputIter,
        inputIter + indexVectorLength, outputIter, f) - outputIter;
  }

  template<typename LHSIterator, typename RHSIterator>
  int run(uint32_t *indexVector, LHSIterator lhsIter, RHSIterator rhsIter) {
    typedef typename common_type<
        typename LHSIterator::value_type::head_type,
        typename RHSIterator::value_type::head_type>::type InputValueType;

    typedef typename OutputIterator::value_type::head_type OutputValueType;

    BinaryFunctor<OutputValueType, InputValueType> f(functorType);

    return thrust::transform(GET_EXECUTION_POLICY(cudaStream), lhsIter,
        lhsIter + indexVectorLength, rhsIter, outputIter, f) - outputIter;
  }

 protected:
  OutputIterator outputIter;
  int indexVectorLength;
  FunctorType functorType;
  cudaStream_t cudaStream;
};

// OutputVectorBinder bind a OutputVector to a output iterator.
template<int NInput, typename FunctorType>
class OutputVectorBinder {
 public:
  explicit OutputVectorBinder(OutputVector outputVector,
                              std::vector<InputVector> inputVectors,
                              uint32_t *indexVector,
                              int indexVectorLength,
                              uint32_t *baseCounts,
                              uint32_t startCount,
                              FunctorType functorType,
                              void *cudaStream) :
      output(outputVector),
      inputs(inputVectors),
      indexVector(indexVector),
      indexVectorLength(indexVectorLength),
      baseCounts(baseCounts),
      startCount(startCount),
      functorType(functorType),
      cudaStream(cudaStream) {}

  int bind() {
    switch (output.Type) {
      case ScratchSpaceOutput:
        return transformScratchSpaceOutput(output.Vector.ScratchSpace);
      case MeasureOutput:
        return transformMeasureOutput(output.Vector.Measure);
      case DimensionOutput:
        return transformDimensionOutput(output.Vector.Dimension);
      default:throw std::invalid_argument("Unsupported output vector type");
    }
  }

 private:
  OutputVector output;
  std::vector<InputVector> inputs;
  uint32_t *indexVector;
  int indexVectorLength;
  uint32_t *baseCounts;
  uint32_t startCount;
  FunctorType functorType;
  void *cudaStream;

  // Declaration of transform functions for different type of output iterators.
  // Their actual definition is in different cu files so that we can compile
  // them in parallel.
  int transformDimensionOutput(DimensionOutputVector output);
  int transformMeasureOutput(MeasureOutputVector output);
  int transformScratchSpaceOutput(ScratchSpaceVector output);

  // Shared by all output iterator;
  template<typename OutputIterator>
  int transform(OutputIterator outputIter) {
    typedef TransformContext<OutputIterator, FunctorType> Context;
    Context ctx(
        outputIter, indexVectorLength, functorType, cudaStream);
    InputVectorBinder<Context, NInput> binder(
        ctx, inputs, indexVector, baseCounts, startCount);
    return binder.bind();
  }
};

}  // namespace ares
#endif  // QUERY_TRANSFORM_HPP_
