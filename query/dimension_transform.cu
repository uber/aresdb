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

#include <algorithm>
#include "query/transform.hpp"

namespace ares {

template<int NInput, typename FunctorType>
int OutputVectorBinder<NInput, FunctorType>::transformDimensionOutput(
    DimensionOutputVector output) {
  switch (output.DataType) {
    case Bool:
      return transform(
          ares::make_dimension_output_iterator<bool>(
              output.DimValues, output.DimNulls));
    case Int8:
      return transform(
          ares::make_dimension_output_iterator<int8_t>(
              output.DimValues, output.DimNulls));
    case Int16:
      return transform(
          ares::make_dimension_output_iterator<int16_t>(
              output.DimValues, output.DimNulls));
    case Int32:
      return transform(
          ares::make_dimension_output_iterator<int32_t>(
              output.DimValues, output.DimNulls));
    case Uint8:
      return transform(
          ares::make_dimension_output_iterator<uint8_t>(
              output.DimValues, output.DimNulls));
    case Uint16:
      return transform(
          ares::make_dimension_output_iterator<
              uint16_t>(
              output.DimValues, output.DimNulls));
    case Uint32:
      return transform(
          ares::make_dimension_output_iterator<
              uint32_t>(
              output.DimValues, output.DimNulls));
    case Int64:
      return transform(ares::make_dimension_output_iterator<int64_t>(
          output.DimValues, output.DimNulls));
    case UUID:
      return transform(ares::make_dimension_output_iterator<UUIDT>(
          output.DimValues, output.DimNulls));
    case Float32:
      return transform(
          ares::make_dimension_output_iterator<float_t>(
              output.DimValues, output.DimNulls));
    default:
      throw std::invalid_argument(
          "Unsupported data type for DimensionOutput");
  }
}

// explicit instantiations.
template int OutputVectorBinder<1,
                                UnaryFunctorType>::transformDimensionOutput(
    DimensionOutputVector output);

template int OutputVectorBinder<2,
                                BinaryFunctorType>::transformDimensionOutput(
    DimensionOutputVector output);

}  // namespace ares
