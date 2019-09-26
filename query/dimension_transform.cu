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

#define BIND_DIMENSION_OUTPUT(dataType) \
            return binder.transform( \
                  ares::make_dimension_output_iterator<dataType>( \
                      output.DimValues, output.DimNulls));

template <int NInput>
class OutputVectorBinderHelper {
 public:
    template<typename OutputVectorBinder>
    int bind(const OutputVectorBinder &binder, DimensionOutputVector output) {
        switch (output.DataType) {
            case Bool:
              BIND_DIMENSION_OUTPUT(bool)
            case Int8:
              BIND_DIMENSION_OUTPUT(int8_t)
            case Int16:
              BIND_DIMENSION_OUTPUT(int16_t)
            case Int32:
              BIND_DIMENSION_OUTPUT(int32_t)
            case Uint8:
              BIND_DIMENSION_OUTPUT(uint8_t)
            case Uint16:
              BIND_DIMENSION_OUTPUT(uint16_t)
            case Uint32:
              BIND_DIMENSION_OUTPUT(uint32_t)
            case Int64:
              BIND_DIMENSION_OUTPUT(int64_t)
            case Float32:
              BIND_DIMENSION_OUTPUT(float_t)
            case UUID:
              BIND_DIMENSION_OUTPUT(UUIDT)
            case GeoPoint:
              BIND_DIMENSION_OUTPUT(GeoPointT)
            default:
              throw std::invalid_argument(
                  "Unsupported data type for DimensionOutput");
          }
    }
};

template<>
class OutputVectorBinderHelper<1>: OutputVectorBinderHelper<2> {
    typedef OutputVectorBinderHelper<2> super_t;

 public:
    template<typename OutputVectorBinder>
    int bind(OutputVectorBinder binder, DimensionOutputVector output) {
        switch (output.DataType) {
            case UUID:
                BIND_DIMENSION_OUTPUT(UUIDT)
             case GeoPoint:
                BIND_DIMENSION_OUTPUT(GeoPointT)
             default:
                return super_t::bind(binder, output);
        }
    }
};

template<int NInput, typename FunctorType>
int OutputVectorBinder<NInput, FunctorType>::transformDimensionOutput(
    DimensionOutputVector output) const {
    OutputVectorBinderHelper<NInput> helper;
    return helper.bind(*this, output);
}

// explicit instantiations.
template int OutputVectorBinder<1,
                                UnaryFunctorType>::transformDimensionOutput(
    DimensionOutputVector output) const;

template int OutputVectorBinder<2,
                                BinaryFunctorType>::transformDimensionOutput(
    DimensionOutputVector output) const;

}  // namespace ares
