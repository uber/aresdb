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
int OutputVectorBinder<NInput, FunctorType>::transformScratchSpaceOutput(
    ScratchSpaceVector output) const {
  switch (output.DataType) {
    #define BIND_SCRATCH__SPACE_OUTPUT(dataType) \
    return transform(ares::make_scratch_space_output_iterator( \
          reinterpret_cast<dataType *>(output.Values), \
          output.NullsOffset));

    case Int32:    BIND_SCRATCH__SPACE_OUTPUT(int32_t)
    case Uint32:   BIND_SCRATCH__SPACE_OUTPUT(uint32_t)
    case Float32:  BIND_SCRATCH__SPACE_OUTPUT(float_t)
    case Int64:    BIND_SCRATCH__SPACE_OUTPUT(int64_t)
    case UUID:     BIND_SCRATCH__SPACE_OUTPUT(UUIDT)
    case GeoPoint: BIND_SCRATCH__SPACE_OUTPUT(GeoPointT)
    default:throw
      std::invalid_argument(
          "Unsupported data type for ScratchSpaceOutput");
  }
}

// explicit instantiations.
template int OutputVectorBinder<1,
                                UnaryFunctorType>::transformScratchSpaceOutput(
    ScratchSpaceVector output) const;

template int OutputVectorBinder<2,
                                BinaryFunctorType>::transformScratchSpaceOutput(
    ScratchSpaceVector output) const;

}  // namespace ares
