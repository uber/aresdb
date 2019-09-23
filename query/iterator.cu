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

#include "query/iterator.hpp"

namespace ares {

template<>
SimpleIterator<UUIDT> make_scratch_space_input_iterator<UUIDT>(
    uint8_t *valueIter, uint32_t nullOffset) {
  UUIDT val;
  val.p1 = 0;
  val.p2 = 0;
  return SimpleIterator<UUIDT>(reinterpret_cast<UUIDT *>(valueIter),
                               nullOffset, val, 0);
}

template<>
SimpleIterator<GeoPointT> make_scratch_space_input_iterator<GeoPointT>(
    uint8_t *valueIter, uint32_t nullOffset) {
  GeoPointT val;
  val.Lat = 0.0;
  val.Long = 0.0;
  return SimpleIterator<GeoPointT>(reinterpret_cast<GeoPointT *>(valueIter),
                               nullOffset, val, 0);
}

} // namespace ares
