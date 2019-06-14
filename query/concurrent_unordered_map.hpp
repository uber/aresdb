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

#ifndef QUERY_CONCURRENT_UNORDERED_MAP_HPP_
#define QUERY_CONCURRENT_UNORDERED_MAP_HPP_

#if defined(SUPPORT_HASH_REDUCTION) && defined(RUN_ON_DEVICE)
#include <hash/concurrent_unordered_map.cuh>
#include <hash/hash_functions.cuh>
#endif
#include <thrust/pair.h>
#if defined(SUPPORT_HASH_REDUCTION) && defined(RUN_ON_DEVICE)
#include <groupby/aggregation_operations.hpp>
#endif
#include <limits>
#include <unordered_map>

namespace ares {
#ifndef RUN_ON_DEVICE
// we duplicate following operators from cudf for host mode only. This is
// because including any cudf files requires c++14 however for host mode.
// We can remove those operators once all of our dependent c++ compilers
// support c++14.
template<typename value_type>
struct max_op {
  constexpr static value_type IDENTITY{
    std::numeric_limits<value_type>::lowest()};

  __host__ __device__
  value_type operator()(value_type new_value, value_type old_value) {
    return (new_value > old_value ? new_value : old_value);
  }
};

template<typename value_type>
struct min_op {
  constexpr static value_type IDENTITY{std::numeric_limits<value_type>::max()};

  __host__ __device__
  value_type operator()(value_type new_value, value_type old_value) {
    return (new_value < old_value ? new_value : old_value);
  }
};

template<typename value_type>
struct count_op {
  constexpr static value_type IDENTITY{0};

  __host__ __device__
  value_type operator()(value_type, value_type old_value) {
    old_value += value_type{1};
    return old_value;
  }
};

template<typename value_type>
struct sum_op {
  constexpr static value_type IDENTITY{0};

  __host__ __device__
  value_type operator()(value_type new_value, value_type old_value) {
    return new_value + old_value;
  }
};
#endif

// host_concurrent_unordered_map is a wrapper for std::unordered_map to provide
// the same interface as cudf's concurrent_unordered_map. This map is not thread
// safe.
template<typename Key, typename Element, typename Hasher, typename Equality>
class host_concurrent_unordered_map {
 public:
  using size_type = size_t;
  using hasher = Hasher;
  using key_equal = Equality;
  using key_type = Key;
  using mapped_type = Element;
  using value_type = thrust::pair<Key, Element>;
  using map_t = std::unordered_map<key_type, mapped_type, hasher, key_equal>;
  using const_iterator = typename map_t::const_iterator;
  using iterator = typename map_t::iterator;

  explicit
  host_concurrent_unordered_map(
      size_type _capacity,
      const mapped_type _identity,
      const key_type _unused_key,
      const Hasher &hf = hasher(), const Equality &eql = key_equal()) :
      m_map(_capacity, hf, eql),
      m_identity(_identity),
      m_unused_key(_unused_key) {}

 private:
  map_t m_map;
  key_type m_unused_key;
  mapped_type m_identity;

 public:
  __host__ key_type
  get_unused_key() const {
    return m_unused_key;
  }

  __host__
  const_iterator begin() const noexcept {
    return m_map.begin();
  }

  __host__ size_type capacity() {
    return m_map.size();
  }

  template<typename aggregation_type,
      class comparison_type = key_equal,
      typename hash_value_type = typename Hasher::result_type>
  __host__ void
  insert(const value_type &x,
         aggregation_type op,
         comparison_type keys_equal){
    key_type key = x.first;
    mapped_type value = x.second;
    m_map[key] = op(m_map[key], value);
  }
};

#if defined(RUN_ON_DEVICE) && defined(SUPPORT_HASH_REDUCTION)
template<typename Key,
    typename Element,
    typename Hasher,
    typename Equality>
using hash_map = concurrent_unordered_map<Key, Element, Hasher, Equality>;

template <typename hash_map_type>
__host_or_device__
typename hash_map_type::value_type* map_value_at(hash_map_type* map,
    size_t i) {
  return map->data() + i;
}
#else
template<typename Key, typename Element, typename Hasher, typename Equality>
using hash_map = host_concurrent_unordered_map<Key, Element, Hasher, Equality>;

template <typename hash_map_type>
__host_or_device__
typename hash_map_type::const_iterator map_value_at(hash_map_type* map,
    size_t i) {
  return std::next(map->begin(), i);
}
#endif

}  // namespace ares

#endif  // QUERY_CONCURRENT_UNORDERED_MAP_HPP_
