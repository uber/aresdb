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

package topology

type topologyShardOwner struct {
	topo Topology
}

// static shard owner
type staticShardOwner struct {
	shards []int
}

// NewTopologyShardOwner return a shard owner based on topology
func NewTopologyShardOwner(topo Topology) ShardOwner {
	return &topologyShardOwner{
		topo: topo,
	}
}

func (t *topologyShardOwner) GetOwnedShards() []int {
	allShards := t.topo.Get().ShardSet().AllIDs()
	ret := make([]int, len(allShards))
	for i, shardID := range allShards {
		ret[i] = int(shardID)
	}
	return ret
}

// NewStaticShardOwner returns shard owner that owns static shards
func NewStaticShardOwner(shards []int) ShardOwner {
	return &staticShardOwner{
		shards: shards,
	}
}

func (s *staticShardOwner) GetOwnedShards() []int {
	return s.shards
}
