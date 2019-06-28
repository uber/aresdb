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

package util

import (
	"fmt"
	"github.com/uber/aresdb/cluster/topology"
	"github.com/uber/aresdb/utils"
)

// CalculateShardAssignment maps shards to hosts
func CalculateShardAssignment(topo topology.Topology) (as map[topology.Host][]uint32, err error) {
	m := topo.Get()
	hosts := m.Hosts()
	shardIDs := m.ShardSet().AllIDs()

	// initialize host map
	for _, host := range hosts {
		as[host] = []uint32{}
	}

	for _, shardID := range shardIDs {
		var shardHosts []topology.Host
		// get routable hosts for current shard
		shardHosts, err = m.RouteShard(shardID)
		if err != nil {
			err = utils.StackError(err, fmt.Sprintf("failed to route shard %d", shardID))
			return
		}
		// pick host with lowest load to route current shard
		var pick topology.Host
		minLoad := len(shardIDs) + 1
		for _, shardHost := range shardHosts {
			load := len(as[shardHost])
			if load < minLoad {
				minLoad = load
				pick = shardHost
			}
		}
		as[pick] = append(as[pick], shardID)
	}
	return
}
