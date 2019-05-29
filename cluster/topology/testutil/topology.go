package testutil

import (
	"fmt"
	"github.com/m3db/m3/src/cluster/shard"
	aresShard "github.com/uber/aresdb/cluster/shard"
	"github.com/uber/aresdb/cluster/topology"
)

const (
	// SelfID is the string used to represent the ID of the origin node.
	SelfID = "self"
)

// MustNewTopologyMap returns a new topology.Map with provided parameters.
// It's a utility method to make tests easier to write.
func MustNewTopologyMap(replicas int, assignment map[string][]shard.Shard) topology.Map {
	v := NewTopologyView(replicas, assignment)
	m, err := v.Map()
	if err != nil {
		panic(err.Error())
	}
	return m
}

// NewTopologyView returns a new TopologyView with provided parameters.
// It's a utility method to make tests easier to write.
func NewTopologyView(replicas int, assignment map[string][]shard.Shard) TopologyView {
	total := 0
	for _, shards := range assignment {
		total += len(shards)
	}

	return TopologyView{
		Replicas:   replicas,
		Assignment: assignment,
	}
}

// TopologyView represents a snaphshot view of a topology.Map.
type TopologyView struct {
	Replicas   int
	Assignment map[string][]shard.Shard
}

// Map returns the topology.Map corresponding to a TopologyView.
func (v TopologyView) Map() (topology.Map, error) {
	var (
		hostShardSets []topology.HostShardSet
		allShards     []shard.Shard
		unique        = make(map[uint32]struct{})
	)

	for hostID, assignedShards := range v.Assignment {
		shardSet := aresShard.NewShardSet(assignedShards)
		host := topology.NewHost(hostID, fmt.Sprintf("%s:9000", hostID))
		hostShardSet := topology.NewHostShardSet(host, shardSet)
		hostShardSets = append(hostShardSets, hostShardSet)
		for _, s := range assignedShards {
			if _, ok := unique[s.ID()]; !ok {
				unique[s.ID()] = struct{}{}
				uniqueShard := shard.NewShard(s.ID()).SetState(shard.Available)
				allShards = append(allShards, uniqueShard)
			}
		}
	}

	shardSet := aresShard.NewShardSet(allShards)

	opts := topology.NewStaticOptions().
		SetHostShardSets(hostShardSets).
		SetShardSet(shardSet)

	return topology.NewStaticMap(opts), nil
}

// HostShardStates is a human-readable way of describing an initial state topology
// on a host-by-host basis.
type HostShardStates map[string][]shard.Shard

// NewStateSnapshot creates a new initial topology state snapshot using HostShardStates
// as input.
func NewStateSnapshot(hostShardStates HostShardStates) *topology.StateSnapshot {
	topoState := &topology.StateSnapshot{
		Origin:      topology.NewHost(SelfID, "127.0.0.1"),
		ShardStates: make(map[topology.ShardID]map[topology.HostID]topology.HostShardState),
	}

	for host, shards := range hostShardStates {
		for _, shard := range shards {
			hostShardStates, ok := topoState.ShardStates[topology.ShardID(shard.ID())]
			if !ok {
				hostShardStates = make(map[topology.HostID]topology.HostShardState)
			}

			hostShardStates[topology.HostID(host)] = topology.HostShardState{
				Host:       topology.NewHost(host, host+"address"),
				ShardState: shard.State(),
			}
			topoState.ShardStates[topology.ShardID(shard.ID())] = hostShardStates
		}
	}

	return topoState
}
