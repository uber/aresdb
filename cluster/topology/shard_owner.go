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
