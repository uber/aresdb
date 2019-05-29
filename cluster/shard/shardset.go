package shard

import (
	"github.com/m3db/m3/src/cluster/shard"
)

// shardSet is the implementation of the interface ShardSet
type shardSet struct {
	shards   []shard.Shard
	ids      []uint32
	shardMap map[uint32]shard.Shard
}

// NewShardSet creates a new sharding scheme with a set of shards
func NewShardSet(shards []shard.Shard) ShardSet {
	ids := make([]uint32, len(shards))
	shardMap := make(map[uint32]shard.Shard, len(shards))
	for i, shard := range shards {
		ids[i] = shard.ID()
		shardMap[shard.ID()] = shard
	}
	return &shardSet{
		shards:   shards,
		ids:      ids,
		shardMap: shardMap,
	}
}

func (s *shardSet) All() []shard.Shard {
	return s.shards[:]
}

func (s *shardSet) AllIDs() []uint32 {
	return s.ids[:]
}

// NewShards returns a new slice of shards with a specified state
func NewShards(ids []uint32, state shard.State) []shard.Shard {
	shards := make([]shard.Shard, len(ids))
	for i, id := range ids {
		shards[i] = shard.NewShard(uint32(id)).SetState(state)
	}
	return shards
}

// IDs returns a new slice of shard IDs for a set of shards
func IDs(shards []shard.Shard) []uint32 {
	ids := make([]uint32, len(shards))
	for i := range ids {
		ids[i] = shards[i].ID()
	}
	return ids
}

// intRange returns a slice of all values between [from, to].
func intRange(from, to uint32) []uint32 {
	var ids []uint32
	for i := from; i <= to; i++ {
		ids = append(ids, i)
	}
	return ids
}

// ShardsRange returns a slice of shards for all ids between [from, to],
// with shard state `s`.
func ShardsRange(from, to uint32, s shard.State) []shard.Shard {
	return NewShards(intRange(from, to), s)
}
