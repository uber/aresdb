// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
