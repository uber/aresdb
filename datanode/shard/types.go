package shard

import "github.com/m3db/m3/src/cluster/shard"

// ShardSet contains a sharding function and a set of shards, this interface
// allows for potentially out of order shard sets
type ShardSet interface {
	// All returns a slice to the shards in this set
	All() []shard.Shard

	// AllIDs returns a slice to the shard IDs in this set
	AllIDs() []uint32
}
