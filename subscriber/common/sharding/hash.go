package sharding

// ShardFn maps a id to a shard
type ShardFn func(id []byte, numShards uint32) uint32

// HashType is the hashing type
type HashType string

// List of supported hashing types
const (
	// Murmur32Hash represents the murmur3 hash.
	Murmur32Hash HashType = "murmur32"

	// ZeroHash always returns 0 as the hash. It is used when sharding is disabled.
	ZeroHash HashType = "zero"

	// DefaultHash is Murmur32Hash
	DefaultHash = Murmur32Hash
)
