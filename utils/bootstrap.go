package utils

import "errors"

var (
	// ErrDatanodeIsBootstrapping raised when trying to bootstrap a datanode that's being bootstrapped.
	ErrDatanodeIsBootstrapping = errors.New("datanode is bootstrapping")

	// ErrDatanodeNotBootstrapped raised when trying to flush/snapshot data for a namespace that's not yet bootstrapped.
	ErrDatanodeNotBootstrapped = errors.New("datanode is not yet bootstrapped")

	// ErrTableShardIsBootstrapping raised when trying to bootstrap a shard that's being bootstrapped.
	ErrTableShardIsBootstrapping = errors.New("table shard is bootstrapping")

	// ErrTableShardNotBootstrappedToFlush raised when trying to flush data for a shard that's not yet bootstrapped.
	ErrTableShardNotBootstrappedToFlush = errors.New("table shard is not yet bootstrapped to flush")

	// ErrTableShardNotBootstrappedToSnapshot raised when trying to snapshot data for a shard that's not yet bootstrapped.
	ErrTableShardNotBootstrappedToSnapshot = errors.New("table shard is not yet bootstrapped to snapshot")

	// ErrTableShardNotBootstrappedToRead raised when trying to read data for a shard that's not yet bootstrapped.
	ErrTableShardNotBootstrappedToRead = errors.New("table shard is not yet bootstrapped to read")

	// ErrBootstrapEnqueued raised when trying to bootstrap and bootstrap becomes enqueued.
	ErrBootstrapEnqueued = errors.New("database bootstrapping enqueued bootstrap")
)
