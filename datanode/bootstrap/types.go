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

package bootstrap

import (
	"encoding/json"
	"errors"
	"github.com/uber/aresdb/cluster/topology"
	"github.com/uber/aresdb/datanode/client"
)

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

// BootstrapStage represents stages of bootstrap
type BootstrapStage string

const (
	Waiting  BootstrapStage = "waiting"
	PeerCopy BootstrapStage = "peercopy"
	Preload  BootstrapStage = "preload"
	Recovery BootstrapStage = "recovery"
	Finished BootstrapStage = "finished"
)

// BootstrapDetail describes details for bootstrap
type BootstrapDetails interface {
	json.Marshaler

	SetSource(source string)
	SetNumColumns(numColumns int)
	SetBootstrapStage(stage BootstrapStage)
	AddVPToCopy(batch int32, columnID uint32)
	MarkVPFinished(batch int32, columnID uint32)
	Clear()
}

// BootstrapState is an enum representing the possible bootstrap states for a shard.
type BootstrapState int

const (
	// BootstrapNotStarted indicates bootstrap has not been started yet.
	BootstrapNotStarted BootstrapState = iota
	// Bootstrapping indicates bootstrap process is in progress.
	Bootstrapping
	// Bootstrapped indicates a bootstrap process has completed.
	Bootstrapped
)

// Bootstrapable defines bootstrapable interface
type Bootstrapable interface {
	Bootstrap(peerSource client.PeerSource, origin string, topo topology.Topology, topoState *topology.StateSnapshot, options Options) error
}

// Options defines options for bootstrap
type Options interface {
	// MaxConcurrentTableShards returns the max number of concurrent bootstrapping table shards
	MaxConcurrentTableShards() int
	// SetMaxConcurrentShards sets the max number of concurrent bootstrapping table shards
	SetMaxConcurrentShards(numShards int) Options
	// MaxConcurrentStreamsPerTableShards returns the max number of current data streams per bootstrapping table shard
	MaxConcurrentStreamsPerTableShards() int
	// SetMaxConcurrentStreamsPerTableShards sets the max number of current data streams per bootstrapping table shard
	SetMaxConcurrentStreamsPerTableShards(numStreams int) Options
	// BootstrapSessionTTL returns the ttl for bootstrap session
	BootstrapSessionTTL() int64
	// SetBootstrapSessionTTL sets the session ttl for bootstrap session
	SetBootstrapSessionTTL(ttl int64) Options
}
