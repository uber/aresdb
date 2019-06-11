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

package datanode

import (
	"github.com/uber/aresdb/cluster"
	"github.com/uber/aresdb/cluster/shard"
	"github.com/uber/aresdb/cluster/topology"
	"github.com/uber/aresdb/datanode/bootstrap"
	"github.com/uber/aresdb/utils"
	"time"
)

type DataNode interface {
	// Options returns the database options.
	Options() Options

	// ShardSet returns the set of shards currently associated with this datanode.
	ShardSet() (shard.ShardSet, error)

	// AssignNamespace sets the namespace.
	AssignNamespace(namespace cluster.Namespace)

	// Namespaces returns the namespace.
	Namespace() cluster.Namespace

	// Host returns the datanode host information.
	Host() topology.Host

	// Bootstrap performs bootstrapping.
	Bootstrap(topo topology.Topology) error

	// TableShardsBootstrapState captures and returns a snapshot of the datanode's bootstrap state for each table shard.
	TableShardsBootstrapState() bootstrap.TableShardsBootstrapState

	// // ShardsBootstrapState captures and returns a snapshot of the datanode's bootstrap state for each shard.
	ShardsBootstrapState() bootstrap.ShardsBootstrapState
}

// BootstrapManager manages the bootstrap process.
type BootstrapManager interface {
	// IsBootstrapped returns whether the datanode is already bootstrapped.
	IsBootstrapped() bool

	// LastBootstrapCompletionTime returns the last bootstrap completion time,
	// if any.
	LastBootstrapCompletionTime() (time.Time, bool)

	// Bootstrap performs bootstrapping for all namespaces and shards owned.
	Bootstrap() error

	// Report reports runtime information.
	Report()
}

// Options represents the options for storage.
type Options interface {
	// SetInstrumentOptions sets the instrumentation options.
	SetInstrumentOptions(value utils.Options) Options

	// InstrumentOptions returns the instrumentation options.
	InstrumentOptions() utils.Options
}

// BootStrapToken used to Acqure/Release token during data purge operations
type BootStrapToken interface {
	// Call AcquireToken to reserve usage token before any data purge operation
	// when return result is true, then you can proceed to the purge operation and later call ReleaseToken to release the token
	// when return result is false, then some bootstrap work is going on, no purge operation is permitted
	AcquireToken(table string, shard uint32) bool
	// Call ReleaseToken wheneven you call AcquireToken with true return value to release token
	ReleaseToken(table string, shard uint32)
}
