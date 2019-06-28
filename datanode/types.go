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
	"github.com/uber/aresdb/cluster/shard"
	"github.com/uber/aresdb/common"
	"github.com/uber/aresdb/datanode/bootstrap"
	"github.com/uber/aresdb/memstore"
	"github.com/uber/aresdb/utils"
	"time"
)

type DataNode interface {
	// Options returns the database options.
	Options() Options

	// ID returns the host id of the DataNode
	ID() string

	// ShardSet returns the set of shards currently associated with this datanode.
	ShardSet() shard.ShardSet

	// Tables
	Tables() []string

	// GetTableShard will get table shard
	GetTableShard(table string, shardID uint32) (*memstore.TableShard, error)

	// Open data node
	Open() error

	// Bootstrap starts data node bootstap
	Bootstrap() error

	// Close data node
	Close()

	// Serve will start serving read and write requests
	// should always call Bootstrap() during server start before Serve()
	Serve()
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
	SetInstrumentOptions(utils.Options) Options

	// InstrumentOptions returns the instrumentation options.
	InstrumentOptions() utils.Options

	// Options returns bootstrap options
	BootstrapOptions() bootstrap.Options

	// SetBootstrapOptions sets bootstrap options
	SetBootstrapOptions(bootstrap.Options) Options

	// SetServerConfig sets server config
	SetServerConfig(common.AresServerConfig) Options

	// SetServerConfig returns server config
	ServerConfig() common.AresServerConfig

	// HTTPWrappers returns http handler wrappers
	HTTPWrappers() []utils.HTTPHandlerWrapper

	// SetHTTPWrappers returns http handler wrappers
	SetHTTPWrappers([]utils.HTTPHandlerWrapper) Options
}
