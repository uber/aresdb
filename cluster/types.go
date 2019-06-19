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

package cluster

import (
	m3shard "github.com/m3db/m3/src/cluster/shard"
	"github.com/uber/aresdb/cluster/topology"
	"github.com/uber/aresdb/utils"
)

// Database is aresDB database.
type Database interface {
	// Options returns the database options.
	Options() Options

	// Namespaces returns the namespaces.
	Namespaces() []Namespace

	// Namespace returns the specified namespace.
	Namespace(ns string) (Namespace, bool)
}

// Namespace is aresDB database namespace
type Namespace interface {
	// ID returns the ID of the namespace
	ID() string

	// Tables returns the tables of the namespace.
	Tables() ([]string, error)

	// Shards returns the shard description
	Shards() ([]m3shard.Shard, error)

	// Topology return the topology description
	Topology() topology.Topology
}

// Options represents the options for storage.
type Options interface {
	// SetInstrumentOptions sets the instrumentation options.
	SetInstrumentOptions(value utils.Options) Options

	// InstrumentOptions returns the instrumentation options.
	InstrumentOptions() utils.Options
}
