package cluster

import (
	m3shard "github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3x/ident"
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
	Namespace(ns ident.ID) (Namespace, bool)
}

// Namespace is aresDB database namespace
type Namespace interface {
	// ID returns the ID of the namespace
	ID() ident.ID

	// Tables returns the tables of the namespace.
	Tables() ([]string, error)

	// Shards returns the shard description
	Shards() ([]m3shard.Shard, error)

	// Topology return the topology description
	Topology() (topology.Topology, error)
}

// Options represents the options for storage.
type Options interface {
	// SetInstrumentOptions sets the instrumentation options.
	SetInstrumentOptions(value utils.Options) Options

	// InstrumentOptions returns the instrumentation options.
	InstrumentOptions() utils.Options
}
