package cluster

import (
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3x/ident"
	"github.com/uber/aresdb/cluster/shard"
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

// database is the internal database interface
type database interface {
	Database

	// GetOwnedNamespaces returns the namespaces this database owns.
	GetOwnedNamespaces() ([]databaseNamespace, error)

	// UpdateOwnedNamespaces updates the namespaces this database owns.
	UpdateOwnedNamespaces(namespaces namespace.Map) error
}

// Namespace is aresDB database namespace
type Namespace interface {
	// Options returns the namespace options
	Options() namespace.Options

	// ID returns the ID of the namespace
	ID() ident.ID

	// Tables returns the tables of the namespace.
	Tables() ([]string, error)

	// Shards returns the shard description
	Shards() ([]Shard, error)

	// Topology return the topology description
	Topology() (topology.Topology, error)
}

type databaseNamespace interface {
	Namespace

	// Close will release the namespace resources and close the namespace.
	Close() error

	// AssignShardSet sets the shard set assignment and returns immediately.
	AssignShardSet(shardSet shard.ShardSet)

	// GetOwnedShards returns the database shards.
	GetOwnedShards() []Shard

	// BootstrapState captures and returns a snapshot of the namespaces' bootstrap state.
	BootstrapState() TableShardBootstrapStates
}

// Shard is aresDB database shard.
type Shard interface {
	// ID returns the ID of the shard.
	ID() uint32

	// IsBootstrapped returns whether the shard is already bootstrapped.
	IsBootstrapped() bool

	// BootstrapState returns the shards' bootstrap state.
	BootstrapState() BootstrapState
}

// DatabaseBootstrapState stores a snapshot of the bootstrap state for all shards across all
// namespaces at a given moment in time.
type DatabaseBootstrapState struct {
	NamespaceBootstrapStates NamespaceBootstrapStates
}

// NamespaceBootstrapStates stores a snapshot of the bootstrap state for all shards across a
// number of namespaces at a given moment in time.
type NamespaceBootstrapStates map[string]TableShardBootstrapStates

// TableShardBootstrapStates stores a snapshot of the bootstrap state for all shards for a given namespace.
type TableShardBootstrapStates map[string]map[uint32]BootstrapState

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

// Options represents the options for storage.
type Options interface {
	// SetInstrumentOptions sets the instrumentation options.
	SetInstrumentOptions(value utils.Options) Options

	// InstrumentOptions returns the instrumentation options.
	InstrumentOptions() utils.Options
}
