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

	// SetBootstrapProcessProvider sets the bootstrap process provider for the database.
	SetBootstrapProcessProvider(value bootstrap.ProcessProvider) Options

	// BootstrapProcessProvider returns the bootstrap process provider for the database.
	BootstrapProcessProvider() bootstrap.ProcessProvider
}
