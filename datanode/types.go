package datanode

import (
	"github.com/uber/aresdb/datanode/bootstrap"
	"github.com/uber/aresdb/diskstore"
	"github.com/uber/aresdb/memstore"
	"github.com/uber/aresdb/metastore"
	"github.com/uber/aresdb/utils"
	"time"
)

// DataNode includes metastore, memstore and diskstore
type DataNode struct {
	metastore.MetaStore
	memstore.MemStore
	diskstore.DiskStore
}

// databaseMediator mediates actions among various datanode managers.
type datanodeMediator interface {
	// Open opens the mediator.
	Open() error

	// IsBootstrapped returns whether the datanode is bootstrapped.
	IsBootstrapped() bool

	// LastBootstrapCompletionTime returns the last bootstrap completion time,
	// if any.
	LastBootstrapCompletionTime() (time.Time, bool)

	// Bootstrap bootstraps the datanode with file operations performed at the end.
	Bootstrap() error

	// DisableFileOps disables file operations.
	DisableFileOps()

	// EnableFileOps enables file operations.
	EnableFileOps()

	// Repair repairs the datanode.
	Repair() error

	// Close closes the mediator.
	Close() error

	// Report reports runtime information.
	Report()

	// LastSuccessfulSnapshotStartTime returns the start time of the last
	// successful snapshot, if any.
	LastSuccessfulSnapshotStartTime() (time.Time, bool)
}

// datanodeBootstrapManager manages the bootstrap process.
type datanodeBootstrapManager interface {
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

// DatanodeBootstrapState stores a snapshot of the bootstrap state for all shards across all
// namespaces at a given moment in time.
type DatanodeBootstrapState struct {
	TableBootstrapState TableBootstrapState
}

// TableBootstrapState stores a snapshot of the bootstrap state for all shards across a
// number of namespaces at a given moment in time.
type TableBootstrapState map[string]ShardBootstrapStates

// ShardBootstrapStates stores a snapshot of the bootstrap state for all shards for a given
// namespace.
type ShardBootstrapStates map[uint32]BootstrapState

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

	// SetBootstrapProcessProvider sets the bootstrap process provider for the database.
	SetBootstrapProcessProvider(value bootstrap.ProcessProvider) Options

	// BootstrapProcessProvider returns the bootstrap process provider for the database.
	BootstrapProcessProvider() bootstrap.ProcessProvider
}
