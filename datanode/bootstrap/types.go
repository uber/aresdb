package bootstrap

import (
	"github.com/uber/aresdb/datanode/topology"
	"time"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/namespace"
)

// ProcessProvider constructs a bootstrap process that can execute a bootstrap run.
type ProcessProvider interface {
	// SetBootstrapper sets the bootstrapper provider to use when running the process.
	SetBootstrapperProvider(bootstrapper BootstrapperProvider)

	// Bootstrapper returns the current bootstrappe provider to use when running the process.
	BootstrapperProvider() BootstrapperProvider

	// Provide constructs a bootstrap process.
	Provide() (Process, error)
}

// Process represents the bootstrap process. Note that a bootstrap process can and will
// be reused so it is important to not rely on state stored in the bootstrap itself
// with the mindset that it will always be set to default values from the constructor.
type Process interface {
	// Run runs the bootstrap process, returning the bootstrap result and any error encountered.
	Run(start time.Time, ns namespace.Metadata, shards []int) (ProcessResult, error)
}

// ProcessResult is the result of a bootstrap process.
type ProcessResult struct {
	DataResult  result.DataBootstrapResult
	IndexResult result.IndexBootstrapResult
}

// ProcessOptions is a set of options for a bootstrap provider.
type ProcessOptions interface {
	// SetTopologyMapProvider sets the TopologyMapProvider.
	SetTopologyMapProvider(value topology.MapProvider) ProcessOptions

	// TopologyMapProvider returns the TopologyMapProvider.
	TopologyMapProvider() topology.MapProvider

	// SetOrigin sets the origin.
	SetOrigin(value topology.Host) ProcessOptions

	// Origin returns the origin.
	Origin() topology.Host

	// Validate validates that the ProcessOptions are correct.
	Validate() error
}

// RunOptions is a set of options for a bootstrap run.
type RunOptions interface {
	// SetInitialTopologyState sets the initial topology state as it was
	// measured before the bootstrap process began.
	SetInitialTopologyState(value *topology.StateSnapshot) RunOptions

	// InitialTopologyState returns the initial topology as it was measured
	// before the bootstrap process began.
	InitialTopologyState() *topology.StateSnapshot
}

// BootstrapperProvider constructs a bootstrapper.
type BootstrapperProvider interface {
	// String returns the name of the bootstrapper.
	String() string

	// Provide constructs a bootstrapper.
	Provide() (Bootstrapper, error)
}

// Strategy describes a bootstrap strategy.
type Strategy int

const (
	// BootstrapSequential describes whether a bootstrap can use the sequential bootstrap strategy.
	BootstrapSequential Strategy = iota
	// BootstrapParallel describes whether a bootstrap can use the parallel bootstrap strategy.
	BootstrapParallel
)

// Bootstrapper is the interface for different bootstrapping mechanisms.  Note that a bootstrapper
// can and will be reused so it is important to not rely on state stored in the bootstrapper itself
// with the mindset that it will always be set to default values from the constructor.
type Bootstrapper interface {
	// String returns the name of the bootstrapper
	String() string

	// Can returns whether a specific bootstrapper strategy can be applied.
	Can(strategy Strategy) bool

	/*
		// BootstrapData performs bootstrapping of data for the given time ranges, returning the bootstrapped
		// series data and the time ranges it's unable to fulfill in parallel. A bootstrapper
		// should only return an error should it want to entirely cancel the bootstrapping of the
		// node, i.e. non-recoverable situation like not being able to read from the filesystem.
		BootstrapData(
			ns namespace.Metadata,
			shardsTimeRanges result.ShardTimeRanges,
			opts RunOptions,
		) (result.DataBootstrapResult, error)

		// BootstrapIndex performs bootstrapping of index blocks for the given time ranges, returning
		// the bootstrapped index blocks and the time ranges it's unable to fulfill in parallel. A bootstrapper
		// should only return an error should it want to entirely cancel the bootstrapping of the
		// node, i.e. non-recoverable situation like not being able to read from the filesystem.
		BootstrapIndex(
			ns namespace.Metadata,
			shardsTimeRanges result.ShardTimeRanges,
			opts RunOptions,
		) (result.IndexBootstrapResult, error)
	*/
}

// Source represents a bootstrap source. Note that a source can and will be reused so
// it is important to not rely on state stored in the source itself with the mindset
// that it will always be set to default values from the constructor.
type Source interface {
	// Can returns whether a specific bootstrapper strategy can be applied.
	Can(strategy Strategy) bool

	// AvailableData returns what time ranges are available for bootstrapping a given set of shards.
	AvailableData(
		ns namespace.Metadata,
		shardsTimeRanges result.ShardTimeRanges,
		runOpts RunOptions,
	) (result.ShardTimeRanges, error)

	// ReadData returns raw series for a given set of shards & specified time ranges and
	// the time ranges it's unable to fulfill. A bootstrapper source should only return
	// an error should it want to entirely cancel the bootstrapping of the node,
	// i.e. non-recoverable situation like not being able to read from the filesystem.
	ReadData(
		ns namespace.Metadata,
		shardsTimeRanges result.ShardTimeRanges,
		runOpts RunOptions,
	) (result.DataBootstrapResult, error)

	// AvailableIndex returns what time ranges are available for bootstrapping.
	AvailableIndex(
		ns namespace.Metadata,
		shardsTimeRanges result.ShardTimeRanges,
		opts RunOptions,
	) (result.ShardTimeRanges, error)

	// ReadIndex returns series index blocks.
	ReadIndex(
		ns namespace.Metadata,
		shardsTimeRanges result.ShardTimeRanges,
		opts RunOptions,
	) (result.IndexBootstrapResult, error)
}
