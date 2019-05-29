package datanode

import (
	"errors"
	"github.com/uber/aresdb/datanode/bootstrap"
	"github.com/uber/aresdb/utils"
	"time"
)

var (
	// defaultBootstrapProcessProvider is the default bootstrap provider for the database.
	defaultBootstrapProcessProvider = bootstrap.NewNoOpProcessProvider()
	timeZero                        time.Time
)

var (
	errNamespaceInitializerNotSet = errors.New("namespace registry initializer not set")
)

// options is the implementation of the interface Options
type options struct {
	instrumentOpts           utils.Options
	bootstrapProcessProvider bootstrap.ProcessProvider
}

// NewOptions creates a new set of storage options with defaults
func NewOptions() Options {
	opts := options{
		utils.NewOptions(),
		defaultBootstrapProcessProvider,
	}
	return &opts
}

func (o *options) SetInstrumentOptions(value utils.Options) Options {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *options) InstrumentOptions() utils.Options {
	return o.instrumentOpts
}

func (o *options) SetBootstrapProcessProvider(value bootstrap.ProcessProvider) Options {
	opts := *o
	opts.bootstrapProcessProvider = value
	return &opts
}

func (o *options) BootstrapProcessProvider() bootstrap.ProcessProvider {
	return o.bootstrapProcessProvider
}
