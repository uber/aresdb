package datanode

import (
	"errors"
	"github.com/uber/aresdb/utils"
)

var (
	errNamespaceInitializerNotSet = errors.New("namespace registry initializer not set")
)

// options is the implementation of the interface Options
type options struct {
	instrumentOpts           utils.Options
}

// NewOptions creates a new set of storage options with defaults
func NewOptions() Options {
	opts := options{
		instrumentOpts: utils.NewOptions(),
		// TODO: bootstrapProcessProvider: defaultBootstrapProcessProvider,
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