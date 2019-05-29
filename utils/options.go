package utils

import (
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/common"
)

const (
	defaultSamplingRate = 1.0
)

// Options represents the options for instrumentation.
type Options interface {
	// ZapLogger returns the zap logger
	Logger() common.Logger

	// MetricsScope returns the metrics scope.
	MetricsScope() tally.Scope

	// SetMetricsSamplingRate sets the metrics sampling rate.
	SetMetricsSamplingRate(value float64) Options

	// SetMetricsSamplingRate returns the metrics sampling rate.
	MetricsSamplingRate() float64
}

type options struct {
	log          common.Logger
	scope        tally.Scope
	samplingRate float64
}

// NewOptions creates new instrument options.
func NewOptions() Options {
	return &options{
		log:          GetLogger(),
		scope:        GetRootReporter().GetRootScope(),
		samplingRate: defaultSamplingRate,
	}
}

func (o *options) Logger() common.Logger {
	return o.log
}

func (o *options) MetricsScope() tally.Scope {
	return o.scope
}

func (o *options) SetMetricsSamplingRate(value float64) Options {
	opts := *o
	opts.samplingRate = value
	return &opts
}

func (o *options) MetricsSamplingRate() float64 {
	return o.samplingRate
}
