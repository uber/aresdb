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
	// Logger returns the logger
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
