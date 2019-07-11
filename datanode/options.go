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

package datanode

import (
	"github.com/uber/aresdb/common"
	"github.com/uber/aresdb/datanode/bootstrap"
	"github.com/uber/aresdb/utils"
)

// options is the implementation of the interface Options
type options struct {
	instrumentOpts utils.Options
	bootstrapOpts  bootstrap.Options
	httpWrappers   []utils.HTTPHandlerWrapper
	cfg            common.AresServerConfig
}

// NewOptions creates a new set of storage options with defaults
func NewOptions() Options {
	opts := options{
		instrumentOpts: utils.NewOptions(),
	}
	return &opts
}

func (o *options) SetInstrumentOptions(value utils.Options) Options {
	o.instrumentOpts = value
	return o
}

func (o *options) InstrumentOptions() utils.Options {
	return o.instrumentOpts
}

func (o *options) BootstrapOptions() bootstrap.Options {
	return o.bootstrapOpts
}

func (o *options) SetBootstrapOptions(bootstrapOptions bootstrap.Options) Options {
	o.bootstrapOpts = bootstrapOptions
	return o
}

func (o *options) SetServerConfig(cfg common.AresServerConfig) Options {
	o.cfg = cfg
	return o
}

func (o *options) ServerConfig() common.AresServerConfig {
	return o.cfg
}

// HttpWrappers return HttpWrappers
func (o *options) HTTPWrappers() []utils.HTTPHandlerWrapper {
	return o.httpWrappers
}

func (o *options) SetHTTPWrappers(wrappers []utils.HTTPHandlerWrapper) Options {
	o.httpWrappers = wrappers
	return o
}
