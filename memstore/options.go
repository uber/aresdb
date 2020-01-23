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

package memstore

import (
	"github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/redolog"
)

type Option func(o *Options)

// class to hold all necessary context objects used in memstore
type Options struct {
	bootstrapToken common.BootStrapToken
	redoLogMaster  *redolog.RedoLogManagerMaster
}

// NewOptions create new options instance
func NewOptions(bootstrapToken common.BootStrapToken, redoLogMaster *redolog.RedoLogManagerMaster, setters ...Option) Options {
	opts := Options{
		bootstrapToken: bootstrapToken,
		redoLogMaster:  redoLogMaster,
	}
	for _, setter := range setters {
		setter(&opts)
	}
	return opts
}
