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

package common

import (
	"io"

	"io/ioutil"

	"github.com/uber-go/tally"
)

// Metrics is the interface for stats reporting based on tally. The application call NewRootScope()
// at start up time to get the root scope and calls closer.Close() before shutdown.
type Metrics interface {
	NewRootScope() (tally.Scope, io.Closer, error)
}

// NewNoopMetrics returns a Metrics that will do nothing for reporting.
func NewNoopMetrics() Metrics {
	return dummyMetrics{}
}

type dummyMetrics struct{}

// NewRootScope returns a no-op scope will do nothing for reporting metrics. The closer will also be
// a no-op closer.
func (dummyMetrics) NewRootScope() (tally.Scope, io.Closer, error) {
	return tally.NoopScope, ioutil.NopCloser(nil), nil
}
