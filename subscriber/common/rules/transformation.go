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

package rules

const (
	noOp string = "noop"
)

var (
	// Note: when add new transformation,
	// make sure transformFuncs and numSourcesPerTransformations should always have the same set of keys

	// transformation functions per each transformation
	transformFuncs = map[string]func(from interface{}, ctx map[string]string) (interface{}, error){}

	// default value for each transformation
	// if not defined, the default value will be what is defined in the transformation config
	// if not defined again, the default value will be nil
	defaultsPerTransformation = map[string]interface{}{
		noOp: nil,
	}
)

// NoOp is a transformation preserve value
// but it can rename the source value into different alias
func NoOp(from interface{}, ctx map[string]string) (interface{}, error) {
	return from, nil
}

// Transform converts source to destination data
func (t TransformationConfig) Transform(from interface{}) (to interface{}, err error) {
	transformFunc, found := transformFuncs[t.Type]
	if !found {
		transformFunc = NoOp
	}

	to, err = transformFunc(from, t.Context)
	return
}
