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

import "github.com/uber/aresdb/utils"

const (
	MatrixDataKey = "matrixData"
	HeadersKey = "headers"
)

// AQLQueryResult represents final result of one AQL query
//
// It has 2 possible formats:
// Time series result format:
// One dimension on each layer:
//  - there is always an outermost time dimension. it stores the start time of
//    the bucket/duration (in seconds since Epoch).
//  - after the time dimension, there could be zero or more layers of additional
//    dimensions (all values are represented as strings). a special "NULL" string
///   is used to represent NULL values.
//  - there is always a single measure, and the measure type is either float64
//    or nil (not *float64);
//
// Non aggregate query result format:
//  - there will be a "headers" key, value will be a list of column names
//  - there will be a "matrixData" key, value will be a 2d arary of values (row formated)
//
// user should use it as only 1 of the 2 formats consistently
type AQLQueryResult map[string]interface{}

// =====  Time series result methods start =====

// Set measure value for dimensions
func (r AQLQueryResult) Set(dimValues []*string, measureValue *float64) {
	null := "NULL"
	var current map[string]interface{} = r
	for i, dimValue := range dimValues {
		if dimValue == nil {
			dimValue = &null
		}

		if i == len(dimValues)-1 {
			if measureValue == nil {
				current[*dimValue] = nil
			} else {
				current[*dimValue] = *measureValue
			}
		} else {
			child := current[*dimValue]
			if child == nil {
				child = make(map[string]interface{})
				current[*dimValue] = child
			}
			current = child.(map[string]interface{})
		}
	}
}

// SetHLL sets hll struct to be the leaves of the nested map.
func (r AQLQueryResult) SetHLL(dimValues []*string, hll HLL) {
	null := "NULL"
	var current map[string]interface{} = r
	for i, dimValue := range dimValues {
		if dimValue == nil {
			dimValue = &null
		}

		if i == len(dimValues)-1 {
			current[*dimValue] = hll
		} else {
			child := current[*dimValue]
			if child == nil {
				child = make(map[string]interface{})
				current[*dimValue] = child
			}
			current = child.(map[string]interface{})
		}
	}
}

// =====  Time series result methods end =====

// =====  Non aggregate query result methods start =====

// Append appends single value to specific row, result rows must be built in order
func (r AQLQueryResult) Append(row int, value interface{}) error {
	if _, ok := r[MatrixDataKey]; !ok {
		r[MatrixDataKey] = [][]interface{}{}
	}

	values := r[MatrixDataKey].([][]interface{})
	if row > len(values) {
		return utils.StackError(nil, "result rows must be built in order")
	}

	if row == len(values) {
		values = append(values, []interface{}{})
	}

	values[row] = append(values[row], value)
	r[MatrixDataKey] = values
	return nil
}

// SetHeaders sets headers field for the results
func (r AQLQueryResult) SetHeaders(headers []string) {
	r[HeadersKey] = headers
}

// =====  Non aggregate query result methods end =====


