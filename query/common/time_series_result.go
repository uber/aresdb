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
	"github.com/uber/aresdb/utils"
)

// AQLTimeSeriesResult
//
// Represents a nested AQL time series result with one dimension on each layer:
//  - there is always an outermost time dimension. it stores the start time of
//    the bucket/duration (in seconds since Epoch).
//  - after the time dimension, there could be zero or more layers of additional
//    dimensions (all values are represented as strings). a special "NULL" string
///   is used to represent NULL values.
//  - there is always a measure, and the measure type is either a 2d array or nil.
type AQLTimeSeriesResult map[string]interface{}

// AppendAggMeasure appends single aggregated measure value to specific dimension
func (r AQLTimeSeriesResult) AppendAggMeasure(dimValues []*string, measureValue *float64) {
	null := "NULL"
	var current map[string]interface{} = r
	for i, dimValue := range dimValues {
		if dimValue == nil {
			dimValue = &null
		}

		if i == len(dimValues)-1 {
			// leaf
			if current[*dimValue] == nil {
				current[*dimValue] = [][]float64{{}}
			}
			measures := current[*dimValue].([][]float64)[0]
			if measureValue != nil {
				measures = append(measures, *measureValue)
			}
			current[*dimValue].([][]float64)[0] = measures
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

// AppendNonAggValue appends single value to specific row
// result rows must be built in order
// TODO: evaluate this design, maybe use separate result type
func (r AQLTimeSeriesResult) AppendNonAggValue(row int, value interface{}) error {
	if _, ok := r["0"]; !ok {
		r["0"] = [][]interface{}{}
	}

	values := r["0"].([][]interface{})
	if row > len(values) +1 {
		return utils.StackError(nil, "result rows must be built in order")
	}

	if row == len(values) {
		values = append(values, []interface{}{})
	}

	values[row] = append(values[row], value)
	return nil
}

// SetHLL sets hll struct to be the leaves of the nested map.
func (r AQLTimeSeriesResult) SetHLL(dimValues []*string, hll HLL) {
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
