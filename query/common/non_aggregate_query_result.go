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

const MatrixDataKey = "matrixData"
const HeadersKey = "headers"

// AQLNonAggregatedQueryResult represents non-aggregated query result
// - there will be a "headers" key, value will be a list of column names
// - there will be a "matrixData" key, value will be a 2d arary of values (row formated)
type AQLNonAggregatedQueryResult map[string]interface{}

// Append appends single value to specific row, result rows must be built in order
func (r AQLNonAggregatedQueryResult) Append(row int, value interface{}) error {
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
func (r AQLNonAggregatedQueryResult) SetHeaders(headers []string) {
	r[HeadersKey] = headers
}
