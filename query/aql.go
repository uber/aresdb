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

package query

import (
	queryCom "github.com/uber/aresdb/query/common"
)

// AQLRequest contains multiple of AQLQueries.
type AQLRequest struct {
	Queries []queryCom.AQLQuery `json:"queries"`
}

// AQLResponse contains results for multiple AQLQueries.
type AQLResponse struct {
	Results      []queryCom.AQLQueryResult `json:"results"`
	Errors       []error                   `json:"errors,omitempty"`
	QueryContext []*AQLQueryContext        `json:"context,omitempty"`
}
