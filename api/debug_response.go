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

package api

import (
	"github.com/uber/aresdb/memstore/vectors"
)

// ShowBatchResponse represents ShowBatch response.
type ShowBatchResponse struct {
	Body struct {
		Columns  []string               `json:"columns"`
		Types    []string               `json:"types"`
		Deleted  []int                  `json:"deleted"`
		Vectors  []vectors.SlicedVector `json:"vectors"`
		StartRow int                    `json:"startRow"`
		NumRows  int                    `json:"numRows"`
	}
}

// ReadUpsertBatchResponse represents ReadUpsertBatch response.
type ReadUpsertBatchResponse struct {
	Data            [][]interface{} `json:"data"`
	ColumnNames     []string        `json:"columnNames"`
	RecordsFiltered int             `json:"recordsFiltered"`
	RecordsTotal    int             `json:"recordsTotal"`
	Draw            int             `json:"draw"`
	Error           string          `json:"error"`
}

// ListRedoLogsResponse represents the ListRedoLogs response.
type ListRedoLogsResponse []string

// ListUpsertBatchesResponse represents the ListUpsertBatches response.
type ListUpsertBatchesResponse []int64
