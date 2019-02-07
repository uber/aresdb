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
	metaCom "github.com/uber/aresdb/metastore/common"
)

// GetTableRequest represents GetTable request.
// swagger:parameters getTable
type GetTableRequest struct {
	//in: path
	TableName string `path:"table" json:"table"`
}

// AddTableRequest represents AddTable request.
// swagger:parameters addTable
type AddTableRequest struct {
	// in: body
	Body metaCom.Table `body:"",omitempty`
}

// AddColumnRequest represents AddColumn request.
// swagger:parameters addColumn
type AddColumnRequest struct {
	// in: path
	TableName string `path:"table" json:"table"`
	// in: body
	Body struct {
		// swagger:allOf
		metaCom.Column
		AddToArchivingSortOrder bool `json:"addToArchivingSortOrder,omitempty"`
	} `body:""`
}

// UpdateTableConfigRequest represents UpdateTableConfig request.
// swagger:parameters updateTableConfig
type UpdateTableConfigRequest struct {
	// in: path
	TableName string `path:"table" json:"table"`
	// in: body
	Body *metaCom.TableConfig `body:"",omitempty`
}

// DeleteTableRequest represents DeleteTable request.
// swagger:parameters deleteTable
type DeleteTableRequest struct {
	// in: path
	TableName string `path:"table" json:"table"`
}

// DeleteColumnRequest represents DeleteColumn request.
// swagger:parameters deleteColumn
type DeleteColumnRequest struct {
	// in: path
	TableName string `path:"table" json:"table"`
	// in: path
	ColumnName string `path:"column" json:"column"`
}

// ListEnumCasesRequest represents ListEnumCases request.
// swagger:parameters listEnumCases
type ListEnumCasesRequest struct {
	// in: path
	TableName string `path:"table" json:"table"`
	// in: path
	ColumnName string `path:"column" json:"column"`
}

// UpdateColumnRequest represents UpdateColumn request.
// Supported for updates:
//   preloadingDays
//   priority
// swagger:parameters updateColumn
type UpdateColumnRequest struct {
	// in: path
	TableName string `path:"table" json:"table"`
	// in: path
	ColumnName string `path:"column" json:"column"`
	// in: body
	Body metaCom.ColumnConfig `body:""`
}

// AddEnumCaseRequest represents AddEnumCase request.
// swagger:parameters addEnumCase
type AddEnumCaseRequest struct {
	// in: path
	TableName string `path:"table" json:"table"`
	// in: path
	ColumnName string `path:"column" json:"column"`
	// in: body
	Body struct {
		EnumCases []string `json:"enumCases"`
	} `body:""`
}
