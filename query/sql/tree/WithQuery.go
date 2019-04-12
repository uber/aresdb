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

package tree

import (
	"fmt"

	"github.com/uber/aresdb/query/sql/util"
)

// WithQuery is WithQuery
type WithQuery struct {
	// Node is INode
	INode
	// Name is name
	Name *Identifier
	// Query is query
	Query *Query
	// ColumnAliases is ColumnAliases
	ColumnAliases []*Identifier
}

// NewWithQuery creates WithQuery
func NewWithQuery(location *NodeLocation, name *Identifier, query *Query, columnAliases []*Identifier) *WithQuery {
	errMsg := fmt.Sprintf("query is null at (line:%d, col:%d)", location.Line, location.CharPosition)
	util.RequireNonNull(query, errMsg)

	return &WithQuery{
		NewNode(location),
		name,
		query,
		columnAliases,
	}
}

// Accept accepts visitor
func (q *WithQuery) Accept(visitor AstVisitor, ctx interface{}) interface{} {
	return visitor.VisitWithQuery(q, ctx)
}
