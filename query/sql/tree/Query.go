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

import "fmt"

// Query is query
type Query struct {
	// Statement is IStatement
	IStatement
	// QueryBody is IQueryBody
	QueryBody IQueryBody
	// With is with
	With *With
	// OrderBy is orderby
	OrderBy *OrderBy
	// Limit is limit
	Limit string
}

// NewQuery creates Query
func NewQuery(location *NodeLocation, with *With, queryBody IQueryBody, order *OrderBy, limit string) *Query {
	if queryBody == nil {
		panic(fmt.Errorf("QueryBody is null at (line:%d, col:%d)", location.Line, location.CharPosition))
	}

	return &Query{
		IStatement: NewStatement(location),
		QueryBody:  queryBody,
		With:       with,
		OrderBy:    order,
		Limit:      limit,
	}
}

// Accept accepts visitor
func (n *Query) Accept(visitor AstVisitor, ctx interface{}) interface{} {
	return visitor.VisitQuery(n, ctx)
}
