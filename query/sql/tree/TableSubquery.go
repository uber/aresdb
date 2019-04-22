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

// TableSubquery is subquery
type TableSubquery struct {
	IQueryBody
	// Query is subquery
	Query *Query
}

// NewTableSubquery creates TableSubquery
func NewTableSubquery(location *NodeLocation, query *Query) *TableSubquery {
	return &TableSubquery{
		NewQueryBody(location),
		query,
	}
}

// Accept accepts visitor
func (q *TableSubquery) Accept(visitor AstVisitor, ctx interface{}) interface{} {
	return visitor.VisitTableSubquery(q, ctx)
}
