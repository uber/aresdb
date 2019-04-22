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

// QuerySpecification is QuerySpecification
type QuerySpecification struct {
	// QueryBody is IQueryBody
	IQueryBody
	// Select is Select
	Select *Select
	// From is IRelation
	From IRelation
	// Where is IExpression
	Where IExpression
	// GroupBy is GroupBy
	GroupBy *GroupBy
	// Having is IExpression
	Having IExpression
	// OrderBy is OrderBy
	OrderBy *OrderBy
	// Limit is limit
	Limit string
}

// NewQuerySpecification creates QuerySpecification
func NewQuerySpecification(location *NodeLocation,
	sel *Select,
	from IRelation,
	where IExpression,
	groupBy *GroupBy,
	having IExpression,
	orderBy *OrderBy,
	limit string) *QuerySpecification {
	return &QuerySpecification{
		NewQueryBody(location),
		sel,
		from,
		where,
		groupBy,
		having,
		orderBy,
		limit,
	}
}

// Accept accepts visitor
func (e *QuerySpecification) Accept(visitor AstVisitor, ctx interface{}) interface{} {
	return visitor.VisitQuerySpecification(e, ctx)
}
