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

// AstVisitor is a visitor
type AstVisitor interface {
	visitNode(node INode, ctx interface{}) interface{}

	process(node INode, ctx interface{}) interface{}

	// VisitAliasedRelation visits the node
	VisitAliasedRelation(aliasedRelation *AliasedRelation, ctx interface{}) interface{}

	// VisitAllColumns visits the node
	VisitAllColumns(allColumns *AllColumns, ctx interface{}) interface{}

	// VisitExpression visits the node
	VisitExpression(exp IExpression, ctx interface{}) interface{}

	// VisitFrameBound visits the node
	VisitFrameBound(frameBound *FrameBound, ctx interface{}) interface{}

	// VisitGroupBy visits the node
	VisitGroupBy(groupby *GroupBy, ctx interface{}) interface{}

	// VisitExpression visits the node
	VisitGroupingElement(groupElement IGroupingElement, ctx interface{}) interface{}

	// VisitIdentifier visits the node
	VisitIdentifier(identifier *Identifier, ctx interface{}) interface{}

	// VisitJoin visits the node
	VisitJoin(join *Join, ctx interface{}) interface{}

	// VisitLogicalBinaryExpression visits the node
	VisitLogicalBinaryExpression(logicalBinaryExpr *LogicalBinaryExpression, ctx interface{}) interface{}

	// VisitOrderBy visits the node
	VisitOrderBy(orderBy *OrderBy, ctx interface{}) interface{}

	// VisitQuery visits the node
	VisitQuery(query *Query, ctx interface{}) interface{}

	// VisitQueryBody visits the node
	VisitQueryBody(queryBody IQueryBody, ctx interface{}) interface{}

	// VisitQuerySpecification visits the node
	VisitQuerySpecification(querySpec *QuerySpecification, ctx interface{}) interface{}

	// VisitRelation visits the node
	VisitRelation(relation IRelation, ctx interface{}) interface{}

	// VisitSelect visits the node
	VisitSelect(sel *Select, ctx interface{}) interface{}

	// VisitSelectItem visits the node
	VisitSelectItem(selectItem ISelectItem, ctx interface{}) interface{}

	// VisitSimpleGroupBy visits the node
	VisitSimpleGroupBy(simpleGroupBy *SimpleGroupBy, ctx interface{}) interface{}

	// VisitSingleColumn visits the node
	VisitSingleColumn(singleColumn *SingleColumn, ctx interface{}) interface{}

	// VisitSortItem visits the node
	VisitSortItem(sortItem *SortItem, ctx interface{}) interface{}

	// VisitStatement visits the node
	VisitStatement(statement IStatement, ctx interface{}) interface{}

	// VisitTable visits the node
	VisitTable(table *Table, ctx interface{}) interface{}

	// VisitTableSubquery visits the node
	VisitTableSubquery(tableSubquery *TableSubquery, ctx interface{}) interface{}

	// VisitWindow visits the node
	VisitWindow(window *Window, ctx interface{}) interface{}

	// VisitWindowFrame visits the node
	VisitWindowFrame(windowFrame *WindowFrame, ctx interface{}) interface{}

	// VisitWith visits the node
	VisitWith(with *With, ctx interface{}) interface{}

	// VisitWithQuery visits the node
	VisitWithQuery(with *WithQuery, ctx interface{}) interface{}
}
