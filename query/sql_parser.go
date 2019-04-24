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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/antlr/antlr4/runtime/Go/antlr"
	"github.com/uber/aresdb/common"
	"github.com/uber/aresdb/query/sql/antlrgen"
	"github.com/uber/aresdb/query/sql/tree"
	"github.com/uber/aresdb/query/sql/util"
)

const (
	_aqlPrefix = "aql_"

	// supported query level
	maxlevelWith  = 1
	maxLevelQuery = 2

	// slice default size
	defaultSliceCap = 10

	// query types
	typeWithQuery = 1
	typeSubQuery  = 2
)

// ExprOrigin defines the expression origin
type ExprOrigin int

const (
	// ExprOriginWhere => the expression origin is from where clause
	ExprOriginWhere ExprOrigin = iota
	// ExprOriginJoinOn => the expression origin is from join on clause
	ExprOriginJoinOn
	// ExprOriginGroupBy => the expression origin is from groupingElement clause
	ExprOriginGroupBy
	// ExprOriginOthers => the expression origin is from other clauses case
	ExprOriginOthers
)

// SQL2AqlContext is the context of ASTVisitor
type SQL2AqlContext struct {
	/*
		Rules Of updating level, levelWith, levelQuery and mapXXX
		1. level: follow Treeprinter indent. Init value: 0
		2. levelWith: increase 1 if VisitWith is called. Init value: 0
		3. levelQuery: increase 1 if withQuery is called in VisitWith or VisitTableSubquery is called. Init value: 0
		4. mapXXX: create a new mapXXX[mapKey] if a new query is added (ie, VisitWithQuery or VisitTableSubquery). Init value: empty map table.
	*/
	// level is current tree level
	level int
	// levelWith is current with level
	levelWith int
	// levelQuery is current query level
	levelQuery int
	// MapQueryIdentifier is a mapping table. key=generateKey(...) value=arrayOfIdentifier.
	// Identifier can be namedQuery identifier or aliasedRelation identifier
	MapQueryIdentifier map[int]string
	// MapMeasures is a mapping table. key=generateKey(...) value=arrayOfMeasure
	MapMeasures map[int][]Measure
	// MapDimensions is a mapping table. key=generateKey(...) value=arrayOfDimension
	MapDimensions map[int][]Dimension
	// MapJoinTables is a mapping table. key=generateKey(...) value=arrayOfJoin
	MapJoinTables map[int][]Join
	// MapRowFilters is a mapping table. key=generateKey(...) value=arrayOfRowFilter
	MapRowFilters map[int][]string
	// MapOrderBy is a mapping table. key=generateKey(...) value=arrayOfSortField
	MapOrderBy map[int][]SortField
	// MapLimit is a mapping table. key=generateKey(...) value=arrayOfLimit
	MapLimit           map[int]int
	mapKey             int
	timeNow            int64
	timeFilter         TimeFilter
	timezone           string
	exprOrigin         ExprOrigin
	fromJSON           []byte
	groupByJSON        []byte
	orderByJSON        []byte
	queryIdentifierSet map[string]int
	exprCheck          bool
	disableMainGroupBy bool
	exprLogicalOp      tree.LogicalBinaryExpType
}

// ASTBuilder is a visitor
type ASTBuilder struct {
	// Logger is a logger from appConfig
	Logger common.Logger
	// IStream is input antlr char stream
	IStream *antlr.CommonTokenStream
	// ParameterPosition is position in sql
	ParameterPosition int
	// SQL2AqlContext is the context of construncting AQL
	SQL2AqlCtx *SQL2AqlContext
	aql        *AQLQuery

	// Flag that indicates whether aggregate function is seen
	aggFuncExists bool
}

func (v *ASTBuilder) defaultResult() interface{} {
	return nil
}

func (v *ASTBuilder) shouldVisitNextChild(node antlr.RuleNode, currentResult interface{}) bool {
	return true
}

func (v *ASTBuilder) aggregateResult(node antlr.ParseTree, aggregate interface{}, nextResult interface{}) interface{} {
	location := v.getLocation(node)
	if nextResult == nil {
		panic(fmt.Errorf("%v operation not yet implemented at (line:%d, col:%d)", node.GetText(), location.Line, location.CharPosition))
	}
	if aggregate == nil {
		return nextResult
	}
	panic(fmt.Errorf("%v operation not yet implemented at (line:%d, col:%d)", node.GetText(), location.Line, location.CharPosition))
}

func (v *ASTBuilder) getQualifiedName(ctx antlrgen.IQualifiedNameContext) *tree.QualifiedName {
	var result *tree.QualifiedName
	if ctxQualifiedName, ok := ctx.(*antlrgen.QualifiedNameContext); ok {
		ctxArr := ctxQualifiedName.AllIdentifier()
		parts := make([]string, len(ctxArr))
		for i, c := range ctxArr {
			if value, ok := v.Visit(c).(*tree.Identifier); ok {
				parts[i] = value.Value
			}
		}
		result = tree.NewQualifiedName(parts, nil)
	}
	return result
}

// VisitTerminal visits the node
func (v *ASTBuilder) VisitTerminal(node antlr.TerminalNode) interface{} { return nil }

// VisitErrorNode visits the node
func (v *ASTBuilder) VisitErrorNode(node antlr.ErrorNode) interface{} { return nil }

// Visit visits the node
func (v *ASTBuilder) Visit(tree antlr.ParseTree) interface{} {
	if tree == nil {
		return nil
	}
	return tree.Accept(v)
}

// VisitChildren visits the node
func (v *ASTBuilder) VisitChildren(node antlr.RuleNode) interface{} {
	result := v.defaultResult()
	if !reflect.ValueOf(node).IsNil() {
		n := node.GetChildCount()
		for i := 0; i < n && v.shouldVisitNextChild(node, result); i++ {
			var c antlr.ParseTree
			c = node.GetChild(i).(antlr.ParseTree)
			childResult := c.Accept(v)
			if v.SQL2AqlCtx.exprCheck == false {
				result = v.aggregateResult(c, result, childResult)
			}
		}
	}

	return result
}

func (v *ASTBuilder) visitIfPresent(ctx antlr.RuleContext, visitResult reflect.Type) interface{} {
	if ctx == nil {
		return reflect.Zero(visitResult).Interface()
	}

	return v.Visit(ctx)
}

func (v *ASTBuilder) visitList(ctxs []antlr.ParserRuleContext) interface{} {
	var res = make([]interface{}, len(ctxs))
	for i, ctx := range ctxs {
		res[i] = v.Visit(ctx)
	}
	return res
}

// ********************** Visit SQL grammar starts ********************

// VisitSingleStatement visits the node
func (v *ASTBuilder) VisitSingleStatement(ctx *antlrgen.SingleStatementContext) interface{} {
	return v.Visit(ctx.Statement()).(tree.INode)
}

// VisitSingleExpression visits the node
func (v *ASTBuilder) VisitSingleExpression(ctx *antlrgen.SingleExpressionContext) interface{} {
	result, _ := v.Visit(ctx.Expression()).(tree.INode)
	return result
}

// ********************** query expressions ********************

// VisitQuery visits the node
func (v *ASTBuilder) VisitQuery(ctx *antlrgen.QueryContext) interface{} {
	v.Logger.Debugf("VisitQuery: %s", ctx.GetText())

	location := v.getLocation(ctx)
	level, levelWith, levelQuery := v.getCtxLevels(v.SQL2AqlCtx)
	if levelQuery >= maxLevelQuery {
		panic(fmt.Errorf("only support %v level subquery at (line:%d, col:%d)",
			maxLevelQuery, location.Line, location.CharPosition))
	}

	// handle with
	v.setCtxLevels(v.SQL2AqlCtx, level, levelWith, levelQuery)
	classWith := reflect.TypeOf((*tree.With)(nil))
	with := v.visitIfPresent(ctx.With(), classWith).(*tree.With)

	// handle queryNoWith
	v.setCtxLevels(v.SQL2AqlCtx, level, levelWith, levelQuery)
	body, _ := v.Visit(ctx.QueryNoWith()).(*tree.Query)
	if body == nil {
		panic(fmt.Errorf("missing queryNoWith body at (line:%d, col:%d)", location.Line, location.CharPosition))
	}

	if levelQuery == 0 {
		if valid, err := v.isValidWithOrSubQuery(v.SQL2AqlCtx); !valid || err != nil {
			panic(fmt.Errorf("line:%d, col:%d isValidWithOrSubQuery: %v, reason :%v",
				location.Line, location.CharPosition, valid, err))
		}
	}

	query := tree.NewQuery(
		v.getLocation(ctx),
		with,
		body.QueryBody,
		body.OrderBy,
		body.Limit,
	)
	query.SetValue(fmt.Sprintf("Query: (%s)", v.getText(ctx.BaseParserRuleContext)))

	// reset SQL2AqlContext
	v.setCtxLevels(v.SQL2AqlCtx, level-1, levelWith, levelQuery)
	return query
}

// VisitWith visits the node
func (v *ASTBuilder) VisitWith(ctx *antlrgen.WithContext) interface{} {
	v.Logger.Debugf("VisitWith: %s", ctx.GetText())

	location := v.getLocation(ctx)
	level, levelWith, levelQuery := v.getCtxLevels(v.SQL2AqlCtx)
	mapKey := v.SQL2AqlCtx.mapKey
	levelWith++
	levelQuery++

	if levelWith > maxlevelWith {
		panic(fmt.Errorf("only support %v level with query at (line:%d, col:%d)",
			maxlevelWith, location.Line, location.CharPosition))
	}

	if ctx.RECURSIVE() != nil {
		panic(fmt.Errorf("RECURSIVE not yet supported at (line:%d, col:%d)",
			location.Line, location.CharPosition))
	}

	ctxArr := ctx.AllNamedQuery()
	arrWithQuery := make([]*tree.WithQuery, len(ctxArr))
	for i, c := range ctxArr {
		v.setCtxLevels(v.SQL2AqlCtx, level, levelWith, levelQuery)
		v.SQL2AqlCtx.mapKey = v.generateKey(levelQuery, typeWithQuery, i)
		v.SQL2AqlCtx.MapMeasures[v.SQL2AqlCtx.mapKey] = make([]Measure, 0, defaultSliceCap)
		v.SQL2AqlCtx.MapDimensions[v.SQL2AqlCtx.mapKey] = make([]Dimension, 0, defaultSliceCap)
		v.SQL2AqlCtx.MapJoinTables[v.SQL2AqlCtx.mapKey] = make([]Join, 0, defaultSliceCap)
		v.SQL2AqlCtx.MapRowFilters[v.SQL2AqlCtx.mapKey] = make([]string, 0, defaultSliceCap)
		v.SQL2AqlCtx.MapOrderBy[v.SQL2AqlCtx.mapKey] = make([]SortField, 0, defaultSliceCap)

		arrWithQuery[i], _ = v.VisitNamedQuery(c.(*antlrgen.NamedQueryContext)).(*tree.WithQuery)
	}
	with := tree.NewWith(v.getLocation(ctx), false, arrWithQuery)
	with.SetValue(fmt.Sprintf("With: (%s)", v.getText(ctx.BaseParserRuleContext)))

	v.setCtxLevels(v.SQL2AqlCtx, level-1, levelWith-1, levelQuery-1)
	v.SQL2AqlCtx.mapKey = mapKey
	return with
}

// VisitNamedQuery visits the node
func (v *ASTBuilder) VisitNamedQuery(ctx *antlrgen.NamedQueryContext) interface{} {
	v.Logger.Debugf("VisitNamedQuery: %s", ctx.GetText())

	location := v.getLocation(ctx)
	level, levelWith, levelQuery := v.getCtxLevels(v.SQL2AqlCtx)

	// handle name
	if ctx.GetName() == nil {
		panic(fmt.Errorf("missing with identifier at (line:%d, col:%d)", location.Line, location.CharPosition))
	}
	identifier := v.getText(ctx.GetName())
	v.SQL2AqlCtx.MapQueryIdentifier[v.SQL2AqlCtx.mapKey] = identifier
	v.addQIdentifier(v.SQL2AqlCtx, identifier, v.SQL2AqlCtx.mapKey)
	name, _ := v.Visit(ctx.GetName()).(*tree.Identifier)

	// handle columnAliases
	var columnAliases []*tree.Identifier
	if ctxColumnAliase, ok := ctx.ColumnAliases().(*antlrgen.ColumnAliasesContext); ok {
		ctxArr := ctxColumnAliase.AllIdentifier()
		columnAliases = make([]*tree.Identifier, len(ctxArr))
		for i, c := range ctxArr {
			v.SQL2AqlCtx.MapMeasures[v.SQL2AqlCtx.mapKey] =
				append(v.SQL2AqlCtx.MapMeasures[v.SQL2AqlCtx.mapKey],
					Measure{
						Alias: v.getText(c),
					})
			columnAliases[i], _ = v.Visit(c).(*tree.Identifier)
		}
	}

	// handle query
	v.setCtxLevels(v.SQL2AqlCtx, level, levelWith, levelQuery)
	ctxQuery, ok := ctx.Query().(*antlrgen.QueryContext)
	if !ok {
		panic(fmt.Errorf("missing with query body at (line:%d, col:%d)", location.Line, location.CharPosition))
	}
	query, _ := v.VisitQuery(ctxQuery).(*tree.Query)
	withQuery := tree.NewWithQuery(v.getLocation(ctx),
		name,
		query,
		columnAliases)
	withQuery.SetValue(fmt.Sprintf("WithQuery: (%s)", v.getText(ctx.BaseParserRuleContext)))

	v.setCtxLevels(v.SQL2AqlCtx, level-1, levelWith, levelQuery)
	return withQuery
}

// VisitQueryNoWith visits the node
func (v *ASTBuilder) VisitQueryNoWith(ctx *antlrgen.QueryNoWithContext) interface{} {
	v.Logger.Debugf("VisitQueryNoWith: %s", ctx.GetText())

	location := v.getLocation(ctx)
	level, levelWith, levelQuery := v.getCtxLevels(v.SQL2AqlCtx)

	// handle queryTerm
	v.setCtxLevels(v.SQL2AqlCtx, level, levelWith, levelQuery)
	term := v.Visit(ctx.QueryTerm())

	// handle ORDER BY
	v.setCtxLevels(v.SQL2AqlCtx, level, levelWith, levelQuery)
	v.SQL2AqlCtx.exprOrigin = ExprOriginOthers
	var orderBy = v.getOrderBy(ctx)

	var query *tree.Query
	if qSpec, ok := term.(*tree.QuerySpecification); ok {
		qSpecNew := tree.NewQuerySpecification(v.getLocation(ctx),
			qSpec.Select, qSpec.From, qSpec.Where, qSpec.GroupBy, qSpec.Having, orderBy, v.GetTextIfPresent(ctx.GetLimit()))
		qSpecNew.SetValue(fmt.Sprintf("QuerySpecification: (%s)", v.getText(ctx.QueryTerm())))

		query = tree.NewQuery(v.getLocation(ctx),
			nil,
			qSpecNew,
			nil,
			"")
	} else if qBody, ok := term.(*tree.QueryBody); ok {
		query = tree.NewQuery(v.getLocation(ctx),
			nil,
			qBody,
			orderBy,
			v.GetTextIfPresent(ctx.GetLimit()))
		if ctx.GetLimit() != nil {
			limit, err := strconv.Atoi(query.Limit)
			if levelQuery == 0 && err == nil {
				v.setCtxLevels(v.SQL2AqlCtx, level, levelWith, levelQuery)
				v.SQL2AqlCtx.MapLimit[v.SQL2AqlCtx.mapKey] = limit
			} else {
				panic(fmt.Errorf("limit on query level %d > 0 not supported at (line:%d, col:%d)",
					levelQuery, location.Line, location.CharPosition))
			}
		}

	} else {
		panic(fmt.Errorf("invalid query term: %v at (line:%d, col:%d)", term, location.Line, location.CharPosition))
	}
	query.SetValue(fmt.Sprintf("Query: (%s)", v.getText(ctx.BaseParserRuleContext)))

	v.setCtxLevels(v.SQL2AqlCtx, level-1, levelWith, levelQuery)
	return query
}

// VisitQuerySpecification visits the node
func (v *ASTBuilder) VisitQuerySpecification(ctx *antlrgen.QuerySpecificationContext) interface{} {
	v.Logger.Debugf("VisitQuerySpecification: %s", ctx.GetText())

	level, levelWith, levelQuery := v.getCtxLevels(v.SQL2AqlCtx)

	// handle from => join/table
	// first process from clause so that subquery/withQuery identifier can be found in expression
	v.SQL2AqlCtx.exprOrigin = ExprOriginJoinOn
	ctxArrRelation := ctx.AllRelation()
	arrRelations := make([]tree.IRelation, len(ctxArrRelation))
	for i, c := range ctxArrRelation {
		v.setCtxLevels(v.SQL2AqlCtx, level, levelWith, levelQuery)
		arrRelations[i], _ = v.Visit(c).(tree.IRelation)
	}

	var myFrom tree.IRelation
	if len(arrRelations) > 0 {
		relationL := arrRelations[0]
		// synthesize implicit join nodes
		for i := 1; i < len(arrRelations); i++ {
			relationR := arrRelations[i]
			relationL = tree.NewJoin(v.getLocation(ctx), tree.IMPLICIT, relationL, relationR, nil)
			relationL.SetValue(fmt.Sprintf("Join: (%s)", v.getText(ctxArrRelation[i])))
		}
		myFrom = relationL
	}

	// handle select => measure
	v.SQL2AqlCtx.exprOrigin = ExprOriginOthers
	ctxArrSelectItem := ctx.AllSelectItem()
	arrSelectItems := make([]tree.ISelectItem, len(ctxArrSelectItem))
	for i, c := range ctxArrSelectItem {
		v.setCtxLevels(v.SQL2AqlCtx, level, levelWith, levelQuery)
		arrSelectItems[i], _ = v.Visit(c).(tree.ISelectItem)

		if i < len(v.SQL2AqlCtx.MapMeasures[v.SQL2AqlCtx.mapKey]) {
			// handle subquery/withQuery with columnAliases,8
			// subquery/withQuery columnalias has higher priority, ignore subquery/withQuery selectSingle identifier
			switch item := arrSelectItems[i].(type) {
			case *tree.SingleColumn:
				v.SQL2AqlCtx.MapMeasures[v.SQL2AqlCtx.mapKey][i].Expr = util.GetSubstring(item.Expression.GetValue())
			case *tree.AllColumns:
				v.SQL2AqlCtx.MapMeasures[v.SQL2AqlCtx.mapKey][i].Expr = v.getText(c)
			}
		} else {
			// handle query or subquery/withQuery w/o columnAliases
			switch item := arrSelectItems[i].(type) {
			case *tree.SingleColumn:
				var alias string
				if item.Alias != nil {
					alias = util.GetSubstring(item.Alias.GetValue())
				}
				v.SQL2AqlCtx.MapMeasures[v.SQL2AqlCtx.mapKey] = append(v.SQL2AqlCtx.MapMeasures[v.SQL2AqlCtx.mapKey],
					Measure{
						Alias: alias,
						Expr:  util.GetSubstring(item.Expression.GetValue()),
					})
			case *tree.AllColumns:
				v.SQL2AqlCtx.MapMeasures[v.SQL2AqlCtx.mapKey] = append(v.SQL2AqlCtx.MapMeasures[v.SQL2AqlCtx.mapKey],
					Measure{
						Expr: v.getText(c),
					})
			}
		}
	}

	// handle where => rowfilter/timefilter
	v.setCtxLevels(v.SQL2AqlCtx, level, levelWith, levelQuery)
	v.SQL2AqlCtx.exprOrigin = ExprOriginWhere
	v.SQL2AqlCtx.exprCheck = true
	v.visitIfPresent(ctx.GetWhere(), reflect.TypeOf((*tree.Expression)(nil)))
	v.SQL2AqlCtx.exprCheck = false
	myWhere := v.visitIfPresent(ctx.GetWhere(), reflect.TypeOf((*tree.Expression)(nil))).(tree.IExpression)

	// handle group by => dimension
	if v.SQL2AqlCtx.disableMainGroupBy && levelQuery == 0 && ctx.GroupBy() != nil {
		// disable group by clause in manin query if with/subquery exists
		location := v.getLocation(ctx.GroupBy())
		panic(fmt.Errorf("group by is not allowed at (line:%d, col:%d) since with/subQuery already has group by",
			location.Line, location.CharPosition))
	}
	v.setCtxLevels(v.SQL2AqlCtx, level, levelWith, levelQuery)
	v.SQL2AqlCtx.exprOrigin = ExprOriginGroupBy
	myGroupBy := v.visitIfPresent(ctx.GroupBy(), reflect.TypeOf((*tree.GroupBy)(nil))).(*tree.GroupBy)
	if ctx.GroupBy() != nil && levelQuery > 0 {
		v.SQL2AqlCtx.disableMainGroupBy = true
	}

	// handle having => not support in AQL
	if ctx.GetHaving() != nil {
		location := v.getLocation(ctx.GetHaving())
		panic(fmt.Errorf("having not yet supported at (line:%d, col:%d)", location.Line, location.CharPosition))
	}
	v.setCtxLevels(v.SQL2AqlCtx, level, levelWith, levelQuery)
	v.SQL2AqlCtx.exprOrigin = ExprOriginOthers
	v.SQL2AqlCtx.exprCheck = true
	v.visitIfPresent(ctx.GetHaving(), reflect.TypeOf((*tree.Expression)(nil)))
	v.SQL2AqlCtx.exprCheck = false
	myHaving := v.visitIfPresent(ctx.GetHaving(), reflect.TypeOf((*tree.Expression)(nil))).(tree.IExpression)

	querySpec := tree.NewQuerySpecification(
		v.getLocation(ctx),
		tree.NewSelect(v.getLocation(ctx), v.isDistinct(ctx.SetQuantifier()), arrSelectItems),
		myFrom,
		myWhere,
		myGroupBy,
		myHaving,
		nil,
		"")
	querySpec.SetValue(fmt.Sprintf("QuerySpecification: (%s)", v.getText(ctx.BaseParserRuleContext)))

	v.setCtxLevels(v.SQL2AqlCtx, level-1, levelWith, levelQuery)
	return querySpec
}

// VisitSelectAll visits the node
func (v *ASTBuilder) VisitSelectAll(ctx *antlrgen.SelectAllContext) interface{} {
	v.Logger.Debugf("VisitSelectAll: %s", ctx.GetText())

	var allColumns *tree.AllColumns
	if ctx.QualifiedName() != nil {
		allColumns = tree.NewAllColumns(v.getLocation(ctx), v.getQualifiedName(ctx.QualifiedName()))
	} else {
		allColumns = tree.NewAllColumns(v.getLocation(ctx), nil)
	}

	allColumns.SetValue(fmt.Sprintf("AllColumns: (%s)", v.getText(ctx.BaseParserRuleContext)))
	return allColumns
}

// VisitSelectSingle visits the node
func (v *ASTBuilder) VisitSelectSingle(ctx *antlrgen.SelectSingleContext) interface{} {
	v.Logger.Debugf("VisitSelectSingle: %s", ctx.GetText())

	v.SQL2AqlCtx.exprCheck = true
	v.Visit(ctx.Expression())
	v.SQL2AqlCtx.exprCheck = false
	expr, _ := v.Visit(ctx.Expression()).(tree.IExpression)
	singleColumn := tree.NewSingleColumn(v.getLocation(ctx),
		expr,
		v.visitIfPresent(ctx.Identifier(), reflect.TypeOf((*tree.Identifier)(nil))).(*tree.Identifier))
	singleColumn.SetValue(fmt.Sprintf("SingleColumn: (%s)", v.getText(ctx.BaseParserRuleContext)))
	return singleColumn
}

// VisitGroupBy visits the node
func (v *ASTBuilder) VisitGroupBy(ctx *antlrgen.GroupByContext) interface{} {
	v.Logger.Debugf("VisitGroupBy: %s", ctx.GetText())

	level, levelWith, levelQuery := v.getCtxLevels(v.SQL2AqlCtx)

	ctxArr := ctx.AllGroupingElement()
	groupingElements := make([]tree.IGroupingElement, len(ctxArr))
	for i, c := range ctxArr {
		v.setCtxLevels(v.SQL2AqlCtx, level, levelWith, levelQuery)
		groupingElements[i], _ = v.Visit(c).(tree.IGroupingElement)
	}

	groupBy := tree.NewGroupBy(
		v.getLocation(ctx),
		v.isDistinct(ctx.SetQuantifier()),
		groupingElements)
	groupBy.SetValue(fmt.Sprintf("GroupBy: (%s)", v.getText(ctx.BaseParserRuleContext)))

	v.setCtxLevels(v.SQL2AqlCtx, level-1, levelWith, levelQuery)
	return groupBy
}

// VisitSingleGroupingSet visits the node
func (v *ASTBuilder) VisitSingleGroupingSet(ctx *antlrgen.SingleGroupingSetContext) interface{} {
	v.Logger.Debugf("VisitSingleGroupingSet: %s", ctx.GetText())

	level, levelWith, levelQuery := v.getCtxLevels(v.SQL2AqlCtx)
	v.setCtxLevels(v.SQL2AqlCtx, level, levelWith, levelQuery)

	ctxArr := ctx.GroupingExpressions().(*antlrgen.GroupingExpressionsContext).AllExpression()
	columns := make([]tree.IExpression, len(ctxArr))
	offset := len(v.SQL2AqlCtx.MapDimensions[v.SQL2AqlCtx.mapKey])
	for i, c := range ctxArr {
		v.SQL2AqlCtx.exprCheck = true
		v.Visit(c)
		v.SQL2AqlCtx.exprCheck = false
		columns[i], _ = v.Visit(c).(tree.IExpression)
		alias, expr := v.lookupSQLExpr(v.SQL2AqlCtx, v.SQL2AqlCtx.mapKey, util.GetSubstring(columns[i].GetValue()))
		if len(v.SQL2AqlCtx.MapDimensions[v.SQL2AqlCtx.mapKey])-offset == i {
			// timeBucket or numbericBucket is added into
			// v.SQL2AqlCtx.MapDimensions[v.SQL2AqlCtx.mapKey] via visitFunctionCall
			v.SQL2AqlCtx.MapDimensions[v.SQL2AqlCtx.mapKey] =
				append(v.SQL2AqlCtx.MapDimensions[v.SQL2AqlCtx.mapKey],
					Dimension{
						Alias: alias,
						Expr:  expr,
					})
		}
	}

	simpleGroupBy := tree.NewSimpleGroupBy(
		v.getLocation(ctx),
		columns)
	simpleGroupBy.SetValue(fmt.Sprintf("SimpleGroupBy: (%s)", v.getText(ctx.BaseParserRuleContext)))

	v.setCtxLevels(v.SQL2AqlCtx, level-1, levelWith, levelQuery)
	return simpleGroupBy
}

// VisitSortItem visits the node
func (v *ASTBuilder) VisitSortItem(ctx *antlrgen.SortItemContext) interface{} {
	v.Logger.Debugf("VisitSortItem: %s", ctx.GetText())

	level, levelWith, levelQuery := v.getCtxLevels(v.SQL2AqlCtx)

	if ctx.Expression() == nil {
		return nil
	}

	var ordering tree.OrderType
	if ctx.GetOrdering() != nil {
		if ctx.GetOrdering().GetText() == tree.OrderTypes[tree.ASC] {
			ordering = tree.ASC
		} else if ctx.GetOrdering().GetText() == tree.OrderTypes[tree.DESC] {
			ordering = tree.DESC
		}
	}

	v.setCtxLevels(v.SQL2AqlCtx, level, levelWith, levelQuery)
	v.SQL2AqlCtx.exprCheck = true
	v.Visit(ctx.Expression())
	v.SQL2AqlCtx.exprCheck = false
	expr := v.Visit(ctx.Expression()).(tree.IExpression)
	_, name := v.lookupSQLExpr(v.SQL2AqlCtx, v.SQL2AqlCtx.mapKey, util.GetSubstring(expr.GetValue()))
	v.SQL2AqlCtx.MapOrderBy[v.SQL2AqlCtx.mapKey] =
		append(v.SQL2AqlCtx.MapOrderBy[v.SQL2AqlCtx.mapKey],
			SortField{
				Name:  name,
				Order: tree.OrderTypes[ordering],
			})

	sortItem := tree.NewSortItem(v.getLocation(ctx), expr, ordering)
	sortItem.SetValue(fmt.Sprintf("SortItem: (%s)", v.getText(ctx.BaseParserRuleContext)))

	v.setCtxLevels(v.SQL2AqlCtx, level-1, levelWith, levelQuery)
	return sortItem
}

// ***************** boolean expressions ******************

// VisitExpression visits the node
func (v *ASTBuilder) VisitExpression(ctx *antlrgen.ExpressionContext) interface{} {
	v.Logger.Debugf("VisitExpression %s\n", v.getText(ctx))

	v.SQL2AqlCtx.exprLogicalOp = tree.NOOP
	return v.VisitChildren(ctx)
}

// VisitLogicalBinary visits the node
func (v *ASTBuilder) VisitLogicalBinary(ctx *antlrgen.LogicalBinaryContext) interface{} {
	location := v.getLocation(ctx)
	if v.SQL2AqlCtx.exprCheck {
		v.Logger.Debugf("VisitLogicalBinary check: %s", ctx.GetText())
		if ctx.GetOperator() == nil {
			panic(fmt.Errorf("missing logicalBinary operator, (line:%d, col:%d)",
				location.Line, location.CharPosition))
		}

		v.Visit(ctx.GetLeft())
		v.Visit(ctx.GetRight())
		return tree.NewExpression(v.getLocation(ctx))
	}

	v.Logger.Debugf("VisitLogicalBinary: %s", ctx.GetText())
	operator := v.getLogicalBinaryOperator(ctx.GetOperator().GetTokenType())
	if operator == tree.OR {
		if v.SQL2AqlCtx.exprOrigin == ExprOriginWhere {
			v.SQL2AqlCtx.MapRowFilters[v.SQL2AqlCtx.mapKey] =
				append(v.SQL2AqlCtx.MapRowFilters[v.SQL2AqlCtx.mapKey], v.getText(ctx.BaseParserRuleContext))
		} else if v.SQL2AqlCtx.exprOrigin == ExprOriginJoinOn {
			last := len(v.SQL2AqlCtx.MapJoinTables[v.SQL2AqlCtx.mapKey]) - 1
			v.SQL2AqlCtx.MapJoinTables[v.SQL2AqlCtx.mapKey][last].Conditions =
				append(v.SQL2AqlCtx.MapJoinTables[v.SQL2AqlCtx.mapKey][last].Conditions, v.getText(ctx.BaseParserRuleContext))
		}

		expr := tree.NewExpression(v.getLocation(ctx))
		expr.SetValue(fmt.Sprintf("LogicalBinaryExpression: (%s)", v.getText(ctx.BaseParserRuleContext)))
		return expr
	}

	left, _ := v.Visit(ctx.GetLeft()).(tree.IExpression)
	right, _ := v.Visit(ctx.GetRight()).(tree.IExpression)
	logicalBinaryExpr := tree.NewLogicalBinaryExpression(
		v.getLocation(ctx),
		operator,
		left,
		right)
	logicalBinaryExpr.SetValue(fmt.Sprintf("LogicalBinaryExpression: (%s)", v.getText(ctx.BaseParserRuleContext)))
	return logicalBinaryExpr
}

// VisitBooleanDefault visits the node
func (v *ASTBuilder) VisitBooleanDefault(ctx *antlrgen.BooleanDefaultContext) interface{} {
	if v.SQL2AqlCtx.exprCheck {
		v.Logger.Debugf("VisitBooleanDefault check: %s", ctx.GetText())
		return v.VisitChildren(ctx)
	}

	v.Logger.Debugf("VisitBooleanDefault: %s", ctx.GetText())
	if v.SQL2AqlCtx.exprOrigin == ExprOriginWhere && !strings.HasPrefix(v.getText(ctx), _aqlPrefix) {
		v.SQL2AqlCtx.MapRowFilters[v.SQL2AqlCtx.mapKey] =
			append(v.SQL2AqlCtx.MapRowFilters[v.SQL2AqlCtx.mapKey], v.getText(ctx.BaseParserRuleContext))
	} else if v.SQL2AqlCtx.exprOrigin == ExprOriginJoinOn {
		last := len(v.SQL2AqlCtx.MapJoinTables[v.SQL2AqlCtx.mapKey]) - 1
		v.SQL2AqlCtx.MapJoinTables[v.SQL2AqlCtx.mapKey][last].Conditions =
			append(v.SQL2AqlCtx.MapJoinTables[v.SQL2AqlCtx.mapKey][last].Conditions, v.getText(ctx.BaseParserRuleContext))
	}

	expr := tree.NewExpression(v.getLocation(ctx))
	expr.SetValue(fmt.Sprintf("BooleanDefault: (%s)", v.getText(ctx.BaseParserRuleContext)))

	return expr
}

// VisitLogicalNot visits the node
func (v *ASTBuilder) VisitLogicalNot(ctx *antlrgen.LogicalNotContext) interface{} {
	v.Logger.Debugf("VisitLogicalNot check: %s", ctx.GetText())

	if v.SQL2AqlCtx.exprCheck {
		return v.VisitChildren(ctx)
	}

	v.Logger.Debugf("VisitLogicalNot: %s", ctx.GetText())
	if v.SQL2AqlCtx.exprOrigin == ExprOriginWhere {
		v.SQL2AqlCtx.MapRowFilters[v.SQL2AqlCtx.mapKey] =
			append(v.SQL2AqlCtx.MapRowFilters[v.SQL2AqlCtx.mapKey], v.getText(ctx.BaseParserRuleContext))
	} else if v.SQL2AqlCtx.exprOrigin == ExprOriginJoinOn {
		last := len(v.SQL2AqlCtx.MapJoinTables[v.SQL2AqlCtx.mapKey]) - 1
		v.SQL2AqlCtx.MapJoinTables[v.SQL2AqlCtx.mapKey][last].Conditions =
			append(v.SQL2AqlCtx.MapJoinTables[v.SQL2AqlCtx.mapKey][last].Conditions, v.getText(ctx.BaseParserRuleContext))
	}

	expr := tree.NewExpression(v.getLocation(ctx))
	expr.SetValue(fmt.Sprintf("LogicalNot: (%s)", v.getText(ctx.BaseParserRuleContext)))

	return expr
}

// *************** from clause *****************

// VisitJoinRelation visits the node
func (v *ASTBuilder) VisitJoinRelation(ctx *antlrgen.JoinRelationContext) interface{} {
	v.Logger.Debugf("VisitJoinRelation: %s", ctx.GetText())

	location := v.getLocation(ctx)
	level, levelWith, levelQuery := v.getCtxLevels(v.SQL2AqlCtx)

	v.setCtxLevels(v.SQL2AqlCtx, level, levelWith, levelQuery)
	left, _ := v.Visit(ctx.GetLeft()).(tree.IRelation)

	var right tree.IRelation
	if ctx.CROSS() != nil {
		v.setCtxLevels(v.SQL2AqlCtx, level, levelWith, levelQuery)
		right, _ = v.VisitSampledRelation(ctx.GetRight().(*antlrgen.SampledRelationContext)).(tree.IRelation)
		join := tree.NewJoin(v.getLocation(ctx), tree.CROSS, left, right, nil)
		join.SetValue(fmt.Sprintf("Join: (%s)", v.getText(ctx.BaseParserRuleContext)))
		return join
	}

	var criteria tree.IJoinCriteria
	if ctx.NATURAL() != nil {
		if levelQuery != 0 {
			panic(fmt.Errorf("natural join not supported at subquery/withQuery at (line:%d, col:%d)",
				location.Line, location.CharPosition))
		}
		v.setCtxLevels(v.SQL2AqlCtx, level, levelWith, levelQuery)
		right, _ = v.VisitSampledRelation(ctx.GetRight().(*antlrgen.SampledRelationContext)).(tree.IRelation)
		criteria = tree.NewNaturalJoin()
	} else {
		v.setCtxLevels(v.SQL2AqlCtx, level, levelWith, levelQuery)
		right, _ = v.Visit(ctx.GetRightRelation()).(tree.IRelation)

		ctxJoinCriteria, ok := ctx.JoinCriteria().(*antlrgen.JoinCriteriaContext)
		if !ok {
			panic(fmt.Errorf("missing join criteria at (line:%d, col:%d)", location.Line, location.CharPosition))
		}
		if ctxJoinCriteria.ON() != nil {
			v.setCtxLevels(v.SQL2AqlCtx, level, levelWith, levelQuery)
			v.SQL2AqlCtx.exprCheck = true
			v.Visit(ctx.JoinCriteria().(*antlrgen.JoinCriteriaContext).
				BooleanExpression())
			v.SQL2AqlCtx.exprCheck = false
			joinOn, _ := v.Visit(ctx.JoinCriteria().(*antlrgen.JoinCriteriaContext).
				BooleanExpression()).(tree.IExpression)
			criteria = tree.NewJoinOn(joinOn)
		} else if ctxJoinCriteria.USING() != nil {
			ctxArr := ctx.JoinCriteria().(*antlrgen.JoinCriteriaContext).AllIdentifier()
			expressions := make([]*tree.Identifier, len(ctxArr))
			for i, c := range ctxArr {
				expressions[i], _ = v.Visit(c).(*tree.Identifier)
			}
			criteria = tree.NewJoinUsing(expressions)
		}
	}

	joinType := v.getJoinType(ctx)
	if joinType != tree.LEFT {
		panic(fmt.Errorf("join type %v not supported yet at (line:%d, col:%d)",
			tree.JoinTypes[joinType], location.Line, location.CharPosition))
	}

	join := tree.NewJoin(v.getLocation(ctx), joinType, left, right, criteria)
	join.SetValue(fmt.Sprintf("Join: (%s)", v.getText(ctx.BaseParserRuleContext)))
	return join
}

// VisitSampledRelation visits the node
func (v *ASTBuilder) VisitSampledRelation(ctx *antlrgen.SampledRelationContext) interface{} {
	v.Logger.Debugf("VisitSampledRelation: %s", ctx.GetText())

	location := v.getLocation(ctx)
	level, levelWith, levelQuery := v.getCtxLevels(v.SQL2AqlCtx)

	v.setCtxLevels(v.SQL2AqlCtx, level, levelWith, levelQuery)
	child, _ := v.VisitAliasedRelation(ctx.AliasedRelation().(*antlrgen.AliasedRelationContext)).(tree.IRelation)
	if ctx.TABLESAMPLE() != nil {
		panic(fmt.Errorf("TABLESAMPLE not implemented at (line:%d, col:%d)", location.Line, location.CharPosition))
	}
	if child != nil {
		child.SetValue(fmt.Sprintf("SampledRelation: (%s)", v.getText(ctx.BaseParserRuleContext)))
	}

	v.setCtxLevels(v.SQL2AqlCtx, level-1, levelWith, levelQuery)
	return child
}

// VisitAliasedRelation visits the node
func (v *ASTBuilder) VisitAliasedRelation(ctx *antlrgen.AliasedRelationContext) interface{} {
	v.Logger.Debugf("VisitAliasedRelation: %s", ctx.GetText())

	level, levelWith, levelQuery := v.getCtxLevels(v.SQL2AqlCtx)
	mapKey := v.SQL2AqlCtx.mapKey

	// handle identifier
	if ctx.Identifier() != nil {
		v.SQL2AqlCtx.MapJoinTables[v.SQL2AqlCtx.mapKey] = append(v.SQL2AqlCtx.MapJoinTables[v.SQL2AqlCtx.mapKey],
			Join{
				Alias: v.getText(ctx.Identifier()),
			})
	} else {
		v.SQL2AqlCtx.MapJoinTables[v.SQL2AqlCtx.mapKey] = append(v.SQL2AqlCtx.MapJoinTables[v.SQL2AqlCtx.mapKey],
			Join{})
	}

	// handle relationPrimary
	v.setCtxLevels(v.SQL2AqlCtx, level, levelWith, levelQuery)
	child, _ := v.Visit(ctx.RelationPrimary()).(tree.IRelation)
	if ctx.Identifier() == nil {
		child.SetValue(fmt.Sprintf("Relation: (%s)", v.getText(ctx.BaseParserRuleContext)))
		v.setCtxLevels(v.SQL2AqlCtx, level-1, levelWith, levelQuery)
		v.SQL2AqlCtx.mapKey = mapKey
		return child
	}

	// handle columnAliases
	var aliasedRelation *tree.AliasedRelation
	identifier, _ := v.Visit(ctx.Identifier()).(*tree.Identifier)
	if ctxColumnAliases, ok := ctx.ColumnAliases().(*antlrgen.ColumnAliasesContext); ok {
		ctxArr := ctxColumnAliases.AllIdentifier()
		columnAliases := make([]*tree.Identifier, len(ctxArr))
		last := len(v.SQL2AqlCtx.MapJoinTables[v.SQL2AqlCtx.mapKey]) - 1
		subqueryKey := v.generateKey(levelQuery+1, typeSubQuery, last)
		for i, c := range ctxArr {
			v.setCtxLevels(v.SQL2AqlCtx, level, levelWith, levelQuery)
			if i < len(v.SQL2AqlCtx.MapMeasures[v.SQL2AqlCtx.mapKey]) {
				v.SQL2AqlCtx.MapMeasures[subqueryKey][i].Alias = v.getText(c)
			} else {
				v.SQL2AqlCtx.MapMeasures[subqueryKey] =
					append(v.SQL2AqlCtx.MapMeasures[v.SQL2AqlCtx.mapKey],
						Measure{
							Alias: v.getText(c),
						})
			}
			columnAliases[i], _ = v.Visit(c).(*tree.Identifier)
		}

		aliasedRelation = tree.NewAliasedRelation(
			v.getLocation(ctx),
			child,
			identifier,
			columnAliases)
	} else {
		aliasedRelation = tree.NewAliasedRelation(
			v.getLocation(ctx),
			child,
			identifier,
			nil)
	}
	aliasedRelation.SetValue(fmt.Sprintf("AliasedRelation: (%s)", v.getText(ctx.BaseParserRuleContext)))

	v.setCtxLevels(v.SQL2AqlCtx, level-1, levelWith, levelQuery)
	v.SQL2AqlCtx.mapKey = mapKey
	return aliasedRelation
}

// VisitTableName visits the node
func (v *ASTBuilder) VisitTableName(ctx *antlrgen.TableNameContext) interface{} {
	v.Logger.Debugf("VisitTableName: %s", ctx.GetText())

	last := len(v.SQL2AqlCtx.MapJoinTables[v.SQL2AqlCtx.mapKey]) - 1
	// check if the table name is a withQ identifier
	name := v.getText(ctx.BaseParserRuleContext)
	qLevel, _, _ := v.getInfoByKey(v.SQL2AqlCtx.mapKey)
	if qLevel == 0 && v.isWithQueryIdentifier(v.SQL2AqlCtx, name) {
		v.SQL2AqlCtx.MapJoinTables[v.SQL2AqlCtx.mapKey][last].Alias = name
	} else {
		v.SQL2AqlCtx.MapJoinTables[v.SQL2AqlCtx.mapKey][last].Table = name
	}

	table := tree.NewTable(v.getLocation(ctx), v.getQualifiedName(ctx.QualifiedName()))
	table.SetValue(fmt.Sprintf("Table: (%s)", v.getText(ctx.BaseParserRuleContext)))

	return table
}

// VisitSubqueryRelation visits the node
func (v *ASTBuilder) VisitSubqueryRelation(ctx *antlrgen.SubqueryRelationContext) interface{} {
	v.Logger.Debugf("VisitSubqueryRelation: %s", ctx.GetText())

	level, levelWith, levelQuery := v.getCtxLevels(v.SQL2AqlCtx)
	// mapKey is the mapKey of parent query
	mapKey := v.SQL2AqlCtx.mapKey
	last := len(v.SQL2AqlCtx.MapJoinTables[v.SQL2AqlCtx.mapKey]) - 1

	v.setCtxLevels(v.SQL2AqlCtx, level-1, levelWith, levelQuery+1)
	// the index in v.SQL2AqlCtx.mapKey is the index of the aliasedRelation in parent from clause
	v.SQL2AqlCtx.mapKey = v.generateKey(levelQuery+1, typeSubQuery, last)
	v.SQL2AqlCtx.MapQueryIdentifier[v.SQL2AqlCtx.mapKey] = v.SQL2AqlCtx.MapJoinTables[mapKey][last].Alias
	v.addQIdentifier(v.SQL2AqlCtx, v.SQL2AqlCtx.MapJoinTables[mapKey][last].Alias, v.SQL2AqlCtx.mapKey)
	v.SQL2AqlCtx.MapMeasures[v.SQL2AqlCtx.mapKey] = make([]Measure, 0, defaultSliceCap)
	v.SQL2AqlCtx.MapDimensions[v.SQL2AqlCtx.mapKey] = make([]Dimension, 0, defaultSliceCap)
	v.SQL2AqlCtx.MapJoinTables[v.SQL2AqlCtx.mapKey] = make([]Join, 0, defaultSliceCap)
	v.SQL2AqlCtx.MapRowFilters[v.SQL2AqlCtx.mapKey] = make([]string, 0, defaultSliceCap)
	v.SQL2AqlCtx.MapOrderBy[v.SQL2AqlCtx.mapKey] = make([]SortField, 0, defaultSliceCap)

	query, _ := v.VisitQuery(ctx.Query().(*antlrgen.QueryContext)).(*tree.Query)
	tableSubquery := tree.NewTableSubquery(v.getLocation(ctx), query)
	tableSubquery.SetValue(fmt.Sprintf("TableSubquery: (%s)", v.getText(ctx.BaseParserRuleContext)))

	return tableSubquery
}

// ********************* primary expressions **********************

// VisitFunctionCall visits the node
func (v *ASTBuilder) VisitFunctionCall(ctx *antlrgen.FunctionCallContext) interface{} {
	v.Logger.Debugf("VisitFunctionCall: %s", ctx.GetText())

	// 1. timebucket and numbericbucket are only from groupBy clause
	// 2. timefilter is only from where clause
	location := v.getLocation(ctx)
	name := v.getText(ctx.QualifiedName())
	udfDef, ok := util.UdfTable[name]
	if ok {
		if ctx.SetQuantifier() != nil || ctx.Filter() != nil || len(ctx.AllSortItem()) != 0 || ctx.AllExpression() == nil {
			panic(fmt.Errorf("quantifier/filter/over/sort not supported in %s function at (line:%d, col:%d)",
				name, location.Line, location.CharPosition))
		}
		if len(ctx.AllExpression()) != udfDef.ArgsNum {
			panic(fmt.Errorf("%s should have %d parameters at (line:%d, col:%d)",
				name, udfDef.ArgsNum, location.Line, location.CharPosition))
		}
		if v.hasORInPath(ctx.BaseParserRuleContext) == tree.OR {
			panic(fmt.Errorf("function %s can not appear in OR logicalBinaryExpression at (line:%d, col:%d)",
				name, location.Line, location.CharPosition))
		}
		if (udfDef.Type == util.Timebucket || udfDef.Type == util.Numericbucket) && v.SQL2AqlCtx.exprOrigin != ExprOriginGroupBy {
			panic(fmt.Errorf("function %s at (line:%d, col:%d) can only appear in group by clause",
				name, location.Line, location.CharPosition))
		}
		if udfDef.Type == util.Timefilter && v.SQL2AqlCtx.exprOrigin != ExprOriginWhere {
			panic(fmt.Errorf("function %s at (line:%d, col:%d) can only appear in where clause",
				name, location.Line, location.CharPosition))
		}

		switch udfDef.Type {
		case util.Timefilter:
			v.setTimefilter(ctx.AllExpression())
		case util.TimeNow:
			v.setTimeNow(ctx.AllExpression())
		case util.Timebucket:
			v.SQL2AqlCtx.MapDimensions[v.SQL2AqlCtx.mapKey] = append(
				v.SQL2AqlCtx.MapDimensions[v.SQL2AqlCtx.mapKey],
				Dimension{
					Expr:           util.TrimQuote(v.getText(ctx.Expression(0))),
					TimeBucketizer: util.TrimQuote(udfDef.Definition),
					TimeUnit:       util.TrimQuote(v.getText(ctx.Expression(1))),
				})
			if len(v.SQL2AqlCtx.timezone) == 0 {
				v.SQL2AqlCtx.timezone = util.TrimQuote(v.getText(ctx.Expression(2)))
			} else if v.SQL2AqlCtx.timezone != util.TrimQuote(v.getText(ctx.Expression(2))) {
				panic(fmt.Errorf("different timebucket timezone %s at (line:%d, col:%d)",
					v.getText(ctx.Expression(2)), location.Line, location.CharPosition))
			}
		case util.Numericbucket:
			v.setNumericBucketizer(ctx.AllExpression(), udfDef.Definition)
		}
	} else if strings.HasPrefix(name, _aqlPrefix) {
		panic(fmt.Errorf("function %s at (line:%d, col:%d) is not registered in AQL udf",
			name, location.Line, location.CharPosition))
	}

	if exist := util.AggregateFunctions[name]; exist {
		v.aggFuncExists = true
	}

	return tree.NewExpression(v.getLocation(ctx))
}

// VisitUnquotedIdentifier visits the node
func (v *ASTBuilder) VisitUnquotedIdentifier(ctx *antlrgen.UnquotedIdentifierContext) interface{} {
	v.Logger.Debugf("VisitUnquotedIdentifier: %s", ctx.GetText())

	identifier := tree.NewIdentifier(v.getLocation(ctx), v.getText(ctx.BaseParserRuleContext), false)
	identifier.SetValue(fmt.Sprintf("Identifier: (%s)", v.getText(ctx.BaseParserRuleContext)))

	return identifier
}

// VisitQuotedIdentifier visits the node
func (v *ASTBuilder) VisitQuotedIdentifier(ctx *antlrgen.QuotedIdentifierContext) interface{} {
	v.Logger.Debugf("VisitQuotedIdentifier: %s", ctx.GetText())

	token := v.getText(ctx.BaseParserRuleContext)
	identifier := strings.Replace(token[1:len(token)-1], "\"\"", "\"", -1)
	quotedIdentifier := tree.NewIdentifier(v.getLocation(ctx), identifier, true)
	quotedIdentifier.SetValue(fmt.Sprintf("QuotedIdentifier: (%s)", v.getText(ctx.BaseParserRuleContext)))

	return quotedIdentifier
}

// VisitDereference visits the node
func (v *ASTBuilder) VisitDereference(ctx *antlrgen.DereferenceContext) interface{} {
	v.Logger.Debugf("VisitDereference: %s", ctx.GetText())

	location := v.getLocation(ctx.GetBase())
	if v.SQL2AqlCtx.mapKey == 0 {
		// reject the expression if subquery/withQuery identifier is used in level 0 query
		primaryExpr := v.getText(ctx.GetBase())
		key := v.isSubOrWithQueryIdentifier(v.SQL2AqlCtx, primaryExpr)
		if key > 0 {
			panic(fmt.Errorf("subquery/withQuery identifier in expression not supported yet. (line:%d, col:%d)",
				location.Line, location.CharPosition))
		}
	}

	return v.VisitChildren(ctx)
}

// ***************** Reserved *****************

// VisitStatementDefault visits the node
func (v *ASTBuilder) VisitStatementDefault(ctx *antlrgen.StatementDefaultContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitUse visits the node
func (v *ASTBuilder) VisitUse(ctx *antlrgen.UseContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitCreateSchema visits the node
func (v *ASTBuilder) VisitCreateSchema(ctx *antlrgen.CreateSchemaContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitDropSchema visits the node
func (v *ASTBuilder) VisitDropSchema(ctx *antlrgen.DropSchemaContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitRenameSchema visits the node
func (v *ASTBuilder) VisitRenameSchema(ctx *antlrgen.RenameSchemaContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitCreateTableAsSelect visits the node
func (v *ASTBuilder) VisitCreateTableAsSelect(ctx *antlrgen.CreateTableAsSelectContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitCreateTable visits the node
func (v *ASTBuilder) VisitCreateTable(ctx *antlrgen.CreateTableContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitDropTable visits the node
func (v *ASTBuilder) VisitDropTable(ctx *antlrgen.DropTableContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitInsertInto visits the node
func (v *ASTBuilder) VisitInsertInto(ctx *antlrgen.InsertIntoContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitDelete visits the node
func (v *ASTBuilder) VisitDelete(ctx *antlrgen.DeleteContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitRenameTable visits the node
func (v *ASTBuilder) VisitRenameTable(ctx *antlrgen.RenameTableContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitRenameColumn visits the node
func (v *ASTBuilder) VisitRenameColumn(ctx *antlrgen.RenameColumnContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitDropColumn visits the node
func (v *ASTBuilder) VisitDropColumn(ctx *antlrgen.DropColumnContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitAddColumn visits the node
func (v *ASTBuilder) VisitAddColumn(ctx *antlrgen.AddColumnContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitCreateView visits the node
func (v *ASTBuilder) VisitCreateView(ctx *antlrgen.CreateViewContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitDropView visits the node
func (v *ASTBuilder) VisitDropView(ctx *antlrgen.DropViewContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitCall visits the node
func (v *ASTBuilder) VisitCall(ctx *antlrgen.CallContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitGrant visits the node
func (v *ASTBuilder) VisitGrant(ctx *antlrgen.GrantContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitRevoke visits the node
func (v *ASTBuilder) VisitRevoke(ctx *antlrgen.RevokeContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitShowGrants visits the node
func (v *ASTBuilder) VisitShowGrants(ctx *antlrgen.ShowGrantsContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitExplain visits the node
func (v *ASTBuilder) VisitExplain(ctx *antlrgen.ExplainContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitShowCreateTable visits the node
func (v *ASTBuilder) VisitShowCreateTable(ctx *antlrgen.ShowCreateTableContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitShowCreateView visits the node
func (v *ASTBuilder) VisitShowCreateView(ctx *antlrgen.ShowCreateViewContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitShowTables visits the node
func (v *ASTBuilder) VisitShowTables(ctx *antlrgen.ShowTablesContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitShowSchemas visits the node
func (v *ASTBuilder) VisitShowSchemas(ctx *antlrgen.ShowSchemasContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitShowCatalogs visits the node
func (v *ASTBuilder) VisitShowCatalogs(ctx *antlrgen.ShowCatalogsContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitShowColumns visits the node
func (v *ASTBuilder) VisitShowColumns(ctx *antlrgen.ShowColumnsContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitShowStats visits the node
func (v *ASTBuilder) VisitShowStats(ctx *antlrgen.ShowStatsContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitShowStatsForQuery visits the node
func (v *ASTBuilder) VisitShowStatsForQuery(ctx *antlrgen.ShowStatsForQueryContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitShowFunctions visits the node
func (v *ASTBuilder) VisitShowFunctions(ctx *antlrgen.ShowFunctionsContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitShowSession visits the node
func (v *ASTBuilder) VisitShowSession(ctx *antlrgen.ShowSessionContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitSetSession visits the node
func (v *ASTBuilder) VisitSetSession(ctx *antlrgen.SetSessionContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitResetSession visits the node
func (v *ASTBuilder) VisitResetSession(ctx *antlrgen.ResetSessionContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitShowPartitions visits the node
func (v *ASTBuilder) VisitShowPartitions(ctx *antlrgen.ShowPartitionsContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitPrepare visits the node
func (v *ASTBuilder) VisitPrepare(ctx *antlrgen.PrepareContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitDeallocate visits the node
func (v *ASTBuilder) VisitDeallocate(ctx *antlrgen.DeallocateContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitExecute visits the node
func (v *ASTBuilder) VisitExecute(ctx *antlrgen.ExecuteContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitDescribeInput visits the node
func (v *ASTBuilder) VisitDescribeInput(ctx *antlrgen.DescribeInputContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitDescribeOutput visits the node
func (v *ASTBuilder) VisitDescribeOutput(ctx *antlrgen.DescribeOutputContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitTableElement visits the node
func (v *ASTBuilder) VisitTableElement(ctx *antlrgen.TableElementContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitColumnDefinition visits the node
func (v *ASTBuilder) VisitColumnDefinition(ctx *antlrgen.ColumnDefinitionContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitLikeClause visits the node
func (v *ASTBuilder) VisitLikeClause(ctx *antlrgen.LikeClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitProperties visits the node
func (v *ASTBuilder) VisitProperties(ctx *antlrgen.PropertiesContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitProperty visits the node
func (v *ASTBuilder) VisitProperty(ctx *antlrgen.PropertyContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitQueryTermDefault visits the node
func (v *ASTBuilder) VisitQueryTermDefault(ctx *antlrgen.QueryTermDefaultContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitSetOperation visits the node
func (v *ASTBuilder) VisitSetOperation(ctx *antlrgen.SetOperationContext) interface{} {
	location := v.getLocation(ctx)
	panic(fmt.Errorf("setOperation at (line:%d, col:%d) not supported yet", location.Line, location.CharPosition))
}

// VisitQueryPrimaryDefault visits the node
func (v *ASTBuilder) VisitQueryPrimaryDefault(ctx *antlrgen.QueryPrimaryDefaultContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitTable visits the node
func (v *ASTBuilder) VisitTable(ctx *antlrgen.TableContext) interface{} {
	location := v.getLocation(ctx)
	panic(fmt.Errorf("table at (line:%d, col:%d) not supported yet", location.Line, location.CharPosition))
}

// VisitInlineTable visits the node
func (v *ASTBuilder) VisitInlineTable(ctx *antlrgen.InlineTableContext) interface{} {
	location := v.getLocation(ctx)
	panic(fmt.Errorf("inlineTable at (line:%d, col:%d) not supported yet", location.Line, location.CharPosition))
}

// VisitSubquery visits the node
func (v *ASTBuilder) VisitSubquery(ctx *antlrgen.SubqueryContext) interface{} {
	location := v.getLocation(ctx)
	panic(fmt.Errorf("subquery at (line:%d, col:%d) not supported yet", location.Line, location.CharPosition))
}

// VisitRollup visits the node
func (v *ASTBuilder) VisitRollup(ctx *antlrgen.RollupContext) interface{} {
	location := v.getLocation(ctx)
	panic(fmt.Errorf("rollup at (line:%d, col:%d) not supported yet", location.Line, location.CharPosition))
}

// VisitCube visits the node
func (v *ASTBuilder) VisitCube(ctx *antlrgen.CubeContext) interface{} {
	location := v.getLocation(ctx)
	panic(fmt.Errorf("cube at (line:%d, col:%d) not supported yet", location.Line, location.CharPosition))
}

// VisitMultipleGroupingSets visits the node
func (v *ASTBuilder) VisitMultipleGroupingSets(ctx *antlrgen.MultipleGroupingSetsContext) interface{} {
	location := v.getLocation(ctx)
	panic(fmt.Errorf("multipleGroupingSets at (line:%d, col:%d) not supported yet", location.Line, location.CharPosition))
}

// VisitGroupingExpressions visits the node
func (v *ASTBuilder) VisitGroupingExpressions(ctx *antlrgen.GroupingExpressionsContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitGroupingSet visits the node
func (v *ASTBuilder) VisitGroupingSet(ctx *antlrgen.GroupingSetContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitSetQuantifier visits the node
func (v *ASTBuilder) VisitSetQuantifier(ctx *antlrgen.SetQuantifierContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitRelationDefault visits the node
func (v *ASTBuilder) VisitRelationDefault(ctx *antlrgen.RelationDefaultContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitJoinType visits the node
func (v *ASTBuilder) VisitJoinType(ctx *antlrgen.JoinTypeContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitJoinCriteria visits the node
func (v *ASTBuilder) VisitJoinCriteria(ctx *antlrgen.JoinCriteriaContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitSampleType visits the node
func (v *ASTBuilder) VisitSampleType(ctx *antlrgen.SampleTypeContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitColumnAliases visits the node
func (v *ASTBuilder) VisitColumnAliases(ctx *antlrgen.ColumnAliasesContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitUnnest visits the node
func (v *ASTBuilder) VisitUnnest(ctx *antlrgen.UnnestContext) interface{} {
	location := v.getLocation(ctx)
	panic(fmt.Errorf("un-nest at (line:%d, col:%d) not supported yet", location.Line, location.CharPosition))
}

// VisitLateral visits the node
func (v *ASTBuilder) VisitLateral(ctx *antlrgen.LateralContext) interface{} {
	location := v.getLocation(ctx)
	panic(fmt.Errorf("lateral at (line:%d, col:%d) not supported yet", location.Line, location.CharPosition))
}

// VisitParenthesizedRelation visits the node
func (v *ASTBuilder) VisitParenthesizedRelation(ctx *antlrgen.ParenthesizedRelationContext) interface{} {
	location := v.getLocation(ctx)
	panic(fmt.Errorf("parenthesizedRelation at (line:%d, col:%d) not supported yet", location.Line, location.CharPosition))
}

// VisitQuantifiedComparison visits the node
func (v *ASTBuilder) VisitQuantifiedComparison(ctx *antlrgen.QuantifiedComparisonContext) interface{} {
	location := v.getLocation(ctx)
	panic(fmt.Errorf("quantifiedComparison at (line:%d, col:%d) not supported yet", location.Line, location.CharPosition))
}

// VisitBetween visits the node
func (v *ASTBuilder) VisitBetween(ctx *antlrgen.BetweenContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitInList visits the node
func (v *ASTBuilder) VisitInList(ctx *antlrgen.InListContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitInSubquery visits the node
func (v *ASTBuilder) VisitInSubquery(ctx *antlrgen.InSubqueryContext) interface{} {
	location := v.getLocation(ctx)
	panic(fmt.Errorf("inSubquery at (line:%d, col:%d) not supported yet", location.Line, location.CharPosition))
}

// VisitLike visits the node
func (v *ASTBuilder) VisitLike(ctx *antlrgen.LikeContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitNullPredicate visits the node
func (v *ASTBuilder) VisitNullPredicate(ctx *antlrgen.NullPredicateContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitDistinctFrom visits the node
func (v *ASTBuilder) VisitDistinctFrom(ctx *antlrgen.DistinctFromContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitValueExpressionDefault visits the node
func (v *ASTBuilder) VisitValueExpressionDefault(ctx *antlrgen.ValueExpressionDefaultContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitConcatenation visits the node
func (v *ASTBuilder) VisitConcatenation(ctx *antlrgen.ConcatenationContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitArithmeticBinary visits the node
func (v *ASTBuilder) VisitArithmeticBinary(ctx *antlrgen.ArithmeticBinaryContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitArithmeticUnary visits the node
func (v *ASTBuilder) VisitArithmeticUnary(ctx *antlrgen.ArithmeticUnaryContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitAtTimeZone visits the node
func (v *ASTBuilder) VisitAtTimeZone(ctx *antlrgen.AtTimeZoneContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitTypeConstructor visits the node
func (v *ASTBuilder) VisitTypeConstructor(ctx *antlrgen.TypeConstructorContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitSpecialDateTimeFunction visits the node
func (v *ASTBuilder) VisitSpecialDateTimeFunction(ctx *antlrgen.SpecialDateTimeFunctionContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitSubstring visits the node
func (v *ASTBuilder) VisitSubstring(ctx *antlrgen.SubstringContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitCast visits the node
func (v *ASTBuilder) VisitCast(ctx *antlrgen.CastContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitLambda visits the node
func (v *ASTBuilder) VisitLambda(ctx *antlrgen.LambdaContext) interface{} {
	location := v.getLocation(ctx)
	panic(fmt.Errorf("lambda at (line:%d, col:%d) not supported yet", location.Line, location.CharPosition))
}

// VisitPredicated visits the node
func (v *ASTBuilder) VisitPredicated(ctx *antlrgen.PredicatedContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitComparison visits the node
func (v *ASTBuilder) VisitComparison(ctx *antlrgen.ComparisonContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitParenthesizedExpression visits the node
func (v *ASTBuilder) VisitParenthesizedExpression(ctx *antlrgen.ParenthesizedExpressionContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitParameter visits the node
func (v *ASTBuilder) VisitParameter(ctx *antlrgen.ParameterContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitNormalize visits the node
func (v *ASTBuilder) VisitNormalize(ctx *antlrgen.NormalizeContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitIntervalLiteral visits the node
func (v *ASTBuilder) VisitIntervalLiteral(ctx *antlrgen.IntervalLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitNumericLiteral visits the node
func (v *ASTBuilder) VisitNumericLiteral(ctx *antlrgen.NumericLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitBooleanLiteral visits the node
func (v *ASTBuilder) VisitBooleanLiteral(ctx *antlrgen.BooleanLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitSimpleCase visits the node
func (v *ASTBuilder) VisitSimpleCase(ctx *antlrgen.SimpleCaseContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitColumnReference \visits the node
func (v *ASTBuilder) VisitColumnReference(ctx *antlrgen.ColumnReferenceContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitNullLiteral visits the node
func (v *ASTBuilder) VisitNullLiteral(ctx *antlrgen.NullLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitRowConstructor visits the node
func (v *ASTBuilder) VisitRowConstructor(ctx *antlrgen.RowConstructorContext) interface{} {
	location := v.getLocation(ctx)
	panic(fmt.Errorf("rowConstructor at (line:%d, col:%d) not supported yet", location.Line, location.CharPosition))
}

// VisitSubscript visits the node
func (v *ASTBuilder) VisitSubscript(ctx *antlrgen.SubscriptContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitSubqueryExpression visits the node
func (v *ASTBuilder) VisitSubqueryExpression(ctx *antlrgen.SubqueryExpressionContext) interface{} {
	location := v.getLocation(ctx)
	panic(fmt.Errorf("subqueryExpression at (line:%d, col:%d) not supported yet", location.Line, location.CharPosition))
}

// VisitBinaryLiteral visits the node
func (v *ASTBuilder) VisitBinaryLiteral(ctx *antlrgen.BinaryLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitCurrentUser visits the node
func (v *ASTBuilder) VisitCurrentUser(ctx *antlrgen.CurrentUserContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitExtract visits the node
func (v *ASTBuilder) VisitExtract(ctx *antlrgen.ExtractContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitStringLiteral visits the node
func (v *ASTBuilder) VisitStringLiteral(ctx *antlrgen.StringLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitArrayConstructor visits the node
func (v *ASTBuilder) VisitArrayConstructor(ctx *antlrgen.ArrayConstructorContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitExists visits the node
func (v *ASTBuilder) VisitExists(ctx *antlrgen.ExistsContext) interface{} {
	location := v.getLocation(ctx)
	panic(fmt.Errorf("exists at (line:%d, col:%d) not supported yet", location.Line, location.CharPosition))
}

// VisitPosition visits the node
func (v *ASTBuilder) VisitPosition(ctx *antlrgen.PositionContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitSearchedCase visits the node
func (v *ASTBuilder) VisitSearchedCase(ctx *antlrgen.SearchedCaseContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitGroupingOperation visits the node
func (v *ASTBuilder) VisitGroupingOperation(ctx *antlrgen.GroupingOperationContext) interface{} {
	location := v.getLocation(ctx)
	panic(fmt.Errorf("groupingOperation at (line:%d, col:%d) not supported yet", location.Line, location.CharPosition))
}

// VisitBasicStringLiteral visits the node
func (v *ASTBuilder) VisitBasicStringLiteral(ctx *antlrgen.BasicStringLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitUnicodeStringLiteral visits the node
func (v *ASTBuilder) VisitUnicodeStringLiteral(ctx *antlrgen.UnicodeStringLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitTimeZoneInterval visits the node
func (v *ASTBuilder) VisitTimeZoneInterval(ctx *antlrgen.TimeZoneIntervalContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitTimeZoneString visits the node
func (v *ASTBuilder) VisitTimeZoneString(ctx *antlrgen.TimeZoneStringContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitComparisonOperator visits the node
func (v *ASTBuilder) VisitComparisonOperator(ctx *antlrgen.ComparisonOperatorContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitComparisonQuantifier visits the node
func (v *ASTBuilder) VisitComparisonQuantifier(ctx *antlrgen.ComparisonQuantifierContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitBooleanValue visits the node
func (v *ASTBuilder) VisitBooleanValue(ctx *antlrgen.BooleanValueContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitInterval visits the node
func (v *ASTBuilder) VisitInterval(ctx *antlrgen.IntervalContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitIntervalField visits the node
func (v *ASTBuilder) VisitIntervalField(ctx *antlrgen.IntervalFieldContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitNormalForm visits the node
func (v *ASTBuilder) VisitNormalForm(ctx *antlrgen.NormalFormContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitSqltype visits the node
func (v *ASTBuilder) VisitSqltype(ctx *antlrgen.SqltypeContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitTypeParameter visits the node
func (v *ASTBuilder) VisitTypeParameter(ctx *antlrgen.TypeParameterContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitBaseType visits the node
func (v *ASTBuilder) VisitBaseType(ctx *antlrgen.BaseTypeContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitWhenClause visits the node
func (v *ASTBuilder) VisitWhenClause(ctx *antlrgen.WhenClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitFilter visits the node
func (v *ASTBuilder) VisitFilter(ctx *antlrgen.FilterContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitExplainFormat visits the node
func (v *ASTBuilder) VisitExplainFormat(ctx *antlrgen.ExplainFormatContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitExplainType visits the node
func (v *ASTBuilder) VisitExplainType(ctx *antlrgen.ExplainTypeContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitPositionalArgument visits the node
func (v *ASTBuilder) VisitPositionalArgument(ctx *antlrgen.PositionalArgumentContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitNamedArgument visits the node
func (v *ASTBuilder) VisitNamedArgument(ctx *antlrgen.NamedArgumentContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitPrivilege visits the node
func (v *ASTBuilder) VisitPrivilege(ctx *antlrgen.PrivilegeContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitQualifiedName visits the node
func (v *ASTBuilder) VisitQualifiedName(ctx *antlrgen.QualifiedNameContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitBackQuotedIdentifier visits the node
func (v *ASTBuilder) VisitBackQuotedIdentifier(ctx *antlrgen.BackQuotedIdentifierContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitDigitIdentifier visits the node
func (v *ASTBuilder) VisitDigitIdentifier(ctx *antlrgen.DigitIdentifierContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitDecimalLiteral visits the node
func (v *ASTBuilder) VisitDecimalLiteral(ctx *antlrgen.DecimalLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitDoubleLiteral visits the node
func (v *ASTBuilder) VisitDoubleLiteral(ctx *antlrgen.DoubleLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitIntegerLiteral visits the node
func (v *ASTBuilder) VisitIntegerLiteral(ctx *antlrgen.IntegerLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

// VisitNonReserved visits the node
func (v *ASTBuilder) VisitNonReserved(ctx *antlrgen.NonReservedContext) interface{} {
	return v.VisitChildren(ctx)
}

// ***************** helpers *****************
type orderByContext interface {
	ORDER() antlr.TerminalNode
	AllSortItem() []antlrgen.ISortItemContext
}

func (v *ASTBuilder) getOrderBy(ctx orderByContext) *tree.OrderBy {
	var orderBy *tree.OrderBy
	if ctx.ORDER() != nil {
		ctxArr := ctx.AllSortItem()
		arrSortItem := make([]*tree.SortItem, len(ctxArr))
		for i, c := range ctxArr {
			arrSortItem[i] = v.Visit(c.(*antlrgen.SortItemContext)).(*tree.SortItem)
		}
		orderBy = tree.NewOrderBy(v.getLocation(ctx.ORDER()), arrSortItem)
		orderBy.SetValue(fmt.Sprintf("OrderBy: (%s)", ctx.ORDER().GetText()))
	}
	return orderBy
}

func (v *ASTBuilder) getLocation(input interface{}) *tree.NodeLocation {
	var token antlr.Token
	switch i := input.(type) {
	case antlr.ParserRuleContext:
		util.RequireNonNull(input, "antlrgenRuleContext is null")
		token = i.GetStart()
	case antlr.TerminalNode:
		util.RequireNonNull(input, "terminalNode is null")
		token = i.GetSymbol()
	case antlr.Token:
		token = i
	default:
		token = nil
	}
	util.RequireNonNull(token, "token is null")
	return &tree.NodeLocation{
		Line:         token.GetLine(),
		CharPosition: token.GetColumn(),
	}
}

// getText extracts string from original input sql
func (v *ASTBuilder) getText(ctx antlr.ParserRuleContext) string {
	return v.IStream.GetTextFromTokens(ctx.GetStart(), ctx.GetStop())
}

func (v *ASTBuilder) setTimefilter(ctx []antlrgen.IExpressionContext) {
	column := util.TrimQuote(v.getText(ctx[0]))
	from := util.TrimQuote(v.getText(ctx[1]))
	to := util.TrimQuote(v.getText(ctx[2]))
	timezone := util.TrimQuote(v.getText(ctx[3]))

	if v.SQL2AqlCtx.timeFilter != (TimeFilter{}) {
		if v.SQL2AqlCtx.timeFilter.Column != column {
			location := v.getLocation(ctx[0])
			panic(fmt.Errorf("different timefilter on %s at (line:%d, col:%d)",
				column, location.Line, location.CharPosition))
		}
		if v.SQL2AqlCtx.timeFilter.From != from {
			location := v.getLocation(ctx[1])
			panic(fmt.Errorf("different timefilter from %s at (line:%d, col:%d)",
				from, location.Line, location.CharPosition))
		}
		if v.SQL2AqlCtx.timeFilter.To != to {
			location := v.getLocation(ctx[2])
			panic(fmt.Errorf("different timefilter to %s at (line:%d, col:%d)",
				to, location.Line, location.CharPosition))
		}
		if len(v.SQL2AqlCtx.timezone) != 0 && v.SQL2AqlCtx.timezone != timezone {
			location := v.getLocation(ctx[2])
			panic(fmt.Errorf("different timefilter timezone %s at (line:%d, col:%d)",
				timezone, location.Line, location.CharPosition))
		}
		return
	}
	v.SQL2AqlCtx.timeFilter = TimeFilter{
		Column: column,
		From:   from,
		To:     to,
	}
	v.SQL2AqlCtx.timezone = timezone
}

func (v *ASTBuilder) setTimeNow(ctx []antlrgen.IExpressionContext) {
	column := util.TrimQuote(v.getText(ctx[0]))
	now := util.TrimQuote(v.getText(ctx[1]))

	var tsNow int64
	var err error
	if tsNow, err = strconv.ParseInt(now, 110, 64); err != nil {
		location := v.getLocation(ctx[1])
		panic(fmt.Errorf("invalid timestamp now on %s at (line:%d, col:%d)",
			column, location.Line, location.CharPosition))
	}
	if v.SQL2AqlCtx.timeNow < tsNow {
		v.SQL2AqlCtx.timeNow = tsNow
	}

	return
}

func (v *ASTBuilder) setNumericBucketizer(ctx []antlrgen.IExpressionContext, def string) {
	switch def {
	case util.NumericBucketTypeBucketWidth:
		location := v.getLocation(ctx[1])
		value, err := strconv.ParseFloat(util.TrimQuote(v.getText(ctx[1])), 64)
		if err != nil {
			panic(fmt.Errorf("expect float value at (line:%d, col:%d)",
				location.Line, location.CharPosition))
		}
		v.SQL2AqlCtx.MapDimensions[v.SQL2AqlCtx.mapKey] = append(
			v.SQL2AqlCtx.MapDimensions[v.SQL2AqlCtx.mapKey],
			Dimension{
				Expr:              util.TrimQuote(v.getText(ctx[0])),
				NumericBucketizer: NumericBucketizerDef{BucketWidth: value},
			})
	case util.NumericBucketTypeLogBase:
		location := v.getLocation(ctx[1])
		value, err := strconv.ParseFloat(util.TrimQuote(v.getText(ctx[1])), 64)
		if err != nil {
			panic(fmt.Errorf("expect float value at (line:%d, col:%d)",
				location.Line, location.CharPosition))
		}
		v.SQL2AqlCtx.MapDimensions[v.SQL2AqlCtx.mapKey] = append(
			v.SQL2AqlCtx.MapDimensions[v.SQL2AqlCtx.mapKey],
			Dimension{
				Expr:              util.TrimQuote(v.getText(ctx[0])),
				NumericBucketizer: NumericBucketizerDef{LogBase: value},
			})
	case util.NumericBucketTypeManualPartitions:
		location := v.getLocation(ctx[1])
		values := strings.Split(util.TrimQuote(v.getText(ctx[1])), ",")
		partitions := make([]float64, len(values), len(values))
		for i, value := range values {
			partition, err := strconv.ParseFloat(value, 64)
			if err != nil {
				panic(fmt.Errorf("expect float value at (line:%d, col:%d)",
					location.Line, location.CharPosition))
			}
			partitions[i] = partition
		}
		v.SQL2AqlCtx.MapDimensions[v.SQL2AqlCtx.mapKey] = append(
			v.SQL2AqlCtx.MapDimensions[v.SQL2AqlCtx.mapKey],
			Dimension{
				Expr:              util.TrimQuote(v.getText(ctx[0])),
				NumericBucketizer: NumericBucketizerDef{ManualPartitions: partitions},
			})
	}
}

// mergeWithOrSubQueries merge all subquery/withQuery's information into v.aql
func (v *ASTBuilder) mergeWithOrSubQueries() {
	for i, join := range v.SQL2AqlCtx.MapJoinTables[0] {
		withOrSubQKey, _ := v.SQL2AqlCtx.queryIdentifierSet[join.Alias]
		if i == 0 {
			v.mergeWithOrSubQuery(withOrSubQKey, false)
		} else {
			v.mergeWithOrSubQuery(withOrSubQKey, true)
		}
	}

	v.aql.Measures = v.SQL2AqlCtx.MapMeasures[0]
	v.aql.Dimensions = v.SQL2AqlCtx.MapDimensions[0]
	if len(v.SQL2AqlCtx.MapOrderBy[0]) != 0 {
		v.aql.Sorts = v.SQL2AqlCtx.MapOrderBy[0]
	}
	v.aql.Filters = v.SQL2AqlCtx.MapRowFilters[0]
	v.aql.TimeFilter = v.SQL2AqlCtx.timeFilter
	v.aql.Timezone = v.SQL2AqlCtx.timezone
	v.aql.Limit = v.SQL2AqlCtx.MapLimit[0]
}

// mergeWithOrSubQuery merge one subquery/withQuery information into v.aql
func (v *ASTBuilder) mergeWithOrSubQuery(key int, ignoreJoin bool) {
	if !ignoreJoin {
		v.aql.Table = v.SQL2AqlCtx.MapJoinTables[key][0].Table
		v.aql.Joins = v.SQL2AqlCtx.MapJoinTables[key][1:]
	}

	for i, measure := range v.SQL2AqlCtx.MapMeasures[key] {
		if index := v.isMeasureInMain(key, i); index > -1 {
			// case1: this measure of subquery/withQuery is not a supportingMeasure
			v.SQL2AqlCtx.MapMeasures[0][index].Filters = v.SQL2AqlCtx.MapRowFilters[key]
			if len(v.SQL2AqlCtx.MapDimensions[0]) == 0 {
				v.SQL2AqlCtx.MapDimensions[0] = v.SQL2AqlCtx.MapDimensions[key]
			}
		} else {
			// case2: this measure of subquery/withQuery is a supportingMeasure
			measure.Filters = v.SQL2AqlCtx.MapRowFilters[key]
			v.aql.SupportingMeasures = append(v.aql.SupportingMeasures, measure)
		}
		if len(v.SQL2AqlCtx.MapDimensions[0]) == 0 && len(v.SQL2AqlCtx.MapDimensions[key]) != 0 {
			v.SQL2AqlCtx.MapDimensions[0] = v.SQL2AqlCtx.MapDimensions[key]
		}
		if len(v.aql.Sorts) == 0 && len(v.SQL2AqlCtx.MapOrderBy[key]) != 0 {
			v.aql.Sorts = v.SQL2AqlCtx.MapOrderBy[key]
		}
	}
}

// isMeasureInMain check a measure of subquery/withQuery is also a measure of level 0 query
// return the index of the measure in level 0 query; otherwise return -1
func (v *ASTBuilder) isMeasureInMain(key, index int) int {
	if len(v.SQL2AqlCtx.MapMeasures[key][index].Alias) != 0 {
		for i, measure := range v.SQL2AqlCtx.MapMeasures[0] {
			if measure.Expr == v.SQL2AqlCtx.MapMeasures[key][index].Alias {
				v.SQL2AqlCtx.MapMeasures[0][i].Expr = v.SQL2AqlCtx.MapMeasures[key][index].Expr
				v.SQL2AqlCtx.MapMeasures[0][i].Alias = v.SQL2AqlCtx.MapMeasures[key][index].Alias
				return i
			}
		}
	} else {
		for i, measure := range v.SQL2AqlCtx.MapMeasures[0] {
			if measure.Expr == v.SQL2AqlCtx.MapMeasures[key][index].Expr {
				return i
			}
		}
	}

	return -1
}

// GetAQL construct AQLQuery via read through SQL2AqlCtx
func (v *ASTBuilder) GetAQL() *AQLQuery {
	var (
		table string
		joins []Join
	)

	if len(v.SQL2AqlCtx.MapQueryIdentifier) == 0 {
		// there is no subquery/withQuery in sql
		table = v.SQL2AqlCtx.MapJoinTables[0][0].Table

		if len(v.SQL2AqlCtx.MapJoinTables[0]) > 1 {
			joins = v.SQL2AqlCtx.MapJoinTables[0][1:]
		}

		v.aql = &AQLQuery{
			Table:      table,
			Joins:      joins,
			Measures:   v.SQL2AqlCtx.MapMeasures[0],
			Dimensions: v.SQL2AqlCtx.MapDimensions[0],
			Filters:    v.SQL2AqlCtx.MapRowFilters[0],
			TimeFilter: v.SQL2AqlCtx.timeFilter,
			Timezone:   v.SQL2AqlCtx.timezone,
			Now:        v.SQL2AqlCtx.timeNow,
			Limit:      v.SQL2AqlCtx.MapLimit[0],
			Sorts:      v.SQL2AqlCtx.MapOrderBy[0],
		}
	} else {
		v.aql = &AQLQuery{
			SupportingMeasures:   make([]Measure, 0, defaultSliceCap),
			SupportingDimensions: make([]Dimension, 0, defaultSliceCap),
		}
		v.mergeWithOrSubQueries()
	}

	return v.aql
}

// GetTextIfPresent visits the node
func (v *ASTBuilder) GetTextIfPresent(token antlr.Token) string {
	var text string
	if token != nil {
		text = token.GetText()
	}
	return text
}

// isDistinct check if DISTINCT quantifier is set
func (v *ASTBuilder) isDistinct(setQuantifier antlrgen.ISetQuantifierContext) bool {
	if setQuantifier != nil && setQuantifier.(*antlrgen.SetQuantifierContext).DISTINCT() != nil {
		return true
	}

	return false
}

// getLogicalBinaryOperator returns an input token's logicalBinaryExpression operator type
func (v *ASTBuilder) getLogicalBinaryOperator(token int) tree.LogicalBinaryExpType {
	switch token {
	case antlrgen.SqlBaseLexerAND:
		return tree.AND
	case antlrgen.SqlBaseLexerOR:
		return tree.OR
	default:
		panic(fmt.Errorf("Unsupported operator: %v", token))
	}
}

func (v *ASTBuilder) getJoinType(ctx *antlrgen.JoinRelationContext) tree.JoinType {
	var joinType tree.JoinType
	if ctx.JoinType() == nil {
		joinType = tree.INNER
	} else if ctx.JoinType().(*antlrgen.JoinTypeContext).LEFT() != nil {
		joinType = tree.LEFT
	} else if ctx.JoinType().(*antlrgen.JoinTypeContext).RIGHT() != nil {
		joinType = tree.RIGHT
	} else if ctx.JoinType().(*antlrgen.JoinTypeContext).FULL() != nil {
		joinType = tree.FULL
	} else {
		joinType = tree.INNER
	}
	return joinType
}

func (v *ASTBuilder) getCtxLevels(s2aCtx *SQL2AqlContext) (level, levelWith, levelQuery int) {
	level = s2aCtx.level + 1
	levelWith = s2aCtx.levelWith
	levelQuery = s2aCtx.levelQuery
	return
}

func (v *ASTBuilder) setCtxLevels(s2aCtx *SQL2AqlContext, level, levelWith, levelQuery int) {
	s2aCtx.level = level
	s2aCtx.levelWith = levelWith
	s2aCtx.levelQuery = levelQuery
}

// generateKey constructs mapKey based on levelQuery and index of the query at the current levelQuery
func (v *ASTBuilder) generateKey(qLevel, qType, index int) int {
	return qLevel*1000 + qType*100 + index
}

func (v *ASTBuilder) getInfoByKey(mapKey int) (qLevel, qType, index int) {
	qLevel = mapKey / 1000
	qType = (mapKey % 1000) / 100
	index = (mapKey % 1000) % 100
	return
}

func (v *ASTBuilder) isValidWithOrSubQuery(s2aCtx *SQL2AqlContext) (bool, error) {
	var isTrue, exist bool
	var err error
	var mapKey int

	// check if from clause in main query(ie. qLevel = 0) mix table with subquery/withQuery
	if isTrue, err = v.isQueryFromMixed(s2aCtx); isTrue {
		return false, err
	}

	// check if all subquery/withQuery from clauses are same
	for i, value := range s2aCtx.MapJoinTables[0] {
		// exit if no subquery/withQuery
		if len(value.Table) > 0 {
			break
		}

		if len(value.Alias) == 0 { // subquery has no identifier
			mapKey = v.generateKey(1, typeSubQuery, i)
		} else {
			mapKey, exist = s2aCtx.queryIdentifierSet[value.Alias]
			if !exist {
				err = fmt.Errorf("cannot find withQuery identifier: %s", value.Alias)
				return false, err
			}
		}

		isTrue, err = v.isSameFromTables(s2aCtx, mapKey)
		if !isTrue {
			return false, err
		}

		isTrue, err = v.isSameGroupBy(s2aCtx, mapKey)
		if !isTrue {
			return false, err
		}

		isTrue, err = v.isSameOrderBy(s2aCtx, mapKey)
		if !isTrue {
			return false, err
		}
	}

	return true, nil
}

// AQL requires that the first level query is either from tables or from subqueries/withQuery
func (v *ASTBuilder) isQueryFromMixed(s2aCtx *SQL2AqlContext) (bool, error) {
	var flagTable bool
	var err error
	if len(s2aCtx.MapJoinTables[0][0].Table) != 0 {
		flagTable = true
	}
	for i, join := range s2aCtx.MapJoinTables[0] {
		if flagTable && len(join.Table) == 0 {
			err = fmt.Errorf("# %d should be a tablename in from clause", i)
			return true, err
		} else if !flagTable && len(join.Table) != 0 {
			err = fmt.Errorf("# %d should be a subquery/withQuery in from clause", i)
			return true, err
		}
	}
	return false, nil
}

// AQL requires that all subqueries or withQuery from clauses are same
func (v *ASTBuilder) isSameFromTables(s2aCtx *SQL2AqlContext, mapKey int) (bool, error) {
	qLevel, _, index := v.getInfoByKey(mapKey)
	// generte from clause json bytes based on the first subquery/withQuery
	if s2aCtx.fromJSON == nil {
		withKeyMin := v.generateKey(qLevel, typeWithQuery, 0)
		subQKeyMin := v.generateKey(qLevel, typeSubQuery, 0)
		var err error
		if s2aCtx.MapJoinTables[withKeyMin] != nil {
			s2aCtx.fromJSON, err = json.Marshal(s2aCtx.MapJoinTables[withKeyMin])
		} else {
			s2aCtx.fromJSON, err = json.Marshal(s2aCtx.MapJoinTables[subQKeyMin])
		}

		if err != nil {
			return false, errors.New("unable to encode from clause of the first subquery/withQuery")
		}

		if index == 0 {
			return true, nil
		}
	}

	// compare current from clause with ctx.fromJSON
	joins, err := json.Marshal(s2aCtx.MapJoinTables[mapKey])
	if err != nil {
		return false, errors.New("unable to encode from clause of this subquery/withQuery")
	}

	return bytes.Equal(s2aCtx.fromJSON, joins), nil
}

// AQL requires that all subqueries or withQuery groupBy clauses are sameo
func (v *ASTBuilder) isSameGroupBy(s2aCtx *SQL2AqlContext, mapKey int) (bool, error) {
	qLevel, _, index := v.getInfoByKey(mapKey)
	// generte group by clause json bytes based on the first subquery/withQuery
	if s2aCtx.groupByJSON == nil {
		withKeyMin := v.generateKey(qLevel, typeWithQuery, 0)
		subQKeyMin := v.generateKey(qLevel, typeSubQuery, 0)
		var err error
		if s2aCtx.MapJoinTables[withKeyMin] != nil {
			s2aCtx.groupByJSON, err = json.Marshal(s2aCtx.MapDimensions[withKeyMin])
		} else {
			s2aCtx.groupByJSON, err = json.Marshal(s2aCtx.MapDimensions[subQKeyMin])
		}
		if err != nil {
			return false, errors.New("unable to encode group by clause of the first subquery/withQuery")
		}

		if index == 0 {
			return true, nil
		}
	}

	// compare current groupBy clause with ctx.groupByJSON
	groupBy, err := json.Marshal(s2aCtx.MapDimensions[mapKey])
	if err != nil {
		return false, errors.New("unable to encode group by clause of this subquery/withQuery")
	}

	return bytes.EqualFold(s2aCtx.groupByJSON, groupBy), nil
}

// AQL requires that all subqueries or withQuery orderBy clauses are same
func (v *ASTBuilder) isSameOrderBy(s2aCtx *SQL2AqlContext, mapKey int) (bool, error) {
	qLevel, _, index := v.getInfoByKey(mapKey)
	// generte group by clause json bytes based on the first subquery/withQuery
	if s2aCtx.orderByJSON == nil {
		withKeyMin := v.generateKey(qLevel, typeWithQuery, 0)
		subQKeyMin := v.generateKey(qLevel, typeSubQuery, 0)
		var err error
		if s2aCtx.MapOrderBy[withKeyMin] != nil {
			s2aCtx.orderByJSON, err = json.Marshal(s2aCtx.MapOrderBy[withKeyMin])
		} else {
			s2aCtx.orderByJSON, err = json.Marshal(s2aCtx.MapOrderBy[subQKeyMin])
		}
		if err != nil {
			return false, errors.New("unable to encode order by clause of the first subquery/withQuery")
		}

		if index == 0 {
			return true, nil
		}
	}

	// compare current groupBy clause with ctx.groupByJSON
	orderBy, err := json.Marshal(s2aCtx.MapOrderBy[mapKey])
	if err != nil {
		return false, errors.New("unable to encode order by clause of this subquery/withQuery")
	}

	return bytes.EqualFold(s2aCtx.orderByJSON, orderBy), nil
}

// addQIdentifier adds subquery/withQuery identifier and its mapKey into queryIdentifierSet
func (v *ASTBuilder) addQIdentifier(s2aCtx *SQL2AqlContext, indentifier string, key int) error {
	if s2aCtx.queryIdentifierSet == nil {
		s2aCtx.queryIdentifierSet = make(map[string]int)
	}
	if _, exist := s2aCtx.queryIdentifierSet[indentifier]; exist {
		return fmt.Errorf("subquery/withQuery identifier: %v already exist", indentifier)
	}
	s2aCtx.queryIdentifierSet[indentifier] = key

	return nil
}

// isWithQueryIdentifier check if name is a withQuery identifier
func (v *ASTBuilder) isWithQueryIdentifier(s2aCtx *SQL2AqlContext, name string) bool {
	withKeyMin := v.generateKey(1, typeWithQuery, 0)
	if s2aCtx.queryIdentifierSet != nil {
		k, exist := s2aCtx.queryIdentifierSet[name]
		if exist && k/withKeyMin == 1 {
			return true
		}
	}

	return false
}

// isSubOrWithQueryIdentifier check if name is a subquery/withQuery identifier
func (v *ASTBuilder) isSubOrWithQueryIdentifier(s2aCtx *SQL2AqlContext, name string) int {
	if s2aCtx.queryIdentifierSet != nil {
		if k, exist := s2aCtx.queryIdentifierSet[name]; exist {
			return k
		}
	}

	return -1
}

// lookupSQLExpr is used by groupBy, orderBy, having clause whose sql expression is select column alias.
// It returns the select column alias and sql expresion.
func (v *ASTBuilder) lookupSQLExpr(s2aCtx *SQL2AqlContext, mapKey int, str string) (alias, sqlExpr string) {
	for _, measure := range s2aCtx.MapMeasures[mapKey] {
		if len(measure.Alias) > 0 && strings.Compare(measure.Alias, str) == 0 {
			alias = str
			sqlExpr = measure.Expr
			return
		}
	}
	sqlExpr = str
	return
}

// check path from expression node to leaf node has logicalBinaryOperator OR
func (v *ASTBuilder) hasORInPath(node *antlr.BaseParserRuleContext) tree.LogicalBinaryExpType {
	parent := node.GetParent()
	op := tree.NOOP
	for parent != nil {
		switch p := parent.(type) {
		case antlrgen.IExpressionContext:
			return op
		case *antlrgen.LogicalBinaryContext:
			if p.GetOperator() != nil && v.getLogicalBinaryOperator(p.GetOperator().GetTokenType()) == tree.OR {
				return tree.OR
			}
			op = tree.AND
		}
		parent = parent.GetParent()
	}
	return op
}

// Parse parses input sql
func Parse(sql string, logger common.Logger) (aql *AQLQuery, err error) {
	defer func() {
		if r := recover(); r != nil {
			var ok bool
			err, ok = r.(error)
			if !ok {
				err = fmt.Errorf("unkonwn error, reason: %v", r)
			}
		}
	}()

	// Setup the input sql
	is := util.NewCaseChangingStream(antlr.NewInputStream(sql), true)

	// Create the Lexer
	lexer := antlrgen.NewSqlBaseLexer(is)
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)

	// Create the Parser
	p := antlrgen.NewSqlBaseParser(stream)

	// Finally parse the sql
	p.GetInterpreter().SetPredictionMode(antlr.PredictionModeSLL)
	parseTree, ok := p.Query().(*antlrgen.QueryContext)
	if !ok {
		err = fmt.Errorf("not a query")
		return nil, err
	}

	// Construct ASTBuilder
	v := &ASTBuilder{
		Logger:            logger,
		IStream:           stream,
		ParameterPosition: 0,
		SQL2AqlCtx: &SQL2AqlContext{
			MapQueryIdentifier: make(map[int]string),
			MapMeasures:        make(map[int][]Measure),
			MapDimensions:      make(map[int][]Dimension),
			MapJoinTables:      make(map[int][]Join),
			MapRowFilters:      make(map[int][]string),
			MapOrderBy:         make(map[int][]SortField),
			MapLimit:           make(map[int]int),
		},
	}
	node := v.VisitQuery(parseTree)
	if _, ok := node.(*tree.Query); ok {
		aql = v.GetAQL()
		aql.SQLQuery = sql
		aqlJSON, _ := json.Marshal(aql)
		logger.Infof("convert SQL:\n%v\nto AQL:\n%v", sql, string(aqlJSON))
	}

	if len(aql.SupportingDimensions) > 0 || len(aql.SupportingMeasures) > 0 {
		err = fmt.Errorf("sub query not supported yet")
		return
	}

	// non agg query overwrite
	if len(aql.Dimensions) == 0 {
		if v.aggFuncExists {
			err = fmt.Errorf("no aggregate functions allowed when no group by specified")
			return
		}
		for _, measure := range aql.Measures {
			aql.Dimensions = append(aql.Dimensions, Dimension{
				Expr: measure.Expr,
			})
		}
		aql.Measures = []Measure{{
			Expr: "1",
		}}
	}

	return
}
