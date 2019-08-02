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

// #include "time_series_aggregate.h"
import "C"

import (
	"github.com/uber/aresdb/cluster/topology"
	"sort"
	"strings"
	"unsafe"

	"fmt"
	memCom "github.com/uber/aresdb/memstore/common"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/query/common"
	"github.com/uber/aresdb/query/expr"
	"github.com/uber/aresdb/utils"
	"strconv"
)

const (
	unsupportedInputType      = "unsupported input type for %s: %s"
	defaultTimezoneTableAlias = "__timezone_lookup"
	geoShapeLimit             = 100
	nonAggregationQueryLimit  = 1000
)

// Compile compiles AQLQueryContext for data feeding and query
// execution. Caller should check for AQLQueryContext.Error.
func (qc *AQLQueryContext) Compile(tableSchemaReader memCom.TableSchemaReader, shardOwner topology.ShardOwner) {
	// processTimezone might append additional joins
	qc.processTimezone()
	if qc.Error != nil {
		return
	}

	// Read schema for every table used.
	qc.readSchema(tableSchemaReader, shardOwner)
	defer qc.releaseSchema()
	if qc.Error != nil {
		return
	}

	// Parse all other SQL expressions to ASTs.
	qc.parseExprs()
	if qc.Error != nil {
		return
	}

	// Resolve data types in the ASTs against schema, also translate enum values.
	qc.resolveTypes()
	if qc.Error != nil {
		return
	}

	// Process join conditions first to collect information about geo join.
	qc.processJoinConditions()
	if qc.Error != nil {
		return
	}

	// Identify prefilters.
	qc.matchPrefilters()

	// Process filters.
	qc.processFilters()
	if qc.Error != nil {
		return
	}

	// Process measure and dimensions.
	qc.processMeasure()
	if qc.Error != nil {
		return
	}
	qc.processDimensions()
	if qc.Error != nil {
		return
	}

	qc.sortUsedColumns()

	qc.sortDimensionColumns()
	if qc.Error != nil {
		return
	}

	// TODO: VM instruction generation
}

// adjustFilterToTimeFilter try to find one rowfilter to be time filter if there is no timefilter for fact table query
func (qc *AQLQueryContext) adjustFilterToTimeFilter() {
	toBeRemovedFilters := []int{}
	timeFilter := common.TimeFilter{}
	for i, filter := range qc.Query.FiltersParsed {
		if e, ok := filter.(*expr.BinaryExpr); ok {
			lhs, isCol := e.LHS.(*expr.VarRef)
			if !isCol {
				continue
			}

			// check if this filter on main table event time column
			tableID, columnID, err := qc.resolveColumn(lhs.Val)
			if err != nil || tableID != 0 || columnID != 0 {
				continue
			}

			val := ""
			// only support number literal or string literal
			switch rhs := e.RHS.(type) {
			case *expr.NumberLiteral:
				val = rhs.String()
			case *expr.StringLiteral:
				val = rhs.Val
			}
			if val == "" {
				continue
			}

			switch e.Op {
			case expr.LT:
				if timeFilter.To == "" {
					// only convert first LT
					timeFilter.To = val
					toBeRemovedFilters = append(toBeRemovedFilters, i)
				} else {
					qc.Error = utils.StackError(nil, "Only one '<' filter allowed for event time column")
					return
				}
			case expr.GTE:
				if timeFilter.From == "" {
					// only convert first GTE
					timeFilter.From = val
					toBeRemovedFilters = append(toBeRemovedFilters, i)
				} else {
					qc.Error = utils.StackError(nil, "Only one '>=' filter allowed for event time column")
					return
				}
			}
		}
	}
	if timeFilter.From != "" || timeFilter.To != "" {
		// processTimeFilter will handle the from is nil case
		if qc.fromTime, qc.toTime, qc.Error = common.ParseTimeFilter(timeFilter, qc.fixedTimezone, utils.Now()); qc.Error != nil {
			return
		}
		// remove from original query filter
		for i := len(toBeRemovedFilters) - 1; i >= 0; i-- {
			index := toBeRemovedFilters[i]
			qc.Query.FiltersParsed = append(qc.Query.FiltersParsed[:index], qc.Query.FiltersParsed[index+1:]...)
		}
	}
}

func (qc *AQLQueryContext) processJoinConditions() {
	if len(qc.Query.Joins) > 8 {
		qc.Error = utils.StackError(nil, "At most %d foreign tables allowed, got: %d", 8, len(qc.Query.Joins))
		return
	}

	qc.OOPK.foreignTables = make([]*foreignTable, len(qc.Query.Joins))
	mainTableSchema := qc.TableSchemaByName[qc.Query.Table]
	for joinTableID, join := range qc.Query.Joins {
		joinSchema := qc.TableSchemaByName[join.Table]
		if isGeoJoin(join) {
			if qc.OOPK.geoIntersection != nil {
				qc.Error = utils.StackError(nil, "At most one geo join allowed")
				return
			}
			qc.matchGeoJoin(joinTableID, mainTableSchema, joinSchema, join.ConditionsParsed)
			if qc.Error != nil {
				return
			}
		} else {
			// we will extract the geo join out of the join conditions since we are going to handle geo intersects
			// as filter instead of an equal join.
			qc.OOPK.foreignTables[joinTableID] = &foreignTable{}
			qc.matchEqualJoin(joinTableID, joinSchema, join.ConditionsParsed)
			if qc.Error != nil {
				return
			}
		}
	}
}

// matchGeoJoin initializes the GeoIntersection struct for later query process use. For now only one geo join is
// allowed per query. If users want to intersect with multiple geo join conditions, they should specify multiple geo
// shapeLatLongs in the geo filter.
// There are following constrictions:
// 1. At most one geo join condition.
// 2. Geo table must be dimension table.
// 3. The join condition must include exactly one shape column and one point column.
// 4. Exactly one geo filter should be specified.
// 5. Geo filter column must be the primary key of the geo table.
// 6. Geo UUIDs must be string in query.
// 7. Geo filter operator must be EQ or IN
// 8. Geo table's fields are not allowed in measures.
// 9. Only one geo dimension allowed.
func (qc *AQLQueryContext) matchGeoJoin(joinTableID int, mainTableSchema *memCom.TableSchema,
	joinSchema *memCom.TableSchema, conditions []expr.Expr) {
	if len(conditions) != 1 {
		qc.Error = utils.StackError(nil, "At most one join condition allowed per geo join")
		return
	}

	if joinSchema.Schema.IsFactTable {
		qc.Error = utils.StackError(nil, "Only dimension table is allowed in geo join")
		return
	}

	// one foreign table primary key columns only.
	if len(joinSchema.Schema.PrimaryKeyColumns) > 1 {
		qc.Error = utils.StackError(nil, "Composite primary key for geo table is not allowed")
		return
	}

	c, _ := conditions[0].(*expr.Call)

	// guaranteed by query rewrite.
	shape, _ := c.Args[0].(*expr.VarRef)
	point, _ := c.Args[1].(*expr.VarRef)

	if shape.TableID != joinTableID+1 {
		qc.Error = utils.StackError(nil, "Only shape in geo table can be referenced as join condition")
		return
	}

	qc.OOPK.geoIntersection = &geoIntersection{
		shapeTableID:  shape.TableID,
		shapeColumnID: shape.ColumnID,
		pointTableID:  point.TableID,
		pointColumnID: point.ColumnID,
		dimIndex:      -1,
		inOrOut:       true,
	}

	// Set column usage for geo points.
	expr.Walk(columnUsageCollector{
		tableScanners: qc.TableScanners,
		usages:        columnUsedByAllBatches,
	}, point)
}

func isGeoJoin(j common.Join) bool {
	if len(j.ConditionsParsed) >= 1 {
		c, ok := j.ConditionsParsed[0].(*expr.Call)
		if !ok {
			return false
		}
		return c.Name == expr.GeographyIntersectsCallName
	}
	return false
}

// list of join conditions enforced for now
// 1. equi-join only
// 2. many-to-one join only
// 3. foreign table must be a dimension table
// 4. one foreign table primary key columns only
// 5. foreign table primary key can have only one column
// 6. every foreign table must be joined directly to the main table, i.e. no bridges?
// 7. up to 8 foreign tables
func (qc *AQLQueryContext) matchEqualJoin(joinTableID int, joinSchema *memCom.TableSchema, conditions []expr.Expr) {
	if len(conditions) != 1 {
		qc.Error = utils.StackError(nil, "%d join conditions expected, got %d", 1, len(conditions))
		return
	}

	// foreign table must be a dimension table
	if joinSchema.Schema.IsFactTable {
		qc.Error = utils.StackError(nil, "join table %s is fact table, only dimension table supported", qc.Query.Table)
		return
	}

	// one foreign table primary key columns only
	if len(joinSchema.Schema.PrimaryKeyColumns) > 1 {
		qc.Error = utils.StackError(nil, "composite key not supported")
		return
	}

	// equi-join only
	e, ok := conditions[0].(*expr.BinaryExpr)
	if !ok {
		qc.Error = utils.StackError(nil, "binary expression expected, got %s", conditions[0].String())
		return
	}
	if e.Op != expr.EQ {
		qc.Error = utils.StackError(nil, "equal join expected, got %s", e.Op.String())
		return
	}

	left, ok := e.LHS.(*expr.VarRef)
	if !ok {
		qc.Error = utils.StackError(nil, "column in join condition expected, got %s", e.LHS.String())
		return
	}

	right, ok := e.RHS.(*expr.VarRef)
	if !ok {
		qc.Error = utils.StackError(nil, "column in join condition expected, got %s", e.RHS.String())
		return
	}

	// main table at left and foreign table at right
	if left.TableID != 0 {
		left, right = right, left
	}

	// every foreign table must be joined directly to the main table
	if left.TableID != 0 || right.TableID != joinTableID+1 {
		qc.Error = utils.StackError(nil, "foreign table must be joined directly to the main table, join condition: %s", e.String())
		return
	}

	// many-to-one join only (join with foreign table's primary key)
	if joinSchema.Schema.PrimaryKeyColumns[0] != right.ColumnID {
		qc.Error = utils.StackError(nil, "join column is not primary key of foreign table")
		return
	}

	qc.OOPK.foreignTables[joinTableID].remoteJoinColumn = left
	// set column usage for join column in main table
	// no need to set usage for remote join column in foreign table since
	// we only use primary key of foreign table to join
	expr.Walk(columnUsageCollector{
		tableScanners: qc.TableScanners,
		usages:        columnUsedByAllBatches,
	}, left)
}

func (qc *AQLQueryContext) parseExprs() {
	var err error

	// Join conditions.
	for i, join := range qc.Query.Joins {
		join.ConditionsParsed = make([]expr.Expr, len(join.Conditions))
		for j, cond := range join.Conditions {
			join.ConditionsParsed[j], err = expr.ParseExpr(cond)
			if err != nil {
				qc.Error = utils.StackError(err, "Failed to parse join condition: %s", cond)
				return
			}
		}
		qc.Query.Joins[i] = join
	}

	qc.fromTime, qc.toTime, qc.Error = common.ParseTimeFilter(qc.Query.TimeFilter, qc.fixedTimezone, utils.Now())
	if qc.Error != nil {
		return
	}

	// Filters.
	qc.Query.FiltersParsed = make([]expr.Expr, len(qc.Query.Filters))
	for i, filter := range qc.Query.Filters {
		qc.Query.FiltersParsed[i], err = expr.ParseExpr(filter)
		if err != nil {
			qc.Error = utils.StackError(err, "Failed to parse filter %s", filter)
			return
		}
	}
	if qc.fromTime == nil && qc.toTime == nil && len(qc.TableScanners) > 0 && qc.TableScanners[0].Schema.Schema.IsFactTable {
		qc.adjustFilterToTimeFilter()
		if qc.Error != nil {
			return
		}
	}

	// Dimensions.
	rawDimensions := qc.Query.Dimensions
	qc.Query.Dimensions = []common.Dimension{}
	for _, dim := range rawDimensions {
		dim.TimeBucketizer = strings.Trim(dim.TimeBucketizer, " ")
		if dim.TimeBucketizer != "" {
			// make sure time column is defined
			if dim.Expr == "" {
				qc.Error = utils.StackError(err, "Failed to parse TimeSeriesBucketizer '%s' since time column is empty ", dim.TimeBucketizer)
				return
			}

			timeColumnExpr, err := expr.ParseExpr(dim.Expr)
			if err != nil {
				qc.Error = utils.StackError(err, "Failed to parse timeColumn '%s'", dim.Expr)
				return
			}

			dim.ExprParsed, err = qc.buildTimeDimensionExpr(dim.TimeBucketizer, timeColumnExpr)
			if err != nil {
				qc.Error = utils.StackError(err, "Failed to parse dimension: %s", dim.TimeBucketizer)
				return
			}
			qc.Query.Dimensions = append(qc.Query.Dimensions, dim)
		} else {
			// dimension is defined as sqlExpression
			dim.ExprParsed, err = expr.ParseExpr(dim.Expr)
			if err != nil {
				qc.Error = utils.StackError(err, "Failed to parse dimension: %s", dim.Expr)
				return
			}
			if _, ok := dim.ExprParsed.(*expr.Wildcard); ok {
				qc.Query.Dimensions = append(qc.Query.Dimensions, qc.getAllColumnsDimension()...)
			} else {
				qc.Query.Dimensions = append(qc.Query.Dimensions, dim)
			}
		}
	}

	// Measures.
	for i, measure := range qc.Query.Measures {
		measure.ExprParsed, err = expr.ParseExpr(measure.Expr)
		if err != nil {
			qc.Error = utils.StackError(err, "Failed to parse measure: %s", measure.Expr)
			return
		}
		measure.FiltersParsed = make([]expr.Expr, len(measure.Filters))
		for j, filter := range measure.Filters {
			measure.FiltersParsed[j], err = expr.ParseExpr(filter)
			if err != nil {
				qc.Error = utils.StackError(err, "Failed to parse measure filter %s", filter)
				return
			}
		}
		qc.Query.Measures[i] = measure
	}
}

func (qc *AQLQueryContext) processTimezone() {
	if timezoneColumn, joinKey, success := parseTimezoneColumnString(qc.Query.Timezone); success {
		timezoneTable := utils.GetConfig().Query.TimezoneTable.TableName
		qc.timezoneTable.tableColumn = timezoneColumn
		for _, join := range qc.Query.Joins {
			if join.Table == timezoneTable {
				qc.timezoneTable.tableAlias = join.Alias
			}
		}
		// append timezone table to joins
		if qc.timezoneTable.tableAlias == "" {
			qc.timezoneTable.tableAlias = defaultTimezoneTableAlias
			qc.Query.Joins = append(qc.Query.Joins, common.Join{
				Table:      timezoneTable,
				Alias:      defaultTimezoneTableAlias,
				Conditions: []string{fmt.Sprintf("%s=%s.id", joinKey, defaultTimezoneTableAlias)},
			})
		}
	} else {
		loc, err := common.ParseTimezone(qc.Query.Timezone)
		if err != nil {
			qc.Error = utils.StackError(err, "timezone Failed to parse: %s", qc.Query.Timezone)
			return
		}
		qc.fixedTimezone = loc
	}
}

func (qc *AQLQueryContext) readSchema(tableSchemaReader memCom.TableSchemaReader, shardOwner topology.ShardOwner) {
	qc.TableScanners = make([]*TableScanner, 1+len(qc.Query.Joins))
	qc.TableIDByAlias = make(map[string]int)
	qc.TableSchemaByName = make(map[string]*memCom.TableSchema)

	tableSchemaReader.RLock()
	defer tableSchemaReader.RUnlock()

	// Main table.
	schema := tableSchemaReader.GetSchemas()[qc.Query.Table]
	if schema == nil {
		qc.Error = utils.StackError(nil, "unknown main table %s", qc.Query.Table)
		return
	}
	qc.TableSchemaByName[qc.Query.Table] = schema
	schema.RLock()
	qc.TableScanners[0] = &TableScanner{}
	qc.TableScanners[0].Schema = schema

	// use user query specified shards
	// or all shards it owns when user did not specify
	if len(qc.Query.Shards) == 0 {
		qc.TableScanners[0].Shards = shardOwner.GetOwnedShards()
	} else {
		qc.TableScanners[0].Shards = qc.Query.Shards
	}

	qc.TableScanners[0].ColumnUsages = make(map[int]columnUsage)
	if schema.Schema.IsFactTable {
		// Archiving cutoff filter usage for fact table.
		qc.TableScanners[0].ColumnUsages[0] = columnUsedByLiveBatches
	}
	qc.TableIDByAlias[qc.Query.Table] = 0

	// Foreign tables.
	for i, join := range qc.Query.Joins {
		schema = tableSchemaReader.GetSchemas()[join.Table]
		if schema == nil {
			qc.Error = utils.StackError(nil, "unknown join table %s", join.Table)
			return
		}

		if qc.TableSchemaByName[join.Table] == nil {
			qc.TableSchemaByName[join.Table] = schema
			// Prevent double locking.
			schema.RLock()
		}

		qc.TableScanners[1+i] = &TableScanner{}
		qc.TableScanners[1+i].Schema = schema
		qc.TableScanners[1+i].ColumnUsages = make(map[int]columnUsage)
		if schema.Schema.IsFactTable {
			// we will only support fact to fact join within same shard
			qc.TableScanners[1+i].Shards = qc.TableScanners[0].Shards
			// Archiving cutoff filter usage for fact table.
			qc.TableScanners[1+i].ColumnUsages[0] = columnUsedByLiveBatches
		} else {
			// for fact to dimension table join
			// we can assume shard zero for dimension table
			// since dimension table is not sharded
			qc.TableScanners[1+i].Shards = []int{0}
		}

		alias := join.Alias
		if alias == "" {
			alias = join.Table
		}
		_, exists := qc.TableIDByAlias[alias]
		if exists {
			qc.Error = utils.StackError(nil, "table alias %s is redefined", alias)
			return
		}
		qc.TableIDByAlias[alias] = 1 + i
	}
}

func (qc *AQLQueryContext) releaseSchema() {
	for _, schema := range qc.TableSchemaByName {
		schema.RUnlock()
	}
}

// resolveColumn resolves the VarRef identifier against the schema,
// and returns the matched tableID (query scoped) and columnID (schema scoped).
func (qc *AQLQueryContext) resolveColumn(identifier string) (int, int, error) {
	tableAlias := qc.Query.Table
	column := identifier
	segments := strings.SplitN(identifier, ".", 2)
	if len(segments) == 2 {
		tableAlias = segments[0]
		column = segments[1]
	}

	tableID, exists := qc.TableIDByAlias[tableAlias]
	if !exists {
		return 0, 0, utils.StackError(nil, "unknown table alias %s", tableAlias)
	}

	columnID, exists := qc.TableScanners[tableID].Schema.ColumnIDs[column]
	if !exists {
		return 0, 0, utils.StackError(nil, "unknown column %s for table alias %s",
			column, tableAlias)
	}

	return tableID, columnID, nil
}

func blockNumericOpsForColumnOverFourBytes(token expr.Token, expressions ...expr.Expr) error {
	if token == expr.UNARY_MINUS || token == expr.BITWISE_NOT ||
		(token >= expr.ADD && token <= expr.BITWISE_LEFT_SHIFT) {
		for _, expression := range expressions {
			if varRef, isVarRef := expression.(*expr.VarRef); isVarRef && memCom.DataTypeBytes(varRef.DataType) > 4 {
				return utils.StackError(nil, "numeric operations not supported for column over 4 bytes length, got %s", expression.String())
			}
		}
	}
	return nil
}

// Rewrite walks the expresison AST and resolves data types bottom up.
// In addition it also translates enum strings and rewrites their predicates.
func (qc *AQLQueryContext) Rewrite(expression expr.Expr) expr.Expr {
	switch e := expression.(type) {
	case *expr.ParenExpr:
		// Strip parenthesis from the input
		return e.Expr
	case *expr.VarRef:
		tableID, columnID, err := qc.resolveColumn(e.Val)
		if err != nil {
			qc.Error = err
			return expression
		}
		column := qc.TableScanners[tableID].Schema.Schema.Columns[columnID]
		if column.Deleted {
			qc.Error = utils.StackError(nil, "column %s of table %s has been deleted",
				column.Name, qc.TableScanners[tableID].Schema.Schema.Name)
			return expression
		}
		dataType := qc.TableScanners[tableID].Schema.ValueTypeByColumn[columnID]
		e.ExprType = common.DataTypeToExprType[dataType]
		e.TableID = tableID
		e.ColumnID = columnID
		dict := qc.TableScanners[tableID].Schema.EnumDicts[column.Name]
		e.EnumDict = dict.Dict
		e.EnumReverseDict = dict.ReverseDict
		e.DataType = dataType
		e.IsHLLColumn = column.HLLConfig.IsHLLColumn
	case *expr.UnaryExpr:
		if expr.IsUUIDColumn(e.Expr) && e.Op != expr.GET_HLL_VALUE {
			qc.Error = utils.StackError(nil, "uuid column type only supports countdistincthll unary expression")
			return expression
		}

		if err := blockNumericOpsForColumnOverFourBytes(e.Op, e.Expr); err != nil {
			qc.Error = err
			return expression
		}

		e.ExprType = e.Expr.Type()
		switch e.Op {
		case expr.EXCLAMATION, expr.NOT, expr.IS_FALSE:
			e.ExprType = expr.Boolean
			// Normalize the operator.
			e.Op = expr.NOT
			e.Expr = expr.Cast(e.Expr, expr.Boolean)
			childExpr := e.Expr
			callRef, isCallRef := childExpr.(*expr.Call)
			if isCallRef && callRef.Name == expr.GeographyIntersectsCallName {
				qc.Error = utils.StackError(nil, "Not %s condition is not allowed", expr.GeographyIntersectsCallName)
				break
			}
		case expr.UNARY_MINUS:
			// Upgrade to signed.
			if e.ExprType < expr.Signed {
				e.ExprType = expr.Signed
			}
		case expr.IS_NULL, expr.IS_NOT_NULL:
			e.ExprType = expr.Boolean
		case expr.IS_TRUE:
			// Strip IS_TRUE if child is already boolean.
			if e.Expr.Type() == expr.Boolean {
				return e.Expr
			}
			// Rewrite to NOT(NOT(child)).
			e.ExprType = expr.Boolean
			e.Op = expr.NOT
			e.Expr = expr.Cast(e.Expr, expr.Boolean)
			return &expr.UnaryExpr{Expr: e, Op: expr.NOT, ExprType: expr.Boolean}
		case expr.BITWISE_NOT:
			// Cast child to unsigned.
			e.ExprType = expr.Unsigned
			e.Expr = expr.Cast(e.Expr, expr.Unsigned)
		case expr.GET_MONTH_START, expr.GET_QUARTER_START, expr.GET_YEAR_START, expr.GET_WEEK_START:
			// Cast child to unsigned.
			e.ExprType = expr.Unsigned
			e.Expr = expr.Cast(e.Expr, expr.Unsigned)
		case expr.GET_DAY_OF_MONTH, expr.GET_DAY_OF_YEAR, expr.GET_MONTH_OF_YEAR, expr.GET_QUARTER_OF_YEAR:
			// Cast child to unsigned.
			e.ExprType = expr.Unsigned
			e.Expr = expr.Cast(e.Expr, expr.Unsigned)
		case expr.GET_HLL_VALUE:
			e.ExprType = expr.Unsigned
			e.Expr = expr.Cast(e.Expr, expr.Unsigned)
		default:
			qc.Error = utils.StackError(nil, "unsupported unary expression %s",
				e.String())
		}
	case *expr.BinaryExpr:
		if err := blockNumericOpsForColumnOverFourBytes(e.Op, e.LHS, e.RHS); err != nil {
			qc.Error = err
			return expression
		}

		if e.Op != expr.EQ && e.Op != expr.NEQ {
			_, isRHSStr := e.RHS.(*expr.StringLiteral)
			_, isLHSStr := e.LHS.(*expr.StringLiteral)
			if isRHSStr || isLHSStr {
				qc.Error = utils.StackError(nil, "string type only support EQ and NEQ operators")
				return expression
			}
		}
		highestType := e.LHS.Type()
		if e.RHS.Type() > highestType {
			highestType = e.RHS.Type()
		}
		switch e.Op {
		case expr.ADD, expr.SUB:
			// Upgrade and cast to highestType.
			e.ExprType = highestType
			if highestType == expr.Float {
				e.LHS = expr.Cast(e.LHS, expr.Float)
				e.RHS = expr.Cast(e.RHS, expr.Float)
			} else if e.Op == expr.SUB {
				// For lhs - rhs, upgrade to signed at least.
				e.ExprType = expr.Signed
			}
		case expr.MUL, expr.MOD:
			// Upgrade and cast to highestType.
			e.ExprType = highestType
			e.LHS = expr.Cast(e.LHS, highestType)
			e.RHS = expr.Cast(e.RHS, highestType)
		case expr.DIV:
			// Upgrade and cast to float.
			e.ExprType = expr.Float
			e.LHS = expr.Cast(e.LHS, expr.Float)
			e.RHS = expr.Cast(e.RHS, expr.Float)
		case expr.BITWISE_AND, expr.BITWISE_OR, expr.BITWISE_XOR,
			expr.BITWISE_LEFT_SHIFT, expr.BITWISE_RIGHT_SHIFT, expr.FLOOR, expr.CONVERT_TZ:
			// Cast to unsigned.
			e.ExprType = expr.Unsigned
			e.LHS = expr.Cast(e.LHS, expr.Unsigned)
			e.RHS = expr.Cast(e.RHS, expr.Unsigned)
		case expr.AND, expr.OR:
			// Cast to boolean.
			e.ExprType = expr.Boolean
			e.LHS = expr.Cast(e.LHS, expr.Boolean)
			e.RHS = expr.Cast(e.RHS, expr.Boolean)
		case expr.LT, expr.LTE, expr.GT, expr.GTE:
			// Cast to boolean.
			e.ExprType = expr.Boolean
			e.LHS = expr.Cast(e.LHS, highestType)
			e.RHS = expr.Cast(e.RHS, highestType)
		case expr.NEQ, expr.EQ:
			// swap lhs and rhs if rhs is VarRef but lhs is not.
			if _, lhsVarRef := e.LHS.(*expr.VarRef); !lhsVarRef {
				if _, rhsVarRef := e.RHS.(*expr.VarRef); rhsVarRef {
					e.LHS, e.RHS = e.RHS, e.LHS
				}
			}

			e.ExprType = expr.Boolean
			// Match enum = 'case' and enum != 'case'.

			lhs, _ := e.LHS.(*expr.VarRef)
			// rhs is bool
			rhsBool, _ := e.RHS.(*expr.BooleanLiteral)
			if lhs != nil && rhsBool != nil {
				if (e.Op == expr.EQ && rhsBool.Val) || (e.Op == expr.NEQ && !rhsBool.Val) {
					return &expr.UnaryExpr{Expr: lhs, Op: expr.IS_TRUE, ExprType: expr.Boolean}
				}
				return &expr.UnaryExpr{Expr: lhs, Op: expr.NOT, ExprType: expr.Boolean}
			}

			// rhs is string enum
			rhs, _ := e.RHS.(*expr.StringLiteral)
			if lhs != nil && rhs != nil && lhs.EnumDict != nil {
				// Enum dictionary translation
				value, exists := lhs.EnumDict[rhs.Val]
				if !exists {
					// Combination of nullable data with not/and/or operators on top makes
					// short circuiting hard.
					// To play it safe we match against an invalid value.
					value = -1
				}
				e.RHS = &expr.NumberLiteral{Int: value, ExprType: expr.Unsigned}
			} else {
				// Cast to highestType.
				e.LHS = expr.Cast(e.LHS, highestType)
				e.RHS = expr.Cast(e.RHS, highestType)
			}

			if rhs != nil && lhs.DataType == memCom.GeoPoint {
				if val, err := memCom.GeoPointFromString(rhs.Val); err != nil {
					qc.Error = err
				} else {
					e.RHS = &expr.GeopointLiteral{
						Val: val,
					}
				}
			}
		case expr.IN:
			return qc.expandINop(e)
		case expr.NOT_IN:
			return &expr.UnaryExpr{
				Op:   expr.NOT,
				Expr: qc.expandINop(e),
			}
		default:
			qc.Error = utils.StackError(nil, "unsupported binary expression %s",
				e.String())
		}
	case *expr.Call:
		e.Name = strings.ToLower(e.Name)
		switch e.Name {
		case expr.ConvertTzCallName:
			if len(e.Args) != 3 {
				qc.Error = utils.StackError(
					nil, "convert_tz must have 3 arguments",
				)
				break
			}
			fromTzStringExpr, isStrLiteral := e.Args[1].(*expr.StringLiteral)
			if !isStrLiteral {
				qc.Error = utils.StackError(nil, "2nd argument of convert_tz must be a string")
				break
			}
			toTzStringExpr, isStrLiteral := e.Args[2].(*expr.StringLiteral)
			if !isStrLiteral {
				qc.Error = utils.StackError(nil, "3rd argument of convert_tz must be a string")
				break
			}
			fromTz, err := common.ParseTimezone(fromTzStringExpr.Val)
			if err != nil {
				qc.Error = utils.StackError(err, "failed to rewrite convert_tz")
				break
			}
			toTz, err := common.ParseTimezone(toTzStringExpr.Val)
			if err != nil {
				qc.Error = utils.StackError(err, "failed to rewrite convert_tz")
				break
			}
			_, fromOffsetInSeconds := utils.Now().In(fromTz).Zone()
			_, toOffsetInSeconds := utils.Now().In(toTz).Zone()
			offsetInSeconds := toOffsetInSeconds - fromOffsetInSeconds
			return &expr.BinaryExpr{
				Op:  expr.ADD,
				LHS: e.Args[0],
				RHS: &expr.NumberLiteral{
					Int:      offsetInSeconds,
					Expr:     strconv.Itoa(offsetInSeconds),
					ExprType: expr.Unsigned,
				},
				ExprType: expr.Unsigned,
			}
		case expr.CountCallName:
			e.ExprType = expr.Unsigned
		case expr.DayOfWeekCallName:
			// dayofweek from ts: (ts / secondsInDay + 4) % 7 + 1
			// ref: https://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html#function_dayofweek
			if len(e.Args) != 1 {
				qc.Error = utils.StackError(nil, "dayofweek takes exactly 1 argument")
				break
			}
			tsExpr := e.Args[0]
			return &expr.BinaryExpr{
				Op:       expr.ADD,
				ExprType: expr.Unsigned,
				RHS: &expr.NumberLiteral{
					Int:      1,
					Expr:     "1",
					ExprType: expr.Unsigned,
				},
				LHS: &expr.BinaryExpr{
					Op:       expr.MOD,
					ExprType: expr.Unsigned,
					RHS: &expr.NumberLiteral{
						Int:      common.DaysPerWeek,
						Expr:     strconv.Itoa(common.DaysPerWeek),
						ExprType: expr.Unsigned,
					},
					LHS: &expr.BinaryExpr{
						Op:       expr.ADD,
						ExprType: expr.Unsigned,
						RHS: &expr.NumberLiteral{
							// offset for
							Int:      common.WeekdayOffset,
							Expr:     strconv.Itoa(common.WeekdayOffset),
							ExprType: expr.Unsigned,
						},
						LHS: &expr.BinaryExpr{
							Op:       expr.DIV,
							ExprType: expr.Unsigned,
							RHS: &expr.NumberLiteral{
								Int:      common.SecondsPerDay,
								Expr:     strconv.Itoa(common.SecondsPerDay),
								ExprType: expr.Unsigned,
							},
							LHS: tsExpr,
						},
					},
				},
			}
			// no-op, this will be over written
		case expr.FromUnixTimeCallName:
			// for now, only the following format is allowed for backward compatibility
			// from_unixtime(time_col / 1000)
			timeColumnDivideErrMsg := "from_unixtime must be time column / 1000"
			timeColDivide, isBinary := e.Args[0].(*expr.BinaryExpr)
			if !isBinary || timeColDivide.Op != expr.DIV {
				qc.Error = utils.StackError(nil, timeColumnDivideErrMsg)
				break
			}
			divisor, isLiteral := timeColDivide.RHS.(*expr.NumberLiteral)
			if !isLiteral || divisor.Int != 1000 {
				qc.Error = utils.StackError(nil, timeColumnDivideErrMsg)
				break
			}
			if par, isParen := timeColDivide.LHS.(*expr.ParenExpr); isParen {
				timeColDivide.LHS = par.Expr
			}
			timeColExpr, isVarRef := timeColDivide.LHS.(*expr.VarRef)
			if !isVarRef {
				qc.Error = utils.StackError(nil, timeColumnDivideErrMsg)
				break
			}
			return timeColExpr
		case expr.HourCallName:
			if len(e.Args) != 1 {
				qc.Error = utils.StackError(nil, "hour takes exactly 1 argument")
				break
			}
			// hour(ts) = (ts % secondsInDay) / secondsInHour
			return &expr.BinaryExpr{
				Op:       expr.DIV,
				ExprType: expr.Unsigned,
				LHS: &expr.BinaryExpr{
					Op:  expr.MOD,
					LHS: e.Args[0],
					RHS: &expr.NumberLiteral{
						Expr:     strconv.Itoa(common.SecondsPerDay),
						Int:      common.SecondsPerDay,
						ExprType: expr.Unsigned,
					},
				},
				RHS: &expr.NumberLiteral{
					Expr:     strconv.Itoa(common.SecondsPerHour),
					Int:      common.SecondsPerHour,
					ExprType: expr.Unsigned,
				},
			}
			// list of literals, no need to cast it for now.
		case expr.ListCallName:
		case expr.GeographyIntersectsCallName:
			if len(e.Args) != 2 {
				qc.Error = utils.StackError(
					nil, "expect 2 argument for %s, but got %s", e.Name, e.String())
				break
			}

			lhsRef, isVarRef := e.Args[0].(*expr.VarRef)
			if !isVarRef || (lhsRef.DataType != memCom.GeoShape && lhsRef.DataType != memCom.GeoPoint) {
				qc.Error = utils.StackError(
					nil, "expect argument to be a valid geo shape or geo point column for %s, but got %s of type %s",
					e.Name, e.Args[0].String(), memCom.DataTypeName[lhsRef.DataType])
				break
			}

			lhsGeoPoint := lhsRef.DataType == memCom.GeoPoint

			rhsRef, isVarRef := e.Args[1].(*expr.VarRef)
			if !isVarRef || (rhsRef.DataType != memCom.GeoShape && rhsRef.DataType != memCom.GeoPoint) {
				qc.Error = utils.StackError(
					nil, "expect argument to be a valid geo shape or geo point column for %s, but got %s of type %s",
					e.Name, e.Args[1].String(), memCom.DataTypeName[rhsRef.DataType])
				break
			}

			rhsGeoPoint := rhsRef.DataType == memCom.GeoPoint

			if lhsGeoPoint == rhsGeoPoint {
				qc.Error = utils.StackError(
					nil, "expect exactly one geo shape column and one geo point column for %s, got %s",
					e.Name, e.String())
				break
			}

			// Switch geo point so that lhs is geo shape and rhs is geo point
			if lhsGeoPoint {
				e.Args[0], e.Args[1] = e.Args[1], e.Args[0]
			}

			e.ExprType = expr.Boolean
		case expr.HexCallName:
			if len(e.Args) != 1 {
				qc.Error = utils.StackError(
					nil, "expect 1 argument for %s, but got %s", e.Name, e.String())
				break
			}
			colRef, isVarRef := e.Args[0].(*expr.VarRef)
			if !isVarRef || colRef.DataType != memCom.UUID {
				qc.Error = utils.StackError(
					nil, "expect 1 argument to be a valid uuid column for %s, but got %s of type %s",
					e.Name, e.Args[0].String(), memCom.DataTypeName[colRef.DataType])
				break
			}
			e.ExprType = e.Args[0].Type()
		case expr.CountDistinctHllCallName:
			if len(e.Args) != 1 {
				qc.Error = utils.StackError(
					nil, "expect 1 argument for %s, but got %s", e.Name, e.String())
				break
			}
			colRef, isVarRef := e.Args[0].(*expr.VarRef)
			if !isVarRef {
				qc.Error = utils.StackError(
					nil, "expect 1 argument to be a column for %s", e.Name)
				break
			}

			e.Name = expr.HllCallName
			// 1. noop when column itself is hll column
			// 2. compute hll on the fly when column is not hll column
			if !colRef.IsHLLColumn {
				e.Args[0] = &expr.UnaryExpr{
					Op:       expr.GET_HLL_VALUE,
					Expr:     colRef,
					ExprType: expr.Unsigned,
				}
			}
			e.ExprType = expr.Unsigned
		case expr.HllCallName:
			if len(e.Args) != 1 {
				qc.Error = utils.StackError(
					nil, "expect 1 argument for %s, but got %s", e.Name, e.String())
				break
			}
			colRef, isVarRef := e.Args[0].(*expr.VarRef)
			if !isVarRef || colRef.DataType != memCom.Uint32 {
				qc.Error = utils.StackError(
					nil, "expect 1 argument to be a valid hll column for %s, but got %s of type %s",
					e.Name, e.Args[0].String(), memCom.DataTypeName[colRef.DataType])
				break
			}
			e.ExprType = e.Args[0].Type()
		case expr.SumCallName, expr.MinCallName, expr.MaxCallName, expr.AvgCallName:
			if len(e.Args) != 1 {
				qc.Error = utils.StackError(
					nil, "expect 1 argument for %s, but got %s", e.Name, e.String())
				break
			}
			// For avg, the expression type should always be float.
			if e.Name == expr.AvgCallName {
				e.Args[0] = expr.Cast(e.Args[0], expr.Float)
			}
			e.ExprType = e.Args[0].Type()
		case expr.LengthCallName, expr.ContainsCallName, expr.ElementAtCallName:
			// validate first argument
			if len(e.Args) == 0 {
				qc.Error = utils.StackError(
					nil, "array function %s requires arguments", e.Name)
				break
			}
			firstArg := e.Args[0]
			vr, ok := firstArg.(*expr.VarRef)
			if !ok || !memCom.IsArrayType(vr.DataType) {
				qc.Error = utils.StackError(
					nil, "array function %s requires first argument to be list type column, but got %s", e.Name, firstArg)
			}

			if e.Name == expr.LengthCallName {
				if len(e.Args) != 1 {
					qc.Error = utils.StackError(
						nil, "array function %s takes exactly 1 argument", e.Name)
					break
				}
				return &expr.UnaryExpr{
					Op:       expr.ARRAY_LENGTH,
					ExprType: expr.Unsigned,
					Expr:     vr,
				}
			} else if e.Name == expr.ContainsCallName {
				if len(e.Args) != 2 {
					qc.Error = utils.StackError(
						nil, "array function %s takes exactly 2 arguments", e.Name)
					break
				}

				secondArg := e.Args[1]
				var literalExpr expr.Expr
				// build rhs literal
				t := memCom.GetItemDataType(vr.DataType)
				switch t {
				case memCom.Bool:
					ok := false
					literalExpr, ok = secondArg.(*expr.BooleanLiteral)
					if !ok {
						qc.Error = utils.StackError(
							nil, "array function %s argument type mismatch", e.Name)
						break
					}
				case memCom.SmallEnum, memCom.BigEnum:
					strLiteral, ok := secondArg.(*expr.StringLiteral)
					if !ok {
						qc.Error = utils.StackError(
							nil, "array function %s argument type mismatch", e.Name)
						break
					}
					if vr.EnumDict != nil {
						// Enum dictionary translation
						value, exists := vr.EnumDict[strLiteral.Val]
						if !exists {
							// Combination of nullable data with not/and/or operators on top makes
							// short circuiting hard.
							// To play it safe we match against an invalid value.
							value = -1
						}
						literalExpr = &expr.NumberLiteral{Int: value, ExprType: expr.Unsigned}
					}
				case memCom.GeoPoint:
					strLiteral, ok := secondArg.(*expr.StringLiteral)
					if !ok {
						qc.Error = utils.StackError(
							nil, "array function %s argument type mismatch", e.Name)
						break
					}
					if val, err := memCom.GeoPointFromString(strLiteral.Val); err != nil {
						qc.Error = err
						break
					} else {
						literalExpr = &expr.GeopointLiteral{
							Val: val,
						}
					}
				}

				return &expr.BinaryExpr{
					Op:       expr.ARRAY_CONTAINS,
					ExprType: expr.Boolean,
					LHS:      vr,
					RHS:      literalExpr,
				}
			} else if e.Name == expr.ElementAtCallName {
				if len(e.Args) != 2 {
					qc.Error = utils.StackError(
						nil, "array function %s takes exactly 2 arguments", e.Name)
					break
				}
				if _, ok := e.Args[1].(*expr.NumberLiteral); !ok {
					qc.Error = utils.StackError(
						nil, "array function %s takes list type column and an index", e.Name)
				}
				return &expr.BinaryExpr{
					Op:       expr.ARRAY_ELEMENT_AT,
					ExprType: common.DataTypeToExprType[memCom.GetItemDataType(vr.DataType)],
					LHS:      vr,
					RHS:      e.Args[1],
				}
			}

		default:
			qc.Error = utils.StackError(nil, "unknown function %s", e.Name)
		}
	case *expr.Case:
		highestType := e.Else.Type()
		for _, whenThen := range e.WhenThens {
			if whenThen.Then.Type() > highestType {
				highestType = whenThen.Then.Type()
			}
		}
		// Cast else and thens to highestType, cast whens to boolean.
		e.Else = expr.Cast(e.Else, highestType)
		for i, whenThen := range e.WhenThens {
			whenThen.When = expr.Cast(whenThen.When, expr.Boolean)
			whenThen.Then = expr.Cast(whenThen.Then, highestType)
			e.WhenThens[i] = whenThen
		}
		e.ExprType = highestType
	}
	return expression
}

// normalizeAndFilters extracts top AND operators and flatten them out to the
// filter slice.
func normalizeAndFilters(filters []expr.Expr) []expr.Expr {
	i := 0
	for i < len(filters) {
		f, _ := filters[i].(*expr.BinaryExpr)
		if f != nil && f.Op == expr.AND {
			filters[i] = f.LHS
			filters = append(filters, f.RHS)
		} else {
			i++
		}
	}
	return filters
}

// resolveTypes walks all expresison ASTs and resolves data types bottom up.
// In addition it also translates enum strings and rewrites their predicates.
func (qc *AQLQueryContext) resolveTypes() {
	// Join conditions.
	for i, join := range qc.Query.Joins {
		for j, cond := range join.ConditionsParsed {
			join.ConditionsParsed[j] = expr.Rewrite(qc, cond)
			if qc.Error != nil {
				return
			}
		}
		qc.Query.Joins[i] = join
	}

	// Dimensions.
	for i, dim := range qc.Query.Dimensions {
		dim.ExprParsed = expr.Rewrite(qc, dim.ExprParsed)
		if qc.Error != nil {
			return
		}
		qc.Query.Dimensions[i] = dim
	}

	// Measures.
	for i, measure := range qc.Query.Measures {
		measure.ExprParsed = expr.Rewrite(qc, measure.ExprParsed)
		if qc.Error != nil {
			return
		}
		for j, filter := range measure.FiltersParsed {
			measure.FiltersParsed[j] = expr.Rewrite(qc, filter)
			if qc.Error != nil {
				return
			}
		}
		measure.FiltersParsed = normalizeAndFilters(measure.FiltersParsed)
		qc.Query.Measures[i] = measure
	}

	// Filters.
	for i, filter := range qc.Query.FiltersParsed {
		qc.Query.FiltersParsed[i] = expr.Rewrite(qc, filter)
		if qc.Error != nil {
			return
		}
	}
	qc.Query.FiltersParsed = normalizeAndFilters(qc.Query.FiltersParsed)
}

// extractFitler processes the specified query level filter and matches it
// against the following formats:
//   column = value
//   column > value
//   column >= value
//   column < value
//   column <= value
//   column
//   not column
// It returns the numeric constant value associated with the filter in a uint32
// space (for all types including float32).
// In addition it also returns the boundaryType for >, >=, <, <= operators.
// Note that since the candidate filters have already been preselected against
// some criterias, this function does not perform full format validation.
func (qc *AQLQueryContext) extractFilter(filterID int) (
	value uint32, boundary boundaryType, success bool) {
	switch f := qc.Query.FiltersParsed[filterID].(type) {
	case *expr.VarRef:
		// Match `column` format
		value = 1
		success = true
	case *expr.UnaryExpr:
		// Match `not column` format
		success = true
	case *expr.BinaryExpr:
		// Match `column op value` format
		rhs, _ := f.RHS.(*expr.NumberLiteral)
		if rhs == nil {
			return
		}
		switch rhs.ExprType {
		case expr.Float:
			*(*float32)(unsafe.Pointer(&value)) = float32(rhs.Val)
		case expr.Signed:
			*(*int32)(unsafe.Pointer(&value)) = int32(rhs.Int)
		case expr.Unsigned:
			value = uint32(rhs.Int)
		default:
			return
		}
		switch f.Op {
		case expr.GTE, expr.LTE:
			boundary = inclusiveBoundary
		case expr.GT, expr.LT:
			boundary = exclusiveBoundary
		}
		success = true
	}
	return
}

// matchPrefilters identifies all prefilters from query level filters,
// stores them in AQLQueryContext.Prefilters,
// and stores their values in TableScanner for future prefilter vector slicing.
func (qc *AQLQueryContext) matchPrefilters() {
	// Format of candidateFilters:
	// [tableID]map[columnID]{filterIDs for lower bound, upper bound, equality}
	// tableID is query scoped, while columnID is schema scoped.
	candidateFilters := make([]map[int][3]int, len(qc.TableScanners))
	for tableID := range qc.TableScanners {
		candidateFilters[tableID] = make(map[int][3]int)
	}

	// Index candidate filters by table/column
	for filterID, filter := range qc.Query.FiltersParsed {
		f, _ := filter.(*expr.BinaryExpr)
		if f == nil {
			switch f := filter.(type) {
			case *expr.VarRef:
				// Match `column` format
				if f.ExprType == expr.Boolean {
					candidateFilters[f.TableID][f.ColumnID] = [3]int{-1, -1, filterID}
				}
			case *expr.UnaryExpr:
				// Match `not column` format
				if f.Op == expr.NOT {
					f, _ := f.Expr.(*expr.VarRef)
					if f != nil && f.ExprType == expr.Boolean {
						candidateFilters[f.TableID][f.ColumnID] = [3]int{-1, -1, filterID}
					}
				}
				// TODO: IS_NULL can be matched as an equality filter.
				// TODO: IS_NOT_NULL can be matched as the final range filter.
			}
			continue
		}

		// Match `column op value` format, where op can be =, <, <=, >, >=.
		if f.Op < expr.EQ || f.Op > expr.GTE {
			continue
		}

		lhs, _ := f.LHS.(*expr.VarRef)
		if lhs == nil {
			continue
		}

		columnToFilterMap := candidateFilters[lhs.TableID]
		filters, exists := columnToFilterMap[lhs.ColumnID]
		if !exists {
			filters = [3]int{-1, -1, -1}
		}
		switch f.Op {
		case expr.GT, expr.GTE:
			filters[0] = filterID
		case expr.LT, expr.LTE:
			filters[1] = filterID
		case expr.EQ:
			filters[2] = filterID
		}
		columnToFilterMap[lhs.ColumnID] = filters
	}

	// Prefilter matching
	for tableID, scanner := range qc.TableScanners {
		// Match in archiving sort column order
		for _, columnID := range scanner.Schema.Schema.ArchivingSortColumns {
			filterIndex, exists := candidateFilters[tableID][columnID]
			if !exists {
				// Stop on first missing column
				break
			}
			// Equality
			if filterIndex[2] >= 0 {
				value, _, success := qc.extractFilter(filterIndex[2])
				if !success {
					// Stop if the value fails to be extracted
					break
				}
				scanner.EqualityPrefilterValues = append(
					scanner.EqualityPrefilterValues, value)
				qc.Prefilters = append(qc.Prefilters, filterIndex[2])
				scanner.ColumnUsages[columnID] |= columnUsedByPrefilter
				// Continue matching the next column
				continue
			}
			// Lower bound
			if filterIndex[0] >= 0 {
				value, boundaryType, success := qc.extractFilter(filterIndex[0])
				if success {
					scanner.RangePrefilterValues[0] = value
					scanner.RangePrefilterBoundaries[0] = boundaryType
					qc.Prefilters = append(qc.Prefilters, filterIndex[0])
					scanner.ColumnUsages[columnID] |= columnUsedByPrefilter
				}
			}
			// Upper bound
			if filterIndex[1] >= 0 {
				value, boundaryType, success := qc.extractFilter(filterIndex[1])
				if success {
					scanner.RangePrefilterValues[1] = value
					scanner.RangePrefilterBoundaries[1] = boundaryType
					qc.Prefilters = append(qc.Prefilters, filterIndex[1])
					scanner.ColumnUsages[columnID] |= columnUsedByPrefilter
				}
			}
			// Stop after the first range filter
			break
		}
	}

	sort.Ints(qc.Prefilters)
}

// columnUsageCollector is the visitor used to traverses an AST, finds VarRef columns
// and sets the usage bits in tableScanners. The VarRef nodes must have already
// been resolved and annotated with TableID and ColumnID.
type columnUsageCollector struct {
	tableScanners []*TableScanner
	usages        columnUsage
}

func (c columnUsageCollector) Visit(expression expr.Expr) expr.Visitor {
	switch e := expression.(type) {
	case *expr.VarRef:
		c.tableScanners[e.TableID].ColumnUsages[e.ColumnID] |= c.usages
	}
	return c
}

// foreignTableColumnDetector detects foreign table columns involved in AST
type foreignTableColumnDetector struct {
	hasForeignTableColumn bool
}

func (c *foreignTableColumnDetector) Visit(expression expr.Expr) expr.Visitor {
	switch e := expression.(type) {
	case *expr.VarRef:
		c.hasForeignTableColumn = c.hasForeignTableColumn || (e.TableID > 0)
	}
	return c
}

// processFilters processes all filters and categorize them into common filters,
// prefilters, and time filters. It also collect column usages from the filters.
func (qc *AQLQueryContext) processFilters() {
	// OOPK engine only supports one measure per query.
	if len(qc.Query.Measures) != 1 {
		qc.Error = utils.StackError(nil, "expect one measure per query, but got %d",
			len(qc.Query.Measures))
		return
	}

	// Categorize common filters and prefilters based on matched prefilters.
	commonFilters := qc.Query.Measures[0].FiltersParsed
	prefilters := qc.Prefilters
	for index, filter := range qc.Query.FiltersParsed {
		if len(prefilters) == 0 || prefilters[0] > index {
			// common filters
			commonFilters = append(commonFilters, filter)
		} else {
			qc.OOPK.Prefilters = append(qc.OOPK.Prefilters, filter)
			prefilters = prefilters[1:]
		}
	}

	var geoFilterFound bool
	for _, filter := range commonFilters {
		foreignTableColumnDetector := foreignTableColumnDetector{}
		expr.Walk(&foreignTableColumnDetector, filter)
		if foreignTableColumnDetector.hasForeignTableColumn {
			var isGeoFilter bool
			if qc.OOPK.geoIntersection != nil {
				geoTableID := qc.OOPK.geoIntersection.shapeTableID
				joinSchema := qc.TableSchemaByName[qc.Query.Joins[geoTableID-1].Table]
				isGeoFilter = qc.matchGeoFilter(filter, geoTableID, joinSchema, geoFilterFound)
				if qc.Error != nil {
					return
				}
			}

			if !isGeoFilter {
				qc.OOPK.ForeignTableCommonFilters = append(qc.OOPK.ForeignTableCommonFilters, filter)
			} else {
				geoFilterFound = true
			}
		} else {
			qc.OOPK.MainTableCommonFilters = append(qc.OOPK.MainTableCommonFilters, filter)
		}
	}

	if qc.OOPK.geoIntersection != nil && !geoFilterFound {
		qc.Error = utils.StackError(nil, "Exact one geo filter is needed if geo intersection"+
			" is used during join")
		return
	}

	// Process time filter.
	qc.processTimeFilter()
	if qc.Error != nil {
		return
	}

	// Collect column usages from the filters.
	for _, f := range qc.OOPK.MainTableCommonFilters {
		expr.Walk(columnUsageCollector{
			tableScanners: qc.TableScanners,
			usages:        columnUsedByAllBatches,
		}, f)
	}

	for _, f := range qc.OOPK.ForeignTableCommonFilters {
		expr.Walk(columnUsageCollector{
			tableScanners: qc.TableScanners,
			usages:        columnUsedByAllBatches,
		}, f)
	}

	for _, f := range qc.OOPK.Prefilters {
		expr.Walk(columnUsageCollector{
			tableScanners: qc.TableScanners,
			usages:        columnUsedByLiveBatches,
		}, f)
	}

	if qc.OOPK.TimeFilters[0] != nil {
		expr.Walk(columnUsageCollector{
			tableScanners: qc.TableScanners,
			usages:        columnUsedByFirstArchiveBatch | columnUsedByLiveBatches,
		}, qc.OOPK.TimeFilters[0])
	}

	if qc.OOPK.TimeFilters[1] != nil {
		expr.Walk(columnUsageCollector{
			tableScanners: qc.TableScanners,
			usages:        columnUsedByLastArchiveBatch | columnUsedByLiveBatches,
		}, qc.OOPK.TimeFilters[1])
	}
}

func getStrFromNumericalOrStrLiteral(e expr.Expr) (string, error) {
	var str string
	if strExpr, ok := e.(*expr.StringLiteral); ok {
		str = strExpr.Val
	} else {
		if numExpr, ok := e.(*expr.NumberLiteral); ok {
			str = numExpr.String()
		} else {
			return str, utils.StackError(nil,
				"Unable to extract string from %s", e.String())
		}
	}
	return str, nil
}

// matchGeoFilter tries to match the filter as a geo filter and prepare shapeUUIDs for aql processor. It returns whether
// the filterExpr is a geo filter.
func (qc *AQLQueryContext) matchGeoFilter(filterExpr expr.Expr, joinTableID int,
	joinSchema *memCom.TableSchema, geoFilterFound bool) (geoFilterFoundInCurrentExpr bool) {
	var shapeUUIDs []string
	invalidOpsFound, geoFilterFoundInCurrentExpr := qc.matchGeoFilterHelper(filterExpr, joinTableID, joinSchema, &shapeUUIDs)
	if qc.Error != nil {
		return
	}
	if geoFilterFoundInCurrentExpr && invalidOpsFound {
		qc.Error = utils.StackError(nil, "Only EQ and IN allowed for geo filters")
		return
	}
	if geoFilterFoundInCurrentExpr && geoFilterFound {
		qc.Error = utils.StackError(nil, "Only one geo filter is allowed")
		return
	}

	if len(shapeUUIDs) > geoShapeLimit {
		qc.Error = utils.StackError(nil, "At most %d gep shapes supported, got %d", geoShapeLimit, len(shapeUUIDs))
		return
	}

	if geoFilterFoundInCurrentExpr {
		qc.OOPK.geoIntersection.shapeUUIDs = shapeUUIDs
	}
	return
}

func (qc *AQLQueryContext) matchGeoFilterHelper(filterExpr expr.Expr, joinTableID int,
	joinSchema *memCom.TableSchema, shapeUUIDs *[]string) (inValidOpFound, foundGeoFilter bool) {
	switch e := filterExpr.(type) {
	case *expr.BinaryExpr:
		if e.Op == expr.OR {
			inValidOpFoundL, foundGeoFilterL := qc.matchGeoFilterHelper(e.LHS, joinTableID, joinSchema, shapeUUIDs)
			inValidOpFoundR, foundGeoFilterR := qc.matchGeoFilterHelper(e.RHS, joinTableID, joinSchema, shapeUUIDs)
			inValidOpFound = inValidOpFoundL || inValidOpFoundR
			foundGeoFilter = foundGeoFilterL || foundGeoFilterR
		} else if e.Op == expr.EQ {
			columnExpr := e.LHS

			if paren, ok := columnExpr.(*expr.ParenExpr); ok {
				columnExpr = paren.Expr
			}
			if column, ok := columnExpr.(*expr.VarRef); ok && column.TableID == joinTableID {
				// geo filter's column must be primary key.
				if joinSchema.Schema.PrimaryKeyColumns[0] != column.ColumnID {
					qc.Error = utils.StackError(nil, "Geo filter column is not the primary key")
					return
				}
				uuidStr, err := getStrFromNumericalOrStrLiteral(e.RHS)
				if err != nil {
					qc.Error = utils.StackError(err,
						"Unable to extract uuid from expression %s", e.RHS.String())
					return
				}
				normalizedUUID, err := utils.NormalizeUUIDString(uuidStr)
				if err != nil {
					qc.Error = err
					return
				}
				foundGeoFilter = true
				*shapeUUIDs = append(*shapeUUIDs, normalizedUUID)
			}
		} else {
			inValidOpFound = true
			// keep traversing to find geo fields
			_, foundGeoFilterL := qc.matchGeoFilterHelper(e.LHS, joinTableID, joinSchema, shapeUUIDs)
			_, foundGeoFilterR := qc.matchGeoFilterHelper(e.RHS, joinTableID, joinSchema, shapeUUIDs)
			foundGeoFilter = foundGeoFilterL || foundGeoFilterR
		}
	case *expr.UnaryExpr:
		inValidOpFound = true
		_, foundGeoFilter = qc.matchGeoFilterHelper(e.Expr, joinTableID, joinSchema, shapeUUIDs)
	}
	return
}

// processTimeFilter processes the time filter by matching it against the time
// column of the main fact table. The time filter will be identified as common
// filter if it does not match with the designated time column.
func (qc *AQLQueryContext) processTimeFilter() {
	from, to := qc.fromTime, qc.toTime

	// Match against time column of the main fact table.
	var timeColumnMatched bool

	tableColumnPair := strings.SplitN(qc.Query.TimeFilter.Column, ".", 2)
	if len(tableColumnPair) < 2 {
		qc.Query.TimeFilter.Column = tableColumnPair[0]
	} else {
		qc.Query.TimeFilter.Column = tableColumnPair[1]
		if tableColumnPair[0] != qc.Query.Table {
			qc.Error = utils.StackError(nil, "timeFilter only supports main table: %s, got: %s", qc.Query.Table, tableColumnPair[0])
			return
		}
	}

	if qc.TableScanners[0].Schema.Schema.IsFactTable {
		if from == nil {
			qc.Error = utils.StackError(nil, "'from' of time filter is missing")
			return
		}

		timeColumn := qc.TableScanners[0].Schema.Schema.Columns[0].Name
		if qc.Query.TimeFilter.Column == "" || qc.Query.TimeFilter.Column == timeColumn {
			timeColumnMatched = true
			qc.Query.TimeFilter.Column = timeColumn
		}
	}

	// TODO: resolve time filter column against foreign tables.
	timeColumnID := 0
	found := false
	if qc.Query.TimeFilter.Column != "" {
		// Validate column existence and type.
		timeColumnID, found = qc.TableScanners[0].Schema.ColumnIDs[qc.Query.TimeFilter.Column]
		if !found {
			qc.Error = utils.StackError(nil, "unknown time filter column %s",
				qc.Query.TimeFilter.Column)
			return
		}
		timeColumnType := qc.TableScanners[0].Schema.ValueTypeByColumn[timeColumnID]
		if timeColumnType != memCom.Uint32 {
			qc.Error = utils.StackError(nil,
				"expect time filter column %s of type Uint32, but got %s",
				qc.Query.TimeFilter.Column, memCom.DataTypeName[timeColumnType])
			return
		}
	}
	fromExpr, toExpr := common.CreateTimeFilterExpr(&expr.VarRef{
		Val:      qc.Query.TimeFilter.Column,
		ExprType: expr.Unsigned,
		TableID:  0,
		ColumnID: timeColumnID,
		DataType: memCom.Uint32,
	}, from, to)

	qc.TableScanners[0].ArchiveBatchIDEnd = int((utils.Now().Unix() + 86399) / 86400)
	if timeColumnMatched {
		qc.OOPK.TimeFilters[0] = fromExpr
		qc.OOPK.TimeFilters[1] = toExpr
		if from != nil {
			qc.TableScanners[0].ArchiveBatchIDStart = int(from.Time.Unix() / 86400)
		}
		if to != nil {
			qc.TableScanners[0].ArchiveBatchIDEnd = int((to.Time.Unix() + 86399) / 86400)
		}
	} else {
		if fromExpr != nil {
			qc.OOPK.MainTableCommonFilters = append(qc.OOPK.MainTableCommonFilters, fromExpr)
		}
		if toExpr != nil {
			qc.OOPK.MainTableCommonFilters = append(qc.OOPK.MainTableCommonFilters, toExpr)
		}
	}
}

// matchAndRewriteGeoDimension tells whether a dimension matches geo join and whether it's a valid
// geo join. It returns the rewritten geo dimension and error. If the err is non nil, it means it's a invalid geo join.
// A valid geo dimension can only in one of the following format:
// 	1. UUID
//  2. hex(UUID)
func (qc *AQLQueryContext) matchAndRewriteGeoDimension(dimExpr expr.Expr) (expr.Expr, error) {
	gc := &geoTableUsageCollector{
		geoIntersection: *qc.OOPK.geoIntersection,
	}

	expr.Walk(gc, dimExpr)
	if !gc.useGeoTable {
		return nil, nil
	}

	if callExpr, ok := dimExpr.(*expr.Call); ok {
		if callExpr.Name != expr.HexCallName {
			return nil, utils.StackError(nil,
				"Only hex function is supported on UUID type, but got %s", callExpr.Name)
		}

		if len(callExpr.Args) != 1 {
			return nil, utils.StackError(nil,
				"Exactly 1 argument allowed for hex, got %d", len(callExpr.Args))
		}

		dimExpr = callExpr.Args[0]
	}

	joinSchema := qc.TableSchemaByName[qc.Query.Joins[gc.geoIntersection.shapeTableID-1].Table]
	if varRefExpr, ok := dimExpr.(*expr.VarRef); ok {
		var err error
		if varRefExpr.ColumnID != joinSchema.Schema.PrimaryKeyColumns[0] {
			err = utils.StackError(nil, "Only geo uuid is allowed in dimensions")
		}

		varRefExpr.DataType = memCom.Uint8
		return varRefExpr, err
	}

	return nil, utils.StackError(nil, "Only hex(uuid) or uuid supported, got %s", dimExpr.String())
}

// geoTableUsageCollector traverses an AST expression tree, finds VarRef columns
// and check whether it uses any geo table columns.
type geoTableUsageCollector struct {
	geoIntersection geoIntersection
	useGeoTable     bool
}

func (g *geoTableUsageCollector) Visit(expression expr.Expr) expr.Visitor {
	switch e := expression.(type) {
	case *expr.VarRef:
		g.useGeoTable = g.useGeoTable || e.TableID == g.geoIntersection.shapeTableID
	}
	return g
}

func (qc *AQLQueryContext) processMeasure() {
	// OOPK engine only supports one measure per query.
	if len(qc.Query.Measures) != 1 {
		qc.Error = utils.StackError(nil, "expect one measure per query, but got %d",
			len(qc.Query.Measures))
		return
	}

	if _, ok := qc.Query.Measures[0].ExprParsed.(*expr.NumberLiteral); ok {
		qc.IsNonAggregationQuery = true
		// in case user forgot to provide limit
		if qc.Query.Limit == 0 {
			qc.Query.Limit = nonAggregationQueryLimit
		}
		return
	}

	// Match and strip the aggregate function.
	aggregate, ok := qc.Query.Measures[0].ExprParsed.(*expr.Call)
	if !ok {
		qc.Error = utils.StackError(nil, "expect aggregate function, but got %s",
			qc.Query.Measures[0].Expr)
		return
	}

	if qc.ReturnHLLData && aggregate.Name != expr.HllCallName {
		qc.Error = utils.StackError(nil, "expect hll aggregate function as client specify 'Accept' as "+
			"'application/hll', but got %s",
			qc.Query.Measures[0].Expr)
		return
	}

	if len(aggregate.Args) != 1 {
		qc.Error = utils.StackError(nil,
			"expect one parameter for aggregate function %s, but got %d",
			aggregate.Name, len(aggregate.Args))
		return
	}
	qc.OOPK.Measure = aggregate.Args[0]
	// default is 4 bytes
	qc.OOPK.MeasureBytes = 4
	switch strings.ToLower(aggregate.Name) {
	case expr.CountCallName:
		qc.OOPK.Measure = &expr.NumberLiteral{
			Int:      1,
			Expr:     "1",
			ExprType: expr.Unsigned,
		}
		qc.OOPK.AggregateType = C.AGGR_SUM_UNSIGNED
	case expr.SumCallName:
		qc.OOPK.MeasureBytes = 8
		switch qc.OOPK.Measure.Type() {
		case expr.Float:
			qc.OOPK.AggregateType = C.AGGR_SUM_FLOAT
		case expr.Signed:
			qc.OOPK.AggregateType = C.AGGR_SUM_SIGNED
		case expr.Unsigned:
			qc.OOPK.AggregateType = C.AGGR_SUM_UNSIGNED
		default:
			qc.Error = utils.StackError(nil,
				unsupportedInputType, expr.SumCallName, qc.OOPK.Measure.String())
			return
		}
	case expr.AvgCallName:
		// 4 bytes for storing average result and another 4 byte for count
		qc.OOPK.MeasureBytes = 8
		// for average, we should always use float type as the agg type.
		qc.OOPK.AggregateType = C.AGGR_AVG_FLOAT
	case expr.MinCallName:
		switch qc.OOPK.Measure.Type() {
		case expr.Float:
			qc.OOPK.AggregateType = C.AGGR_MIN_FLOAT
		case expr.Signed:
			qc.OOPK.AggregateType = C.AGGR_MIN_SIGNED
		case expr.Unsigned:
			qc.OOPK.AggregateType = C.AGGR_MIN_UNSIGNED
		default:
			qc.Error = utils.StackError(nil,
				unsupportedInputType, expr.MinCallName, qc.OOPK.Measure.String())
			return
		}
	case expr.MaxCallName:
		switch qc.OOPK.Measure.Type() {
		case expr.Float:
			qc.OOPK.AggregateType = C.AGGR_MAX_FLOAT
		case expr.Signed:
			qc.OOPK.AggregateType = C.AGGR_MAX_SIGNED
		case expr.Unsigned:
			qc.OOPK.AggregateType = C.AGGR_MAX_UNSIGNED
		default:
			qc.Error = utils.StackError(nil,
				unsupportedInputType, expr.MaxCallName, qc.OOPK.Measure.String())
			return
		}
	case expr.HllCallName:
		qc.OOPK.AggregateType = C.AGGR_HLL
	default:
		qc.Error = utils.StackError(nil,
			"unsupported aggregate function: %s", aggregate.Name)
		return
	}
}

func (qc *AQLQueryContext) getAllColumnsDimension() (columns []common.Dimension) {
	// only main table columns wildcard match supported
	for _, column := range qc.TableScanners[0].Schema.Schema.Columns {
		if !column.Deleted && column.Type != metaCom.GeoShape {
			columns = append(columns, common.Dimension{
				ExprParsed: &expr.VarRef{Val: column.Name},
				Expr:       column.Name,
			})
		}
	}
	return
}

func (qc *AQLQueryContext) processDimensions() {
	// Copy dimension ASTs.
	qc.OOPK.Dimensions = make([]expr.Expr, len(qc.Query.Dimensions))
	for i, dim := range qc.Query.Dimensions {
		// TODO: support numeric bucketizer.
		qc.OOPK.Dimensions[i] = dim.ExprParsed
		if dim.ExprParsed.Type() == expr.GeoShape {
			qc.Error = utils.StackError(nil,
				"GeoShape can not be used for dimension: %s", dim.Expr)
			return
		}
	}

	if qc.OOPK.geoIntersection != nil {
		gc := &geoTableUsageCollector{
			geoIntersection: *qc.OOPK.geoIntersection,
		}
		// Check whether measure and dimensions are referencing any geo table columns.
		expr.Walk(gc, qc.OOPK.Measure)

		if gc.useGeoTable {
			qc.Error = utils.StackError(nil,
				"Geo table column is not allowed to be used in measure: %s", qc.OOPK.Measure.String())
			return
		}

		foundGeoJoin := false
		for i, dimExpr := range qc.OOPK.Dimensions {
			geoDimExpr, err := qc.matchAndRewriteGeoDimension(dimExpr)
			if err != nil {
				qc.Error = err
				return
			}

			if geoDimExpr != nil {
				if foundGeoJoin {
					qc.Error = utils.StackError(nil,
						"Only one geo dimension allowed: %s", dimExpr.String())
					return
				}
				foundGeoJoin = true
				qc.OOPK.Dimensions[i] = geoDimExpr
				qc.OOPK.geoIntersection.dimIndex = i
			}
		}
	}

	// Collect column usage from measure and dimensions
	expr.Walk(columnUsageCollector{
		tableScanners: qc.TableScanners,
		usages:        columnUsedByAllBatches,
	}, qc.OOPK.Measure)

	for _, dim := range qc.OOPK.Dimensions {
		expr.Walk(columnUsageCollector{
			tableScanners: qc.TableScanners,
			usages:        columnUsedByAllBatches,
		}, dim)
	}
}

// Sort dimension columns based on the data width in bytes
// dimension columns in OOPK will not be reordered, but a mapping
// from original id to ordered offsets (value and validity) in
// dimension vector will be stored.
// GeoUUID dimension will be 1 bytes. VarRef expression will use column data length,
// others will be default to 4 bytes.
func (qc *AQLQueryContext) sortDimensionColumns() {
	orderedIndex := 0
	numDimensions := len(qc.OOPK.Dimensions)
	qc.OOPK.DimensionVectorIndex = make([]int, numDimensions)
	byteWidth := 1 << uint(len(qc.OOPK.NumDimsPerDimWidth)-1)
	for byteIndex := range qc.OOPK.NumDimsPerDimWidth {
		for originIndex, dim := range qc.OOPK.Dimensions {
			dataBytes := common.GetDimensionDataBytes(dim)
			if dataBytes == byteWidth {
				// record value offset, null offset pair
				// null offsets will have to add total dim bytes later
				qc.OOPK.DimensionVectorIndex[originIndex] = orderedIndex
				qc.OOPK.NumDimsPerDimWidth[byteIndex]++
				qc.OOPK.DimRowBytes += dataBytes
				orderedIndex++
			}
		}
		byteWidth >>= 1
	}
	// plus one byte per dimension column for validity
	qc.OOPK.DimRowBytes += numDimensions

	if !qc.IsNonAggregationQuery {
		// no dimension size checking for non-aggregation query
		if qc.OOPK.DimRowBytes > C.MAX_DIMENSION_BYTES {
			qc.Error = utils.StackError(nil, "maximum dimension bytes: %d, got: %d", C.MAX_DIMENSION_BYTES, qc.OOPK.DimRowBytes)
			return
		}
	}
}

func (qc *AQLQueryContext) sortUsedColumns() {
	for _, scanner := range qc.TableScanners {
		scanner.Columns = make([]int, 0, len(scanner.ColumnUsages))
		scanner.ColumnsByIDs = make(map[int]int)
		// Unsorted/uncompressed columns
		for columnID := range scanner.ColumnUsages {
			if utils.IndexOfInt(scanner.Schema.Schema.ArchivingSortColumns, columnID) < 0 {
				scanner.ColumnsByIDs[columnID] = len(scanner.Columns)
				scanner.Columns = append(scanner.Columns, columnID)
			}
		}
		// Sorted/compressed columns
		for i := len(scanner.Schema.Schema.ArchivingSortColumns) - 1; i >= 0; i-- {
			columnID := scanner.Schema.Schema.ArchivingSortColumns[i]
			_, found := scanner.ColumnUsages[columnID]
			if found {
				scanner.ColumnsByIDs[columnID] = len(scanner.Columns)
				scanner.Columns = append(scanner.Columns, columnID)
			}
		}
	}
}

func parseTimezoneColumnString(timezoneColumnString string) (column, joinKey string, success bool) {
	exp, err := expr.ParseExpr(timezoneColumnString)
	if err != nil {
		return
	}
	if c, ok := exp.(*expr.Call); ok {
		if len(c.Args) == 1 {
			return c.Name, c.Args[0].String(), true
		}
	}
	return
}

func (qc *AQLQueryContext) expandINop(e *expr.BinaryExpr) (expandedExpr expr.Expr) {
	lhs, ok := e.LHS.(*expr.VarRef)
	if !ok {
		qc.Error = utils.StackError(nil, "lhs of IN or NOT_IN must be a valid column")
	}
	rhs := e.RHS
	switch rhsTyped := rhs.(type) {
	case *expr.Call:
		expandedExpr = &expr.BooleanLiteral{Val: false}
		for _, value := range rhsTyped.Args {
			switch expandedExpr.(type) {
			case *expr.BooleanLiteral:
				expandedExpr = qc.Rewrite(&expr.BinaryExpr{
					Op:  expr.EQ,
					LHS: lhs,
					RHS: value,
				}).(*expr.BinaryExpr)
			default:
				lastExpr := expandedExpr
				expandedExpr = &expr.BinaryExpr{
					Op:  expr.OR,
					LHS: lastExpr,
					RHS: qc.Rewrite(&expr.BinaryExpr{
						Op:  expr.EQ,
						LHS: lhs,
						RHS: value,
					}).(*expr.BinaryExpr),
				}
			}
		}
		break
	default:
		qc.Error = utils.StackError(nil, "only EQ and IN operators are supported for geo fields")
	}
	return
}
