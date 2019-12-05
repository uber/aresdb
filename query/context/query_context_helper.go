package context

import (
	"strings"
	"strconv"
	"github.com/uber/aresdb/query/common"
	"github.com/uber/aresdb/query/expr"
	memCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/utils"

)

type QueryContextHelper struct {
	QCOptions QueryContextOptions
}

func (qc *QueryContextHelper) NormalizeAndFilters(filters []expr.Expr) []expr.Expr {
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

// resolveColumn resolves the VarRef identifier against the schema,
// and returns the matched tableID (query scoped) and columnID (schema scoped).
func (qc *QueryContextHelper) ResolveColumn(identifier string) (int, int, error) {
	tableAlias := qc.QCOptions.GetQuery().Table
	column := identifier
	segments := strings.SplitN(identifier, ".", 2)
	if len(segments) == 2 {
		tableAlias = segments[0]
		column = segments[1]
	}

	tableID, exists := qc.QCOptions.GetTableID(tableAlias)
	if !exists {
		return 0, 0, utils.StackError(nil, "unknown table alias %s", tableAlias)
	}

	columnID, exists := qc.QCOptions.GetSchema(tableID).ColumnIDs[column]
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

func blockInt64(expressions ...expr.Expr) error {
	for _, expression := range expressions {
		if varRef, isVarRef := expression.(*expr.VarRef); isVarRef && memCom.Int64 == varRef.DataType {
			return utils.StackError(nil, "binary transformation not allowed for int64 fields, got %s", expression.String())
		}
	}
	return nil
}

func (qc *QueryContextHelper) expandINop(e *expr.BinaryExpr) (expandedExpr expr.Expr) {
	lhs, ok := e.LHS.(*expr.VarRef)
	if !ok {
		qc.QCOptions.SetError(utils.StackError(nil, "lhs of IN or NOT_IN must be a valid column"))
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
		qc.QCOptions.SetError(utils.StackError(nil, "only EQ and IN operators are supported for geo fields"))
	}
	return
}

// Rewrite walks the expresison AST and resolves data types bottom up.
// In addition it also translates enum strings and rewrites their predicates.
func (qc *QueryContextHelper) Rewrite(expression expr.Expr) expr.Expr {
	switch e := expression.(type) {
	case *expr.ParenExpr:
		// Strip parenthesis from the input
		return e.Expr
	case *expr.VarRef:
		tableID, columnID, err := qc.ResolveColumn(e.Val)
		if err != nil {
			qc.QCOptions.SetError(err)
			return expression
		}
		column := qc.QCOptions.GetSchema(tableID).Schema.Columns[columnID]
		if column.Deleted {
			qc.QCOptions.SetError(utils.StackError(nil, "column %s of table %s has been deleted",
				column.Name, qc.QCOptions.GetSchema(tableID).Schema.Name))
			return expression
		}
		dataType := qc.QCOptions.GetSchema(tableID).ValueTypeByColumn[columnID]
		e.ExprType = common.DataTypeToExprType[dataType]
		e.TableID = tableID
		e.ColumnID = columnID
		dict := qc.QCOptions.GetSchema(tableID).EnumDicts[column.Name]
		e.EnumDict = dict.Dict
		e.EnumReverseDict = dict.ReverseDict
		e.DataType = dataType
		e.IsHLLColumn = column.HLLConfig.IsHLLColumn
	case *expr.UnaryExpr:
		if expr.IsUUIDColumn(e.Expr) && e.Op != expr.GET_HLL_VALUE {
			qc.QCOptions.SetError(utils.StackError(nil, "uuid column type only supports countdistincthll unary expression"))
			return expression
		}

		if err := blockNumericOpsForColumnOverFourBytes(e.Op, e.Expr); err != nil {
			qc.QCOptions.SetError(err)
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
				qc.QCOptions.SetError(utils.StackError(nil, "Not %s condition is not allowed", expr.GeographyIntersectsCallName))
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
			qc.QCOptions.SetError(utils.StackError(nil, "unsupported unary expression %s", e.String()))
		}
	case *expr.BinaryExpr:
		if err := blockNumericOpsForColumnOverFourBytes(e.Op, e.LHS, e.RHS); err != nil {
			qc.QCOptions.SetError(err)
			return expression
		}

		// TODO: @shz support int64 binary transform
		if err := blockInt64(e.LHS, e.RHS); err != nil {
			qc.QCOptions.SetError(err)
			return expression
		}

		if e.Op != expr.EQ && e.Op != expr.NEQ {
			_, isRHSStr := e.RHS.(*expr.StringLiteral)
			_, isLHSStr := e.LHS.(*expr.StringLiteral)
			if isRHSStr || isLHSStr {
				qc.QCOptions.SetError(utils.StackError(nil, "string type only support EQ and NEQ operators"))
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

			if rhs != nil && e.LHS.Type() == expr.GeoPoint {
				if val, err := memCom.GeoPointFromString(rhs.Val); err != nil {
					qc.QCOptions.SetError(err)
				} else {
					e.RHS = &expr.GeopointLiteral{
						Val: val,
					}
				}
			} else if rhs != nil && e.LHS.Type() == expr.UUID {
				if val, err := memCom.UUIDFromString(rhs.Val); err != nil {
					qc.QCOptions.SetError(err)

				} else {
					e.RHS = &expr.UUIDLiteral{
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
			qc.QCOptions.SetError(utils.StackError(nil, "unsupported binary expression %s", e.String()))
		}
	case *expr.Call:
		e.Name = strings.ToLower(e.Name)
		switch e.Name {
		case expr.ConvertTzCallName:
			if len(e.Args) != 3 {
				qc.QCOptions.SetError(utils.StackError(
					nil, "convert_tz must have 3 arguments",
				))
				break
			}
			fromTzStringExpr, isStrLiteral := e.Args[1].(*expr.StringLiteral)
			if !isStrLiteral {
				qc.QCOptions.SetError(utils.StackError(nil, "2nd argument of convert_tz must be a string"))
				break
			}
			toTzStringExpr, isStrLiteral := e.Args[2].(*expr.StringLiteral)
			if !isStrLiteral {
				qc.QCOptions.SetError(utils.StackError(nil, "3rd argument of convert_tz must be a string"))
				break
			}
			fromTz, err := common.ParseTimezone(fromTzStringExpr.Val)
			if err != nil {
				qc.QCOptions.SetError(utils.StackError(err, "failed to rewrite convert_tz"))
				break
			}
			toTz, err := common.ParseTimezone(toTzStringExpr.Val)
			if err != nil {
				qc.QCOptions.SetError(utils.StackError(err, "failed to rewrite convert_tz"))
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
				qc.QCOptions.SetError(utils.StackError(nil, "dayofweek takes exactly 1 argument"))
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
				qc.QCOptions.SetError(utils.StackError(nil, timeColumnDivideErrMsg))
				break
			}
			divisor, isLiteral := timeColDivide.RHS.(*expr.NumberLiteral)
			if !isLiteral || divisor.Int != 1000 {
				qc.QCOptions.SetError(utils.StackError(nil, timeColumnDivideErrMsg))
				break
			}
			if par, isParen := timeColDivide.LHS.(*expr.ParenExpr); isParen {
				timeColDivide.LHS = par.Expr
			}
			timeColExpr, isVarRef := timeColDivide.LHS.(*expr.VarRef)
			if !isVarRef {
				qc.QCOptions.SetError(utils.StackError(nil, timeColumnDivideErrMsg))
				break
			}
			return timeColExpr
		case expr.HourCallName:
			if len(e.Args) != 1 {
				qc.QCOptions.SetError(utils.StackError(nil, "hour takes exactly 1 argument"))
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
				qc.QCOptions.SetError(utils.StackError(
					nil, "expect 2 argument for %s, but got %s", e.Name, e.String()))
				break
			}

			lhsRef, isVarRef := e.Args[0].(*expr.VarRef)
			if !isVarRef || (lhsRef.DataType != memCom.GeoShape && lhsRef.DataType != memCom.GeoPoint) {
				qc.QCOptions.SetError(utils.StackError(
					nil, "expect argument to be a valid geo shape or geo point column for %s, but got %s of type %s",
					e.Name, e.Args[0].String(), memCom.DataTypeName[lhsRef.DataType]))
				break
			}

			lhsGeoPoint := lhsRef.DataType == memCom.GeoPoint

			rhsRef, isVarRef := e.Args[1].(*expr.VarRef)
			if !isVarRef || (rhsRef.DataType != memCom.GeoShape && rhsRef.DataType != memCom.GeoPoint) {
				qc.QCOptions.SetError(utils.StackError(
					nil, "expect argument to be a valid geo shape or geo point column for %s, but got %s of type %s",
					e.Name, e.Args[1].String(), memCom.DataTypeName[rhsRef.DataType]))
				break
			}

			rhsGeoPoint := rhsRef.DataType == memCom.GeoPoint

			if lhsGeoPoint == rhsGeoPoint {
				qc.QCOptions.SetError(utils.StackError(
					nil, "expect exactly one geo shape column and one geo point column for %s, got %s",
					e.Name, e.String()))
				break
			}

			// Switch geo point so that lhs is geo shape and rhs is geo point
			if lhsGeoPoint {
				e.Args[0], e.Args[1] = e.Args[1], e.Args[0]
			}

			e.ExprType = expr.Boolean
		case expr.HexCallName:
			if len(e.Args) != 1 {
				qc.QCOptions.SetError(utils.StackError(
					nil, "expect 1 argument for %s, but got %s", e.Name, e.String()))
				break
			}
			colRef, isVarRef := e.Args[0].(*expr.VarRef)
			if !isVarRef || colRef.DataType != memCom.UUID {
				qc.QCOptions.SetError(utils.StackError(
					nil, "expect 1 argument to be a valid uuid column for %s, but got %s of type %s",
					e.Name, e.Args[0].String(), memCom.DataTypeName[colRef.DataType]))
				break
			}
			e.ExprType = e.Args[0].Type()
		case expr.CountDistinctHllCallName:
			if len(e.Args) != 1 {
				qc.QCOptions.SetError(utils.StackError(
					nil, "expect 1 argument for %s, but got %s", e.Name, e.String()))
				break
			}
			colRef, isVarRef := e.Args[0].(*expr.VarRef)
			if !isVarRef {
				qc.QCOptions.SetError(utils.StackError(
					nil, "expect 1 argument to be a column for %s", e.Name))
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
				qc.QCOptions.SetError(utils.StackError(
					nil, "expect 1 argument for %s, but got %s", e.Name, e.String()))
				break
			}
			colRef, isVarRef := e.Args[0].(*expr.VarRef)
			if !isVarRef || colRef.DataType != memCom.Uint32 {
				qc.QCOptions.SetError(utils.StackError(
					nil, "expect 1 argument to be a valid hll column for %s, but got %s of type %s",
					e.Name, e.Args[0].String(), memCom.DataTypeName[colRef.DataType]))
				break
			}
			e.ExprType = e.Args[0].Type()
		case expr.SumCallName, expr.MinCallName, expr.MaxCallName, expr.AvgCallName:
			if len(e.Args) != 1 {
				qc.QCOptions.SetError(utils.StackError(
					nil, "expect 1 argument for %s, but got %s", e.Name, e.String()))
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
				qc.QCOptions.SetError(utils.StackError(
					nil, "array function %s requires arguments", e.Name))
				break
			}
			firstArg := e.Args[0]
			vr, ok := firstArg.(*expr.VarRef)
			if !ok || !memCom.IsArrayType(vr.DataType) {
				qc.QCOptions.SetError(utils.StackError(
					nil, "array function %s requires first argument to be array type column, but got %s", e.Name, firstArg))
			}

			if e.Name == expr.LengthCallName {
				if len(e.Args) != 1 {
					qc.QCOptions.SetError(utils.StackError(
						nil, "array function %s takes exactly 1 argument", e.Name))
					break
				}
				return &expr.UnaryExpr{
					Op:       expr.ARRAY_LENGTH,
					ExprType: expr.Unsigned,
					Expr:     vr,
				}
			} else if e.Name == expr.ContainsCallName {
				if len(e.Args) != 2 {
					qc.QCOptions.SetError(utils.StackError(
						nil, "array function %s takes exactly 2 arguments", e.Name))
					break
				}

				secondArg := e.Args[1]
				var literalExpr expr.Expr
				// build rhs literal
				t := memCom.GetElementDataType(vr.DataType)
				switch t {
				case memCom.Bool:
					ok := false
					literalExpr, ok = secondArg.(*expr.BooleanLiteral)
					if !ok {
						qc.QCOptions.SetError(utils.StackError(
							nil, "array function %s argument type mismatch", e.Name))
						break
					}
				case memCom.SmallEnum, memCom.BigEnum:
					if qc.QCOptions.IsDataOnly() {
						// if the request is from broker, it should be already a number literal
						literalExpr, ok = secondArg.(*expr.NumberLiteral)
						if !ok {
							qc.QCOptions.SetError(utils.StackError(
								nil, "array function %s argument type mismatch, expecting number literal", e.Name))
						}
						break
					}
					strLiteral, ok := secondArg.(*expr.StringLiteral)
					if !ok {
						qc.QCOptions.SetError(utils.StackError(
							nil, "array function %s argument type mismatch, expecting string literal", e.Name))
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
						qc.QCOptions.SetError(utils.StackError(
							nil, "array function %s argument type mismatch, expecting string literal", e.Name))
						break
					}
					val, err := memCom.GeoPointFromString(strLiteral.Val)
					if err != nil {
						qc.QCOptions.SetError(err)
						break
					}
					literalExpr = &expr.GeopointLiteral{
						Val: val,
					}
				case memCom.UUID:
					strLiteral, ok := secondArg.(*expr.StringLiteral)
					if !ok {
						qc.QCOptions.SetError(utils.StackError(nil, "array function %s needs uuid string literal", e.Name))
						break
					}
					val, err := memCom.UUIDFromString(strLiteral.Val)
					if err != nil {
						qc.QCOptions.SetError(err)
						break
					}
					literalExpr = &expr.UUIDLiteral{
						Val: val,
					}
				case memCom.Uint8, memCom.Uint16, memCom.Uint32, memCom.Int8, memCom.Int16, memCom.Int32, memCom.Float32:
					ok := false
					literalExpr, ok = secondArg.(*expr.NumberLiteral)
					if !ok {
						qc.QCOptions.SetError(utils.StackError(
							nil, "array function %s argument type mismatch", e.Name))
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
					qc.QCOptions.SetError(utils.StackError(
						nil, "array function %s takes exactly 2 arguments", e.Name))
					break
				}
				if _, ok := e.Args[1].(*expr.NumberLiteral); !ok {
					qc.QCOptions.SetError(utils.StackError(
						nil, "array function %s takes array type column and an index", e.Name))
				}
				return &expr.BinaryExpr{
					Op:       expr.ARRAY_ELEMENT_AT,
					ExprType: common.DataTypeToExprType[memCom.GetElementDataType(vr.DataType)],
					LHS:      vr,
					RHS:      e.Args[1],
				}
			}

		default:
			qc.QCOptions.SetError(utils.StackError(nil, "unknown function %s", e.Name))
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
