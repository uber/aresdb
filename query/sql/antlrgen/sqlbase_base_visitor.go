// Code generated from query/sql/SqlBase.g4 by ANTLR 4.7.1. DO NOT EDIT.

package antlrgen // SqlBase
import "github.com/antlr/antlr4/runtime/Go/antlr"

type BaseSqlBaseVisitor struct {
	*antlr.BaseParseTreeVisitor
}

func (v *BaseSqlBaseVisitor) VisitSingleStatement(ctx *SingleStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitSingleExpression(ctx *SingleExpressionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitStatementDefault(ctx *StatementDefaultContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitUse(ctx *UseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitCreateSchema(ctx *CreateSchemaContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitDropSchema(ctx *DropSchemaContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitRenameSchema(ctx *RenameSchemaContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitCreateTableAsSelect(ctx *CreateTableAsSelectContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitCreateTable(ctx *CreateTableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitDropTable(ctx *DropTableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitInsertInto(ctx *InsertIntoContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitDelete(ctx *DeleteContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitRenameTable(ctx *RenameTableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitRenameColumn(ctx *RenameColumnContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitDropColumn(ctx *DropColumnContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitAddColumn(ctx *AddColumnContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitCreateView(ctx *CreateViewContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitDropView(ctx *DropViewContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitCall(ctx *CallContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitGrant(ctx *GrantContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitRevoke(ctx *RevokeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitShowGrants(ctx *ShowGrantsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitExplain(ctx *ExplainContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitShowCreateTable(ctx *ShowCreateTableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitShowCreateView(ctx *ShowCreateViewContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitShowTables(ctx *ShowTablesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitShowSchemas(ctx *ShowSchemasContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitShowCatalogs(ctx *ShowCatalogsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitShowColumns(ctx *ShowColumnsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitShowStats(ctx *ShowStatsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitShowStatsForQuery(ctx *ShowStatsForQueryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitShowFunctions(ctx *ShowFunctionsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitShowSession(ctx *ShowSessionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitSetSession(ctx *SetSessionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitResetSession(ctx *ResetSessionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitShowPartitions(ctx *ShowPartitionsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitPrepare(ctx *PrepareContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitDeallocate(ctx *DeallocateContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitExecute(ctx *ExecuteContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitDescribeInput(ctx *DescribeInputContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitDescribeOutput(ctx *DescribeOutputContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitQuery(ctx *QueryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitWith(ctx *WithContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitTableElement(ctx *TableElementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitColumnDefinition(ctx *ColumnDefinitionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitLikeClause(ctx *LikeClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitProperties(ctx *PropertiesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitProperty(ctx *PropertyContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitQueryNoWith(ctx *QueryNoWithContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitQueryTermDefault(ctx *QueryTermDefaultContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitSetOperation(ctx *SetOperationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitQueryPrimaryDefault(ctx *QueryPrimaryDefaultContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitTable(ctx *TableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitInlineTable(ctx *InlineTableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitSubquery(ctx *SubqueryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitSortItem(ctx *SortItemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitQuerySpecification(ctx *QuerySpecificationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitGroupBy(ctx *GroupByContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitSingleGroupingSet(ctx *SingleGroupingSetContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitRollup(ctx *RollupContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitCube(ctx *CubeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitMultipleGroupingSets(ctx *MultipleGroupingSetsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitGroupingExpressions(ctx *GroupingExpressionsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitGroupingSet(ctx *GroupingSetContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitNamedQuery(ctx *NamedQueryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitSetQuantifier(ctx *SetQuantifierContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitSelectSingle(ctx *SelectSingleContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitSelectAll(ctx *SelectAllContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitRelationDefault(ctx *RelationDefaultContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitJoinRelation(ctx *JoinRelationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitJoinType(ctx *JoinTypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitJoinCriteria(ctx *JoinCriteriaContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitSampledRelation(ctx *SampledRelationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitSampleType(ctx *SampleTypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitAliasedRelation(ctx *AliasedRelationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitColumnAliases(ctx *ColumnAliasesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitTableName(ctx *TableNameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitSubqueryRelation(ctx *SubqueryRelationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitUnnest(ctx *UnnestContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitLateral(ctx *LateralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitParenthesizedRelation(ctx *ParenthesizedRelationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitExpression(ctx *ExpressionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitLogicalNot(ctx *LogicalNotContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitBooleanDefault(ctx *BooleanDefaultContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitLogicalBinary(ctx *LogicalBinaryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitPredicated(ctx *PredicatedContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitComparison(ctx *ComparisonContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitQuantifiedComparison(ctx *QuantifiedComparisonContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitBetween(ctx *BetweenContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitInList(ctx *InListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitInSubquery(ctx *InSubqueryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitLike(ctx *LikeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitNullPredicate(ctx *NullPredicateContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitDistinctFrom(ctx *DistinctFromContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitValueExpressionDefault(ctx *ValueExpressionDefaultContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitConcatenation(ctx *ConcatenationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitArithmeticBinary(ctx *ArithmeticBinaryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitArithmeticUnary(ctx *ArithmeticUnaryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitAtTimeZone(ctx *AtTimeZoneContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitDereference(ctx *DereferenceContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitTypeConstructor(ctx *TypeConstructorContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitSpecialDateTimeFunction(ctx *SpecialDateTimeFunctionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitSubstring(ctx *SubstringContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitCast(ctx *CastContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitLambda(ctx *LambdaContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitParenthesizedExpression(ctx *ParenthesizedExpressionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitParameter(ctx *ParameterContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitNormalize(ctx *NormalizeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitIntervalLiteral(ctx *IntervalLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitNumericLiteral(ctx *NumericLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitBooleanLiteral(ctx *BooleanLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitSimpleCase(ctx *SimpleCaseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitColumnReference(ctx *ColumnReferenceContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitNullLiteral(ctx *NullLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitRowConstructor(ctx *RowConstructorContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitSubscript(ctx *SubscriptContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitSubqueryExpression(ctx *SubqueryExpressionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitBinaryLiteral(ctx *BinaryLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitCurrentUser(ctx *CurrentUserContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitExtract(ctx *ExtractContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitStringLiteral(ctx *StringLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitArrayConstructor(ctx *ArrayConstructorContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitFunctionCall(ctx *FunctionCallContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitExists(ctx *ExistsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitPosition(ctx *PositionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitSearchedCase(ctx *SearchedCaseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitGroupingOperation(ctx *GroupingOperationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitBasicStringLiteral(ctx *BasicStringLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitUnicodeStringLiteral(ctx *UnicodeStringLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitTimeZoneInterval(ctx *TimeZoneIntervalContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitTimeZoneString(ctx *TimeZoneStringContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitComparisonOperator(ctx *ComparisonOperatorContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitComparisonQuantifier(ctx *ComparisonQuantifierContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitBooleanValue(ctx *BooleanValueContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitInterval(ctx *IntervalContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitIntervalField(ctx *IntervalFieldContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitNormalForm(ctx *NormalFormContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitSqltype(ctx *SqltypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitTypeParameter(ctx *TypeParameterContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitBaseType(ctx *BaseTypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitWhenClause(ctx *WhenClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitFilter(ctx *FilterContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitExplainFormat(ctx *ExplainFormatContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitExplainType(ctx *ExplainTypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitPositionalArgument(ctx *PositionalArgumentContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitNamedArgument(ctx *NamedArgumentContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitPrivilege(ctx *PrivilegeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitQualifiedName(ctx *QualifiedNameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitUnquotedIdentifier(ctx *UnquotedIdentifierContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitQuotedIdentifier(ctx *QuotedIdentifierContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitBackQuotedIdentifier(ctx *BackQuotedIdentifierContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitDigitIdentifier(ctx *DigitIdentifierContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitDecimalLiteral(ctx *DecimalLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitDoubleLiteral(ctx *DoubleLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitIntegerLiteral(ctx *IntegerLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseVisitor) VisitNonReserved(ctx *NonReservedContext) interface{} {
	return v.VisitChildren(ctx)
}
