// Code generated from query/sql/SqlBase.g4 by ANTLR 4.7.1. DO NOT EDIT.

package antlrgen // SqlBase
import "github.com/antlr/antlr4/runtime/Go/antlr"

// A complete Visitor for a parse tree produced by SqlBaseParser.
type SqlBaseVisitor interface {
	antlr.ParseTreeVisitor

	// Visit a parse tree produced by SqlBaseParser#singleStatement.
	VisitSingleStatement(ctx *SingleStatementContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#singleExpression.
	VisitSingleExpression(ctx *SingleExpressionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#statementDefault.
	VisitStatementDefault(ctx *StatementDefaultContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#use.
	VisitUse(ctx *UseContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#createSchema.
	VisitCreateSchema(ctx *CreateSchemaContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#dropSchema.
	VisitDropSchema(ctx *DropSchemaContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#renameSchema.
	VisitRenameSchema(ctx *RenameSchemaContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#createTableAsSelect.
	VisitCreateTableAsSelect(ctx *CreateTableAsSelectContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#createTable.
	VisitCreateTable(ctx *CreateTableContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#dropTable.
	VisitDropTable(ctx *DropTableContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#insertInto.
	VisitInsertInto(ctx *InsertIntoContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#delete.
	VisitDelete(ctx *DeleteContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#renameTable.
	VisitRenameTable(ctx *RenameTableContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#renameColumn.
	VisitRenameColumn(ctx *RenameColumnContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#dropColumn.
	VisitDropColumn(ctx *DropColumnContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#addColumn.
	VisitAddColumn(ctx *AddColumnContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#createView.
	VisitCreateView(ctx *CreateViewContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#dropView.
	VisitDropView(ctx *DropViewContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#call.
	VisitCall(ctx *CallContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#grant.
	VisitGrant(ctx *GrantContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#revoke.
	VisitRevoke(ctx *RevokeContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#showGrants.
	VisitShowGrants(ctx *ShowGrantsContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#explain.
	VisitExplain(ctx *ExplainContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#showCreateTable.
	VisitShowCreateTable(ctx *ShowCreateTableContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#showCreateView.
	VisitShowCreateView(ctx *ShowCreateViewContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#showTables.
	VisitShowTables(ctx *ShowTablesContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#showSchemas.
	VisitShowSchemas(ctx *ShowSchemasContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#showCatalogs.
	VisitShowCatalogs(ctx *ShowCatalogsContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#showColumns.
	VisitShowColumns(ctx *ShowColumnsContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#showStats.
	VisitShowStats(ctx *ShowStatsContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#showStatsForQuery.
	VisitShowStatsForQuery(ctx *ShowStatsForQueryContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#showFunctions.
	VisitShowFunctions(ctx *ShowFunctionsContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#showSession.
	VisitShowSession(ctx *ShowSessionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#setSession.
	VisitSetSession(ctx *SetSessionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#resetSession.
	VisitResetSession(ctx *ResetSessionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#showPartitions.
	VisitShowPartitions(ctx *ShowPartitionsContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#prepare.
	VisitPrepare(ctx *PrepareContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#deallocate.
	VisitDeallocate(ctx *DeallocateContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#execute.
	VisitExecute(ctx *ExecuteContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#describeInput.
	VisitDescribeInput(ctx *DescribeInputContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#describeOutput.
	VisitDescribeOutput(ctx *DescribeOutputContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#query.
	VisitQuery(ctx *QueryContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#with.
	VisitWith(ctx *WithContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#tableElement.
	VisitTableElement(ctx *TableElementContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#columnDefinition.
	VisitColumnDefinition(ctx *ColumnDefinitionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#likeClause.
	VisitLikeClause(ctx *LikeClauseContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#properties.
	VisitProperties(ctx *PropertiesContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#property.
	VisitProperty(ctx *PropertyContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#queryNoWith.
	VisitQueryNoWith(ctx *QueryNoWithContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#queryTermDefault.
	VisitQueryTermDefault(ctx *QueryTermDefaultContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#setOperation.
	VisitSetOperation(ctx *SetOperationContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#queryPrimaryDefault.
	VisitQueryPrimaryDefault(ctx *QueryPrimaryDefaultContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#table.
	VisitTable(ctx *TableContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#inlineTable.
	VisitInlineTable(ctx *InlineTableContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#subquery.
	VisitSubquery(ctx *SubqueryContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#sortItem.
	VisitSortItem(ctx *SortItemContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#querySpecification.
	VisitQuerySpecification(ctx *QuerySpecificationContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#groupBy.
	VisitGroupBy(ctx *GroupByContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#singleGroupingSet.
	VisitSingleGroupingSet(ctx *SingleGroupingSetContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#rollup.
	VisitRollup(ctx *RollupContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#cube.
	VisitCube(ctx *CubeContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#multipleGroupingSets.
	VisitMultipleGroupingSets(ctx *MultipleGroupingSetsContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#groupingExpressions.
	VisitGroupingExpressions(ctx *GroupingExpressionsContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#groupingSet.
	VisitGroupingSet(ctx *GroupingSetContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#namedQuery.
	VisitNamedQuery(ctx *NamedQueryContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#setQuantifier.
	VisitSetQuantifier(ctx *SetQuantifierContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#selectSingle.
	VisitSelectSingle(ctx *SelectSingleContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#selectAll.
	VisitSelectAll(ctx *SelectAllContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#relationDefault.
	VisitRelationDefault(ctx *RelationDefaultContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#joinRelation.
	VisitJoinRelation(ctx *JoinRelationContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#joinType.
	VisitJoinType(ctx *JoinTypeContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#joinCriteria.
	VisitJoinCriteria(ctx *JoinCriteriaContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#sampledRelation.
	VisitSampledRelation(ctx *SampledRelationContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#sampleType.
	VisitSampleType(ctx *SampleTypeContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#aliasedRelation.
	VisitAliasedRelation(ctx *AliasedRelationContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#columnAliases.
	VisitColumnAliases(ctx *ColumnAliasesContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#tableName.
	VisitTableName(ctx *TableNameContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#subqueryRelation.
	VisitSubqueryRelation(ctx *SubqueryRelationContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#unnest.
	VisitUnnest(ctx *UnnestContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#lateral.
	VisitLateral(ctx *LateralContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#parenthesizedRelation.
	VisitParenthesizedRelation(ctx *ParenthesizedRelationContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#expression.
	VisitExpression(ctx *ExpressionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#logicalNot.
	VisitLogicalNot(ctx *LogicalNotContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#booleanDefault.
	VisitBooleanDefault(ctx *BooleanDefaultContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#logicalBinary.
	VisitLogicalBinary(ctx *LogicalBinaryContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#predicated.
	VisitPredicated(ctx *PredicatedContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#comparison.
	VisitComparison(ctx *ComparisonContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#quantifiedComparison.
	VisitQuantifiedComparison(ctx *QuantifiedComparisonContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#between.
	VisitBetween(ctx *BetweenContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#inList.
	VisitInList(ctx *InListContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#inSubquery.
	VisitInSubquery(ctx *InSubqueryContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#like.
	VisitLike(ctx *LikeContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#nullPredicate.
	VisitNullPredicate(ctx *NullPredicateContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#distinctFrom.
	VisitDistinctFrom(ctx *DistinctFromContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#valueExpressionDefault.
	VisitValueExpressionDefault(ctx *ValueExpressionDefaultContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#concatenation.
	VisitConcatenation(ctx *ConcatenationContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#arithmeticBinary.
	VisitArithmeticBinary(ctx *ArithmeticBinaryContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#arithmeticUnary.
	VisitArithmeticUnary(ctx *ArithmeticUnaryContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#atTimeZone.
	VisitAtTimeZone(ctx *AtTimeZoneContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#dereference.
	VisitDereference(ctx *DereferenceContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#typeConstructor.
	VisitTypeConstructor(ctx *TypeConstructorContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#specialDateTimeFunction.
	VisitSpecialDateTimeFunction(ctx *SpecialDateTimeFunctionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#substring.
	VisitSubstring(ctx *SubstringContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#cast.
	VisitCast(ctx *CastContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#lambda.
	VisitLambda(ctx *LambdaContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#parenthesizedExpression.
	VisitParenthesizedExpression(ctx *ParenthesizedExpressionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#parameter.
	VisitParameter(ctx *ParameterContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#normalize.
	VisitNormalize(ctx *NormalizeContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#intervalLiteral.
	VisitIntervalLiteral(ctx *IntervalLiteralContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#numericLiteral.
	VisitNumericLiteral(ctx *NumericLiteralContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#booleanLiteral.
	VisitBooleanLiteral(ctx *BooleanLiteralContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#simpleCase.
	VisitSimpleCase(ctx *SimpleCaseContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#columnReference.
	VisitColumnReference(ctx *ColumnReferenceContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#nullLiteral.
	VisitNullLiteral(ctx *NullLiteralContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#rowConstructor.
	VisitRowConstructor(ctx *RowConstructorContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#subscript.
	VisitSubscript(ctx *SubscriptContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#subqueryExpression.
	VisitSubqueryExpression(ctx *SubqueryExpressionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#binaryLiteral.
	VisitBinaryLiteral(ctx *BinaryLiteralContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#currentUser.
	VisitCurrentUser(ctx *CurrentUserContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#extract.
	VisitExtract(ctx *ExtractContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#stringLiteral.
	VisitStringLiteral(ctx *StringLiteralContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#arrayConstructor.
	VisitArrayConstructor(ctx *ArrayConstructorContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#functionCall.
	VisitFunctionCall(ctx *FunctionCallContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#exists.
	VisitExists(ctx *ExistsContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#position.
	VisitPosition(ctx *PositionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#searchedCase.
	VisitSearchedCase(ctx *SearchedCaseContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#groupingOperation.
	VisitGroupingOperation(ctx *GroupingOperationContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#basicStringLiteral.
	VisitBasicStringLiteral(ctx *BasicStringLiteralContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#unicodeStringLiteral.
	VisitUnicodeStringLiteral(ctx *UnicodeStringLiteralContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#timeZoneInterval.
	VisitTimeZoneInterval(ctx *TimeZoneIntervalContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#timeZoneString.
	VisitTimeZoneString(ctx *TimeZoneStringContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#comparisonOperator.
	VisitComparisonOperator(ctx *ComparisonOperatorContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#comparisonQuantifier.
	VisitComparisonQuantifier(ctx *ComparisonQuantifierContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#booleanValue.
	VisitBooleanValue(ctx *BooleanValueContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#interval.
	VisitInterval(ctx *IntervalContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#intervalField.
	VisitIntervalField(ctx *IntervalFieldContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#normalForm.
	VisitNormalForm(ctx *NormalFormContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#sqltype.
	VisitSqltype(ctx *SqltypeContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#typeParameter.
	VisitTypeParameter(ctx *TypeParameterContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#baseType.
	VisitBaseType(ctx *BaseTypeContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#whenClause.
	VisitWhenClause(ctx *WhenClauseContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#filter.
	VisitFilter(ctx *FilterContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#explainFormat.
	VisitExplainFormat(ctx *ExplainFormatContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#explainType.
	VisitExplainType(ctx *ExplainTypeContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#positionalArgument.
	VisitPositionalArgument(ctx *PositionalArgumentContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#namedArgument.
	VisitNamedArgument(ctx *NamedArgumentContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#privilege.
	VisitPrivilege(ctx *PrivilegeContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#qualifiedName.
	VisitQualifiedName(ctx *QualifiedNameContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#unquotedIdentifier.
	VisitUnquotedIdentifier(ctx *UnquotedIdentifierContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#quotedIdentifier.
	VisitQuotedIdentifier(ctx *QuotedIdentifierContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#backQuotedIdentifier.
	VisitBackQuotedIdentifier(ctx *BackQuotedIdentifierContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#digitIdentifier.
	VisitDigitIdentifier(ctx *DigitIdentifierContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#decimalLiteral.
	VisitDecimalLiteral(ctx *DecimalLiteralContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#doubleLiteral.
	VisitDoubleLiteral(ctx *DoubleLiteralContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#integerLiteral.
	VisitIntegerLiteral(ctx *IntegerLiteralContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#nonReserved.
	VisitNonReserved(ctx *NonReservedContext) interface{}
}
