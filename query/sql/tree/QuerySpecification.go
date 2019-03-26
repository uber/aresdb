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
