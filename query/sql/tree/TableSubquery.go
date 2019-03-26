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
