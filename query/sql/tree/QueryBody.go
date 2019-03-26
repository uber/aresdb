package tree

// IQueryBody is interface
type IQueryBody interface {
	// IRelation is interface
	IRelation
}

// QueryBody is QueryBody
type QueryBody struct {
	IRelation
}

// NewQueryBody creates QueryBody
func NewQueryBody(location *NodeLocation) *QueryBody {
	relation := NewRelation(location)
	return &QueryBody{
		relation,
	}
}

// Accept accepts visitor
func (q *QueryBody) Accept(visitor AstVisitor, ctx interface{}) interface{} {
	return visitor.VisitQueryBody(q, ctx)
}
