package tree

import "fmt"

// Query is query
type Query struct {
	// Statement is IStatement
	IStatement
	// QueryBody is IQueryBody
	QueryBody IQueryBody
	// With is with
	With *With
	// OrderBy is orderby
	OrderBy *OrderBy
	// Limit is limit
	Limit string
}

// NewQuery creates Query
func NewQuery(location *NodeLocation, with *With, queryBody IQueryBody, order *OrderBy, limit string) *Query {
	if queryBody == nil {
		panic(fmt.Errorf("QueryBody is null at (line:%d, col:%d)", location.Line, location.CharPosition))
	}

	return &Query{
		IStatement: NewStatement(location),
		QueryBody:  queryBody,
		With:       with,
		OrderBy:    order,
		Limit:      limit,
	}
}

// Accept accepts visitor
func (n *Query) Accept(visitor AstVisitor, ctx interface{}) interface{} {
	return visitor.VisitQuery(n, ctx)
}
