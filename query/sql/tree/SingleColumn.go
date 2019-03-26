package tree

// SingleColumn is select SingleColumn
type SingleColumn struct {
	// SelectItem is interface
	ISelectItem
	// Alias is column alias
	Alias *Identifier
	// Expression is IExpression
	Expression IExpression
}

// NewSingleColumn creates SingleColumn
func NewSingleColumn(location *NodeLocation, expr IExpression, alias *Identifier) *SingleColumn {
	return &SingleColumn{
		NewSelectItem(location),
		alias,
		expr,
	}
}

// Accept accepts visitor
func (q *SingleColumn) Accept(visitor AstVisitor, ctx interface{}) interface{} {
	return visitor.VisitSingleColumn(q, ctx)
}
