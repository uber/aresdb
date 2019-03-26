package tree

// Identifier is Identifier
type Identifier struct {
	// Expression is IExpression
	IExpression
	// Value is Identifier text
	Value string
	// Delimited is flag
	Delimited bool
}

// NewIdentifier creates Identifier
func NewIdentifier(location *NodeLocation, value string, delimited bool) *Identifier {
	return &Identifier{
		NewExpression(location),
		value,
		delimited,
	}
}

// Accept accepts visitor
func (e *Identifier) Accept(visitor AstVisitor, ctx interface{}) interface{} {
	return visitor.VisitIdentifier(e, ctx)
}
