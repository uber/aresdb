package tree

// IExpression is interface
type IExpression interface {
	// INode is interface
	INode
}

// Expression is an IExpression
type Expression struct {
	// Node is an INode
	*Node
}

// NewExpression creates Expression
func NewExpression(location *NodeLocation) *Expression {
	return &Expression{
		NewNode(location),
	}
}

// Accept accepts visitor
func (e *Expression) Accept(visitor AstVisitor, ctx interface{}) interface{} {
	return visitor.VisitExpression(e, ctx)
}
