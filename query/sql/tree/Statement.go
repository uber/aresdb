package tree

// IStatement is interface
type IStatement interface {
	// INode is interface
	INode
}

// Statement is Statement
type Statement struct {
	// Node is INode
	INode
}

// NewStatement creates Statement
func NewStatement(location *NodeLocation) *Statement {
	return &Statement{
		NewNode(location),
	}
}

// Accept accepts visitor
func (q *Statement) Accept(visitor AstVisitor, ctx interface{}) interface{} {
	return visitor.VisitStatement(q, ctx)
}
