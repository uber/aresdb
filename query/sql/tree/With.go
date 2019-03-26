package tree

// With is With
type With struct {
	// Node is INode
	INode
	// Recursive is with Recursive
	Recursive bool
	// Queries is WithQuery
	Queries []*WithQuery
}

// NewWith creates With
func NewWith(location *NodeLocation, recursive bool, queries []*WithQuery) *With {
	return &With{
		NewNode(location),
		recursive,
		queries,
	}
}

// Accept accepts visitor
func (n *With) Accept(visitor AstVisitor, ctx interface{}) interface{} {
	return visitor.VisitWith(n, ctx)
}
