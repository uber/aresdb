package tree

// ISelectItem is interface
type ISelectItem interface {
	// INode is interface
	INode
}

// SelectItem is SelectItem
type SelectItem struct {
	// Node is INode
	INode
}

// NewSelectItem creates SelectItem
func NewSelectItem(location *NodeLocation) *SelectItem {
	return &SelectItem{
		NewNode(location),
	}
}

// Accept accepts visitor
func (r *SelectItem) Accept(visitor AstVisitor, ctx interface{}) interface{} {
	return visitor.VisitSelectItem(r, ctx)
}
