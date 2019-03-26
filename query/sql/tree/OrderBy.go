package tree

// OrderBy is OrderBy
type OrderBy struct {
	// Node is INode
	INode
	// SortItems is list of SortItems
	SortItems []*SortItem
}

// NewOrderBy creates OrderBy
func NewOrderBy(location *NodeLocation, sortItems []*SortItem) *OrderBy {
	return &OrderBy{
		NewNode(location),
		sortItems,
	}
}

// Accept accepts visitor
func (n *OrderBy) Accept(visitor AstVisitor, ctx interface{}) interface{} {
	return visitor.VisitOrderBy(n, ctx)
}
