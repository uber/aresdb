package tree

// Select is select
type Select struct {
	// Node is INode
	INode
	// Distinct is distinct
	Distinct bool
	// SelectItems is list of ISelectItems
	SelectItems []ISelectItem
}

// NewSelect creates Select
func NewSelect(location *NodeLocation, distinct bool, selectItems []ISelectItem) *Select {
	return &Select{
		NewNode(location),
		distinct,
		selectItems,
	}
}

// Accept accepts visitor
func (r *Select) Accept(visitor AstVisitor, ctx interface{}) interface{} {
	return visitor.VisitSelect(r, ctx)
}
