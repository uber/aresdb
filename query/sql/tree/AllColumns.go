package tree

// AllColumns are AllColumns
type AllColumns struct {
	// SelectItem is ISelectItem
	ISelectItem
	// Prefix is QualifiedName
	Prefix *QualifiedName
}

// NewAllColumns creates AllColumns
func NewAllColumns(location *NodeLocation, prefix *QualifiedName) *AllColumns {
	return &AllColumns{
		NewSelectItem(location),
		prefix,
	}
}

// Accept accepts visitor
func (n *AllColumns) Accept(visitor AstVisitor, ctx interface{}) interface{} {
	return visitor.VisitAllColumns(n, ctx)
}
