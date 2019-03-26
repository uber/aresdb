package tree

// IGroupingElement is interface
type IGroupingElement interface {
	// INode is interface
	INode
}

// GroupingElement is IGroupingElement
type GroupingElement struct {
	// Node is INode
	INode
}

// NewGroupingElement creates GroupingElement
func NewGroupingElement(location *NodeLocation) *GroupingElement {
	return &GroupingElement{
		NewNode(location),
	}
}

// Accept accepts visitor
func (g *GroupingElement) Accept(visitor AstVisitor, ctx interface{}) interface{} {
	return visitor.VisitGroupingElement(g, ctx)
}
