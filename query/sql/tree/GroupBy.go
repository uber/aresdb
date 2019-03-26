package tree

// GroupBy is GroupBy
type GroupBy struct {
	// Node is INode
	INode
	IsDistinct       bool
	GroupingElements []IGroupingElement
}

// NewGroupBy creates GroupBy
func NewGroupBy(location *NodeLocation, distinct bool, elements []IGroupingElement) *GroupBy {
	return &GroupBy{
		NewNode(location),
		distinct,
		elements,
	}
}

// Accept accepts visitor
func (e *GroupBy) Accept(visitor AstVisitor, ctx interface{}) interface{} {
	return visitor.VisitGroupBy(e, ctx)
}
