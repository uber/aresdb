package tree

// IRelation is interface
type IRelation interface {
	// INode is interface
	INode
}

// Relation is IRelation
type Relation struct {
	INode
}

// NewRelation create Relation
func NewRelation(location *NodeLocation) *Relation {
	return &Relation{
		NewNode(location),
	}
}

// Accept accepts visitor
func (r *Relation) Accept(visitor AstVisitor, ctx interface{}) interface{} {
	return visitor.VisitRelation(r, ctx)
}
