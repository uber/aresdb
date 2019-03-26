package tree

// NodeLocation is position
type NodeLocation struct {
	// Line is #line
	Line int
	// CharPosition is #col
	CharPosition int
}

// INode is interface
type INode interface {
	SetValue(value string)
	GetValue() string
	Accept(visitor AstVisitor, ctx interface{}) interface{}
}

// Node is INode
type Node struct {
	// Location is node location
	Location *NodeLocation
	value    string
}

// NewNode creates Node
func NewNode(location *NodeLocation) *Node {
	return &Node{
		Location: location,
	}
}

// SetValue sets node token value
func (n *Node) SetValue(value string) {
	n.value = value
}

// GetValue returns node token value
func (n *Node) GetValue() string {
	return n.value
}

// Accept accepts visitor
func (n *Node) Accept(visitor AstVisitor, ctx interface{}) interface{} {
	return visitor.visitNode(n, ctx)
}
