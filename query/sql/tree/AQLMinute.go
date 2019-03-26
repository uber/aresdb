package tree

// AQLMinute is ...
type AQLMinute struct {
	INode

	Arguments []IExpression
}

// NewAQLMinute is ...
func NewAQLMinute(location *NodeLocation, arguments []IExpression) *AQLMinute {
	return &AQLMinute{
		INode:     NewNode(location),
		Arguments: arguments,
	}
}
