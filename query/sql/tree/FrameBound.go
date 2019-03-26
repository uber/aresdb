package tree

// FrameBound is boundary
type FrameBound struct {
	INode
	FrameType     FrameBoundType
	Value         IExpression
	OriginalValue IExpression
}

// FrameBoundType is boundary type
type FrameBoundType int

const (
	// UNBOUNDEDPRECEDING is ...
	UNBOUNDEDPRECEDING FrameBoundType = iota
	// PRECEDING is ...
	PRECEDING
	// CURRENTROW is ...
	CURRENTROW
	// FOLLOWING is ...
	FOLLOWING
	// UNBOUNDEDFOLLOWING is ...
	UNBOUNDEDFOLLOWING
)

// FrameBoundTypes is FrameBoundType strings
var FrameBoundTypes = [...]string{
	"UNBOUNDED_PRECEDING",
	"PRECEDING",
	"CURRENT_ROW",
	"FOLLOWING",
	"UNBOUNDED_FOLLOWING",
}

// NewFrameBound creates FrameBound
func NewFrameBound(location *NodeLocation, frameType FrameBoundType, value, originalValue IExpression) *FrameBound {
	return &FrameBound{
		NewNode(location),
		frameType,
		value,
		originalValue,
	}
}

// Accept accepts visitor
func (e *FrameBound) Accept(visitor AstVisitor, ctx interface{}) interface{} {
	return visitor.VisitFrameBound(e, ctx)
}
