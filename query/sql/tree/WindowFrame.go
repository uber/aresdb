package tree

import (
	"fmt"

	"github.com/uber/aresdb/query/sql/util"
)

// WindowFrame is window range
type WindowFrame struct {
	INode
	WindowType WindowFrameType
	Start      *FrameBound
	End        *FrameBound
}

// WindowFrameType is window range type
type WindowFrameType int

const (
	// RANGE is range
	RANGE WindowFrameType = iota
	// ROWS is row range
	ROWS
)

// WindowFrameTypes is WindowFrameType strings
var WindowFrameTypes = [...]string{
	"RANGE",
	"ROWS",
}

// NewWindowFrame creates WindowFrame
func NewWindowFrame(location *NodeLocation, windowType WindowFrameType, start, end *FrameBound) *WindowFrame {
	errMsg := fmt.Sprintf("start is null at (line:%d, col:%d)", location.Line, location.CharPosition)
	util.RequireNonNull(start, errMsg)
	errMsg = fmt.Sprintf("end is null at (line:%d, col:%d)", location.Line, location.CharPosition)
	util.RequireNonNull(end, errMsg)
	return &WindowFrame{
		NewNode(location),
		windowType,
		start,
		end,
	}
}

// Accept accepts visitor
func (q *WindowFrame) Accept(visitor AstVisitor, ctx interface{}) interface{} {
	return visitor.VisitWindowFrame(q, ctx)
}
