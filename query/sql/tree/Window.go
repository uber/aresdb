package tree

import (
	"fmt"

	"github.com/uber/aresdb/query/sql/util"
)

// Window is window
type Window struct {
	INode
	PartitionBy []IExpression
	OrderBy     *OrderBy
	Frame       *WindowFrame
}

// NewWindow creates window
func NewWindow(location *NodeLocation, partitionBy []IExpression, orderBy *OrderBy, frame *WindowFrame) *Window {
	errMsg := fmt.Sprintf("partitionBy is null at (line:%d, col:%d)", location.Line, location.CharPosition)
	util.RequireNonNull(partitionBy, errMsg)
	errMsg = fmt.Sprintf("orderBy is null at (line:%d, col:%d)", location.Line, location.CharPosition)
	util.RequireNonNull(orderBy, errMsg)
	errMsg = fmt.Sprintf("frame is null at (line:%d, col:%d)", location.Line, location.CharPosition)
	util.RequireNonNull(frame, errMsg)
	return &Window{
		NewNode(location),
		partitionBy,
		orderBy,
		frame,
	}
}

// Accept accepts visitor
func (q *Window) Accept(visitor AstVisitor, ctx interface{}) interface{} {
	return visitor.VisitWindow(q, ctx)
}
