package tree

import (
	"fmt"

	"github.com/uber/aresdb/query/sql/util"
)

// SimpleGroupBy is group by
type SimpleGroupBy struct {
	IGroupingElement
	// Columns is group by columns
	Columns []IExpression
}

// NewSimpleGroupBy creates SimpleGroupBy
func NewSimpleGroupBy(location *NodeLocation, expressions []IExpression) *SimpleGroupBy {
	errMsg := fmt.Sprintf("simpleGroupByExpressions is null at (line:%d, col:%d)", location.Line, location.CharPosition)
	util.RequireNonNull(expressions, errMsg)
	return &SimpleGroupBy{
		NewGroupingElement(location),
		expressions,
	}
}

// Accept accepts visitor
func (q *SimpleGroupBy) Accept(visitor AstVisitor, ctx interface{}) interface{} {
	return visitor.VisitSimpleGroupBy(q, ctx)
}
