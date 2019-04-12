//  Copyright (c) 2017-2018 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tree

import (
	"fmt"

	"github.com/uber/aresdb/query/sql/util"
)

// LogicalBinaryExpression is IExpression
type LogicalBinaryExpression struct {
	IExpression
	LogicType LogicalBinaryExpType
	Left      IExpression
	Right     IExpression
}

// LogicalBinaryExpType is logical binary exp type
type LogicalBinaryExpType int

const (
	// AND is logic
	AND LogicalBinaryExpType = iota
	// OR is logic
	OR
	// NOOP is logic
	NOOP
)

// LogicalBinaryExpTypes is logical binary exp type strings
var LogicalBinaryExpTypes = [...]string{
	"AND",
	"OR",
	"NOOP",
}

// NewLogicalBinaryExpression creates LogicalBinaryExpression
func NewLogicalBinaryExpression(location *NodeLocation, logicType LogicalBinaryExpType,
	left, right IExpression) *LogicalBinaryExpression {
	errMsg := fmt.Sprintf("type is null at (line:%d, col:%d)", location.Line, location.CharPosition)
	util.RequireNonNull(logicType, errMsg)
	errMsg = fmt.Sprintf("left is null at (line:%d, col:%d)", location.Line, location.CharPosition)
	util.RequireNonNull(left, errMsg)
	errMsg = fmt.Sprintf("right is null at (line:%d, col:%d)", location.Line, location.CharPosition)
	util.RequireNonNull(right, errMsg)

	return &LogicalBinaryExpression{
		NewExpression(location),
		logicType,
		left,
		right,
	}
}

// Accept accepts visitor
func (e *LogicalBinaryExpression) Accept(visitor AstVisitor, ctx interface{}) interface{} {
	return visitor.VisitLogicalBinaryExpression(e, ctx)
}
