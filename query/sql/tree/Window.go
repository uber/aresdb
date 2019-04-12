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
