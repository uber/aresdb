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
