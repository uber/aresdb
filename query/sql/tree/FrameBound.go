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
