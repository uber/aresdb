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

// GroupBy is GroupBy
type GroupBy struct {
	// Node is INode
	INode
	IsDistinct       bool
	GroupingElements []IGroupingElement
}

// NewGroupBy creates GroupBy
func NewGroupBy(location *NodeLocation, distinct bool, elements []IGroupingElement) *GroupBy {
	return &GroupBy{
		NewNode(location),
		distinct,
		elements,
	}
}

// Accept accepts visitor
func (e *GroupBy) Accept(visitor AstVisitor, ctx interface{}) interface{} {
	return visitor.VisitGroupBy(e, ctx)
}
