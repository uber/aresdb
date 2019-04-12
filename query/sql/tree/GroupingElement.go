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

// IGroupingElement is interface
type IGroupingElement interface {
	// INode is interface
	INode
}

// GroupingElement is IGroupingElement
type GroupingElement struct {
	// Node is INode
	INode
}

// NewGroupingElement creates GroupingElement
func NewGroupingElement(location *NodeLocation) *GroupingElement {
	return &GroupingElement{
		NewNode(location),
	}
}

// Accept accepts visitor
func (g *GroupingElement) Accept(visitor AstVisitor, ctx interface{}) interface{} {
	return visitor.VisitGroupingElement(g, ctx)
}
