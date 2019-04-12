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

// NodeLocation is position
type NodeLocation struct {
	// Line is #line
	Line int
	// CharPosition is #col
	CharPosition int
}

// INode is interface
type INode interface {
	SetValue(value string)
	GetValue() string
	Accept(visitor AstVisitor, ctx interface{}) interface{}
}

// Node is INode
type Node struct {
	// Location is node location
	Location *NodeLocation
	value    string
}

// NewNode creates Node
func NewNode(location *NodeLocation) *Node {
	return &Node{
		Location: location,
	}
}

// SetValue sets node token value
func (n *Node) SetValue(value string) {
	n.value = value
}

// GetValue returns node token value
func (n *Node) GetValue() string {
	return n.value
}

// Accept accepts visitor
func (n *Node) Accept(visitor AstVisitor, ctx interface{}) interface{} {
	return visitor.visitNode(n, ctx)
}
