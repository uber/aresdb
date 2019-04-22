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

// ISelectItem is interface
type ISelectItem interface {
	// INode is interface
	INode
}

// SelectItem is SelectItem
type SelectItem struct {
	// Node is INode
	INode
}

// NewSelectItem creates SelectItem
func NewSelectItem(location *NodeLocation) *SelectItem {
	return &SelectItem{
		NewNode(location),
	}
}

// Accept accepts visitor
func (r *SelectItem) Accept(visitor AstVisitor, ctx interface{}) interface{} {
	return visitor.VisitSelectItem(r, ctx)
}
