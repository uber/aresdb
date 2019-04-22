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

// OrderBy is OrderBy
type OrderBy struct {
	// Node is INode
	INode
	// SortItems is list of SortItems
	SortItems []*SortItem
}

// NewOrderBy creates OrderBy
func NewOrderBy(location *NodeLocation, sortItems []*SortItem) *OrderBy {
	return &OrderBy{
		NewNode(location),
		sortItems,
	}
}

// Accept accepts visitor
func (n *OrderBy) Accept(visitor AstVisitor, ctx interface{}) interface{} {
	return visitor.VisitOrderBy(n, ctx)
}
