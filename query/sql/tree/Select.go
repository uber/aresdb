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

// Select is select
type Select struct {
	// Node is INode
	INode
	// Distinct is distinct
	Distinct bool
	// SelectItems is list of ISelectItems
	SelectItems []ISelectItem
}

// NewSelect creates Select
func NewSelect(location *NodeLocation, distinct bool, selectItems []ISelectItem) *Select {
	return &Select{
		NewNode(location),
		distinct,
		selectItems,
	}
}

// Accept accepts visitor
func (r *Select) Accept(visitor AstVisitor, ctx interface{}) interface{} {
	return visitor.VisitSelect(r, ctx)
}
