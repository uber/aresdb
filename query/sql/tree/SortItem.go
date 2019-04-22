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

// SortItem is SortItem
type SortItem struct {
	// Node is INode
	INode
	Expr  IExpression
	Order OrderType
}

// OrderType is order type
type OrderType int

const (
	// ASC is ASC
	ASC OrderType = iota
	// DESC is DESC
	DESC
)

// OrderTypes is order type strings
var OrderTypes = [...]string{
	"ASC",
	"DESC",
}

// NewSortItem creates SortItem
func NewSortItem(location *NodeLocation, expr IExpression, order OrderType) *SortItem {
	return &SortItem{
		NewNode(location),
		expr,
		order,
	}
}

// Accept accepts visitor
func (q *SortItem) Accept(visitor AstVisitor, ctx interface{}) interface{} {
	return visitor.VisitSortItem(q, ctx)
}
