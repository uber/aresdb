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

// IRelation is interface
type IRelation interface {
	// INode is interface
	INode
}

// Relation is IRelation
type Relation struct {
	INode
}

// NewRelation create Relation
func NewRelation(location *NodeLocation) *Relation {
	return &Relation{
		NewNode(location),
	}
}

// Accept accepts visitor
func (r *Relation) Accept(visitor AstVisitor, ctx interface{}) interface{} {
	return visitor.VisitRelation(r, ctx)
}
