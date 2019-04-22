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

// Identifier is Identifier
type Identifier struct {
	// Expression is IExpression
	IExpression
	// Value is Identifier text
	Value string
	// Delimited is flag
	Delimited bool
}

// NewIdentifier creates Identifier
func NewIdentifier(location *NodeLocation, value string, delimited bool) *Identifier {
	return &Identifier{
		NewExpression(location),
		value,
		delimited,
	}
}

// Accept accepts visitor
func (e *Identifier) Accept(visitor AstVisitor, ctx interface{}) interface{} {
	return visitor.VisitIdentifier(e, ctx)
}
