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

// FunctionCall is ...
type FunctionCall struct {
	IExpression

	QualifiedName *QualifiedName
	Filter        IExpression
	Window        *Window
	Distinct      bool
	Arguments     []IExpression
	OrderBy       *OrderBy
}

// NewFunctionCall is ...
func NewFunctionCall(location *NodeLocation, qualifiedName *QualifiedName, filter IExpression,
	window *Window, distinct bool, arguments []IExpression, orderBy *OrderBy) *FunctionCall {
	return &FunctionCall{
		IExpression:   NewExpression(location),
		QualifiedName: qualifiedName,
		Window:        window,
		Filter:        filter,
		Distinct:      distinct,
		Arguments:     arguments,
		OrderBy:       orderBy,
	}
}
