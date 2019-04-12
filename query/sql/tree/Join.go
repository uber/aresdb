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

import (
	"fmt"

	"github.com/uber/aresdb/query/sql/util"
)

// Join is Join
type Join struct {
	// IRelation is interface
	IRelation
	// Type is join type
	Type JoinType
	// Left is IRelation
	Left IRelation
	// Right is IRelation
	Right IRelation
	// Criteria is on condition
	Criteria IJoinCriteria
}

// JoinType is join type
type JoinType int

const (
	// CROSS is CROSS
	CROSS JoinType = iota
	// INNER is INNER
	INNER
	// LEFT is LEFT
	LEFT
	// RIGHT is RIGHT
	RIGHT
	// FULL is FULL
	FULL
	// IMPLICIT is IMPLICIT
	IMPLICIT
)

// JoinTypes is join type strings
var JoinTypes = [...]string{
	"CROSS",
	"INNER",
	"LEFT",
	"RIGHT",
	"FULL",
	"IMPLICIT",
}

// NewJoin creates Join
func NewJoin(location *NodeLocation, joinType JoinType, left, right IRelation, criteria IJoinCriteria) *Join {
	errMsg := fmt.Sprintf("left is null at (line:%d, col:%d)", location.Line, location.CharPosition)
	util.RequireNonNull(left, errMsg)
	errMsg = fmt.Sprintf("right is null at (line:%d, col:%d)", location.Line, location.CharPosition)
	util.RequireNonNull(right, errMsg)

	if joinType != LEFT {
		panic(fmt.Errorf("only support left join at (line:%d, col:%d)", location.Line, location.CharPosition))
	}

	return &Join{
		NewRelation(location),
		joinType,
		left,
		right,
		criteria,
	}
}

// Accept accepts visitor
func (e *Join) Accept(visitor AstVisitor, ctx interface{}) interface{} {
	return visitor.VisitJoin(e, ctx)
}
