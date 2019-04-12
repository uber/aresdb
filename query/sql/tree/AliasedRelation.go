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
	"github.com/uber/aresdb/query/sql/util"
)

// AliasedRelation is alias relation
type AliasedRelation struct {
	IRelation
	Relation    IRelation
	Alias       *Identifier
	ColumnNames []*Identifier
}

// NewAliasedRelation creates NewAliasedRelation
func NewAliasedRelation(
	location *NodeLocation,
	relation IRelation,
	alias *Identifier,
	columns []*Identifier) *AliasedRelation {
	util.RequireNonNull(relation, "relation is null")
	util.RequireNonNull(alias, "alias is null")
	return &AliasedRelation{
		NewRelation(location),
		relation,
		alias,
		columns,
	}
}

// Accept accepts visitor
func (n *AliasedRelation) Accept(visitor AstVisitor, ctx interface{}) interface{} {
	return visitor.VisitAliasedRelation(n, ctx)
}
