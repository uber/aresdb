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
