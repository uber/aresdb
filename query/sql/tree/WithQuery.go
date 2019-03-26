package tree

import (
	"fmt"

	"github.com/uber/aresdb/query/sql/util"
)

// WithQuery is WithQuery
type WithQuery struct {
	// Node is INode
	INode
	// Name is name
	Name *Identifier
	// Query is query
	Query *Query
	// ColumnAliases is ColumnAliases
	ColumnAliases []*Identifier
}

// NewWithQuery creates WithQuery
func NewWithQuery(location *NodeLocation, name *Identifier, query *Query, columnAliases []*Identifier) *WithQuery {
	errMsg := fmt.Sprintf("query is null at (line:%d, col:%d)", location.Line, location.CharPosition)
	util.RequireNonNull(query, errMsg)

	return &WithQuery{
		NewNode(location),
		name,
		query,
		columnAliases,
	}
}

// Accept accepts visitor
func (q *WithQuery) Accept(visitor AstVisitor, ctx interface{}) interface{} {
	return visitor.VisitWithQuery(q, ctx)
}
