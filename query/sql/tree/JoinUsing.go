package tree

import (
	"github.com/uber/aresdb/query/sql/util"
)

// JoinUsing is natural join
type JoinUsing struct {
	// IJoinCriteria is interface
	IJoinCriteria
	Columns []*Identifier
}

// NewJoinUsing creates JoinUsing
func NewJoinUsing(columns []*Identifier) *JoinUsing {
	util.RequireNonNull(columns, "columns is null")
	return &JoinUsing{
		Columns: columns,
	}
}

// GetNodes returns empty
func (j *JoinUsing) GetNodes() []INode {
	return []INode{}
}
