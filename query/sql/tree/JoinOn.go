package tree

import (
	"github.com/uber/aresdb/query/sql/util"
)

// JoinOn is natural join
type JoinOn struct {
	// IJoinCriteria is interface
	IJoinCriteria
	Expr IExpression
}

// NewJoinOn creates JoinOn
func NewJoinOn(expr IExpression) *JoinOn {
	util.RequireNonNull(expr, "expression is null")
	return &JoinOn{
		Expr: expr,
	}
}

// GetNodes returns empty
func (j *JoinOn) GetNodes() []INode {
	return []INode{
		j.Expr,
	}
}
