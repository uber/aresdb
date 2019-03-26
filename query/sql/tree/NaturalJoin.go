package tree

// NaturalJoin is natural join
type NaturalJoin struct {
	// IJoinCriteria is interface
	IJoinCriteria
}

// NewNaturalJoin creates NaturalJoin
func NewNaturalJoin() *NaturalJoin {
	return &NaturalJoin{}
}

// GetNodes returns empty
func (n *NaturalJoin) GetNodes() []INode {
	return nil
}
