package tree

// SortItem is SortItem
type SortItem struct {
	// Node is INode
	INode
	Expr  IExpression
	Order OrderType
}

// OrderType is order type
type OrderType int

const (
	// ASC is ASC
	ASC OrderType = iota
	// DESC is DESC
	DESC
)

// OrderTypes is order type strings
var OrderTypes = [...]string{
	"ASC",
	"DESC",
}

// NewSortItem creates SortItem
func NewSortItem(location *NodeLocation, expr IExpression, order OrderType) *SortItem {
	return &SortItem{
		NewNode(location),
		expr,
		order,
	}
}

// Accept accepts visitor
func (q *SortItem) Accept(visitor AstVisitor, ctx interface{}) interface{} {
	return visitor.VisitSortItem(q, ctx)
}
