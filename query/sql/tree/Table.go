package tree

// Table is table name
type Table struct {
	IQueryBody
	// Name is table name
	Name *QualifiedName
}

// NewTable creates table name
func NewTable(location *NodeLocation, name *QualifiedName) *Table {
	return &Table{
		NewQueryBody(location),
		name,
	}
}

// Accept accepts visitor
func (q *Table) Accept(visitor AstVisitor, ctx interface{}) interface{} {
	return visitor.VisitTable(q, ctx)
}
