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
