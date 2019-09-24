// Modifications Copyright (c) 2017-2018 Uber Technologies, Inc.
// Copyright (c) 2013-2016 Errplane Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package expr

import (
	"bytes"
	"fmt"
	memCom "github.com/uber/aresdb/memstore/common"
	"strconv"
	"strings"
)

// Type defines data types for expression evaluation.
// Expression types are determined at query compilation time, type castings are
// generated when apprioperiate. Notice that word widths are not specified here.
type Type int

const (
	UnknownType Type = iota
	Boolean
	Unsigned
	Signed
	Float
	GeoPoint
	GeoShape
)

var typeNames = map[Type]string{
	UnknownType: "Unknown",
	Boolean:     "Boolean",
	Unsigned:    "Unsigned",
	Signed:      "Signed",
	Float:       "Float",
	GeoPoint:    "GeoPoint",
	GeoShape:    "GeoShape",
}

// constants for call names.
const (
	ConvertTzCallName           = "convert_tz"
	CountCallName               = "count"
	DayOfWeekCallName           = "dayofweek"
	FromUnixTimeCallName        = "from_unixtime"
	GeographyIntersectsCallName = "geography_intersects"
	HexCallName                 = "hex"
	// hll aggregation function applies to hll columns
	HllCallName = "hll"
	// countdistincthll aggregation function applies to all columns, hll value is computed on the fly
	CountDistinctHllCallName = "countdistincthll"
	HourCallName             = "hour"
	ListCallName             = ""
	MaxCallName              = "max"
	MinCallName              = "min"
	SumCallName              = "sum"
	AvgCallName              = "avg"
	// array functions
	LengthCallName    = "length"
	ContainsCallName  = "contains"
	ElementAtCallName = "element_at"
)

func (t Type) String() string {
	return typeNames[t]
}

func (t Type) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString(`"`)
	buffer.WriteString(typeNames[t])
	buffer.WriteString(`"`)
	return buffer.Bytes(), nil
}

// Expr represents an expression that can be evaluated to a value.
type Expr interface {
	expr()
	String() string
	Type() Type
}

func (*BinaryExpr) expr()      {}
func (*BooleanLiteral) expr()  {}
func (*Call) expr()            {}
func (*Case) expr()            {}
func (*Distinct) expr()        {}
func (*NullLiteral) expr()     {}
func (*NumberLiteral) expr()   {}
func (*ParenExpr) expr()       {}
func (*StringLiteral) expr()   {}
func (*UnaryExpr) expr()       {}
func (*UnknownLiteral) expr()  {}
func (*VarRef) expr()          {}
func (*Wildcard) expr()        {}
func (*GeopointLiteral) expr() {}

// walkNames will walk the Expr and return the database fields
func walkNames(exp Expr) []string {
	switch expr := exp.(type) {
	case *VarRef:
		return []string{expr.Val}
	case *Call:
		if len(expr.Args) == 0 {
			return nil
		}
		lit, ok := expr.Args[0].(*VarRef)
		if !ok {
			return nil
		}

		return []string{lit.Val}
	case *UnaryExpr:
		return walkNames(expr.Expr)
	case *BinaryExpr:
		var ret []string
		ret = append(ret, walkNames(expr.LHS)...)
		ret = append(ret, walkNames(expr.RHS)...)
		return ret
	case *Case:
		var ret []string
		for _, cond := range expr.WhenThens {
			ret = append(ret, walkNames(cond.When)...)
			ret = append(ret, walkNames(cond.Then)...)
		}
		if expr.Else != nil {
			ret = append(ret, walkNames(expr.Else)...)
		}
		return ret
	case *ParenExpr:
		return walkNames(expr.Expr)
	}

	return nil
}

// walkFunctionCalls walks the Field of a query for any function calls made
func walkFunctionCalls(exp Expr) []*Call {
	switch expr := exp.(type) {
	case *Call:
		return []*Call{expr}
	case *UnaryExpr:
		return walkFunctionCalls(expr.Expr)
	case *BinaryExpr:
		var ret []*Call
		ret = append(ret, walkFunctionCalls(expr.LHS)...)
		ret = append(ret, walkFunctionCalls(expr.RHS)...)
		return ret
	case *Case:
		var ret []*Call
		for _, cond := range expr.WhenThens {
			ret = append(ret, walkFunctionCalls(cond.When)...)
			ret = append(ret, walkFunctionCalls(cond.Then)...)
		}
		if expr.Else != nil {
			ret = append(ret, walkFunctionCalls(expr.Else)...)
		}
		return ret
	case *ParenExpr:
		return walkFunctionCalls(expr.Expr)
	}

	return nil
}

// VarRef represents a reference to a variable.
type VarRef struct {
	Val      string
	ExprType Type

	// The following fields are populated for convenience after the query is
	// validated against the schema.

	// ID of the table in the query scope (0 for the main table, 1+ for foreign
	// tables).
	TableID int
	// ID of the column in the schema.
	ColumnID int
	// Enum dictionary for enum typed column. Can only be accessed while holding
	// the schema lock.
	EnumDict map[string]int `json:"-"`
	// Setting enum reverse dict requires holding the schema lock,
	// while reading from it does not require holding the schema lock.
	EnumReverseDict []string `json:"-"`

	DataType memCom.DataType

	// Whether this column is hll column (can run hll directly)
	IsHLLColumn bool
}

// Type returns the type.
func (r *VarRef) Type() Type {
	return r.ExprType
}

// String returns a string representation of the variable reference.
func (r *VarRef) String() string {
	return r.Val
}

// Call represents a function call.
type Call struct {
	Name     string
	Args     []Expr
	ExprType Type
}

// Type returns the type.
func (c *Call) Type() Type {
	return c.ExprType
}

// String returns a string representation of the call.
func (c *Call) String() string {
	// Join arguments.
	var strs []string
	for _, arg := range c.Args {
		if arg == nil {
			strs = append(strs, "ERROR_ARGUMENT_NIL")
		} else {
			strs = append(strs, arg.String())
		}
	}

	// Write function name and args.
	return fmt.Sprintf("%s(%s)", c.Name, strings.Join(strs, ", "))
}

// WhenThen represents a when-then conditional expression pair in a case expression.
type WhenThen struct {
	When Expr
	Then Expr
}

// Case represents a CASE WHEN .. THEN .. ELSE .. THEN expression.
type Case struct {
	WhenThens []WhenThen
	Else      Expr
	ExprType  Type
}

// Type returns the type.
func (c *Case) Type() Type {
	return c.ExprType
}

// String returns a string representation of the expression.
func (c *Case) String() string {
	whenThens := make([]string, len(c.WhenThens))
	for i, whenThen := range c.WhenThens {
		whenThens[i] = fmt.Sprintf("WHEN %s THEN %s", whenThen.When.String(), whenThen.Then.String())
	}
	if c.Else == nil {
		return fmt.Sprintf("CASE %s END", strings.Join(whenThens, " "))
	}
	return fmt.Sprintf("CASE %s ELSE %s END", strings.Join(whenThens, " "), c.Else.String())
}

// Distinct represents a DISTINCT expression.
type Distinct struct {
	// Identifier following DISTINCT
	Val string
}

// Type returns the type.
func (d *Distinct) Type() Type {
	return UnknownType
}

// String returns a string representation of the expression.
func (d *Distinct) String() string {
	return fmt.Sprintf("DISTINCT %s", d.Val)
}

// NewCall returns a new call expression from this expressions.
func (d *Distinct) NewCall() *Call {
	return &Call{
		Name: "distinct",
		Args: []Expr{
			&VarRef{Val: d.Val},
		},
	}
}

// NumberLiteral represents a numeric literal.
type NumberLiteral struct {
	Val      float64
	Int      int
	Expr     string
	ExprType Type
}

// Type returns the type.
func (l *NumberLiteral) Type() Type {
	return l.ExprType
}

// String returns a string representation of the literal.
func (l *NumberLiteral) String() string {
	if l.Expr != "" {
		return l.Expr
	}

	if l.Type() == Float {
		return strconv.FormatFloat(l.Val, 'f', 3, 64)
	}

	return strconv.FormatInt(int64(l.Int), 10)
}

// BooleanLiteral represents a boolean literal.
type BooleanLiteral struct {
	Val bool
}

// Type returns the type.
func (l *BooleanLiteral) Type() Type {
	return Boolean
}

// String returns a string representation of the literal.
func (l *BooleanLiteral) String() string {
	if l.Val {
		return "true"
	}
	return "false"
}

// isTrueLiteral returns true if the expression is a literal "true" value.
func isTrueLiteral(expr Expr) bool {
	if expr, ok := expr.(*BooleanLiteral); ok {
		return expr.Val == true
	}
	return false
}

// isFalseLiteral returns true if the expression is a literal "false" value.
func isFalseLiteral(expr Expr) bool {
	if expr, ok := expr.(*BooleanLiteral); ok {
		return expr.Val == false
	}
	return false
}

// StringLiteral represents a string literal.
type StringLiteral struct {
	Val string
}

// Type returns the type.
func (l *StringLiteral) Type() Type {
	return UnknownType
}

// String returns a string representation of the literal.
func (l *StringLiteral) String() string { return QuoteString(l.Val) }

// GeopointLiteral represents a literal for GeoPoint
type GeopointLiteral struct {
	Val [2]float32
}

// Type returns the type.
func (l *GeopointLiteral) Type() Type {
	return GeoPoint
}

// String returns a string representation of the literal.
func (l *GeopointLiteral) String() string {
	return fmt.Sprintf("point(%f, %f)", l.Val[0], l.Val[1])
}

// NullLiteral represents a NULL literal.
type NullLiteral struct{}

// Type returns the type.
func (l *NullLiteral) Type() Type {
	return UnknownType
}

// String returns "NULL".
func (l *NullLiteral) String() string { return "NULL" }

// UnknownLiteral represents an UNKNOWN literal.
type UnknownLiteral struct{}

// Type returns the type.
func (l *UnknownLiteral) Type() Type {
	return UnknownType
}

// String returns "UNKNOWN".
func (l *UnknownLiteral) String() string { return "UNKNOWN" }

// UnaryExpr represents an operation on a single expression.
type UnaryExpr struct {
	Op       Token
	Expr     Expr
	ExprType Type
}

// Type returns the type.
func (e *UnaryExpr) Type() Type {
	return e.ExprType
}

// String returns a string representation of the unary expression.
func (e *UnaryExpr) String() string {
	if e.Op.isDerivedUnaryOperator() {
		return fmt.Sprintf("%s %s", e.Expr.String(), e.Op.String())
	}
	return fmt.Sprintf("%s(%s)", e.Op.String(), e.Expr.String())
}

// BinaryExpr represents an operation between two expressions.
type BinaryExpr struct {
	Op       Token
	LHS      Expr
	RHS      Expr
	ExprType Type
}

// Type returns the type.
func (e *BinaryExpr) Type() Type {
	return e.ExprType
}

// String returns a string representation of the binary expression.
func (e *BinaryExpr) String() string {
	return fmt.Sprintf("%s %s %s", e.LHS.String(), e.Op.String(), e.RHS.String())
}

// ParenExpr represents a parenthesized expression.
type ParenExpr struct {
	Expr     Expr
	ExprType Type // used for type casting
}

// Type returns the type.
func (e *ParenExpr) Type() Type {
	if e.ExprType != UnknownType {
		return e.ExprType
	}
	return e.Expr.Type()
}

// String returns a string representation of the parenthesized expression.
func (e *ParenExpr) String() string {
	if e.Expr == nil {
		return "(ERROR_PAREN_EXPRESSION_NIL)"
	}
	return fmt.Sprintf("(%s)", e.Expr.String())
}

// Wildcard represents a wild card expression.
type Wildcard struct{}

// Type returns the type.
func (e *Wildcard) Type() Type {
	return UnknownType
}

// String returns a string representation of the wildcard.
func (e *Wildcard) String() string { return "*" }

// CloneExpr returns a deep copy of the expression.
func CloneExpr(expr Expr) Expr {
	if expr == nil {
		return nil
	}
	switch expr := expr.(type) {
	case *UnaryExpr:
		return &UnaryExpr{Op: expr.Op, Expr: CloneExpr(expr.Expr)}
	case *BinaryExpr:
		return &BinaryExpr{Op: expr.Op, LHS: CloneExpr(expr.LHS), RHS: CloneExpr(expr.RHS)}
	case *BooleanLiteral:
		return &BooleanLiteral{Val: expr.Val}
	case *Call:
		args := make([]Expr, len(expr.Args))
		for i, arg := range expr.Args {
			args[i] = CloneExpr(arg)
		}
		return &Call{Name: expr.Name, Args: args}
	case *Case:
		conds := make([]WhenThen, len(expr.WhenThens))
		for i, cond := range expr.WhenThens {
			conds[i].When = CloneExpr(cond.When)
			conds[i].Then = CloneExpr(cond.Then)
		}
		var elce Expr
		if expr.Else != nil {
			elce = CloneExpr(expr.Else)
		}
		return &Case{WhenThens: conds, Else: elce}
	case *Distinct:
		return &Distinct{Val: expr.Val}
	case *NumberLiteral:
		return &NumberLiteral{Val: expr.Val, Expr: expr.Expr}
	case *ParenExpr:
		return &ParenExpr{Expr: CloneExpr(expr.Expr)}
	case *StringLiteral:
		return &StringLiteral{Val: expr.Val}
	case *GeopointLiteral:
		return &GeopointLiteral{Val: expr.Val}
	case *VarRef:
		return &VarRef{Val: expr.Val}
	case *Wildcard:
		return &Wildcard{}
	}
	panic("unreachable")
}

// Visitor can be called by Walk to traverse an AST hierarchy.
// The Visit() function is called once per expression.
type Visitor interface {
	Visit(Expr) Visitor
}

// Walk traverses an expression hierarchy in depth-first order.
func Walk(v Visitor, expr Expr) {
	if expr == nil {
		return
	}

	if v = v.Visit(expr); v == nil {
		return
	}

	switch e := expr.(type) {
	case *UnaryExpr:
		Walk(v, e.Expr)

	case *BinaryExpr:
		Walk(v, e.LHS)
		Walk(v, e.RHS)

	case *Case:
		for _, cond := range e.WhenThens {
			Walk(v, cond.When)
			Walk(v, cond.Then)
		}
		if e.Else != nil {
			Walk(v, e.Else)
		}

	case *Call:
		for _, expr := range e.Args {
			Walk(v, expr)
		}

	case *ParenExpr:
		Walk(v, e.Expr)

	}
}

// WalkFunc traverses an expression hierarchy in depth-first order.
func WalkFunc(e Expr, fn func(Expr)) {
	Walk(walkFuncVisitor(fn), e)
}

type walkFuncVisitor func(Expr)

func (fn walkFuncVisitor) Visit(e Expr) Visitor { fn(e); return fn }

// Rewriter can be called by Rewrite to replace nodes in the AST hierarchy.
// The Rewrite() function is called once per expression.
type Rewriter interface {
	Rewrite(Expr) Expr
}

// Rewrite recursively invokes the rewriter to replace each expression.
// Nodes are traversed depth-first and rewritten from leaf to root.
func Rewrite(r Rewriter, expr Expr) Expr {
	switch e := expr.(type) {
	case *UnaryExpr:
		e.Expr = Rewrite(r, e.Expr)

	case *BinaryExpr:
		e.LHS = Rewrite(r, e.LHS)
		e.RHS = Rewrite(r, e.RHS)

	case *Case:
		for i, cond := range e.WhenThens {
			cond.When = Rewrite(r, cond.When)
			cond.Then = Rewrite(r, cond.Then)
			e.WhenThens[i] = cond
		}
		if e.Else != nil {
			e.Else = Rewrite(r, e.Else)
		}

	case *ParenExpr:
		e.Expr = Rewrite(r, e.Expr)

	case *Call:
		for i, expr := range e.Args {
			e.Args[i] = Rewrite(r, expr)
		}
	}

	return r.Rewrite(expr)
}

// RewriteFunc rewrites an expression hierarchy.
func RewriteFunc(e Expr, fn func(Expr) Expr) Expr {
	return Rewrite(rewriterFunc(fn), e)
}

type rewriterFunc func(Expr) Expr

func (fn rewriterFunc) Rewrite(e Expr) Expr { return fn(e) }

// IsUUIDColumn returns whether an Expr is UUID
func IsUUIDColumn(expression Expr) bool {
	if varRef, ok := expression.(*VarRef); ok {
		return varRef.DataType == memCom.UUID
	}
	return false
}

// Cast returns an expression that casts the input to the desired type.
// The returned expression AST will be used directly for VM instruction
// generation of the desired types.
func Cast(e Expr, t Type) Expr {
	// Input type is already desired.
	if e.Type() == t {
		return e
	}
	// Type casting is only required if at least one side is float.
	// We do not cast (or check for overflow) among boolean, signed and unsigned.
	if e.Type() != Float && t != Float {
		return e
	}
	// Data type for NumberLiteral can be changed directly.
	l, _ := e.(*NumberLiteral)
	if l != nil {
		l.ExprType = t
		return l
	}
	// Use ParenExpr to respresent a VM type cast.
	return &ParenExpr{Expr: e, ExprType: t}
}
