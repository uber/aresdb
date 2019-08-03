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

package expr_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/uber/aresdb/query/expr"
)

// Ensure the parser can parse expressions into an AST.
func TestParser_ParseExpr(t *testing.T) {
	var tests = []struct {
		s    string
		expr expr.Expr
		err  string
	}{
		// Primitives
		{s: `100`, expr: &expr.NumberLiteral{Val: 100, Int: 100, Expr: "100", ExprType: expr.Unsigned}},
		{s: `'foo bar'`, expr: &expr.StringLiteral{Val: "foo bar"}},
		{s: `true`, expr: &expr.BooleanLiteral{Val: true}},
		{s: `false`, expr: &expr.BooleanLiteral{Val: false}},
		{s: `my_ident`, expr: &expr.VarRef{Val: "my_ident"}},
		{s: `*`, expr: &expr.Wildcard{}},

		// Simple binary expression
		{
			s: `1 + 2`,
			expr: &expr.BinaryExpr{
				Op:  expr.ADD,
				LHS: &expr.NumberLiteral{Val: 1, Int: 1, Expr: "1", ExprType: expr.Unsigned},
				RHS: &expr.NumberLiteral{Val: 2, Int: 2, Expr: "2", ExprType: expr.Unsigned},
			},
		},

		// Binary expression with LHS precedence
		{
			s: `1 * 2 + 3`,
			expr: &expr.BinaryExpr{
				Op: expr.ADD,
				LHS: &expr.BinaryExpr{
					Op:  expr.MUL,
					LHS: &expr.NumberLiteral{Val: 1, Int: 1, Expr: "1", ExprType: expr.Unsigned},
					RHS: &expr.NumberLiteral{Val: 2, Int: 2, Expr: "2", ExprType: expr.Unsigned},
				},
				RHS: &expr.NumberLiteral{Val: 3, Int: 3, Expr: "3", ExprType: expr.Unsigned},
			},
		},

		// Binary expression with RHS precedence
		{
			s: `1 + 2 * 3`,
			expr: &expr.BinaryExpr{
				Op:  expr.ADD,
				LHS: &expr.NumberLiteral{Val: 1, Int: 1, Expr: "1", ExprType: expr.Unsigned},
				RHS: &expr.BinaryExpr{
					Op:  expr.MUL,
					LHS: &expr.NumberLiteral{Val: 2, Int: 2, Expr: "2", ExprType: expr.Unsigned},
					RHS: &expr.NumberLiteral{Val: 3, Int: 3, Expr: "3", ExprType: expr.Unsigned},
				},
			},
		},

		// Binary expression with LHS paren group.
		{
			s: `(1 + 2) * 3`,
			expr: &expr.BinaryExpr{
				Op: expr.MUL,
				LHS: &expr.ParenExpr{
					Expr: &expr.BinaryExpr{
						Op:  expr.ADD,
						LHS: &expr.NumberLiteral{Val: 1, Int: 1, Expr: "1", ExprType: expr.Unsigned},
						RHS: &expr.NumberLiteral{Val: 2, Int: 2, Expr: "2", ExprType: expr.Unsigned},
					},
				},
				RHS: &expr.NumberLiteral{Val: 3, Int: 3, Expr: "3", ExprType: expr.Unsigned},
			},
		},

		// Binary expression with no precedence, tests left associativity.
		{
			s: `1 * 2 * 3`,
			expr: &expr.BinaryExpr{
				Op: expr.MUL,
				LHS: &expr.BinaryExpr{
					Op:  expr.MUL,
					LHS: &expr.NumberLiteral{Val: 1, Int: 1, Expr: "1", ExprType: expr.Unsigned},
					RHS: &expr.NumberLiteral{Val: 2, Int: 2, Expr: "2", ExprType: expr.Unsigned},
				},
				RHS: &expr.NumberLiteral{Val: 3, Int: 3, Expr: "3", ExprType: expr.Unsigned},
			},
		},

		// Binary expression with IN.
		{
			s: "id IN (12, 18)",
			expr: &expr.BinaryExpr{
				Op:  expr.IN,
				LHS: &expr.VarRef{Val: "id"},
				RHS: &expr.Call{Name: "", Args: []expr.Expr{
					&expr.NumberLiteral{Val: 12, Int: 12, Expr: "12", ExprType: expr.Unsigned},
					&expr.NumberLiteral{Val: 18, Int: 18, Expr: "18", ExprType: expr.Unsigned},
				}},
			},
		},
		{
			s: "id IN (12)",
			expr: &expr.BinaryExpr{
				Op:  expr.IN,
				LHS: &expr.VarRef{Val: "id"},
				RHS: &expr.Call{Name: "", Args: []expr.Expr{
					&expr.NumberLiteral{Val: 12, Int: 12, Expr: "12", ExprType: expr.Unsigned},
				}},
			},
		},
		{
			s: "id IN (12, 15)",
			expr: &expr.BinaryExpr{
				Op:  expr.IN,
				LHS: &expr.VarRef{Val: "id"},
				RHS: &expr.Call{Name: "", Args: []expr.Expr{
					&expr.NumberLiteral{Val: 12, Int: 12, Expr: "12", ExprType: expr.Unsigned},
					&expr.NumberLiteral{Val: 15, Int: 15, Expr: "15", ExprType: expr.Unsigned},
				}},
			},
		},
		// Binary expression with NOT IN.
		{
			s: "id NOT IN (12, 18)",
			expr: &expr.BinaryExpr{
				Op:  expr.NOT_IN,
				LHS: &expr.VarRef{Val: "id"},
				RHS: &expr.Call{Name: "", Args: []expr.Expr{
					&expr.NumberLiteral{Val: 12, Int: 12, Expr: "12", ExprType: expr.Unsigned},
					&expr.NumberLiteral{Val: 18, Int: 18, Expr: "18", ExprType: expr.Unsigned},
				}},
			},
		},
		{
			s: "id NOT IN (12, 15)",
			expr: &expr.BinaryExpr{
				Op:  expr.NOT_IN,
				LHS: &expr.VarRef{Val: "id"},
				RHS: &expr.Call{Name: "", Args: []expr.Expr{
					&expr.NumberLiteral{Val: 12, Int: 12, Expr: "12", ExprType: expr.Unsigned},
					&expr.NumberLiteral{Val: 15, Int: 15, Expr: "15", ExprType: expr.Unsigned},
				}},
			},
		},
		// Unary expression.
		{
			s: "not now",
			expr: &expr.UnaryExpr{
				Op:   expr.NOT,
				Expr: &expr.VarRef{Val: "now"},
			},
		},
		{
			s: "!today",
			expr: &expr.UnaryExpr{
				Op:   expr.EXCLAMATION,
				Expr: &expr.VarRef{Val: "today"},
			},
		},
		{
			s: "-c",
			expr: &expr.UnaryExpr{
				Op:   expr.UNARY_MINUS,
				Expr: &expr.VarRef{Val: "c"},
			},
		},
		{
			s: "not ! a + b and c",
			expr: &expr.BinaryExpr{
				Op: expr.AND,
				LHS: &expr.UnaryExpr{
					Op: expr.NOT,
					Expr: &expr.BinaryExpr{
						Op: expr.ADD,
						LHS: &expr.UnaryExpr{
							Op:   expr.EXCLAMATION,
							Expr: &expr.VarRef{Val: "a"},
						},
						RHS: &expr.VarRef{Val: "b"},
					},
				},
				RHS: &expr.VarRef{Val: "c"},
			},
		},
		// Derived unary expression.
		{
			s: "a is null",
			expr: &expr.UnaryExpr{
				Op:   expr.IS_NULL,
				Expr: &expr.VarRef{Val: "a"},
			},
		},
		{
			s: "a is not null",
			expr: &expr.UnaryExpr{
				Op:   expr.IS_NOT_NULL,
				Expr: &expr.VarRef{Val: "a"},
			},
		},
		{
			s: "a is unknown",
			expr: &expr.UnaryExpr{
				Op:   expr.IS_NULL,
				Expr: &expr.VarRef{Val: "a"},
			},
		},
		{
			s: "a is true",
			expr: &expr.UnaryExpr{
				Op:   expr.IS_TRUE,
				Expr: &expr.VarRef{Val: "a"},
			},
		},
		{
			s: "a is not true",
			expr: &expr.UnaryExpr{
				Op:   expr.IS_FALSE,
				Expr: &expr.VarRef{Val: "a"},
			},
		},
		{
			s: "not ! a is not true and c",
			expr: &expr.BinaryExpr{
				Op: expr.AND,
				LHS: &expr.UnaryExpr{
					Op: expr.NOT,
					Expr: &expr.UnaryExpr{
						Op: expr.IS_FALSE,
						Expr: &expr.UnaryExpr{
							Op:   expr.EXCLAMATION,
							Expr: &expr.VarRef{Val: "a"},
						},
					},
				},
				RHS: &expr.VarRef{Val: "c"},
			},
		},

		// Complex binary expression.
		{
			s: `value + 3 < 30 AND 1 + 2 OR true`,
			expr: &expr.BinaryExpr{
				Op: expr.OR,
				LHS: &expr.BinaryExpr{
					Op: expr.AND,
					LHS: &expr.BinaryExpr{
						Op: expr.LT,
						LHS: &expr.BinaryExpr{
							Op:  expr.ADD,
							LHS: &expr.VarRef{Val: "value"},
							RHS: &expr.NumberLiteral{Val: 3, Int: 3, Expr: "3", ExprType: expr.Unsigned},
						},
						RHS: &expr.NumberLiteral{Val: 30, Int: 30, Expr: "30", ExprType: expr.Unsigned},
					},
					RHS: &expr.BinaryExpr{
						Op:  expr.ADD,
						LHS: &expr.NumberLiteral{Val: 1, Int: 1, Expr: "1", ExprType: expr.Unsigned},
						RHS: &expr.NumberLiteral{Val: 2, Int: 2, Expr: "2", ExprType: expr.Unsigned},
					},
				},
				RHS: &expr.BooleanLiteral{Val: true},
			},
		},

		// Case
		{
			s: "case when a then b end",
			expr: &expr.Case{
				WhenThens: []expr.WhenThen{
					{
						When: &expr.VarRef{Val: "a"},
						Then: &expr.VarRef{Val: "b"},
					},
				},
			},
		},
		{
			s: "case when a then b else c end",
			expr: &expr.Case{
				WhenThens: []expr.WhenThen{
					{
						When: &expr.VarRef{Val: "a"},
						Then: &expr.VarRef{Val: "b"},
					},
				},
				Else: &expr.VarRef{Val: "c"},
			},
		},
		{
			s: "case when a then b when a2 then b2 else c end",
			expr: &expr.Case{
				WhenThens: []expr.WhenThen{
					{
						When: &expr.VarRef{Val: "a"},
						Then: &expr.VarRef{Val: "b"},
					},
					{
						When: &expr.VarRef{Val: "a2"},
						Then: &expr.VarRef{Val: "b2"},
					},
				},
				Else: &expr.VarRef{Val: "c"},
			},
		},
		{
			s:   "case end",
			err: "found END, expected WHEN at line 1, char 6",
		},
		{
			s:   "case else b end",
			err: "found ELSE, expected WHEN at line 1, char 6",
		},
		{
			s:   "case when a then b",
			err: "found EOF, expected END at line 1, char 20",
		},

		// Function call (empty)
		{
			s: `my_func()`,
			expr: &expr.Call{
				Name: "my_func",
			},
		},

		// Function call (multi-arg)
		{
			s: `my_func(1, -2 + 3)`,
			expr: &expr.Call{
				Name: "my_func",
				Args: []expr.Expr{
					&expr.NumberLiteral{Val: 1, Int: 1, Expr: "1", ExprType: expr.Unsigned},
					&expr.BinaryExpr{
						Op:  expr.ADD,
						LHS: &expr.NumberLiteral{Val: -2, Int: -2, Expr: "-2", ExprType: expr.Signed},
						RHS: &expr.NumberLiteral{Val: 3, Int: 3, Expr: "3", ExprType: expr.Unsigned},
					},
				},
			},
		},
		{
			s: `my_func1(field1, 'foo')`,
			expr: &expr.Call{
				Name: "my_func1",
				Args: []expr.Expr{
					&expr.VarRef{Val: "field1"},
					&expr.StringLiteral{
						Val: "foo",
					},
				},
			},
		},
	}

	for i, tt := range tests {
		exprV, err := expr.NewParser(strings.NewReader(tt.s)).ParseExpr(0)
		if !reflect.DeepEqual(tt.err, errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.s, tt.err, err)
		} else if tt.err == "" && !reflect.DeepEqual(tt.expr, exprV) {
			t.Errorf("%d. %q\n\nexpr mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.s, tt.expr, exprV)
		}
	}
}

// Ensure a string can be quoted.
func TestQuote(t *testing.T) {
	for i, tt := range []struct {
		in  string
		out string
	}{
		{``, `''`},
		{`foo`, `'foo'`},
		{"foo\nbar", `'foo\nbar'`},
		{`foo bar\\`, `'foo bar\\\\'`},
		{`'foo'`, `'\'foo\''`},
	} {
		if out := expr.QuoteString(tt.in); tt.out != out {
			t.Errorf("%d. %s: mismatch: %s != %s", i, tt.in, tt.out, out)
		}
	}
}

// Ensure an identifier's segments can be quoted.
func TestQuoteIdent(t *testing.T) {
	for i, tt := range []struct {
		ident []string
		s     string
	}{
		{[]string{``}, ``},
		{[]string{`select`}, `"select"`},
		{[]string{`in-bytes`}, `"in-bytes"`},
		{[]string{`foo`, `bar`}, `"foo".bar`},
		{[]string{`foo`, ``, `bar`}, `"foo"..bar`},
		{[]string{`foo bar`, `baz`}, `"foo bar".baz`},
		{[]string{`foo.bar`, `baz`}, `"foo.bar".baz`},
		{[]string{`foo.bar`, `rp`, `baz`}, `"foo.bar"."rp".baz`},
		{[]string{`foo.bar`, `rp`, `1baz`}, `"foo.bar"."rp"."1baz"`},
	} {
		if s := expr.QuoteIdent(tt.ident...); tt.s != s {
			t.Errorf("%d. %s: mismatch: %s != %s", i, tt.ident, tt.s, s)
		}
	}
}

// errstring converts an error to its string representation.
func errstring(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}
