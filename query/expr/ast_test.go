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
	"testing"

	"encoding/json"
	"github.com/stretchr/testify/assert"
)

// Ensure an AST node can be rewritten.
func TestRewrite(t *testing.T) {
	expression, _ := ParseExpr(`time > 1 OR foo = 2`)

	// Flip LHS & RHS in all binary expressions.
	act := RewriteFunc(expression, func(e Expr) Expr {
		switch e := e.(type) {
		case *BinaryExpr:
			return &BinaryExpr{Op: e.Op, LHS: e.RHS, RHS: e.LHS}
		default:
			return e
		}
	})

	// Verify that everything is flipped.
	if act := act.String(); act != `2 = foo OR 1 > time` {
		t.Fatalf("unexpected result: %s", act)
	}
}

func TestTypeMarshal(t *testing.T) {
	for tp, n := range typeNames {
		b, err := json.Marshal(tp)
		assert.NoError(t, err)
		assert.Equal(t, "\""+n+"\"", string(b))
	}
}
