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

// Ensure the scanner can scan tokens correctly.
func TestScanner_Scan(t *testing.T) {
	var tests = []struct {
		s   string
		tok expr.Token
		lit string
		pos expr.Pos
	}{
		// Special tokens (EOF, ILLEGAL, WS)
		{s: ``, tok: expr.EOF},
		{s: `#`, tok: expr.ILLEGAL, lit: `#`},
		{s: ` `, tok: expr.WS, lit: " "},
		{s: "\t", tok: expr.WS, lit: "\t"},
		{s: "\n", tok: expr.WS, lit: "\n"},
		{s: "\r", tok: expr.WS, lit: "\n"},
		{s: "\r\n", tok: expr.WS, lit: "\n"},
		{s: "\rX", tok: expr.WS, lit: "\n"},
		{s: "\n\r", tok: expr.WS, lit: "\n\n"},
		{s: " \n\t \r\n\t", tok: expr.WS, lit: " \n\t \n\t"},
		{s: " foo", tok: expr.WS, lit: " "},

		// Numeric operators
		{s: `*`, tok: expr.MUL},
		{s: `/`, tok: expr.DIV},

		// Logical operators
		{s: `AND`, tok: expr.AND},
		{s: `and`, tok: expr.AND},
		{s: `OR`, tok: expr.OR},
		{s: `or`, tok: expr.OR},

		{s: `=`, tok: expr.EQ},
		{s: `<>`, tok: expr.NEQ},
		{s: `! `, tok: expr.EXCLAMATION},
		{s: `<`, tok: expr.LT},
		{s: `<=`, tok: expr.LTE},
		{s: `>`, tok: expr.GT},
		{s: `>=`, tok: expr.GTE},

		// Misc tokens
		{s: `(`, tok: expr.LPAREN},
		{s: `)`, tok: expr.RPAREN},
		{s: `,`, tok: expr.COMMA},
		{s: `.`, tok: expr.DOT},

		// Identifiers
		{s: `foo`, tok: expr.IDENT, lit: `foo`},
		{s: `_foo`, tok: expr.IDENT, lit: `_foo`},
		{s: `Zx12_3U_-`, tok: expr.IDENT, lit: `Zx12_3U_`},
		{s: "`foo`", tok: expr.IDENT, lit: `foo`},
		{s: "`foo\\\\bar`", tok: expr.IDENT, lit: `foo\bar`},
		{s: "`foo\\bar`", tok: expr.BADESCAPE, lit: `\b`, pos: expr.Pos{Line: 0, Char: 5}},
		{s: "`foo\"bar\"`", tok: expr.IDENT, lit: `foo"bar"`},
		{s: "test`", tok: expr.BADSTRING, lit: "", pos: expr.Pos{Line: 0, Char: 3}},
		{s: "`test", tok: expr.BADSTRING, lit: `test`},

		{s: `true`, tok: expr.TRUE},
		{s: `false`, tok: expr.FALSE},

		// Strings
		{s: `'testing 123!'`, tok: expr.STRING, lit: `testing 123!`},
		{s: `"testing 123!"`, tok: expr.STRING, lit: `testing 123!`},
		{s: `'foo\nbar'`, tok: expr.STRING, lit: "foo\nbar"},
		{s: `'foo\\bar'`, tok: expr.STRING, lit: "foo\\bar"},
		{s: `'test`, tok: expr.BADSTRING, lit: `test`},
		{s: "'test\nfoo", tok: expr.BADSTRING, lit: `test`},
		{s: `'test\g'`, tok: expr.BADESCAPE, lit: `\g`, pos: expr.Pos{Line: 0, Char: 6}},

		// Numbers
		{s: `100`, tok: expr.NUMBER, lit: `100`},
		{s: `100.23`, tok: expr.NUMBER, lit: `100.23`},
		{s: `+100.23`, tok: expr.NUMBER, lit: `+100.23`},
		{s: `-100.23`, tok: expr.NUMBER, lit: `-100.23`},
		{s: `10.3s`, tok: expr.NUMBER, lit: `10.3`},

		// Keywords
		{s: `ALL`, tok: expr.ALL},
		{s: `AS`, tok: expr.AS},
		{s: `ASC`, tok: expr.ASC},
		{s: `BEGIN`, tok: expr.BEGIN},
		{s: `BY`, tok: expr.BY},
		{s: `DEFAULT`, tok: expr.DEFAULT},
		{s: `DELETE`, tok: expr.DELETE},
		{s: `DESC`, tok: expr.DESC},
		{s: `DROP`, tok: expr.DROP},
		{s: `END`, tok: expr.END},
		{s: `EXISTS`, tok: expr.EXISTS},
		{s: `FIELD`, tok: expr.FIELD},
		{s: `FROM`, tok: expr.FROM},
		{s: `GROUP`, tok: expr.GROUP},
		{s: `IF`, tok: expr.IF},
		{s: `INNER`, tok: expr.INNER},
		{s: `INSERT`, tok: expr.INSERT},
		{s: `KEY`, tok: expr.KEY},
		{s: `KEYS`, tok: expr.KEYS},
		{s: `LIMIT`, tok: expr.LIMIT},
		{s: `NOT`, tok: expr.NOT},
		{s: `OFFSET`, tok: expr.OFFSET},
		{s: `ON`, tok: expr.ON},
		{s: `ORDER`, tok: expr.ORDER},
		{s: `SELECT`, tok: expr.SELECT},
		{s: `TO`, tok: expr.TO},
		{s: `VALUES`, tok: expr.VALUES},
		{s: `WHERE`, tok: expr.WHERE},
		{s: `WITH`, tok: expr.WITH},
		{s: `seLECT`, tok: expr.SELECT}, // case insensitive
	}

	for i, tt := range tests {
		s := expr.NewScanner(strings.NewReader(tt.s))
		tok, pos, lit := s.Scan()
		if tt.tok != tok {
			t.Errorf("%d. %q token mismatch: exp=%q got=%q <%q>", i, tt.s, tt.tok, tok, lit)
		} else if tt.pos.Line != pos.Line || tt.pos.Char != pos.Char {
			t.Errorf("%d. %q pos mismatch: exp=%#v got=%#v", i, tt.s, tt.pos, pos)
		} else if tt.lit != lit {
			t.Errorf("%d. %q literal mismatch: exp=%q got=%q", i, tt.s, tt.lit, lit)
		}
	}
}

// Ensure the scanner can scan a series of tokens correctly.
func TestScanner_Scan_Multi(t *testing.T) {
	type result struct {
		tok expr.Token
		pos expr.Pos
		lit string
	}
	exp := []result{
		{tok: expr.SELECT, pos: expr.Pos{Line: 0, Char: 0}, lit: ""},
		{tok: expr.WS, pos: expr.Pos{Line: 0, Char: 6}, lit: " "},
		{tok: expr.IDENT, pos: expr.Pos{Line: 0, Char: 7}, lit: "value"},
		{tok: expr.WS, pos: expr.Pos{Line: 0, Char: 12}, lit: " "},
		{tok: expr.FROM, pos: expr.Pos{Line: 0, Char: 13}, lit: ""},
		{tok: expr.WS, pos: expr.Pos{Line: 0, Char: 17}, lit: " "},
		{tok: expr.IDENT, pos: expr.Pos{Line: 0, Char: 18}, lit: "myseries"},
		{tok: expr.WS, pos: expr.Pos{Line: 0, Char: 26}, lit: " "},
		{tok: expr.WHERE, pos: expr.Pos{Line: 0, Char: 27}, lit: ""},
		{tok: expr.WS, pos: expr.Pos{Line: 0, Char: 32}, lit: " "},
		{tok: expr.IDENT, pos: expr.Pos{Line: 0, Char: 33}, lit: "a"},
		{tok: expr.WS, pos: expr.Pos{Line: 0, Char: 34}, lit: " "},
		{tok: expr.EQ, pos: expr.Pos{Line: 0, Char: 35}, lit: ""},
		{tok: expr.WS, pos: expr.Pos{Line: 0, Char: 36}, lit: " "},
		{tok: expr.STRING, pos: expr.Pos{Line: 0, Char: 36}, lit: "b"},
		{tok: expr.EOF, pos: expr.Pos{Line: 0, Char: 40}, lit: ""},
	}

	// Create a scanner.
	v := `SELECT value from myseries WHERE a = 'b'`
	s := expr.NewScanner(strings.NewReader(v))

	// Continually scan until we reach the end.
	var act []result
	for {
		tok, pos, lit := s.Scan()
		act = append(act, result{tok, pos, lit})
		if tok == expr.EOF {
			break
		}
	}

	// Verify the token counts match.
	if len(exp) != len(act) {
		t.Fatalf("token count mismatch: exp=%d, got=%d", len(exp), len(act))
	}

	// Verify each token matches.
	for i := range exp {
		if !reflect.DeepEqual(exp[i], act[i]) {
			t.Fatalf("%d. token mismatch:\n\nexp=%#v\n\ngot=%#v", i, exp[i], act[i])
		}
	}
}

// Ensure the library can correctly scan strings.
func TestScanString(t *testing.T) {
	var tests = []struct {
		in  string
		out string
		err string
	}{
		{in: `""`, out: ``},
		{in: `"foo bar"`, out: `foo bar`},
		{in: `'foo bar'`, out: `foo bar`},
		{in: `"foo\nbar"`, out: "foo\nbar"},
		{in: `"foo\\bar"`, out: `foo\bar`},
		{in: `"foo\"bar"`, out: `foo"bar`},
		{in: `'foo\'bar'`, out: `foo'bar`},

		{in: `"foo` + "\n", out: `foo`, err: "bad string"}, // newline in string
		{in: `"foo`, out: `foo`, err: "bad string"},        // unclosed quotes
		{in: `"foo\xbar"`, out: `\x`, err: "bad escape"},   // invalid escape
	}

	for i, tt := range tests {
		out, err := expr.ScanString(strings.NewReader(tt.in))
		if tt.err != errstring(err) {
			t.Errorf("%d. %s: error: exp=%s, got=%s", i, tt.in, tt.err, err)
		} else if tt.out != out {
			t.Errorf("%d. %s: out: exp=%s, got=%s", i, tt.in, tt.out, out)
		}
	}
}
