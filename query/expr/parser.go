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
	"io"
	"strconv"
	"strings"
)

// Parser represents an InfluxQL parser.
type Parser struct {
	s *bufScanner
}

// NewParser returns a new instance of Parser.
func NewParser(r io.Reader) *Parser {
	return &Parser{s: newBufScanner(r)}
}

// ParseExpr parses an expression string and returns its AST representation.
func ParseExpr(s string) (Expr, error) { return NewParser(strings.NewReader(s)).ParseExpr(0) }

// parseInt parses a string and returns an integer literal.
func (p *Parser) parseInt(min, max int) (int, error) {
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok != NUMBER {
		return 0, newParseError(tokstr(tok, lit), []string{"number"}, pos)
	}

	// Return an error if the number has a fractional part.
	if strings.Contains(lit, ".") {
		return 0, &ParseError{Message: "number must be an integer", Pos: pos}
	}

	// Convert string to int.
	n, err := strconv.Atoi(lit)
	if err != nil {
		return 0, &ParseError{Message: err.Error(), Pos: pos}
	} else if min > n || n > max {
		return 0, &ParseError{
			Message: fmt.Sprintf("invalid value %d: must be %d <= n <= %d", n, min, max),
			Pos:     pos,
		}
	}

	return n, nil
}

// parseUInt32 parses a string and returns a 32-bit unsigned integer literal.
func (p *Parser) parseUInt32() (uint32, error) {
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok != NUMBER {
		return 0, newParseError(tokstr(tok, lit), []string{"number"}, pos)
	}

	// Convert string to unsigned 32-bit integer
	n, err := strconv.ParseUint(lit, 10, 32)
	if err != nil {
		return 0, &ParseError{Message: err.Error(), Pos: pos}
	}

	return uint32(n), nil
}

// parseUInt64 parses a string and returns a 64-bit unsigned integer literal.
func (p *Parser) parseUInt64() (uint64, error) {
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok != NUMBER {
		return 0, newParseError(tokstr(tok, lit), []string{"number"}, pos)
	}

	// Convert string to unsigned 64-bit integer
	n, err := strconv.ParseUint(lit, 10, 64)
	if err != nil {
		return 0, &ParseError{Message: err.Error(), Pos: pos}
	}

	return uint64(n), nil
}

// parseIdent parses an identifier.
func (p *Parser) parseIdent() (string, error) {
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok != IDENT {
		return "", newParseError(tokstr(tok, lit), []string{"identifier"}, pos)
	}
	return lit, nil
}

// parseIdentList parses a comma delimited list of identifiers.
func (p *Parser) parseIdentList() ([]string, error) {
	// Parse first (required) identifier.
	ident, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	idents := []string{ident}

	// Parse remaining (optional) identifiers.
	for {
		if tok, _, _ := p.scanIgnoreWhitespace(); tok != COMMA {
			p.unscan()
			return idents, nil
		}

		if ident, err = p.parseIdent(); err != nil {
			return nil, err
		}

		idents = append(idents, ident)
	}
}

// parseSegmentedIdents parses a segmented identifiers.
// e.g.,  "db"."rp".measurement  or  "db"..measurement
func (p *Parser) parseSegmentedIdents() ([]string, error) {
	ident, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	idents := []string{ident}

	// Parse remaining (optional) identifiers.
	for {
		if tok, _, _ := p.scan(); tok != DOT {
			// No more segments so we're done.
			p.unscan()
			break
		}

		if ch := p.peekRune(); ch == '/' {
			// Next segment is a regex so we're done.
			break
		} else if ch == '.' {
			// Add an empty identifier.
			idents = append(idents, "")
			continue
		}

		// Parse the next identifier.
		if ident, err = p.parseIdent(); err != nil {
			return nil, err
		}

		idents = append(idents, ident)
	}

	if len(idents) > 3 {
		msg := fmt.Sprintf("too many segments in %s", QuoteIdent(idents...))
		return nil, &ParseError{Message: msg}
	}

	return idents, nil
}

// parserString parses a string.
func (p *Parser) parseString() (string, error) {
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok != STRING {
		return "", newParseError(tokstr(tok, lit), []string{"string"}, pos)
	}
	return lit, nil
}

// peekRune returns the next rune that would be read by the scanner.
func (p *Parser) peekRune() rune {
	r, _, _ := p.s.s.r.ReadRune()
	if r != eof {
		_ = p.s.s.r.UnreadRune()
	}

	return r
}

// parseOptionalTokenAndInt parses the specified token followed
// by an int, if it exists.
func (p *Parser) parseOptionalTokenAndInt(t Token) (int, error) {
	// Check if the token exists.
	if tok, _, _ := p.scanIgnoreWhitespace(); tok != t {
		p.unscan()
		return 0, nil
	}

	// Scan the number.
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok != NUMBER {
		return 0, newParseError(tokstr(tok, lit), []string{"number"}, pos)
	}

	// Return an error if the number has a fractional part.
	if strings.Contains(lit, ".") {
		msg := fmt.Sprintf("fractional parts not allowed in %s", t.String())
		return 0, &ParseError{Message: msg, Pos: pos}
	}

	// Parse number.
	n, _ := strconv.ParseInt(lit, 10, 64)

	if n < 0 {
		msg := fmt.Sprintf("%s must be >= 0", t.String())
		return 0, &ParseError{Message: msg, Pos: pos}
	}

	return int(n), nil
}

// parseVarRef parses a reference to a measurement or field.
func (p *Parser) parseVarRef() (*VarRef, error) {
	// Parse the segments of the variable ref.
	segments, err := p.parseSegmentedIdents()
	if err != nil {
		return nil, err
	}

	vr := &VarRef{Val: strings.Join(segments, ".")}

	return vr, nil
}

func rewriteIsOp(expr Expr) (Token, error) {
	affirmative := true
	if unary, ok := expr.(*UnaryExpr); ok {
		if unary.Op == NOT {
			affirmative = false
			expr = unary.Expr
		} else {
			return IS, fmt.Errorf("bad literal %s following IS", expr.String())
		}
	}
	switch e := expr.(type) {
	case *NullLiteral:
		if affirmative {
			return IS_NULL, nil
		}
		return IS_NOT_NULL, nil
	case *UnknownLiteral:
		if affirmative {
			return IS_NULL, nil
		}
		return IS_NOT_NULL, nil
	case *BooleanLiteral:
		if affirmative == e.Val {
			return IS_TRUE, nil
		}

		return IS_FALSE, nil
	}
	return IS, fmt.Errorf("bad literal %s following IS (NOT)", expr.String())
}

func rewriteIsExpr(expr Expr) (Expr, error) {
	e, ok := expr.(*BinaryExpr)
	if !ok {
		return expr, nil
	}

	if e.Op == IS {
		op, err := rewriteIsOp(e.RHS)
		if err != nil {
			return nil, err
		}
		expr, err := rewriteIsExpr(e.LHS)
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Op: op, Expr: expr}, nil
	}

	var err error
	e.LHS, err = rewriteIsExpr(e.LHS)
	if err != nil {
		return nil, err
	}
	e.RHS, err = rewriteIsExpr(e.RHS)
	if err != nil {
		return nil, err
	}
	return expr, nil
}

// ParseExpr parses an expression.
// binOpPrcdncLb: binary operator precedence lower bound.
// Any binary operator with a lower precedence than that will cause ParseExpr to stop.
// This is used for parsing binary operators following a unary operator.
func (p *Parser) ParseExpr(binOpPrcdncLb int) (Expr, error) {
	var err error
	// Dummy root node.
	root := &BinaryExpr{}

	// Parse a non-binary expression type to start.
	// This variable will always be the root of the expression tree.
	root.RHS, err = p.parseUnaryExpr(false)
	if err != nil {
		return nil, err
	}

	// Loop over operations and unary exprs and build a tree based on precendence.
	for {
		// If the next token is NOT an operator then return the expression.
		op, pos, lit := p.scanIgnoreWhitespace()
		if op == NOT {
			op, pos, lit = p.scanIgnoreWhitespace()
			if op == IN {
				op = NOT_IN
			} else {
				return nil, newParseError(tokstr(op, lit), []string{"IN"}, pos)
			}
		}
		if !op.isBinaryOperator() || op.Precedence() < binOpPrcdncLb {
			p.unscan()
			return rewriteIsExpr(root.RHS)
		}

		// Otherwise parse the next expression.
		var rhs Expr
		if rhs, err = p.parseUnaryExpr(op == IN || op == NOT_IN); err != nil {
			return nil, err
		}

		// Find the right spot in the tree to add the new expression by
		// descending the RHS of the expression tree until we reach the last
		// BinaryExpr or a BinaryExpr whose RHS has an operator with
		// precedence >= the operator being added.
		for node := root; ; {
			r, ok := node.RHS.(*BinaryExpr)
			if !ok || r.Op.Precedence() >= op.Precedence() {
				// Add the new expression here and break.
				node.RHS = &BinaryExpr{LHS: node.RHS, RHS: rhs, Op: op}
				break
			}
			node = r
		}
	}
}

// parseUnaryExpr parses an non-binary expression.
// TODO: shz@ revisit inclusion parameter when open sourcing
func (p *Parser) parseUnaryExpr(inclusion bool) (Expr, error) {
	// If the first token is a LPAREN then parse it as its own grouped expression.
	if tok, _, _ := p.scanIgnoreWhitespace(); tok == LPAREN {
		expr, err := p.ParseExpr(0)
		if err != nil {
			return nil, err
		}
		tok, pos, lit := p.scanIgnoreWhitespace()
		if tok == RPAREN {
			// Expect an RPAREN at the end.
			if inclusion {
				return &Call{Args: []Expr{expr}}, nil
			}
			return &ParenExpr{Expr: expr}, nil
		} else if tok == COMMA {
			// Parse a tuple as a function call with empty name.
			var args []Expr
			args = append(args, expr)

			for {
				// Parse an expression argument.
				arg, err := p.ParseExpr(0)
				if err != nil {
					return nil, err
				}
				args = append(args, arg)

				// If there's not a comma next then stop parsing arguments.
				if tok, _, _ := p.scan(); tok != COMMA {
					p.unscan()
					break
				}
			}

			// There should be a right parentheses at the end.
			if tok, pos, lit := p.scan(); tok != RPAREN {
				return nil, newParseError(tokstr(tok, lit), []string{")"}, pos)
			}

			return &Call{Args: args}, nil
		} else {
			return nil, newParseError(tokstr(tok, lit), []string{")"}, pos)
		}

	}
	p.unscan()

	// Read next token.
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok.isUnaryOperator() {
		expr, err := p.ParseExpr(tok.Precedence())
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Op: tok, Expr: expr}, nil
	}

	switch tok {
	case CASE:
		return p.parseCase()
	case IDENT:
		// If the next immediate token is a left parentheses, parse as function call.
		// Otherwise parse as a variable reference.
		if tok0, _, _ := p.scan(); tok0 == LPAREN {
			return p.parseCall(lit)
		}

		p.unscan() // unscan the last token (wasn't an LPAREN)
		p.unscan() // unscan the IDENT token

		// Parse it as a VarRef.
		return p.parseVarRef()
	case DISTINCT:
		// If the next immediate token is a left parentheses, parse as function call.
		// Otherwise parse as a Distinct expression.
		tok0, pos, lit := p.scan()
		if tok0 == LPAREN {
			return p.parseCall("distinct")
		} else if tok0 == WS {
			tok1, pos, lit := p.scanIgnoreWhitespace()
			if tok1 != IDENT {
				return nil, newParseError(tokstr(tok1, lit), []string{"identifier"}, pos)
			}
			return &Distinct{Val: lit}, nil
		}

		return nil, newParseError(tokstr(tok0, lit), []string{"(", "identifier"}, pos)
	case STRING:
		return &StringLiteral{Val: lit}, nil
	case NUMBER:
		v, _ := strconv.ParseFloat(lit, 64)
		e := &NumberLiteral{Val: v, Expr: lit}
		var err error
		e.Int, err = strconv.Atoi(e.Expr)
		if err != nil {
			e.ExprType = Float
			e.Int = int(v)
		} else if e.Int >= 0 {
			e.ExprType = Unsigned
		} else {
			e.ExprType = Signed
		}
		return e, nil
	case NULL:
		return &NullLiteral{}, nil
	case UNKNOWN:
		return &UnknownLiteral{}, nil
	case TRUE, FALSE:
		return &BooleanLiteral{Val: (tok == TRUE)}, nil
	case MUL:
		return &Wildcard{}, nil
	default:
		return nil, newParseError(tokstr(tok, lit), []string{"identifier", "string", "number", "bool"}, pos)
	}
}

// Assumes CASE token has been scanned.
func (p *Parser) parseCase() (*Case, error) {
	var kase Case
	var err error
	tok, pos, lit := p.scanIgnoreWhitespace()
	for tok == WHEN {
		var cond WhenThen

		cond.When, err = p.ParseExpr(0)
		if err != nil {
			return nil, err
		}

		tok, pos, lit = p.scanIgnoreWhitespace()
		if tok != THEN {
			return nil, newParseError(tokstr(tok, lit), []string{"THEN"}, pos)
		}

		cond.Then, err = p.ParseExpr(0)
		if err != nil {
			return nil, err
		}

		kase.WhenThens = append(kase.WhenThens, cond)
		tok, pos, lit = p.scanIgnoreWhitespace()
	}

	if len(kase.WhenThens) == 0 {
		return nil, newParseError(tokstr(tok, lit), []string{"WHEN"}, pos)
	}

	if tok == ELSE {
		kase.Else, err = p.ParseExpr(0)
		if err != nil {
			return nil, err
		}
		tok, pos, lit = p.scanIgnoreWhitespace()
	}

	if tok != END {
		return nil, newParseError(tokstr(tok, lit), []string{"END"}, pos)
	}
	return &kase, nil
}

// parseCall parses a function call.
// This function assumes the function name and LPAREN have been consumed.
func (p *Parser) parseCall(name string) (*Call, error) {
	name = strings.ToLower(name)
	// If there's a right paren then just return immediately.
	if tok, _, _ := p.scan(); tok == RPAREN {
		return &Call{Name: name}, nil
	}
	p.unscan()

	// Otherwise parse function call arguments.
	var args []Expr
	for {
		// Parse an expression argument.
		arg, err := p.ParseExpr(0)
		if err != nil {
			return nil, err
		}
		args = append(args, arg)

		// If there's not a comma next then stop parsing arguments.
		if tok, _, _ := p.scan(); tok != COMMA {
			p.unscan()
			break
		}
	}

	// There should be a right parentheses at the end.
	if tok, pos, lit := p.scan(); tok != RPAREN {
		return nil, newParseError(tokstr(tok, lit), []string{")"}, pos)
	}

	return &Call{Name: name, Args: args}, nil
}

// scan returns the next token from the underlying scanner.
func (p *Parser) scan() (tok Token, pos Pos, lit string) { return p.s.Scan() }

// scanIgnoreWhitespace scans the next non-whitespace token.
func (p *Parser) scanIgnoreWhitespace() (tok Token, pos Pos, lit string) {
	tok, pos, lit = p.scan()
	if tok == WS {
		tok, pos, lit = p.scan()
	}
	return
}

// consumeWhitespace scans the next token if it's whitespace.
func (p *Parser) consumeWhitespace() {
	if tok, _, _ := p.scan(); tok != WS {
		p.unscan()
	}
}

// unscan pushes the previously read token back onto the buffer.
func (p *Parser) unscan() { p.s.Unscan() }

// QuoteString returns a quoted string.
func QuoteString(s string) string {
	return `'` + strings.NewReplacer("\n", `\n`, `\`, `\\`, `'`, `\'`).Replace(s) + `'`
}

// QuoteIdent returns a quoted identifier from multiple bare identifiers.
func QuoteIdent(segments ...string) string {
	r := strings.NewReplacer("\n", `\n`, `\`, `\\`, `"`, `\"`)

	var buf bytes.Buffer
	for i, segment := range segments {
		needQuote := IdentNeedsQuotes(segment) ||
			((i < len(segments)-1) && segment != "") // not last segment && not ""

		if needQuote {
			_ = buf.WriteByte('"')
		}

		_, _ = buf.WriteString(r.Replace(segment))

		if needQuote {
			_ = buf.WriteByte('"')
		}

		if i < len(segments)-1 {
			_ = buf.WriteByte('.')
		}
	}
	return buf.String()
}

// IdentNeedsQuotes returns true if the ident string given would require quotes.
func IdentNeedsQuotes(ident string) bool {
	// check if this identifier is a keyword
	tok := Lookup(ident)
	if tok != IDENT {
		return true
	}
	for i, r := range ident {
		if i == 0 && !isIdentFirstChar(r) {
			return true
		} else if i > 0 && !isIdentChar(r) {
			return true
		}
	}
	return false
}

// split splits a string into a slice of runes.
func split(s string) (a []rune) {
	for _, ch := range s {
		a = append(a, ch)
	}
	return
}

// ParseError represents an error that occurred during parsing.
type ParseError struct {
	Message  string
	Found    string
	Expected []string
	Pos      Pos
}

// newParseError returns a new instance of ParseError.
func newParseError(found string, expected []string, pos Pos) *ParseError {
	return &ParseError{Found: found, Expected: expected, Pos: pos}
}

// Error returns the string representation of the error.
func (e *ParseError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("%s at line %d, char %d", e.Message, e.Pos.Line+1, e.Pos.Char+1)
	}
	return fmt.Sprintf("found %s, expected %s at line %d, char %d", e.Found, strings.Join(e.Expected, ", "), e.Pos.Line+1, e.Pos.Char+1)
}
