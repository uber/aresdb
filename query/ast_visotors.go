package query

import (
	"github.com/uber/aresdb/query/expr"
	memCom "github.com/uber/aresdb/memstore/common"
)

// columnUsageCollector is the visitor used to traverses an AST, finds VarRef columns
// and sets the usage bits in tableScanners. The VarRef nodes must have already
// been resolved and annotated with TableID and ColumnID.
type columnUsageCollector struct {
	tableScanners []*TableScanner
	usages        columnUsage
}

func (c columnUsageCollector) Visit(expression expr.Expr) expr.Visitor {
	switch e := expression.(type) {
	case *expr.VarRef:
		c.tableScanners[e.TableID].ColumnUsages[e.ColumnID] |= c.usages
	}
	return c
}

// foreignTableColumnDetector detects foreign table columns involved in AST
type foreignTableColumnDetector struct {
	hasForeignTableColumn bool
}

func (c *foreignTableColumnDetector) Visit(expression expr.Expr) expr.Visitor {
	switch e := expression.(type) {
	case *expr.VarRef:
		c.hasForeignTableColumn = c.hasForeignTableColumn || (e.TableID > 0)
	}
	return c
}

// geoTableUsageCollector traverses an AST expression tree, finds VarRef columns
// and check whether it uses any geo table columns.
type geoTableUsageCollector struct {
	geoIntersection geoIntersection
	useGeoTable     bool
}

func (g *geoTableUsageCollector) Visit(expression expr.Expr) expr.Visitor {
	switch e := expression.(type) {
	case *expr.VarRef:
		g.useGeoTable = g.useGeoTable || e.TableID == g.geoIntersection.shapeTableID
	}
	return g
}

// stringOperandCollector traverses an AST tree to see if any string operands exists
type stringOperandCollector struct {
	// string operation not supported, flag and report error
	stringOperandSeen bool
}

func (slc *stringOperandCollector) Visit(expression expr.Expr) expr.Visitor {
	if slc.stringOperandSeen == true {
		return nil
	}
	if _, ok := expression.(*expr.StringLiteral); ok {
		slc.stringOperandSeen = true
		return nil
	}
	if varRefExpr, ok := expression.(*expr.VarRef); ok {
		slc.stringOperandSeen = varRefExpr.DataType == memCom.BigEnum || varRefExpr.DataType == memCom.SmallEnum
	}
	return slc
}