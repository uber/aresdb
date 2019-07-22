package common

import (
	"github.com/uber/aresdb/query/expr"
	memCom "github.com/uber/aresdb/memstore/common"
)

// DataTypeToExprType maps data type from the column schema format to
// expression AST format.
var DataTypeToExprType = map[memCom.DataType]expr.Type{
	memCom.Bool:      expr.Boolean,
	memCom.Int8:      expr.Signed,
	memCom.Int16:     expr.Signed,
	memCom.Int32:     expr.Signed,
	memCom.Int64:     expr.Signed,
	memCom.Uint8:     expr.Unsigned,
	memCom.Uint16:    expr.Unsigned,
	memCom.Uint32:    expr.Unsigned,
	memCom.Float32:   expr.Float,
	memCom.SmallEnum: expr.Unsigned,
	memCom.BigEnum:   expr.Unsigned,
	memCom.GeoPoint:  expr.GeoPoint,
	memCom.GeoShape:  expr.GeoShape,
}