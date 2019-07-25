package common

import (
	memCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/query/expr"
)

// GetDimensionDataType gets DataType for given expr
func GetDimensionDataType(expression expr.Expr) memCom.DataType {
	if e, ok := expression.(*expr.VarRef); ok {
		return e.DataType
	}
	switch expression.Type() {
	case expr.Boolean:
		return memCom.Bool
	case expr.Unsigned:
		return memCom.Uint32
	case expr.Signed:
		return memCom.Int32
	case expr.Float:
		return memCom.Float32
	default:
		return memCom.Uint32
	}
}

// GetDimensionDataBytes gets num bytes for given expr
func GetDimensionDataBytes(expression expr.Expr) int {
	return memCom.DataTypeBytes(GetDimensionDataType(expression))
}

// DimValueVectorSize returns the size of final dim value vector on host side.
func DimValResVectorSize(resultSize int, numDimsPerDimWidth DimCountsPerDimWidth) int {
	totalDims := 0
	for _, numDims := range numDimsPerDimWidth {
		totalDims += int(numDims)
	}

	dimBytes := 1 << uint(len(numDimsPerDimWidth)-1)
	var totalBytes int
	for _, numDims := range numDimsPerDimWidth {
		totalBytes += dimBytes * resultSize * int(numDims)
		dimBytes >>= 1
	}

	totalBytes += totalDims * resultSize
	return totalBytes
}
