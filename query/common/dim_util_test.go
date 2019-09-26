package common

import (
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber/aresdb/query/expr"
	memCom "github.com/uber/aresdb/memstore/common"
)

var _ = ginkgo.Describe("dim utils", func() {
	ginkgo.It("DimValResVectorSize should work", func() {
		Ω(DimValResVectorSize(3, DimCountsPerDimWidth{0, 0, 1, 1, 1})).Should(Equal(30))
		Ω(DimValResVectorSize(3, DimCountsPerDimWidth{0, 0, 2, 1, 1})).Should(Equal(45))
		Ω(DimValResVectorSize(3, DimCountsPerDimWidth{0, 0, 1, 0, 0})).Should(Equal(15))
		Ω(DimValResVectorSize(3, DimCountsPerDimWidth{0, 0, 1, 1, 0})).Should(Equal(24))
		Ω(DimValResVectorSize(3, DimCountsPerDimWidth{0, 0, 1, 0, 1})).Should(Equal(21))
		Ω(DimValResVectorSize(3, DimCountsPerDimWidth{0, 0, 0, 1, 1})).Should(Equal(15))
		Ω(DimValResVectorSize(3, DimCountsPerDimWidth{0, 0, 0, 1, 0})).Should(Equal(9))
		Ω(DimValResVectorSize(3, DimCountsPerDimWidth{0, 0, 0, 0, 1})).Should(Equal(6))
		Ω(DimValResVectorSize(0, DimCountsPerDimWidth{0, 0, 1, 1, 1})).Should(Equal(0))
	})

	ginkgo.It("GetDimensionDataType should work", func() {
		expr1 := &expr.VarRef{DataType: memCom.Uint32}
		Ω(GetDimensionDataType(expr1)).Should(Equal(memCom.Uint32))
		expr1 = &expr.VarRef{DataType: memCom.Float32}
		Ω(GetDimensionDataType(expr1)).Should(Equal(memCom.Float32))

		expr2 := &expr.Call{ExprType: expr.Unsigned}
		Ω(GetDimensionDataType(expr2)).Should(Equal(memCom.Uint32))
		expr2 = &expr.Call{ExprType: expr.Float}
		Ω(GetDimensionDataType(expr2)).Should(Equal(memCom.Float32))

		exprGeo := &expr.GeopointLiteral{Val :[2]float32{0.0, 0.0}}
		Ω(GetDimensionDataType(exprGeo)).Should(Equal(memCom.GeoPoint))
		exprUUID := &expr.UUIDLiteral{Val :[2]uint64{0, 0}}
		Ω(GetDimensionDataType(exprUUID)).Should(Equal(memCom.UUID))
	})
})
