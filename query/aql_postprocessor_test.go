//  Copyright (c) 2017-2018 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package query

import (
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	memCom "github.com/uber/aresdb/memstore/common"
	queryCom "github.com/uber/aresdb/query/common"
	"github.com/uber/aresdb/query/expr"
	"github.com/uber/aresdb/utils"
	"time"
	"unsafe"
	"net/http/httptest"
)

var _ = ginkgo.Describe("AQL postprocessor", func() {
	ginkgo.It("works on empty result", func() {
		ctx := &AQLQueryContext{}
		ctx.Postprocess()
		Ω(ctx.Results).Should(Equal(queryCom.AQLQueryResult{}))
	})

	ginkgo.It("works on one dimension and one row", func() {
		ctx := &AQLQueryContext{
			Query: &AQLQuery{
				Dimensions: []Dimension{
					{Expr: ""},
				},
			},
		}
		oopkContext := OOPKContext{
			Dimensions: []expr.Expr{
				&expr.VarRef{
					ExprType: expr.Unsigned,
					DataType: memCom.Uint32,
				},
			},
			Measure: &expr.NumberLiteral{
				ExprType: expr.Float,
			},
			MeasureBytes:         4,
			DimRowBytes:          5,
			DimensionVectorIndex: []int{0},
			NumDimsPerDimWidth:   queryCom.DimCountsPerDimWidth{0, 0, 1, 0, 0},
			ResultSize:           1,
			dimensionVectorH:     unsafe.Pointer(&[]uint8{12, 0, 0, 0, 1}[0]),
			measureVectorH:       unsafe.Pointer(&[]float32{3.2}[0]),
		}

		ctx.OOPK = oopkContext
		ctx.initResultFlushContext()
		ctx.Postprocess()
		Ω(ctx.Results).Should(Equal(queryCom.AQLQueryResult{
			"12": float64(float32(3.2)),
		}))
	})

	ginkgo.It("works on two dimensions and two rows", func() {
		ctx := &AQLQueryContext{
			Query: &AQLQuery{
				Dimensions: []Dimension{
					{Expr: ""},
					{Expr: ""},
				},
			},
		}
		oopkContext := OOPKContext{
			Dimensions: []expr.Expr{
				&expr.VarRef{
					ExprType:        expr.Unsigned,
					DataType:        memCom.BigEnum,
					EnumReverseDict: []string{"zero", "one", "two"},
				},
				&expr.NumberLiteral{
					ExprType: expr.Signed,
				},
			},
			Measure: &expr.NumberLiteral{
				ExprType: expr.Float,
			},
			MeasureBytes:       4,
			DimRowBytes:        8,
			NumDimsPerDimWidth: queryCom.DimCountsPerDimWidth{0, 0, 1, 1, 0},
			DimensionVectorIndex: []int{
				1,
				0,
			},
			ResultSize:       2,
			dimensionVectorH: unsafe.Pointer(&[]uint8{12, 0, 0, 0, 0, 0, 0, 0, 2, 0, 2, 0, 1, 0, 1, 1}[0]),
			measureVectorH:   unsafe.Pointer(&[]float32{3.2, 6.4}[0]),
		}

		ctx.OOPK = oopkContext
		ctx.initResultFlushContext()
		ctx.Postprocess()
		Ω(ctx.Results).Should(Equal(queryCom.AQLQueryResult{
			"two": map[string]interface{}{
				"12":   float64(float32(3.2)),
				"NULL": float64(float32(6.4)),
			},
		}))
	})

	ginkgo.It("works on float dimension and nil measure", func() {
		ctx := &AQLQueryContext{
			Query: &AQLQuery{
				Dimensions: []Dimension{
					{Expr: ""},
				},
			},
		}
		oopkContext := OOPKContext{
			Dimensions: []expr.Expr{
				&expr.VarRef{
					ExprType: expr.Float,
					DataType: memCom.Float32,
				},
			},
			Measure: &expr.NumberLiteral{
				ExprType: expr.UnknownType,
			},
			DimRowBytes:        5,
			NumDimsPerDimWidth: queryCom.DimCountsPerDimWidth{0, 0, 1, 0, 0},
			DimensionVectorIndex: []int{
				0,
			},
			ResultSize:       1,
			dimensionVectorH: unsafe.Pointer(&[]uint8{0, 0, 0, 0, 0}[0]),
			measureVectorH:   unsafe.Pointer(&[]float32{3.2}[0]),
		}

		ctx.OOPK = oopkContext
		*(*float32)(oopkContext.dimensionVectorH) = 3.2
		*(*uint8)(utils.MemAccess(oopkContext.dimensionVectorH, 4)) = 1

		ctx.initResultFlushContext()
		ctx.Postprocess()
		Ω(ctx.Results).Should(Equal(queryCom.AQLQueryResult{
			"3.2": nil,
		}))
	})

	ginkgo.It("works for non agg queries", func() {
		ctx := &AQLQueryContext{
			Query: &AQLQuery{
				Dimensions: []Dimension{
					{Expr: "someField"},
				},
			},
			isNonAggregationQuery: true,
		}
		oopkContext := OOPKContext{
			Dimensions: []expr.Expr{
				&expr.VarRef{
					ExprType: expr.Float,
					DataType: memCom.Float32,
				},
			},
			DimRowBytes:        5,
			NumDimsPerDimWidth: queryCom.DimCountsPerDimWidth{0, 0, 1, 0, 0},
			DimensionVectorIndex: []int{
				0,
			},
			ResultSize:       1,
			dimensionVectorH: unsafe.Pointer(&[]uint8{0, 0, 0, 0, 0}[0]),
		}

		ctx.OOPK = oopkContext
		*(*float32)(oopkContext.dimensionVectorH) = 3.2
		*(*uint8)(utils.MemAccess(oopkContext.dimensionVectorH, 4)) = 1

		ctx.initResultFlushContext()
		ctx.Postprocess()
		Ω(ctx.Results).Should(BeNil())
		ctx.flushResultBuffer()
		Ω(ctx.Results).Should(Equal(queryCom.AQLQueryResult{
			"matrixData": [][]interface{}{{"3.2"}},
		}))
	})

	ginkgo.It("time Unit formatting works", func() {
		query := &AQLQuery{
			Dimensions: []Dimension{
				{
					TimeBucketizer: "h",
				},
				{
					Expr: "",
				},
			},
		}
		ctx := &AQLQueryContext{
			Query: query,
		}
		oopkContext := OOPKContext{
			Dimensions: []expr.Expr{
				&expr.VarRef{
					ExprType: expr.Unsigned,
					DataType: memCom.Uint32,
				},
				&expr.NumberLiteral{
					ExprType: expr.Signed,
				},
			},
			Measure: &expr.NumberLiteral{
				ExprType: expr.Float,
			},
			MeasureBytes:       4,
			DimRowBytes:        10,
			NumDimsPerDimWidth: queryCom.DimCountsPerDimWidth{0, 0, 2, 0, 0},
			DimensionVectorIndex: []int{
				0,
				1,
			},
			ResultSize:       2,
			dimensionVectorH: unsafe.Pointer(&[]uint8{190, 0, 0, 0, 250, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0, 1, 1, 1, 1}[0]),
			measureVectorH:   unsafe.Pointer(&[]float32{3.2, 6.4}[0]),
		}

		ctx.OOPK = oopkContext
		ctx.initResultFlushContext()
		ctx.Postprocess()
		Ω(ctx.Results).Should(Equal(queryCom.AQLQueryResult{
			"1970-01-01 00:00": map[string]interface{}{
				"2": 6.400000095367432,
			},
		}))

		ctx.Query.Dimensions[0].TimeBucketizer = "time of day"
		ctx.Results = nil
		ctx.Postprocess()
		Ω(ctx.Results).Should(Equal(queryCom.AQLQueryResult{
			"00:03": map[string]interface{}{
				"2": float64(float32(3.2)),
			},
			"00:04": map[string]interface{}{
				"2": 6.400000095367432,
			},
		}))

		ctx.Query.Dimensions[0].TimeBucketizer = "hour of day"
		ctx.Results = nil
		ctx.Postprocess()
		Ω(ctx.Results).Should(Equal(queryCom.AQLQueryResult{
			"00:00": map[string]interface{}{
				"2": 6.400000095367432,
			},
		}))

		ctx.Query.Dimensions[0].TimeBucketizer = "hour of week"
		ctx.Results = nil
		ctx.Postprocess()
		Ω(ctx.Results).Should(Equal(queryCom.AQLQueryResult{
			"Monday 00:03": map[string]interface{}{
				"2": float64(float32(3.2)),
			},
			"Monday 00:04": map[string]interface{}{
				"2": 6.400000095367432,
			},
		}))

		ctx.Query.Dimensions[0].TimeBucketizer = "minute"
		ctx.Results = nil
		ctx.Postprocess()
		Ω(ctx.Results).Should(Equal(queryCom.AQLQueryResult{
			"1970-01-01 00:03": map[string]interface{}{
				"2": float64(float32(3.2)),
			},
			"1970-01-01 00:04": map[string]interface{}{
				"2": 6.400000095367432,
			},
		}))

		ctx.Query.Dimensions[0].TimeBucketizer = "hour"
		ctx.Results = nil
		ctx.Postprocess()
		Ω(ctx.Results).Should(Equal(queryCom.AQLQueryResult{
			"1970-01-01 00:00": map[string]interface{}{
				"2": 6.400000095367432,
			},
		}))

		ctx.Query.Dimensions[0].TimeBucketizer = "some invalid bucketizer"
		ctx.Results = nil
		ctx.Postprocess()
		Ω(ctx.Results).Should(Equal(queryCom.AQLQueryResult{
			"190": map[string]interface{}{
				"2": float64(float32(3.2)),
			},
			"250": map[string]interface{}{
				"2": 6.400000095367432,
			},
		}))

		ctx.OOPK.dimensionVectorH = unsafe.Pointer(&[]uint8{1, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0, 1, 1, 1, 1}[0])
		ctx.Query.Dimensions[0].TimeBucketizer = "day of week"
		ctx.Results = nil
		ctx.Postprocess()
		Ω(ctx.Results).Should(Equal(queryCom.AQLQueryResult{
			"Tuesday": map[string]interface{}{
				"2": float64(float32(3.2)),
			},
			"Wednesday": map[string]interface{}{
				"2": 6.400000095367432,
			},
		}))

	})

	ginkgo.It("time dimension remedy should work", func() {
		query := &AQLQuery{
			Dimensions: []Dimension{
				{
					Expr:           "reqeust_at",
					TimeBucketizer: "d",
					TimeUnit:       "second",
				},
				{
					Expr: "1",
				},
			},
			TimeFilter: TimeFilter{
				Column: "request_at",
				From:   "-1d",
				To:     "0d",
			},
		}
		tzloc, _ := time.LoadLocation("Africa/Algiers")
		ctx := &AQLQueryContext{
			Query:         query,
			fixedTimezone: time.UTC,
		}
		ctx1 := &AQLQueryContext{
			Query:         query,
			fixedTimezone: tzloc,
		}
		ctx.parseExprs()
		Ω(ctx.Error).Should(BeNil())
		ctx1.parseExprs()
		Ω(ctx1.Error).Should(BeNil())
		oopkContext := OOPKContext{
			Dimensions: []expr.Expr{
				&expr.VarRef{
					ExprType: expr.Unsigned,
					DataType: memCom.Uint32,
				},
				&expr.NumberLiteral{
					ExprType: expr.Signed,
				},
			},
			Measure: &expr.NumberLiteral{
				ExprType: expr.Float,
			},
			MeasureBytes:       4,
			DimRowBytes:        10,
			NumDimsPerDimWidth: queryCom.DimCountsPerDimWidth{0, 0, 2, 0, 0},
			DimensionVectorIndex: []int{
				0,
				1,
			},
			ResultSize:       2,
			dimensionVectorH: unsafe.Pointer(&[]uint8{12, 100, 0, 0, 13, 100, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0, 1, 1, 1, 1}[0]),
			measureVectorH:   unsafe.Pointer(&[]float32{3.2, 6.4}[0]),
		}

		ctx.OOPK = oopkContext
		ctx.initResultFlushContext()
		ctx.Postprocess()
		Ω(ctx.Results).Should(Equal(queryCom.AQLQueryResult{
			"25612": map[string]interface{}{
				"2": float64(float32(3.2)),
			},
			"25613": map[string]interface{}{
				"2": 6.400000095367432,
			},
		}))

		ctx1.OOPK = oopkContext
		ctx1.initResultFlushContext()
		ctx1.Postprocess()
		Ω(ctx1.Results).Should(Equal(queryCom.AQLQueryResult{
			"22012": map[string]interface{}{
				"2": float64(float32(3.2)),
			},
			"22013": map[string]interface{}{
				"2": 6.400000095367432,
			},
		}))
	})

	ginkgo.It("readMeasure should work", func() {
		// read an 8 bytes int64
		measureVectorInt64 := [1]int64{1}
		measureAST := &expr.NumberLiteral{
			ExprType: expr.Signed,
		}

		measureVal := readMeasure(unsafe.Pointer(&measureVectorInt64[0]), measureAST, 8)
		Ω(measureVal).ShouldNot(BeNil())
		Ω(*measureVal).Should(Equal(1.0))

		// read an 8 bytes uint64
		measureVectorUint64 := [1]uint64{1 << 33}
		measureAST = &expr.NumberLiteral{
			ExprType: expr.Unsigned,
		}

		measureVal = readMeasure(unsafe.Pointer(&measureVectorUint64[0]), measureAST, 8)
		Ω(measureVal).ShouldNot(BeNil())
		Ω(*measureVal).Should(Equal(8.589934592e+09))

		// read an 8 bytes float64
		measureVectorFloat64 := [1]float64{2.0}
		measureAST = &expr.NumberLiteral{
			ExprType: expr.Float,
		}

		measureVal = readMeasure(unsafe.Pointer(&measureVectorFloat64[0]), measureAST, 8)
		Ω(measureVal).ShouldNot(BeNil())
		Ω(*measureVal).Should(Equal(2.0))

		// read a 4 bytes float
		measureVectorFloat32 := [2]float32{1.0, 0}
		measureAST = &expr.NumberLiteral{
			ExprType: expr.Float,
		}
		measureVal = readMeasure(unsafe.Pointer(&measureVectorFloat32[0]), measureAST, 4)
		Ω(measureVal).ShouldNot(BeNil())
		Ω(*measureVal).Should(BeEquivalentTo(1.0))

		// read a 4 bytes int32
		measureVectorInt32 := [2]int32{1,2}
		measureAST = &expr.NumberLiteral{
			ExprType: expr.Signed,
		}
		measureVal = readMeasure(unsafe.Pointer(&measureVectorInt32[0]), measureAST, 4)
		Ω(measureVal).ShouldNot(BeNil())
		Ω(*measureVal).Should(BeEquivalentTo(1))

		// read a 4 bytes uint32
		measureVectorUint32 := [2]uint32{1,2}
		measureAST = &expr.NumberLiteral{
			ExprType: expr.Unsigned,
		}
		measureVal = readMeasure(unsafe.Pointer(&measureVectorUint32[0]), measureAST, 4)
		Ω(measureVal).ShouldNot(BeNil())
		Ω(*measureVal).Should(BeEquivalentTo(1))
	})

	ginkgo.It("works for eager flushing non agg queries", func() {
		w := httptest.NewRecorder()
		ctx := &AQLQueryContext{
			Query: &AQLQuery{
				Dimensions: []Dimension{
					{Expr: "someField"},
				},
			},
			isNonAggregationQuery: true,
			ResponseWriter:        w,
		}
		oopkContext := OOPKContext{
			Dimensions: []expr.Expr{
				&expr.VarRef{
					ExprType: expr.Float,
					DataType: memCom.Float32,
				},
			},
			DimRowBytes:        5,
			NumDimsPerDimWidth: queryCom.DimCountsPerDimWidth{0, 0, 1, 0, 0},
			DimensionVectorIndex: []int{
				0,
			},
			ResultSize:       1,
			dimensionVectorH: unsafe.Pointer(&[]uint8{0, 0, 0, 0, 0}[0]),
		}

		ctx.OOPK = oopkContext
		*(*float32)(oopkContext.dimensionVectorH) = 3.2
		*(*uint8)(utils.MemAccess(oopkContext.dimensionVectorH, 4)) = 1

		ctx.initResultFlushContext()
		ctx.flushResultBuffer()
		Ω(w.Body.String()).Should(Equal(`["3.2"],`))

		ctx.OOPK.done = true
		ctx.flushResultBuffer()
		Ω(w.Body.String()).Should(Equal(`["3.2"],["3.2"]`))
	})
})
