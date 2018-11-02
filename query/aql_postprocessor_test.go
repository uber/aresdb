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
	memCom "code.uber.internal/data/ares/memstore/common"
	"code.uber.internal/data/ares/memutils"
	queryCom "code.uber.internal/data/ares/query/common"
	"code.uber.internal/data/ares/query/expr"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"time"
	"unsafe"
)

var _ = ginkgo.Describe("AQL postprocessor", func() {
	ginkgo.It("works on empty result", func() {
		ctx := &AQLQueryContext{}
		Ω(ctx.Postprocess()).Should(Equal(queryCom.AQLTimeSeriesResult{}))
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
			Measure: &expr.VarRef{
				ExprType: expr.Float,
				DataType: memCom.Float32,
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

		Ω(ctx.Postprocess()).Should(Equal(queryCom.AQLTimeSeriesResult{
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
			Measure: &expr.VarRef{
				ExprType: expr.Float,
				DataType: memCom.Float32,
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
		Ω(ctx.Postprocess()).Should(Equal(queryCom.AQLTimeSeriesResult{
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
			Measure: &expr.VarRef{
				ExprType: expr.UnknownType,
				DataType: memCom.Unknown,
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
		*(*uint8)(memutils.MemAccess(oopkContext.dimensionVectorH, 4)) = 1

		Ω(ctx.Postprocess()).Should(Equal(queryCom.AQLTimeSeriesResult{
			"3.2": nil,
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
			Measure: &expr.VarRef{
				ExprType: expr.Float,
				DataType: memCom.Float32,
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
		Ω(ctx.Postprocess()).Should(Equal(queryCom.AQLTimeSeriesResult{
			"1970-01-01 00:00": map[string]interface{}{
				"2": 6.400000095367432,
			},
		}))

		ctx.Query.Dimensions[0].TimeBucketizer = "time of day"
		Ω(ctx.Postprocess()).Should(Equal(queryCom.AQLTimeSeriesResult{
			"00:03": map[string]interface{}{
				"2": float64(float32(3.2)),
			},
			"00:04": map[string]interface{}{
				"2": 6.400000095367432,
			},
		}))

		ctx.Query.Dimensions[0].TimeBucketizer = "hour of day"
		Ω(ctx.Postprocess()).Should(Equal(queryCom.AQLTimeSeriesResult{
			"00:00": map[string]interface{}{
				"2": 6.400000095367432,
			},
		}))

		ctx.Query.Dimensions[0].TimeBucketizer = "hour of week"
		Ω(ctx.Postprocess()).Should(Equal(queryCom.AQLTimeSeriesResult{
			"Monday 00:03": map[string]interface{}{
				"2": float64(float32(3.2)),
			},
			"Monday 00:04": map[string]interface{}{
				"2": 6.400000095367432,
			},
		}))

		ctx.Query.Dimensions[0].TimeBucketizer = "minute"
		Ω(ctx.Postprocess()).Should(Equal(queryCom.AQLTimeSeriesResult{
			"1970-01-01 00:03": map[string]interface{}{
				"2": float64(float32(3.2)),
			},
			"1970-01-01 00:04": map[string]interface{}{
				"2": 6.400000095367432,
			},
		}))

		ctx.Query.Dimensions[0].TimeBucketizer = "hour"
		Ω(ctx.Postprocess()).Should(Equal(queryCom.AQLTimeSeriesResult{
			"1970-01-01 00:00": map[string]interface{}{
				"2": 6.400000095367432,
			},
		}))

		ctx.Query.Dimensions[0].TimeBucketizer = "some invalid bucketizer"
		Ω(ctx.Postprocess()).Should(Equal(queryCom.AQLTimeSeriesResult{
			"190": map[string]interface{}{
				"2": float64(float32(3.2)),
			},
			"250": map[string]interface{}{
				"2": 6.400000095367432,
			},
		}))

		ctx.OOPK.dimensionVectorH = unsafe.Pointer(&[]uint8{1, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0, 1, 1, 1, 1}[0])
		ctx.Query.Dimensions[0].TimeBucketizer = "day of week"
		Ω(ctx.Postprocess()).Should(Equal(queryCom.AQLTimeSeriesResult{
			"Tuesday": map[string]interface{}{
				"2": float64(float32(3.2)),
			},
			"Wednesday": map[string]interface{}{
				"2": 6.400000095367432,
			},
		}))

	})

	ginkgo.It("works on dimensions time remedy", func() {
		query := &AQLQuery{
			Dimensions: []Dimension{
				{
					Expr:           "reqeust_at",
					TimeBucketizer: "d",
					TimeUnit:       "second",
				},
				{
					Expr: "",
				},
			},
			TimeFilter: TimeFilter{
				Column: "request_at",
				From:   "-1d",
				To:     "0d",
			},
		}
		tzloc, _ := time.LoadLocation("America/Los_Angeles")
		ctx := &AQLQueryContext{
			Query:         query,
			fixedTimezone: time.UTC,
		}
		ctx1 := &AQLQueryContext{
			Query:         query,
			fixedTimezone: tzloc,
		}
		ctx.parseExprs()
		ctx1.parseExprs()
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
			Measure: &expr.VarRef{
				ExprType: expr.Float,
				DataType: memCom.Float32,
			},
			MeasureBytes:       4,
			DimRowBytes:        10,
			NumDimsPerDimWidth: queryCom.DimCountsPerDimWidth{0, 0, 2, 0, 0},
			DimensionVectorIndex: []int{
				0,
				1,
			},
			ResultSize:       2,
			dimensionVectorH: unsafe.Pointer(&[]uint8{12, 0, 0, 0, 13, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0, 1, 1, 1, 1}[0]),
			measureVectorH:   unsafe.Pointer(&[]float32{3.2, 6.4}[0]),
		}

		ctx.OOPK = oopkContext
		Ω(ctx.Postprocess()).Should(Equal(queryCom.AQLTimeSeriesResult{
			"12": map[string]interface{}{
				"2": float64(float32(3.2)),
			},
			"13": map[string]interface{}{
				"2": 6.400000095367432,
			},
		}))

		ctx1.OOPK = oopkContext
		Ω(ctx1.Postprocess()).Should(Equal(queryCom.AQLTimeSeriesResult{
			"25212": map[string]interface{}{
				"2": float64(float32(3.2)),
			},
			"25213": map[string]interface{}{
				"2": 6.400000095367432,
			},
		}))

	})
})
