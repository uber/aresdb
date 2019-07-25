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

package broker

import (
	"errors"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
	memCom "github.com/uber/aresdb/memstore/common"
	memComMocks "github.com/uber/aresdb/memstore/common/mocks"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/query/common"
	"github.com/uber/aresdb/query/expr"
	"net/http/httptest"
)

var _ = ginkgo.Describe("query compiler", func() {
	table1 := &metaCom.Table{
		Name: "table1",
		Columns: []metaCom.Column{
			{Name: "field1", Type: "Uint32"},
			{Name: "field2", Type: "Uint16"},
		},
	}
	tableSchema1 := memCom.NewTableSchema(table1)

	table2 := &metaCom.Table{
		Name: "table2",
		Columns: []metaCom.Column{
			{Name: "field2"},
		},
	}
	tableSchema2 := memCom.NewTableSchema(table2)

	ginkgo.It("should work happy path", func() {
		mockTableSchemaReader := memComMocks.TableSchemaReader{}
		mockTableSchemaReader.On("RLock").Return(nil)
		mockTableSchemaReader.On("RUnlock").Return(nil)
		mockTableSchemaReader.On("GetSchema", "table1").Return(tableSchema1, nil)
		mockTableSchemaReader.On("GetSchema", "table2").Return(tableSchema2, nil)

		qc := NewQueryContext(&common.AQLQuery{
			Table: "table1",
			Joins: []common.Join{
				{Table: "table2", Conditions: []string{"table1.field2 = table2.field2"}},
			},
			Dimensions: []common.Dimension{
				{
					Expr: "field1",
				},
			},
			Measures: []common.Measure{
				{
					Expr: "count(*)",
				},
			},
			SQLQuery: "SELECT count(*) FROM table1 JOIN table2 ON table1.field2 = table2.field2 GROUP BY field1",
		}, false, httptest.NewRecorder())
		qc.Compile(&mockTableSchemaReader)
		Ω(qc.Error).Should(BeNil())
		Ω(qc.AQLQuery).Should(Equal(&common.AQLQuery{
			Table: "table1",
			Joins: []common.Join{
				{
					Table:      "table2",
					Conditions: []string{"table1.field2 = table2.field2"},
					ConditionsParsed: []expr.Expr{
						&expr.BinaryExpr{
							Op: expr.EQ,
							LHS: &expr.VarRef{
								Val:      "table1.field2",
								ExprType: 2,
								ColumnID: 1,
								DataType: memCom.Uint16,
							},
							RHS: &expr.VarRef{
								Val:     "table2.field2",
								TableID: 1,
							},
							ExprType: 1,
						},
					},
				},
			},
			Dimensions: []common.Dimension{
				{
					Expr:       "field1",
					ExprParsed: &expr.VarRef{Val: "field1", ExprType: 2, TableID: 0, ColumnID: 0, DataType: memCom.Uint32},
				},
			},
			Measures: []common.Measure{
				{
					Expr:          "count(*)",
					ExprParsed:    &expr.Call{Name: "count", Args: []expr.Expr{&expr.Wildcard{}}, ExprType: 2},
					FiltersParsed: []expr.Expr{},
				},
			},
			FiltersParsed: []expr.Expr{},
			SQLQuery:      "SELECT count(*) FROM table1 JOIN table2 ON table1.field2 = table2.field2 GROUP BY field1",
		}))

		Ω(qc.NumDimsPerDimWidth).Should(Equal(common.DimCountsPerDimWidth{0, 0, 1, 0, 0}))
		Ω(qc.DimensionVectorIndex).Should(Equal([]int{0}))
		Ω(qc.DimRowBytes).Should(Equal(5))

		qc = NewQueryContext(&common.AQLQuery{
			Table: "table1",
			Joins: nil,
			Dimensions: []common.Dimension{
				{
					Expr: "*",
				},
			},
			Measures: []common.Measure{
				{
					Expr:       "1",
					ExprParsed: &expr.NumberLiteral{Val: 1, Int: 1, Expr: "1", ExprType: 2},
				},
			},
			Limit:    0,
			SQLQuery: "SELECT * FROM table1",
		}, false, httptest.NewRecorder())
		qc.Compile(&mockTableSchemaReader)
		Ω(qc.Error).Should(BeNil())
		Ω(qc.AQLQuery).Should(Equal(&common.AQLQuery{
			Table: "table1",
			Joins: nil,
			Dimensions: []common.Dimension{
				{Expr: "field1", ExprParsed: &expr.VarRef{Val: "field1", ExprType: 2, DataType: memCom.Uint32}},
				{Expr: "field2", ExprParsed: &expr.VarRef{Val: "field2", ColumnID: 1, ExprType: 2, DataType: memCom.Uint16}},
			},
			Measures: []common.Measure{
				{
					Expr:          "1",
					ExprParsed:    &expr.NumberLiteral{Val: 1, Int: 1, Expr: "1", ExprType: 2},
					FiltersParsed: []expr.Expr{},
				},
			},
			FiltersParsed: []expr.Expr{},
			Limit:         nonAggregationQueryLimit,
			SQLQuery:      "SELECT * FROM table1",
		}))
	})

	ginkgo.It("should fail invalid table names", func() {
		mockTableSchemaReader := memComMocks.TableSchemaReader{}
		mockTableSchemaReader.On("RLock").Return(nil)
		mockTableSchemaReader.On("RUnlock").Return(nil)
		mockTableSchemaReader.On("GetSchema", "tableNonExist").Return(nil, errors.New("not found"))

		qc := NewQueryContext(&common.AQLQuery{
			Table: "tableNonExist",
			Joins: nil,
			Dimensions: []common.Dimension{
				{
					Expr: "*",
				},
			},
			Measures: []common.Measure{
				{
					Expr:       "1",
					ExprParsed: &expr.NumberLiteral{Val: 1, Int: 1, Expr: "1", ExprType: 2},
				},
			},
			Limit:    nonAggregationQueryLimit,
			SQLQuery: "SELECT * FROM tableNonExist",
		}, false, httptest.NewRecorder())
		qc.Compile(&mockTableSchemaReader)
		Ω(qc.Error).ShouldNot(BeNil())

		mockTableSchemaReader.On("GetSchema", "table1").Return(tableSchema1, nil)
		mockTableSchemaReader.On("GetSchema", "foreignTableNonExsit").Return(nil, errors.New("no found"))
		qc = NewQueryContext(&common.AQLQuery{
			Table: "table1",
			Joins: []common.Join{
				{Table: "foreignTableNonExsit"},
			},
			Dimensions: []common.Dimension{
				{
					Expr: "*",
				},
			},
			Measures: []common.Measure{
				{
					Expr:       "1",
					ExprParsed: &expr.NumberLiteral{Val: 1, Int: 1, Expr: "1", ExprType: 2},
				},
			},
			Limit: nonAggregationQueryLimit,
		}, false, httptest.NewRecorder())
		qc.Compile(&mockTableSchemaReader)
		Ω(qc.Error).ShouldNot(BeNil())
	})

	ginkgo.It("should fail more than 1 measure", func() {
		mockTableSchemaReader := memComMocks.TableSchemaReader{}
		mockTableSchemaReader.On("RLock").Return(nil)
		mockTableSchemaReader.On("RUnlock").Return(nil)
		mockTableSchemaReader.On("GetSchema", mock.Anything).Return(tableSchema1, nil)

		qc := NewQueryContext(&common.AQLQuery{
			Table: "table1",
			Joins: nil,
			Dimensions: []common.Dimension{
				{
					Expr: "dim1",
				},
			},
			Measures: []common.Measure{
				{
					Expr: "measure1",
				},
				{
					Expr: "measure2",
				},
			},
			Limit: nonAggregationQueryLimit,
		}, false, httptest.NewRecorder())
		qc.Compile(&mockTableSchemaReader)
		Ω(qc.Error).ShouldNot(BeNil())
	})

	ginkgo.It("processMeasures should return error", func() {

		// invalid measure to parse
		qc := QueryContext{
			AQLQuery: &common.AQLQuery{
				Table: "tableNonExistent",
				Measures: []common.Measure{
					{Expr: "foo("},
				},
			},
		}

		qc.processMeasures()
		Ω(qc.Error.Error()).Should(ContainSubstring("Failed to parse measure"))

		// invalid measure expr type
		qc.Error = nil
		qc.AQLQuery.Measures[0].Expr = "1 = 2"
		qc.processMeasures()
		Ω(qc.Error.Error()).Should(ContainSubstring("expect aggregate function"))

		// invalid table
		qc.Error = nil
		qc.AQLQuery.Measures[0].Expr = "foo"
		qc.processMeasures()
		Ω(qc.Error.Error()).Should(ContainSubstring("unknown table"))

		// invalid number of args
		qc.Error = nil
		qc.AQLQuery.Measures[0].Expr = "sum(f1, f2)"
		qc.processMeasures()
		Ω(qc.Error.Error()).Should(ContainSubstring("expect 1 argument"))

		// invalid callname for hll query
		qc.Error = nil
		qc.ReturnHLLBinary = true
		qc.AQLQuery.Measures[0].Expr = "count(*)"
		qc.processMeasures()
		Ω(qc.Error.Error()).Should(ContainSubstring("expect hll aggregate function"))
	})

	ginkgo.It("expandINOp should work", func() {
		query := &common.AQLQuery{
			Table:   "table1",
			Filters: []string{"id in (1, 2)"},
		}
		tableSchema := &memCom.TableSchema{
			ColumnIDs: map[string]int{
				"time_col": 0,
				"id":       1,
			},
			Schema: metaCom.Table{
				Name:        "table1",
				IsFactTable: true,
				Columns: []metaCom.Column{
					{Name: "time_col", Type: metaCom.Uint32},
					{Name: "id", Type: metaCom.Uint16},
				},
			},
			ValueTypeByColumn: []memCom.DataType{
				memCom.Uint32,
				memCom.Uint16,
			},
		}
		qc := QueryContext{
			AQLQuery: query,
			TableSchemaByName: map[string]*memCom.TableSchema{
				"table1": tableSchema,
			},
			TableIDByAlias: map[string]int{
				"table1": 0,
			},
			Tables: []*memCom.TableSchema{
				tableSchema,
			},
		}

		qc.processFilters()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.AQLQuery.FiltersParsed).Should(HaveLen(1))
		Ω(qc.AQLQuery.FiltersParsed[0].String()).Should(Equal("id = 1 OR id = 2"))

		qc.AQLQuery.Filters[0] = "id in ()"
		qc.processFilters()
		Ω(qc.Error).ShouldNot(BeNil())

		qc.Error = nil
		qc.AQLQuery.Filters[0] = "id in (1)"
		qc.processFilters()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.AQLQuery.FiltersParsed[0].String()).Should(Equal("id = 1"))

		qc.Error = nil
		qc.AQLQuery.Filters[0] = "id in ('1')"
		qc.processFilters()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.AQLQuery.FiltersParsed[0].String()).Should(Equal("id = '1'"))

		qc.Error = nil
		qc.AQLQuery.Filters[0] = "id in (1,2,3)"
		qc.processFilters()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.AQLQuery.FiltersParsed[0].String()).Should(Equal("id = 1 OR id = 2 OR id = 3"))

		qc.Error = nil
		qc.AQLQuery.Filters[0] = "id not in (1,2,3)"
		qc.processFilters()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.AQLQuery.FiltersParsed[0].String()).Should(Equal("NOT(id = 1 OR id = 2 OR id = 3)"))
	})

	ginkgo.It("rewrite should work", func() {
		qc := QueryContext{}

		// paren
		Ω(qc.Rewrite(&expr.ParenExpr{Expr: &expr.StringLiteral{Val: "foo"}})).Should(Equal(&expr.StringLiteral{Val: "foo"}))

		// unary
		Ω(qc.Rewrite(&expr.UnaryExpr{
			Op:   expr.NOT,
			Expr: &expr.VarRef{ExprType: expr.Signed}},
		)).Should(Equal(&expr.UnaryExpr{
			Op:       expr.NOT,
			ExprType: expr.Boolean,
			Expr:     &expr.VarRef{ExprType: expr.Signed},
		}))
		Ω(qc.Rewrite(&expr.UnaryExpr{
			Op:       expr.UNARY_MINUS,
			ExprType: expr.Boolean,
			Expr:     &expr.VarRef{ExprType: expr.Signed},
		})).Should(Equal(&expr.UnaryExpr{
			Op:       expr.UNARY_MINUS,
			ExprType: expr.Signed,
			Expr:     &expr.VarRef{ExprType: expr.Signed},
		}))
		Ω(qc.Rewrite(&expr.UnaryExpr{
			Op:       expr.IS_NULL,
			ExprType: expr.Signed,
			Expr:     &expr.VarRef{ExprType: expr.Signed},
		})).Should(Equal(&expr.UnaryExpr{
			Op:       expr.IS_NULL,
			ExprType: expr.Boolean,
			Expr:     &expr.VarRef{ExprType: expr.Signed},
		}))
		Ω(qc.Rewrite(&expr.UnaryExpr{
			Op:       expr.IS_TRUE,
			ExprType: expr.Boolean,
			Expr:     &expr.VarRef{ExprType: expr.Boolean},
		})).Should(Equal(&expr.VarRef{ExprType: expr.Boolean}))

		// binary

		// cast to highest type for add
		Ω(qc.Rewrite(&expr.BinaryExpr{
			Op:       expr.ADD,
			ExprType: expr.Signed,
			LHS:      &expr.VarRef{Val: "f", ExprType: expr.Float},
			RHS:      &expr.VarRef{Val: "f", ExprType: expr.Signed},
		})).Should(Equal(&expr.BinaryExpr{
			Op:       expr.ADD,
			ExprType: expr.Float,
			LHS:      &expr.VarRef{Val: "f", ExprType: expr.Float},
			RHS: &expr.ParenExpr{
				Expr:     &expr.VarRef{Val: "f", ExprType: expr.Signed},
				ExprType: expr.Float,
			},
		}))

		// cast to signed for sub
		Ω(qc.Rewrite(&expr.BinaryExpr{
			Op:       expr.SUB,
			ExprType: expr.Unsigned,
			LHS:      &expr.VarRef{Val: "f", ExprType: expr.Unsigned},
			RHS:      &expr.VarRef{Val: "f", ExprType: expr.Unsigned},
		})).Should(Equal(&expr.BinaryExpr{
			Op:       expr.SUB,
			ExprType: expr.Signed,
			LHS:      &expr.VarRef{Val: "f", ExprType: expr.Unsigned},
			RHS:      &expr.VarRef{Val: "f", ExprType: expr.Unsigned},
		}))

		// cast to highest type for mul
		Ω(qc.Rewrite(&expr.BinaryExpr{
			Op:       expr.MUL,
			ExprType: expr.Unsigned,
			LHS:      &expr.VarRef{Val: "f", ExprType: expr.Unsigned},
			RHS:      &expr.VarRef{Val: "f", ExprType: expr.Signed},
		})).Should(Equal(&expr.BinaryExpr{
			Op:       expr.MUL,
			ExprType: expr.Signed,
			LHS:      &expr.VarRef{Val: "f", ExprType: expr.Unsigned},
			RHS:      &expr.VarRef{Val: "f", ExprType: expr.Signed},
		}))

		// cast to float type for div
		Ω(qc.Rewrite(&expr.BinaryExpr{
			Op:       expr.DIV,
			ExprType: expr.Unsigned,
			LHS:      &expr.VarRef{Val: "f", ExprType: expr.Unsigned},
			RHS:      &expr.VarRef{Val: "f", ExprType: expr.Unsigned},
		})).Should(Equal(&expr.BinaryExpr{
			Op:       expr.DIV,
			ExprType: expr.Float,
			LHS: &expr.ParenExpr{
				Expr:     &expr.VarRef{Val: "f", ExprType: expr.Unsigned},
				ExprType: expr.Float,
			},
			RHS: &expr.ParenExpr{
				Expr:     &expr.VarRef{Val: "f", ExprType: expr.Unsigned},
				ExprType: expr.Float,
			},
		}))

		// cast to unsigned for bitwise
		Ω(qc.Rewrite(&expr.BinaryExpr{
			Op:       expr.BITWISE_AND,
			ExprType: expr.Signed,
			LHS:      &expr.VarRef{Val: "f", ExprType: expr.Signed},
			RHS:      &expr.VarRef{Val: "f", ExprType: expr.Signed},
		})).Should(Equal(&expr.BinaryExpr{
			Op:       expr.BITWISE_AND,
			ExprType: expr.Unsigned,
			LHS:      &expr.VarRef{Val: "f", ExprType: expr.Signed},
			RHS:      &expr.VarRef{Val: "f", ExprType: expr.Signed},
		}))

		// cast to boolean for and and or
		Ω(qc.Rewrite(&expr.BinaryExpr{
			Op:       expr.AND,
			ExprType: expr.Signed,
			LHS:      &expr.VarRef{Val: "f", ExprType: expr.Signed},
			RHS:      &expr.VarRef{Val: "f", ExprType: expr.Signed},
		})).Should(Equal(&expr.BinaryExpr{
			Op:       expr.AND,
			ExprType: expr.Boolean,
			LHS:      &expr.VarRef{Val: "f", ExprType: expr.Signed},
			RHS:      &expr.VarRef{Val: "f", ExprType: expr.Signed},
		}))

		// cast to boolean for comparison
		Ω(qc.Rewrite(&expr.BinaryExpr{
			Op:       expr.LT,
			ExprType: expr.Signed,
			LHS:      &expr.VarRef{Val: "f", ExprType: expr.Signed},
			RHS:      &expr.VarRef{Val: "f", ExprType: expr.Signed},
		})).Should(Equal(&expr.BinaryExpr{
			Op:       expr.LT,
			ExprType: expr.Boolean,
			LHS:      &expr.VarRef{Val: "f", ExprType: expr.Signed},
			RHS:      &expr.VarRef{Val: "f", ExprType: expr.Signed},
		}))

		// EQ NEQ
		// rhs bool
		Ω(qc.Rewrite(&expr.BinaryExpr{
			Op:       expr.EQ,
			ExprType: expr.Signed,
			LHS:      &expr.BooleanLiteral{Val: true},
			RHS:      &expr.VarRef{Val: "f", ExprType: expr.Signed},
		})).Should(Equal(&expr.UnaryExpr{
			Expr:     &expr.VarRef{Val: "f", ExprType: expr.Signed},
			Op:       expr.IS_TRUE,
			ExprType: expr.Boolean,
		}))
		Ω(qc.Rewrite(&expr.BinaryExpr{
			Op:       expr.EQ,
			ExprType: expr.Signed,
			LHS:      &expr.BooleanLiteral{Val: false},
			RHS:      &expr.VarRef{Val: "f", ExprType: expr.Signed},
		})).Should(Equal(&expr.UnaryExpr{
			Expr:     &expr.VarRef{Val: "f", ExprType: expr.Signed},
			Op:       expr.NOT,
			ExprType: expr.Boolean,
		}))
		// rhs enum
		Ω(qc.Rewrite(&expr.BinaryExpr{
			Op:       expr.EQ,
			ExprType: expr.Signed,
			LHS:      &expr.StringLiteral{Val: "foo"},
			RHS:      &expr.VarRef{Val: "f", ExprType: expr.Unsigned, EnumDict: map[string]int{"foo": 1}},
		})).Should(Equal(&expr.BinaryExpr{
			LHS:      &expr.VarRef{Val: "f", ExprType: expr.Unsigned, EnumDict: map[string]int{"foo": 1}},
			RHS:      &expr.NumberLiteral{Int: 1, ExprType: expr.Unsigned},
			Op:       expr.EQ,
			ExprType: expr.Boolean,
		}))
		// rhs geopoint
		pointStr := "POINT (30 10)"
		val, _ := memCom.GeoPointFromString(pointStr)
		Ω(qc.Rewrite(&expr.BinaryExpr{
			Op:       expr.EQ,
			ExprType: expr.Signed,
			LHS:      &expr.VarRef{Val: "f", DataType: memCom.GeoPoint},
			RHS:      &expr.StringLiteral{Val: "POINT (30 10)"},
		})).Should(Equal(&expr.BinaryExpr{
			LHS:      &expr.VarRef{Val: "f", DataType: memCom.GeoPoint},
			RHS:      &expr.GeopointLiteral{Val: val},
			Op:       expr.EQ,
			ExprType: expr.Boolean,
		}))
	})

	ginkgo.It("rewrite should fail", func() {
		qc := QueryContext{
			TableIDByAlias: map[string]int{"t": 0},
			Tables: []*memCom.TableSchema{
				{
					Schema: metaCom.Table{
						Name: "t",
						Columns: []metaCom.Column{
							{Deleted: true},
						},
					},
					ColumnIDs: map[string]int{"f": 0},
				},
			},
			AQLQuery: &common.AQLQuery{
				Table: "t",
			},
		}

		// deleted column
		qc.Rewrite(&expr.VarRef{Val: "f"})
		Ω(qc.Error.Error()).Should(ContainSubstring("has been deleted"))

		// unary
		qc.Error = nil
		qc.Rewrite(&expr.UnaryExpr{Op: expr.NOT, Expr: &expr.VarRef{DataType: memCom.UUID}})
		Ω(qc.Error.Error()).Should(ContainSubstring("uuid column type only supports"))
		qc.Error = nil
		qc.Rewrite(&expr.UnaryExpr{Op: expr.UNARY_MINUS, Expr: &expr.VarRef{DataType: memCom.GeoPoint}})
		Ω(qc.Error.Error()).Should(ContainSubstring("numeric operations not supported for column over 4 bytes length"))

		// binary
		qc.Error = nil
		qc.Rewrite(&expr.BinaryExpr{
			Op:       expr.SUB,
			ExprType: expr.Boolean,
			LHS:      &expr.VarRef{ExprType: expr.GeoPoint, DataType: memCom.GeoPoint},
			RHS:      &expr.VarRef{ExprType: expr.GeoPoint, DataType: memCom.GeoPoint},
		})
		Ω(qc.Error.Error()).Should(ContainSubstring("numeric operations not supported for column over 4 bytes length"))

		qc.Error = nil
		qc.Rewrite(&expr.BinaryExpr{
			Op:       expr.SUB,
			ExprType: expr.Boolean,
			LHS:      &expr.StringLiteral{Val: "foo"},
			RHS:      &expr.StringLiteral{Val: "foo"},
		})
		Ω(qc.Error.Error()).Should(ContainSubstring("string type only support EQ and NEQ operators"))

		// call
		qc.Error = nil
		qc.Rewrite(&expr.Call{
			Name: "someRandomCallName",
		})
		Ω(qc.Error.Error()).Should(ContainSubstring("unknown function"))
	})

	ginkgo.It("convert_tz should work", func() {
		query := &common.AQLQuery{
			Table: "table1",
			Filters: []string{
				"convert_tz(table1.time_col, 'GMT', 'America/Phoenix') = 2",
				"convert_tz(from_unixtime(table1.time_col / 1000), 'GMT', 'America/Phoenix') = 2",
			},
		}
		tableSchema := &memCom.TableSchema{
			ColumnIDs: map[string]int{
				"time_col": 0,
				"id":       1,
			},
			Schema: metaCom.Table{
				Name:        "table1",
				IsFactTable: true,
				Columns: []metaCom.Column{
					{Name: "time_col", Type: metaCom.Uint32},
					{Name: "id", Type: metaCom.Uint16},
				},
			},
			ValueTypeByColumn: []memCom.DataType{
				memCom.Uint32,
				memCom.Uint16,
			},
		}
		qc := QueryContext{
			AQLQuery: query,
			TableSchemaByName: map[string]*memCom.TableSchema{
				"table1": tableSchema,
			},
			TableIDByAlias: map[string]int{
				"table1": 0,
			},
			Tables: []*memCom.TableSchema{
				tableSchema,
			},
		}

		qc.processFilters()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.AQLQuery.FiltersParsed).Should(HaveLen(2))
		Ω(qc.AQLQuery.FiltersParsed[0].String()).Should(Equal("table1.time_col + -25200 = 2"))
		Ω(qc.AQLQuery.FiltersParsed[1].String()).Should(Equal("table1.time_col + -25200 = 2"))

		qc.AQLQuery.Filters = []string{"convert_tz(from_unixtime(table1.time_col), 'GMT', 'America/Phoenix') = 2"}
		qc.processFilters()
		Ω(qc.Error).ShouldNot(BeNil())
		Ω(qc.Error.Error()).Should(ContainSubstring("from_unixtime must be time column / 1000"))
	})
})
