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

	"time"
	"unsafe"

	"github.com/uber-go/tally"
	"github.com/uber/aresdb/common"
	"github.com/uber/aresdb/memstore"
	memCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/memstore/mocks"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/query/expr"
	"github.com/uber/aresdb/utils"
)

var _ = ginkgo.Describe("AQL compiler", func() {
	ginkgo.It("parses timezone", func() {
		qc := &AQLQueryContext{
			Query: &AQLQuery{Timezone: "timezone(city_id)"},
		}
		utils.Init(common.AresServerConfig{Query: common.QueryConfig{TimezoneTable: common.TimezoneConfig{
			TableName: "api_cities",
		}}}, common.NewLoggerFactory().GetDefaultLogger(), common.NewLoggerFactory().GetDefaultLogger(), tally.NewTestScope("test", nil))

		qc.processTimezone()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.timezoneTable.tableColumn).Should(Equal("timezone"))
		Ω(qc.Query.Joins).Should(HaveLen(1))
		Ω(qc.fixedTimezone).Should(BeNil())
	})

	ginkgo.It("parses expressions", func() {
		q := &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{
					Expr: "count(*)",
				},
			},
		}
		qc := &AQLQueryContext{
			Query: q,
		}
		qc.parseExprs()
		Ω(qc.Error).Should(BeNil())
		Ω(q.Measures[0].expr).Should(Equal(&expr.Call{
			Name: "count",
			Args: []expr.Expr{
				&expr.Wildcard{},
			},
		}))

		qc.Query = &AQLQuery{
			Table: "trips",
			Joins: []Join{
				{
					Table: "api_cities",
					Conditions: []string{
						"city_id = api_cities.id",
					},
				},
			},
			Dimensions: []Dimension{
				{
					Expr: "status",
				},
			},
			Measures: []Measure{
				{
					Expr: "count(*)",
					Filters: []string{
						"not is_faresplit",
					},
				},
			},
			Filters: []string{
				"marketplace='personal_transport'",
			},
		}
		qc.parseExprs()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.Query.Joins[0].conditions[0]).Should(Equal(&expr.BinaryExpr{
			Op:  expr.EQ,
			LHS: &expr.VarRef{Val: "city_id"},
			RHS: &expr.VarRef{Val: "api_cities.id"},
		}))
		Ω(qc.Query.Dimensions[0].expr).Should(Equal(&expr.VarRef{Val: "status"}))
		Ω(qc.Query.Measures[0].expr).Should(Equal(&expr.Call{
			Name: "count",
			Args: []expr.Expr{
				&expr.Wildcard{},
			},
		}))
		Ω(qc.Query.Measures[0].filters[0]).Should(Equal(&expr.UnaryExpr{
			Op:   expr.NOT,
			Expr: &expr.VarRef{Val: "is_faresplit"},
		}))
		Ω(qc.Query.filters[0]).Should(Equal(&expr.BinaryExpr{
			Op:  expr.EQ,
			LHS: &expr.VarRef{Val: "marketplace"},
			RHS: &expr.StringLiteral{Val: "personal_transport"},
		}))

		qc.Query = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{
					Expr: "count(",
				},
			},
		}
		qc.parseExprs()
		Ω(qc.Error).ShouldNot(BeNil())
	})

	ginkgo.It("reads schema", func() {
		store := new(mocks.MemStore)
		store.On("RLock").Return()
		store.On("RUnlock").Return()
		tripsSchema := &memstore.TableSchema{
			Schema: metaCom.Table{IsFactTable: true},
		}
		apiCitiesSchema := &memstore.TableSchema{}
		store.On("GetSchemas").Return(map[string]*memstore.TableSchema{
			"trips":      tripsSchema,
			"api_cities": apiCitiesSchema,
		})
		qc := &AQLQueryContext{
			Query: &AQLQuery{
				Table: "trips",
				Joins: []Join{
					{Table: "api_cities", Alias: "cts"},
					{Table: "trips", Alias: "tx"},
				},
			},
		}
		qc.readSchema(store)
		Ω(qc.Error).Should(BeNil())
		Ω(qc.TableIDByAlias).Should(Equal(map[string]int{
			"trips": 0,
			"cts":   1,
			"tx":    2,
		}))
		Ω(qc.TableScanners).Should(Equal([]*TableScanner{
			{
				Schema:       tripsSchema,
				Shards:       []int{0},
				ColumnUsages: map[int]columnUsage{0: columnUsedByLiveBatches},
			},
			{
				Schema:       apiCitiesSchema,
				Shards:       []int{0},
				ColumnUsages: map[int]columnUsage{},
			},
			{
				Schema:       tripsSchema,
				Shards:       []int{0},
				ColumnUsages: map[int]columnUsage{0: columnUsedByLiveBatches},
			},
		}))
		Ω(qc.TableSchemaByName).Should(Equal(map[string]*memstore.TableSchema{
			"trips":      tripsSchema,
			"api_cities": apiCitiesSchema,
		}))
		qc.releaseSchema()

		qc = &AQLQueryContext{
			Query: &AQLQuery{
				Table: "tripsy",
			},
		}
		qc.readSchema(store)
		Ω(qc.Error).ShouldNot(BeNil())
		qc.releaseSchema()

		qc = &AQLQueryContext{
			Query: &AQLQuery{
				Table: "trips",
				Joins: []Join{
					{Table: "trips"},
				},
			},
		}
		qc.readSchema(store)
		Ω(qc.Error).ShouldNot(BeNil())
		qc.releaseSchema()
	})

	ginkgo.It("numerical operations on column over 4 bytes long not supported", func() {
		qc := &AQLQueryContext{
			TableIDByAlias: map[string]int{
				"trips": 0,
			},
			TableScanners: []*TableScanner{
				{
					Schema: &memstore.TableSchema{
						ValueTypeByColumn: []memCom.DataType{
							memCom.Int64,
						},
						ColumnIDs: map[string]int{
							"hex_id": 0,
						},
						Schema: metaCom.Table{
							Columns: []metaCom.Column{
								{Name: "hex_id", Type: metaCom.Int64},
							},
						},
					},
				},
			},
		}
		qc.Query = &AQLQuery{
			Table: "trips",
			Dimensions: []Dimension{
				{Expr: "hex_id"},
			},
			Measures: []Measure{
				{Expr: "count(*)"},
			},
		}
		qc.parseExprs()
		Ω(qc.Error).Should(BeNil())

		qc.resolveTypes()
		Ω(qc.Error).Should(BeNil())

		qc.Query = &AQLQuery{
			Table: "trips",
			Dimensions: []Dimension{
				{Expr: "hex_id+1"},
			},
			Measures: []Measure{
				{Expr: "count(*)"},
			},
		}
		qc.parseExprs()
		Ω(qc.Error).Should(BeNil())

		qc.resolveTypes()
		Ω(qc.Error).ShouldNot(BeNil())
		Ω(qc.Error.Error()).Should(ContainSubstring("numeric operations not supported for column over 4 bytes length"))
	})

	ginkgo.It("resolves data types", func() {
		dict := map[string]int{
			"completed": 3,
		}
		qc := &AQLQueryContext{
			TableIDByAlias: map[string]int{
				"trips": 0,
			},
			TableScanners: []*TableScanner{
				{
					Schema: &memstore.TableSchema{
						ValueTypeByColumn: []memCom.DataType{
							memCom.Float32,
							memCom.Uint16,
							memCom.SmallEnum,
							memCom.Bool,
						},
						EnumDicts: map[string]memstore.EnumDict{
							"status": {
								Dict: dict,
							},
						},
						ColumnIDs: map[string]int{
							"fare":     0,
							"city_id":  1,
							"status":   2,
							"is_first": 3,
						},
						Schema: metaCom.Table{
							Columns: []metaCom.Column{
								{Name: "fare", Type: metaCom.Float32},
								{Name: "city_id", Type: metaCom.Uint16},
								{Name: "status", Type: metaCom.SmallEnum},
								{Name: "is_first", Type: metaCom.Bool},
							},
						},
					},
				},
			},
		}
		qc.Query = &AQLQuery{
			Table: "trips",
			Dimensions: []Dimension{
				{Expr: "-city_id"},
				{Expr: "~fare"},
				{Expr: "city_id-city_id"},
				{Expr: "city_id*fare"},
				{Expr: "1/2"},
				{Expr: "1.2|2.3"},
				{Expr: "case when 1.3 then 2 else 3.2 end"},
			},
			Measures: []Measure{
				{Expr: "count(*)"},
				{Expr: "Sum(fare+1)"},
			},
			Filters: []string{
				"status='completed'",
				"!is_first",
				"fare is not null",
				"is_first is true",
				"city_id is true",
				"1.2 or 2.3",
				"1 < 1.2",
				"status='incompleted'",
				"1 != 1.2",
				"is_first = false",
			},
		}
		qc.parseExprs()
		Ω(qc.Error).Should(BeNil())

		qc.resolveTypes()
		Ω(qc.Error).Should(BeNil())

		Ω(qc.Query.Dimensions[0].expr.Type()).Should(Equal(expr.Signed))
		Ω(qc.Query.Dimensions[1].expr).Should(Equal(&expr.UnaryExpr{
			Op:       expr.BITWISE_NOT,
			ExprType: expr.Unsigned,
			Expr: &expr.ParenExpr{ // explicit type casting
				ExprType: expr.Unsigned,
				Expr: &expr.VarRef{
					Val:      "fare",
					ExprType: expr.Float,
					DataType: memCom.Float32,
				},
			},
		}))
		Ω(qc.Query.Dimensions[2].expr).Should(Equal(&expr.BinaryExpr{
			Op:       expr.SUB,
			ExprType: expr.Signed,
			LHS: &expr.VarRef{
				Val:      "city_id",
				ColumnID: 1,
				ExprType: expr.Unsigned,
				DataType: memCom.Uint16,
			},
			RHS: &expr.VarRef{
				Val:      "city_id",
				ColumnID: 1,
				ExprType: expr.Unsigned,
				DataType: memCom.Uint16,
			},
		}))
		Ω(qc.Query.Dimensions[3].expr).Should(Equal(&expr.BinaryExpr{
			Op:       expr.MUL,
			ExprType: expr.Float,
			LHS: &expr.ParenExpr{ // explicit type casting
				ExprType: expr.Float,
				Expr: &expr.VarRef{
					Val:      "city_id",
					ColumnID: 1,
					ExprType: expr.Unsigned,
					DataType: memCom.Uint16,
				},
			},
			RHS: &expr.VarRef{
				Val:      "fare",
				ExprType: expr.Float,
				DataType: memCom.Float32,
			},
		}))
		Ω(qc.Query.Dimensions[4].expr).Should(Equal(&expr.BinaryExpr{
			Op:       expr.DIV,
			ExprType: expr.Float,
			LHS: &expr.NumberLiteral{
				Val:      1,
				Int:      1,
				Expr:     "1",
				ExprType: expr.Float,
			},
			RHS: &expr.NumberLiteral{
				Val:      2,
				Int:      2,
				Expr:     "2",
				ExprType: expr.Float,
			},
		}))
		Ω(qc.Query.Dimensions[5].expr).Should(Equal(&expr.BinaryExpr{
			Op:       expr.BITWISE_OR,
			ExprType: expr.Unsigned,
			LHS: &expr.NumberLiteral{
				Val:      1.2,
				Int:      1,
				Expr:     "1.2",
				ExprType: expr.Unsigned,
			},
			RHS: &expr.NumberLiteral{
				Val:      2.3,
				Int:      2,
				Expr:     "2.3",
				ExprType: expr.Unsigned,
			},
		}))
		Ω(qc.Query.Dimensions[6].expr).Should(Equal(&expr.Case{
			ExprType: expr.Float,
			Else: &expr.NumberLiteral{
				Val:      3.2,
				Int:      3,
				Expr:     "3.2",
				ExprType: expr.Float,
			},
			WhenThens: []expr.WhenThen{
				{
					When: &expr.NumberLiteral{
						Val:      1.3,
						Int:      1,
						Expr:     "1.3",
						ExprType: expr.Boolean,
					},
					Then: &expr.NumberLiteral{
						Val:      2,
						Int:      2,
						Expr:     "2",
						ExprType: expr.Float,
					},
				},
			},
		}))

		Ω(qc.Query.Measures[0].expr.Type()).Should(Equal(expr.Unsigned))
		Ω(qc.Query.Measures[1].expr).Should(Equal(&expr.Call{
			Name:     "sum",
			ExprType: expr.Float,
			Args: []expr.Expr{
				&expr.BinaryExpr{
					Op:       expr.ADD,
					ExprType: expr.Float,
					LHS: &expr.VarRef{
						Val:      "fare",
						ExprType: expr.Float,
						DataType: memCom.Float32,
					},
					RHS: &expr.NumberLiteral{
						Expr:     "1",
						Val:      1,
						Int:      1,
						ExprType: expr.Float,
					},
				},
			},
		}))

		Ω(qc.Query.filters[0]).Should(Equal(&expr.BinaryExpr{
			Op:       expr.EQ,
			ExprType: expr.Boolean,
			LHS: &expr.VarRef{
				Val:      "status",
				ColumnID: 2,
				ExprType: expr.Unsigned,
				EnumDict: dict,
				DataType: memCom.SmallEnum,
			},
			RHS: &expr.NumberLiteral{Int: 3, ExprType: expr.Unsigned},
		}))
		Ω(qc.Query.filters[1].Type()).Should(Equal(expr.Boolean))
		Ω(qc.Query.filters[1].(*expr.UnaryExpr).Op).Should(Equal(expr.NOT))
		Ω(qc.Query.filters[2].Type()).Should(Equal(expr.Boolean))
		Ω(qc.Query.filters[3]).Should(Equal(&expr.VarRef{
			Val:      "is_first",
			ColumnID: 3,
			ExprType: expr.Boolean,
			DataType: memCom.Bool,
		}))
		Ω(qc.Query.filters[4]).Should(Equal(&expr.UnaryExpr{
			Op:       expr.NOT,
			ExprType: expr.Boolean,
			Expr: &expr.UnaryExpr{
				Op:       expr.NOT,
				ExprType: expr.Boolean,
				Expr: &expr.VarRef{
					Val:      "city_id",
					ColumnID: 1,
					ExprType: expr.Unsigned,
					DataType: memCom.Uint16,
				},
			},
		}))
		Ω(qc.Query.filters[5]).Should(Equal(&expr.BinaryExpr{
			Op:       expr.OR,
			ExprType: expr.Boolean,
			LHS: &expr.NumberLiteral{
				Val:      1.2,
				Int:      1,
				Expr:     "1.2",
				ExprType: expr.Boolean,
			},
			RHS: &expr.NumberLiteral{
				Val:      2.3,
				Int:      2,
				Expr:     "2.3",
				ExprType: expr.Boolean,
			},
		}))
		Ω(qc.Query.filters[6]).Should(Equal(&expr.BinaryExpr{
			Op:       expr.LT,
			ExprType: expr.Boolean,
			LHS: &expr.NumberLiteral{
				Val:      1,
				Int:      1,
				Expr:     "1",
				ExprType: expr.Float,
			},
			RHS: &expr.NumberLiteral{
				Val:      1.2,
				Int:      1,
				Expr:     "1.2",
				ExprType: expr.Float,
			},
		}))
		Ω(qc.Query.filters[7]).Should(Equal(&expr.BinaryExpr{
			Op:       expr.EQ,
			ExprType: expr.Boolean,
			LHS: &expr.VarRef{
				Val:      "status",
				ColumnID: 2,
				ExprType: expr.Unsigned,
				EnumDict: dict,
				DataType: memCom.SmallEnum,
			},
			RHS: &expr.NumberLiteral{Int: -1, ExprType: expr.Unsigned},
		}))
		Ω(qc.Query.filters[8]).Should(Equal(&expr.BinaryExpr{
			Op:       expr.NEQ,
			ExprType: expr.Boolean,
			LHS: &expr.NumberLiteral{
				Val:      1,
				Int:      1,
				Expr:     "1",
				ExprType: expr.Float,
			},
			RHS: &expr.NumberLiteral{
				Val:      1.2,
				Int:      1,
				Expr:     "1.2",
				ExprType: expr.Float,
			},
		}))
		Ω(qc.Query.filters[9]).Should(Equal(&expr.UnaryExpr{
			Op: expr.NOT,
			Expr: &expr.VarRef{Val: "is_first",
				ColumnID: 3,
				ExprType: expr.Boolean,
				DataType: memCom.Bool},
			ExprType: expr.Boolean,
		}))
	})

	ginkgo.It("returns error on type resolution failure", func() {
		qc := &AQLQueryContext{
			TableScanners: []*TableScanner{
				{
					Schema: &memstore.TableSchema{},
				},
			},
		}
		// column not found for main table
		q := &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "sum(columnx)"},
			},
		}
		qc.Query = q
		qc.parseExprs()

		qc.resolveTypes()
		Ω(qc.Error).ShouldNot(BeNil())

		// table not found
		qc.Query = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "sum(tablex.columnx)"},
			},
		}
		qc.parseExprs()
		qc = &AQLQueryContext{
			Query: q,
		}
		qc.resolveTypes()
		Ω(qc.Error).ShouldNot(BeNil())

		// column not found for the specified table
		qc.Query = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "sum(trips.columnx)"},
			},
		}
		qc.parseExprs()
		qc = &AQLQueryContext{
			Query: q,
			TableScanners: []*TableScanner{
				{
					Schema: &memstore.TableSchema{},
				},
			},
			TableIDByAlias: map[string]int{
				"trips": 0,
			},
		}
		qc.resolveTypes()
		Ω(qc.Error).ShouldNot(BeNil())

		// column has been deleted
		qc.Query = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "sum(columnx)"},
			},
		}
		qc.parseExprs()
		qc = &AQLQueryContext{
			Query: q,
			TableScanners: []*TableScanner{
				{
					Schema: &memstore.TableSchema{
						ColumnIDs: map[string]int{
							"columnx": 0,
						},
						Schema: metaCom.Table{
							Columns: []metaCom.Column{
								{Name: "columnx", Deleted: true},
							},
						},
					},
				},
			},
		}
		qc.resolveTypes()
		Ω(qc.Error).ShouldNot(BeNil())

		// too many arguments for sum
		qc.Query = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "sum(columnx, columnx)"},
			},
		}
		qc.parseExprs()
		qc = &AQLQueryContext{
			Query: q,
			TableScanners: []*TableScanner{
				{
					Schema: &memstore.TableSchema{
						ColumnIDs: map[string]int{
							"columnx": 0,
						},
						Schema: metaCom.Table{
							Columns: []metaCom.Column{
								{Name: "columnx", Type: metaCom.Float32},
							},
						},
					},
				},
			},
		}
		qc.resolveTypes()
		Ω(qc.Error).ShouldNot(BeNil())

		// unknown function call
		qc.Query = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "exit()"},
			},
		}
		qc.parseExprs()
		qc = &AQLQueryContext{
			Query: q,
			TableScanners: []*TableScanner{
				{
					Schema: &memstore.TableSchema{},
				},
			},
		}
		qc.resolveTypes()
		Ω(qc.Error).ShouldNot(BeNil())
	})

	ginkgo.It("matches prefilters", func() {
		schema := &memstore.TableSchema{
			ValueTypeByColumn: []memCom.DataType{
				memCom.Uint8,
				memCom.Int16,
				memCom.Bool,
				memCom.Float32,
			},
			ColumnIDs: map[string]int{
				"status":   0,
				"city_id":  1,
				"is_first": 2,
				"fare":     3,
			},
			Schema: metaCom.Table{
				Columns: []metaCom.Column{
					{Name: "status", Type: metaCom.Uint8},
					{Name: "city_id", Type: metaCom.Int16},
					{Name: "is_first", Type: metaCom.Bool},
					{Name: "fare", Type: metaCom.Float32},
				},
				ArchivingSortColumns: []int{1, 2, 0, 3},
			},
		}

		qc := &AQLQueryContext{
			TableIDByAlias: map[string]int{
				"trips": 0,
			},
			TableScanners: []*TableScanner{
				{Schema: schema},
			},
		}

		// Unmatched
		qc.Query = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count()"},
			},
			Filters: []string{
				"status=3",
			},
		}
		qc.parseExprs()

		qc.resolveTypes()
		qc.matchPrefilters()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.Prefilters).Should(BeNil())
		Ω(qc.TableScanners[0].EqualityPrefilterValues).Should(BeNil())
		Ω(qc.TableScanners[0].RangePrefilterBoundaries[0]).Should(Equal(noBoundary))
		Ω(qc.TableScanners[0].RangePrefilterBoundaries[1]).Should(Equal(noBoundary))

		// Matched one equality
		qc = &AQLQueryContext{
			TableIDByAlias: map[string]int{
				"trips": 0,
			},
			TableScanners: []*TableScanner{
				{Schema: schema, ColumnUsages: map[int]columnUsage{}},
			},
		}
		qc.Query = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count()"},
			},
			Filters: []string{
				"status=3",
				"is_first = true",
				"city_id=12",
			},
		}
		qc.parseExprs()

		qc.resolveTypes()
		qc.matchPrefilters()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.Prefilters).Should(Equal([]int{2}))
		Ω(qc.TableScanners[0].EqualityPrefilterValues).Should(Equal([]uint32{12}))
		Ω(qc.TableScanners[0].RangePrefilterBoundaries[0]).Should(Equal(noBoundary))
		Ω(qc.TableScanners[0].RangePrefilterBoundaries[1]).Should(Equal(noBoundary))

		// Matched one range
		qc = &AQLQueryContext{
			TableIDByAlias: map[string]int{
				"trips": 0,
			},
			TableScanners: []*TableScanner{
				{Schema: schema, ColumnUsages: map[int]columnUsage{}},
			},
		}
		qc.Query = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count()"},
			},
			Filters: []string{
				"is_first",
				"city_id>=12",
				"city_id<16",
			},
		}
		qc.parseExprs()

		qc.resolveTypes()
		qc.matchPrefilters()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.Prefilters).Should(Equal([]int{1, 2}))
		Ω(qc.TableScanners[0].EqualityPrefilterValues).Should(BeNil())
		Ω(qc.TableScanners[0].RangePrefilterBoundaries[0]).Should(
			Equal(inclusiveBoundary))
		Ω(qc.TableScanners[0].RangePrefilterBoundaries[1]).Should(
			Equal(exclusiveBoundary))
		Ω(qc.TableScanners[0].RangePrefilterValues[0]).Should(Equal(uint32(12)))
		Ω(qc.TableScanners[0].RangePrefilterValues[1]).Should(Equal(uint32(16)))

		// Matched two equalities
		qc = &AQLQueryContext{
			TableIDByAlias: map[string]int{
				"trips": 0,
			},
			TableScanners: []*TableScanner{
				{Schema: schema, ColumnUsages: map[int]columnUsage{}},
			},
		}
		qc.Query = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count()"},
			},
			Filters: []string{
				"is_first",
				"city_id=12",
				"status!=2",
				"2=status",
			},
		}
		qc.parseExprs()

		qc.resolveTypes()
		qc.matchPrefilters()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.Prefilters).Should(Equal([]int{0, 1, 3}))
		Ω(qc.TableScanners[0].EqualityPrefilterValues).Should(Equal([]uint32{12, 1, 2}))
		Ω(qc.TableScanners[0].RangePrefilterBoundaries[0]).Should(Equal(noBoundary))
		Ω(qc.TableScanners[0].RangePrefilterBoundaries[1]).Should(Equal(noBoundary))

		// Matched two equalities and one range
		qc = &AQLQueryContext{
			TableIDByAlias: map[string]int{
				"trips": 0,
			},
			TableScanners: []*TableScanner{
				{Schema: schema, ColumnUsages: map[int]columnUsage{}},
			},
		}
		qc.Query = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count()"},
			},
			Filters: []string{
				"not is_first",
				"city_id=12",
				"status<10",
			},
		}
		qc.parseExprs()

		qc.resolveTypes()
		qc.matchPrefilters()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.Prefilters).Should(Equal([]int{0, 1, 2}))
		Ω(qc.TableScanners[0].EqualityPrefilterValues).Should(Equal([]uint32{12, 0}))
		Ω(qc.TableScanners[0].RangePrefilterBoundaries[0]).Should(Equal(noBoundary))
		Ω(qc.TableScanners[0].RangePrefilterBoundaries[1]).Should(
			Equal(exclusiveBoundary))
		Ω(qc.TableScanners[0].RangePrefilterValues[1]).Should(Equal(uint32(10)))

		// Matched four equalities
		qc = &AQLQueryContext{
			TableIDByAlias: map[string]int{
				"trips": 0,
			},
			TableScanners: []*TableScanner{
				{Schema: schema, ColumnUsages: map[int]columnUsage{}},
			},
		}
		qc.Query = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count()"},
			},
			Filters: []string{
				"not is_first",
				"city_id=12",
				"status=10",
				"fare=8",
			},
		}
		qc.parseExprs()

		qc.resolveTypes()
		qc.matchPrefilters()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.Prefilters).Should(Equal([]int{0, 1, 2, 3}))
		var f float32 = 8
		Ω(qc.TableScanners[0].EqualityPrefilterValues).Should(
			Equal([]uint32{12, 0, 10, *(*uint32)(unsafe.Pointer(&f))}))
		Ω(qc.TableScanners[0].RangePrefilterBoundaries[0]).Should(Equal(noBoundary))
		Ω(qc.TableScanners[0].RangePrefilterBoundaries[1]).Should(Equal(noBoundary))
	})

	ginkgo.It("normalizes filters", func() {
		Ω(normalizeAndFilters(nil)).Should(BeNil())

		filters := []expr.Expr{
			&expr.VarRef{Val: "city_id"},
		}
		Ω(normalizeAndFilters(filters)).Should(Equal(filters))

		filters = []expr.Expr{
			&expr.BinaryExpr{
				Op:  expr.AND,
				LHS: &expr.VarRef{Val: "is_first"},
				RHS: &expr.VarRef{Val: "is_last"},
			},
		}
		Ω(normalizeAndFilters(filters)).Should(Equal([]expr.Expr{
			&expr.VarRef{Val: "is_first"},
			&expr.VarRef{Val: "is_last"},
		}))

		filters = []expr.Expr{
			&expr.BinaryExpr{
				Op: expr.AND,
				LHS: &expr.BinaryExpr{
					Op:  expr.AND,
					LHS: &expr.VarRef{Val: "a"},
					RHS: &expr.VarRef{Val: "b"},
				},
				RHS: &expr.VarRef{Val: "is_last"},
			},
		}
		Ω(normalizeAndFilters(filters)).Should(Equal([]expr.Expr{
			&expr.VarRef{Val: "a"},
			&expr.VarRef{Val: "is_last"},
			&expr.VarRef{Val: "b"},
		}))
	})

	ginkgo.It("processes common filters and prefilters", func() {
		schema := &memstore.TableSchema{
			ValueTypeByColumn: []memCom.DataType{
				memCom.Uint8,
				memCom.Int16,
				memCom.Bool,
				memCom.Float32,
			},
			ColumnIDs: map[string]int{
				"status":   0,
				"city_id":  1,
				"is_first": 2,
				"fare":     3,
			},
			Schema: metaCom.Table{
				Columns: []metaCom.Column{
					{Name: "status", Type: metaCom.Uint8},
					{Name: "city_id", Type: metaCom.Int16},
					{Name: "is_first", Type: metaCom.Bool},
					{Name: "fare", Type: metaCom.Float32},
				},
				ArchivingSortColumns: []int{1, 2, 0, 3},
			},
		}

		qc := &AQLQueryContext{
			TableIDByAlias: map[string]int{
				"trips": 0,
			},
			TableScanners: []*TableScanner{
				{Schema: schema, ColumnUsages: map[int]columnUsage{}},
			},
		}
		qc.Query = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count()", Filters: []string{"status"}},
			},
			Filters: []string{
				"is_first",
				"city_id>=12",
				"city_id<16",
			},
		}
		qc.processTimezone()
		qc.parseExprs()

		qc.resolveTypes()
		qc.matchPrefilters()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.Prefilters).Should(Equal([]int{1, 2}))

		qc.processFilters()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.OOPK.MainTableCommonFilters).Should(Equal([]expr.Expr{
			&expr.VarRef{Val: "status", ExprType: expr.Unsigned, ColumnID: 0, DataType: memCom.Uint8},
			&expr.VarRef{Val: "is_first", ExprType: expr.Boolean, ColumnID: 2, DataType: memCom.Bool},
		}))
		Ω(qc.OOPK.Prefilters).Should(Equal([]expr.Expr{
			&expr.BinaryExpr{
				ExprType: expr.Boolean,
				Op:       expr.GTE,
				LHS:      &expr.VarRef{Val: "city_id", ExprType: expr.Signed, ColumnID: 1, DataType: memCom.Int16},
				RHS:      &expr.NumberLiteral{Val: 12, Int: 12, Expr: "12", ExprType: expr.Unsigned},
			},
			&expr.BinaryExpr{
				ExprType: expr.Boolean,
				Op:       expr.LT,
				LHS:      &expr.VarRef{Val: "city_id", ExprType: expr.Signed, ColumnID: 1, DataType: memCom.Int16},
				RHS:      &expr.NumberLiteral{Val: 16, Int: 16, Expr: "16", ExprType: expr.Unsigned},
			},
		}))

		Ω(qc.TableScanners[0].ColumnUsages).Should(Equal(map[int]columnUsage{
			0: columnUsedByAllBatches,
			1: columnUsedByLiveBatches | columnUsedByPrefilter,
			2: columnUsedByAllBatches,
		}))

		qc.Query = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count()", Filters: []string{"status"}},
			},
			Filters: []string{
				"city_id<'a string'",
			},
		}
		qc.processTimezone()
		qc.parseExprs()

		qc.resolveTypes()
		Ω(qc.Error.Error()).Should(ContainSubstring("string type only support EQ and NEQ operators"))
	})

	ginkgo.It("processes matched time filters", func() {
		table := metaCom.Table{
			IsFactTable: true,
			Columns: []metaCom.Column{
				{Name: "request_at", Type: metaCom.Uint32},
			},
		}

		schema := memstore.NewTableSchema(&table)

		q := &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count()"},
			},
			Dimensions: []Dimension{Dimension{Expr: "request_at", TimeBucketizer: "week"}},
			TimeFilter: TimeFilter{
				From: "-1d",
				To:   "0d",
			},
		}
		qc := &AQLQueryContext{
			Query: q,
			TableIDByAlias: map[string]int{
				"trips": 0,
			},
			TableScanners: []*TableScanner{
				{Schema: schema, ColumnUsages: map[int]columnUsage{0: columnUsedByLiveBatches}},
			},
		}
		utils.SetClockImplementation(func() time.Time {
			return time.Date(2017, 9, 20, 16, 51, 0, 0, time.UTC)
		})
		qc.processTimezone()
		qc.parseExprs()
		qc.processFilters()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.OOPK.TimeFilters[0]).Should(Equal(&expr.BinaryExpr{
			ExprType: expr.Boolean,
			Op:       expr.GTE,
			LHS: &expr.VarRef{
				Val:      "request_at",
				ExprType: expr.Unsigned,
				DataType: memCom.Uint32,
			},
			RHS: &expr.NumberLiteral{
				ExprType: expr.Unsigned,
				Int:      1505779200,
				Expr:     "1505779200",
			},
		}))
		Ω(qc.OOPK.TimeFilters[1]).Should(Equal(&expr.BinaryExpr{
			ExprType: expr.Boolean,
			Op:       expr.LT,
			LHS: &expr.VarRef{
				Val:      "request_at",
				ExprType: expr.Unsigned,
				DataType: memCom.Uint32,
			},
			RHS: &expr.NumberLiteral{
				ExprType: expr.Unsigned,
				Int:      1505952000,
				Expr:     "1505952000",
			},
		}))
		Ω(qc.TableScanners[0].ColumnUsages).Should(Equal(map[int]columnUsage{
			0: columnUsedByLiveBatches | columnUsedByFirstArchiveBatch | columnUsedByLastArchiveBatch,
		}))
		Ω(qc.TableScanners[0].ArchiveBatchIDStart).Should(Equal(17428))
		Ω(qc.TableScanners[0].ArchiveBatchIDEnd).Should(Equal(17430))

		// filter with {table}.{column} format
		q = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count()"},
			},
			Dimensions: []Dimension{Dimension{Expr: "request_at", TimeBucketizer: "week"}},
			TimeFilter: TimeFilter{
				Column: "trips.request_at",
				From:   "-1d",
				To:     "0d",
			},
		}
		qc = &AQLQueryContext{
			Query: q,
			TableIDByAlias: map[string]int{
				"trips": 0,
			},
			TableScanners: []*TableScanner{
				{Schema: schema, ColumnUsages: map[int]columnUsage{0: columnUsedByLiveBatches}},
			},
		}

		qc.processTimezone()
		qc.parseExprs()
		qc.processFilters()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.OOPK.TimeFilters[0]).Should(Equal(&expr.BinaryExpr{
			ExprType: expr.Boolean,
			Op:       expr.GTE,
			LHS: &expr.VarRef{
				Val:      "request_at",
				ExprType: expr.Unsigned,
				DataType: memCom.Uint32,
			},
			RHS: &expr.NumberLiteral{
				ExprType: expr.Unsigned,
				Int:      1505779200,
				Expr:     "1505779200",
			},
		}))
		Ω(qc.OOPK.TimeFilters[1]).Should(Equal(&expr.BinaryExpr{
			ExprType: expr.Boolean,
			Op:       expr.LT,
			LHS: &expr.VarRef{
				Val:      "request_at",
				ExprType: expr.Unsigned,
				DataType: memCom.Uint32,
			},
			RHS: &expr.NumberLiteral{
				ExprType: expr.Unsigned,
				Int:      1505952000,
				Expr:     "1505952000",
			},
		}))
		Ω(qc.TableScanners[0].ColumnUsages).Should(Equal(map[int]columnUsage{
			0: columnUsedByLiveBatches | columnUsedByFirstArchiveBatch | columnUsedByLastArchiveBatch,
		}))
		Ω(qc.TableScanners[0].ArchiveBatchIDStart).Should(Equal(17428))
		Ω(qc.TableScanners[0].ArchiveBatchIDEnd).Should(Equal(17430))
	})

	ginkgo.It("processes unmatched time filters", func() {
		table := metaCom.Table{
			Columns: []metaCom.Column{
				{Name: "uuid", Type: metaCom.UUID},
				{Name: "request_at", Type: metaCom.Uint32},
			},
			IsFactTable: false,
		}
		schema := memstore.NewTableSchema(&table)
		q := &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count()"},
			},
			Dimensions: []Dimension{Dimension{Expr: "request_at", TimeBucketizer: "week"}},
			TimeFilter: TimeFilter{
				Column: "request_at",
				From:   "-1d",
				To:     "0d",
			},
		}
		qc := &AQLQueryContext{
			Query: q,
			TableIDByAlias: map[string]int{
				"trips": 0,
			},
			TableScanners: []*TableScanner{
				{Schema: schema, ColumnUsages: map[int]columnUsage{}},
			},
		}
		utils.SetClockImplementation(func() time.Time {
			return time.Date(2017, 9, 20, 16, 51, 0, 0, time.UTC)
		})
		qc.processTimezone()
		qc.parseExprs()
		qc.processFilters()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.OOPK.TimeFilters[0]).Should(BeNil())
		Ω(qc.OOPK.TimeFilters[1]).Should(BeNil())
		Ω(qc.OOPK.MainTableCommonFilters[0]).Should(Equal(&expr.BinaryExpr{
			ExprType: expr.Boolean,
			Op:       expr.GTE,
			LHS: &expr.VarRef{
				Val:      "request_at",
				ColumnID: 1,
				ExprType: expr.Unsigned,
				DataType: memCom.Uint32,
			},
			RHS: &expr.NumberLiteral{
				ExprType: expr.Unsigned,
				Int:      1505779200,
				Expr:     "1505779200",
			},
		}))
		Ω(qc.OOPK.MainTableCommonFilters[1]).Should(Equal(&expr.BinaryExpr{
			ExprType: expr.Boolean,
			Op:       expr.LT,
			LHS: &expr.VarRef{
				Val:      "request_at",
				ExprType: expr.Unsigned,
				ColumnID: 1,
				DataType: memCom.Uint32,
			},
			RHS: &expr.NumberLiteral{
				ExprType: expr.Unsigned,
				Int:      1505952000,
				Expr:     "1505952000",
			},
		}))
		Ω(qc.TableScanners[0].ArchiveBatchIDStart).Should(Equal(0))
		Ω(qc.TableScanners[0].ArchiveBatchIDEnd).Should(Equal(17430))

		// filter with {table}.{column} format
		q = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count()"},
			},
			Dimensions: []Dimension{Dimension{Expr: "request_at", TimeBucketizer: "week"}},
			TimeFilter: TimeFilter{
				Column: "trips.request_at",
				From:   "-1d",
				To:     "0d",
			},
		}
		qc = &AQLQueryContext{
			Query: q,
			TableIDByAlias: map[string]int{
				"trips": 0,
			},
			TableScanners: []*TableScanner{
				{Schema: schema, ColumnUsages: map[int]columnUsage{}},
			},
		}
		qc.processTimezone()
		qc.parseExprs()
		qc.processFilters()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.OOPK.TimeFilters[0]).Should(BeNil())
		Ω(qc.OOPK.TimeFilters[1]).Should(BeNil())
		Ω(qc.OOPK.MainTableCommonFilters[0]).Should(Equal(&expr.BinaryExpr{
			ExprType: expr.Boolean,
			Op:       expr.GTE,
			LHS: &expr.VarRef{
				Val:      "request_at",
				ExprType: expr.Unsigned,
				ColumnID: 1,
				DataType: memCom.Uint32,
			},
			RHS: &expr.NumberLiteral{
				ExprType: expr.Unsigned,
				Int:      1505779200,
				Expr:     "1505779200",
			},
		}))
		Ω(qc.OOPK.MainTableCommonFilters[1]).Should(Equal(&expr.BinaryExpr{
			ExprType: expr.Boolean,
			Op:       expr.LT,
			LHS: &expr.VarRef{
				Val:      "request_at",
				ExprType: expr.Unsigned,
				ColumnID: 1,
				DataType: memCom.Uint32,
			},
			RHS: &expr.NumberLiteral{
				ExprType: expr.Unsigned,
				Int:      1505952000,
				Expr:     "1505952000",
			},
		}))
		Ω(qc.TableScanners[0].ArchiveBatchIDStart).Should(Equal(0))
		Ω(qc.TableScanners[0].ArchiveBatchIDEnd).Should(Equal(17430))
	})

	ginkgo.It("'from' should be defined for time filters", func() {
		schema := &memstore.TableSchema{
			ColumnIDs: map[string]int{
				"request_at": 0,
			},
			Schema: metaCom.Table{
				IsFactTable: true,
				Columns: []metaCom.Column{
					{Name: "request_at", Type: metaCom.Uint32},
				},
			},
		}
		q := &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count()"},
			},
			Dimensions: []Dimension{Dimension{Expr: "request_at", TimeBucketizer: "week"}},
			TimeFilter: TimeFilter{
				To: "now",
			},
		}
		qc := &AQLQueryContext{
			Query: q,
			TableIDByAlias: map[string]int{
				"trips": 0,
			},
			TableScanners: []*TableScanner{
				{Schema: schema, ColumnUsages: map[int]columnUsage{0: columnUsedByLiveBatches}},
			},
		}
		qc.processTimezone()
		qc.parseExprs()
		qc.processFilters()
		Ω(qc.Error).ShouldNot(BeNil())
	})

	ginkgo.It("processes measure and dimensions", func() {

		table := metaCom.Table{
			Columns: []metaCom.Column{
				{Name: "status", Type: metaCom.Uint8},
				{Name: "city_id", Type: metaCom.Uint16},
				{Name: "is_first", Type: metaCom.Bool},
				{Name: "fare", Type: metaCom.Float32},
				{Name: "request_at", Type: metaCom.Uint32},
			},
		}
		schema := memstore.NewTableSchema(&table)

		qc := &AQLQueryContext{
			TableIDByAlias: map[string]int{
				"trips": 0,
			},
			TableScanners: []*TableScanner{
				{Schema: schema, ColumnUsages: map[int]columnUsage{}},
			},
		}
		qc.Query = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "sum(fare)"},
			},
			Dimensions: []Dimension{
				{Expr: "city_id"},
				{Expr: "request_at", TimeBucketizer: "m"},
			},
		}
		qc.parseExprs()
		Ω(qc.Error).Should(BeNil())
		qc.resolveTypes()
		Ω(qc.Error).Should(BeNil())
		qc.processMeasureAndDimensions()
		Ω(qc.Error).Should(BeNil())

		Ω(qc.OOPK.Measure).Should(Equal(&expr.VarRef{
			Val:      "fare",
			ColumnID: 3,
			ExprType: expr.Float,
			DataType: memCom.Float32,
		}))
		Ω(qc.OOPK.Dimensions).Should(Equal([]expr.Expr{
			&expr.VarRef{
				Val:      "city_id",
				ColumnID: 1,
				ExprType: expr.Unsigned,
				DataType: memCom.Uint16,
			},
			&expr.BinaryExpr{
				Op: expr.FLOOR,
				LHS: &expr.VarRef{
					Val:      "request_at",
					ColumnID: 4,
					ExprType: expr.Unsigned,
					DataType: memCom.Uint32,
				},
				RHS: &expr.NumberLiteral{
					Int:      60,
					Expr:     "60",
					ExprType: expr.Unsigned,
				},
				ExprType: expr.Unsigned,
			},
		}))
		Ω(qc.TableScanners[0].ColumnUsages).Should(Equal(map[int]columnUsage{
			1: columnUsedByAllBatches,
			3: columnUsedByAllBatches,
			4: columnUsedByAllBatches,
		}))
	})

	ginkgo.It("sorts used columns", func() {
		schema := &memstore.TableSchema{
			Schema: metaCom.Table{
				ArchivingSortColumns: []int{1, 3, 5},
			},
		}

		qc := &AQLQueryContext{
			TableScanners: []*TableScanner{
				{
					Schema: schema,
					ColumnUsages: map[int]columnUsage{
						1: columnUsedByAllBatches,
						2: columnUsedByAllBatches,
						3: columnUsedByAllBatches,
					},
				},
			},
		}

		qc.sortUsedColumns()
		Ω(qc.TableScanners[0].Columns).Should(Equal([]int{2, 3, 1}))
	})

	ginkgo.It("sort dimension columns", func() {
		qc := &AQLQueryContext{
			OOPK: OOPKContext{
				Dimensions: []expr.Expr{
					&expr.VarRef{
						Val:      "a",
						DataType: memCom.Uint16,
					},
					&expr.VarRef{
						Val:      "b",
						DataType: memCom.Uint32,
					},
					&expr.VarRef{
						Val:      "c",
						DataType: memCom.Bool,
					},
					&expr.Call{
						Name: "d",
					},
				},
			},
		}
		qc.sortDimensionColumns()
		Ω(qc.OOPK.DimensionVectorIndex).Should(Equal([]int{2, 0, 3, 1}))
		Ω(qc.OOPK.DimRowBytes).Should(Equal(15))
	})

	ginkgo.It("processJoinConditions", func() {
		tripsSchema := &memstore.TableSchema{
			ColumnIDs: map[string]int{
				"request_at": 0,
				"city_id":    1,
			},
			Schema: metaCom.Table{
				Name:        "trips",
				IsFactTable: true,
				Columns: []metaCom.Column{
					{Name: "request_at", Type: metaCom.Uint32},
					{Name: "city_id", Type: metaCom.Uint16},
				},
			},
			ValueTypeByColumn: []memCom.DataType{
				memCom.Uint32,
				memCom.Uint16,
			},
		}

		badJoinSchema := &memstore.TableSchema{
			ColumnIDs: map[string]int{
				"id": 0,
			},
			Schema: metaCom.Table{
				Name:        "bad_table",
				IsFactTable: true,
				Columns: []metaCom.Column{
					{Name: "id", Type: metaCom.Uint32},
				},
				PrimaryKeyColumns: []int{0},
			},
			ValueTypeByColumn: []memCom.DataType{
				memCom.Uint32,
			},
		}

		apiCitySchema := &memstore.TableSchema{
			ColumnIDs: map[string]int{
				"id": 0,
			},
			Schema: metaCom.Table{
				Name:        "api_cities",
				IsFactTable: false,
				Columns: []metaCom.Column{
					{Name: "id", Type: metaCom.Uint32},
				},
				PrimaryKeyColumns: []int{0},
			},
			ValueTypeByColumn: []memCom.DataType{
				memCom.Uint32,
			},
		}

		qc := &AQLQueryContext{}
		goodQuery := &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count()"},
			},
			Joins: []Join{
				{
					Table: "api_cities",
					Alias: "",
					Conditions: []string{
						"trips.city_id = api_cities.id",
					},
				},
			},
		}
		qc.Query = goodQuery
		qc.parseExprs()
		Ω(qc.Error).Should(BeNil())

		qc = &AQLQueryContext{
			Query: goodQuery,
			TableSchemaByName: map[string]*memstore.TableSchema{
				"trips":      tripsSchema,
				"api_cities": apiCitySchema,
				"bad_table":  badJoinSchema,
			},
			TableIDByAlias: map[string]int{
				"trips":      0,
				"api_cities": 1,
				"bad_table":  2,
			},
			TableScanners: []*TableScanner{
				{Schema: tripsSchema, ColumnUsages: make(map[int]columnUsage)},
				{Schema: apiCitySchema, ColumnUsages: make(map[int]columnUsage)},
				{Schema: badJoinSchema, ColumnUsages: make(map[int]columnUsage)},
			},
		}
		qc.resolveTypes()
		qc.processJoinConditions()
		Ω(qc.Error).Should(BeNil())

		Ω(qc.TableScanners[0].ColumnUsages[1]).Should(Equal(columnUsedByAllBatches))

		// non dimension table join
		badQuery := &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count()"},
			},
			Joins: []Join{
				{
					Table: "bad_table",
					Alias: "",
					Conditions: []string{
						"trips.city_id > bad_table.id",
					},
				},
			},
		}
		qc.Query = badQuery
		qc.parseExprs()
		qc.Error = nil
		qc.resolveTypes()
		qc.processJoinConditions()
		Ω(qc.Error).ShouldNot(BeNil())

		// not equal join
		qc.Query = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count()"},
			},
			Joins: []Join{
				{
					Table: "api_cities",
					Alias: "",
					Conditions: []string{
						"trips.city_id > api_cities.id",
					},
				},
			},
		}
		qc.parseExprs()
		qc.Error = nil
		qc.resolveTypes()
		qc.processJoinConditions()
		Ω(qc.Error).ShouldNot(BeNil())

		// more than one join condition specified
		badQuery = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count()"},
			},
			Joins: []Join{
				{
					Table: "api_cities",
					Alias: "",
					Conditions: []string{
						"trips.city_id = api_cities.id",
						"trips.city_id = api_cities.id",
					},
				},
			},
		}
		qc.Query = badQuery
		qc.parseExprs()
		qc.Error = nil
		qc.resolveTypes()
		qc.processJoinConditions()
		Ω(qc.Error).ShouldNot(BeNil())

		// only column ref should be specified in join condition
		qc.Query = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count()"},
			},
			Joins: []Join{
				{
					Table: "api_cities",
					Alias: "",
					Conditions: []string{
						"api_cities.city_id = 1",
					},
				},
			},
		}
		qc.parseExprs()
		qc.Error = nil
		qc.resolveTypes()
		qc.processJoinConditions()
		Ω(qc.Error).ShouldNot(BeNil())

		// main table column not specified in join condition
		qc.Query = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count()"},
			},
			Joins: []Join{
				{
					Table: "api_cities",
					Alias: "",
					Conditions: []string{
						"api_cities.id = api_cities.id",
					},
				},
			},
		}
		qc.parseExprs()
		qc.Error = nil
		qc.resolveTypes()
		qc.processJoinConditions()
		Ω(qc.Error).ShouldNot(BeNil())
	})

	ginkgo.It("processes foreign table related filters", func() {
		tripsSchema := &memstore.TableSchema{
			ValueTypeByColumn: []memCom.DataType{
				memCom.Uint32,
				memCom.Uint16,
			},
			ColumnIDs: map[string]int{
				"request_at": 0,
				"city_id":    1,
			},
			Schema: metaCom.Table{
				IsFactTable: true,
				Columns: []metaCom.Column{
					{Name: "request_at", Type: metaCom.Uint32},
					{Name: "city_id", Type: metaCom.Uint16},
				},
			},
		}
		apiCitiesSchema := &memstore.TableSchema{
			ValueTypeByColumn: []memCom.DataType{
				memCom.Uint16,
				memCom.BigEnum,
			},
			ColumnIDs: map[string]int{
				"id":   0,
				"name": 1,
			},
			Schema: metaCom.Table{
				Columns: []metaCom.Column{
					{Name: "id", Type: metaCom.Uint16},
					{Name: "name", Type: metaCom.BigEnum},
				},
			},
			EnumDicts: map[string]memstore.EnumDict{
				"name": {
					Capacity: 65535,
					Dict: map[string]int{
						"paris": 0,
					},
					ReverseDict: []string{"paris"},
				},
			},
		}
		qc := &AQLQueryContext{
			TableIDByAlias: map[string]int{
				"trips":      0,
				"api_cities": 1,
			},
			TableSchemaByName: map[string]*memstore.TableSchema{
				"trips":      tripsSchema,
				"api_cities": apiCitiesSchema,
			},
			TableScanners: []*TableScanner{
				{Schema: tripsSchema, ColumnUsages: map[int]columnUsage{}},
				{Schema: apiCitiesSchema, ColumnUsages: map[int]columnUsage{}},
			},
		}
		qc.Query = &AQLQuery{
			Table: "trips",
			Joins: []Join{
				{
					Table: "api_cities",
					Conditions: []string{
						"trips.city_id = api_cities.id",
					},
				},
			},
			Measures: []Measure{
				{Expr: "count()"},
			},
			Dimensions: []Dimension{{Expr: "request_at", TimeBucketizer: "m"}},
			Filters:    []string{"api_cities.name = 'paris'", "city_id=1"},
			TimeFilter: TimeFilter{
				Column: "request_at",
				From:   "-1d",
				To:     "0d",
			},
		}
		qc.parseExprs()
		utils.SetClockImplementation(func() time.Time {
			return time.Date(2017, 9, 20, 16, 51, 0, 0, time.UTC)
		})
		qc.resolveTypes()
		qc.matchPrefilters()
		qc.processTimezone()
		qc.processFilters()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.OOPK.TimeFilters[0]).ShouldNot(BeNil())
		Ω(qc.OOPK.TimeFilters[1]).ShouldNot(BeNil())
		Ω(qc.OOPK.MainTableCommonFilters[0]).Should(Equal(&expr.BinaryExpr{
			ExprType: expr.Boolean,
			Op:       expr.EQ,
			LHS: &expr.VarRef{
				Val:      "city_id",
				ColumnID: 1,
				ExprType: expr.Unsigned,
				DataType: memCom.Uint16,
			},
			RHS: &expr.NumberLiteral{
				Val:      1,
				ExprType: expr.Unsigned,
				Int:      1,
				Expr:     "1",
			},
		}))
		Ω(qc.OOPK.ForeignTableCommonFilters[0]).Should(Equal(&expr.BinaryExpr{
			ExprType: expr.Boolean,
			Op:       expr.EQ,
			LHS: &expr.VarRef{
				Val:      "api_cities.name",
				TableID:  1,
				ColumnID: 1,
				EnumDict: map[string]int{
					"paris": 0,
				},
				EnumReverseDict: []string{"paris"},
				DataType:        memCom.BigEnum,
				ExprType:        expr.Unsigned,
			},
			RHS: &expr.NumberLiteral{
				ExprType: expr.Unsigned,
				Int:      0,
				Expr:     "",
			},
		}))
	})

	ginkgo.It("processes hyperloglog", func() {
		table := metaCom.Table{
			IsFactTable: true,
			Columns: []metaCom.Column{
				{Name: "request_at", Type: metaCom.Uint32},
				{Name: "client_uuid_hll", Type: metaCom.UUID, HLLConfig: metaCom.HLLConfig{IsHLLColumn: true}},
			},
		}
		tripsSchema := memstore.NewTableSchema(&table)

		qc := &AQLQueryContext{
			TableIDByAlias: map[string]int{
				"trips": 0,
			},
			TableSchemaByName: map[string]*memstore.TableSchema{
				"trips": tripsSchema,
			},
			TableScanners: []*TableScanner{
				{Schema: tripsSchema, ColumnUsages: map[int]columnUsage{}},
			},
		}

		qc.Query = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "countDistinctHll(request_at)"},
			},
			TimeFilter: TimeFilter{
				Column: "request_at",
				From:   "-1d",
				To:     "0d",
			},
		}
		qc.Error = nil
		qc.parseExprs()

		qc.resolveTypes()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.Query.Measures[0].expr.String()).Should(Equal("hll(GET_HLL_VALUE(request_at))"))

		qc.Query = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "hll(client_uuid_hll)"},
			},
			TimeFilter: TimeFilter{
				Column: "request_at",
				From:   "-1d",
				To:     "0d",
			},
		}
		qc.Error = nil
		qc.parseExprs()
		qc.resolveTypes()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.Query.Measures[0].expr.String()).Should(Equal("hll(client_uuid_hll)"))

		qc.Query = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "countDistinctHLL(client_uuid_hll)"},
			},
			TimeFilter: TimeFilter{
				Column: "request_at",
				From:   "-1d",
				To:     "0d",
			},
		}
		qc.Error = nil
		qc.parseExprs()
		qc.resolveTypes()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.Query.Measures[0].expr.String()).Should(Equal("hll(client_uuid_hll)"))
	})

	ginkgo.It("process geo intersection in foreign table", func() {
		store := new(mocks.MemStore)
		store.On("RLock").Return()
		store.On("RUnlock").Return()

		orderSchema := &memstore.TableSchema{
			ColumnIDs: map[string]int{
				"request_at":      0,
				"restaurant_uuid": 1,
			},
			Schema: metaCom.Table{
				Name:        "orders",
				IsFactTable: true,
				Columns: []metaCom.Column{
					{Name: "request_at", Type: metaCom.Uint32},
					{Name: "restaurant_uuid", Type: metaCom.UUID},
				},
			},
			ValueTypeByColumn: []memCom.DataType{
				memCom.Uint32,
				memCom.UUID,
			},
		}

		merchantInfoSchema := &memstore.TableSchema{
			ColumnIDs: map[string]int{
				"uuid":     0,
				"location": 1,
			},
			Schema: metaCom.Table{
				Name:        "merchant_info",
				IsFactTable: false,
				Columns: []metaCom.Column{
					{Name: "uuid", Type: metaCom.UUID},
					{Name: "location", Type: metaCom.GeoPoint},
				},
				PrimaryKeyColumns: []int{0},
			},
			ValueTypeByColumn: []memCom.DataType{
				memCom.Uint32,
				memCom.GeoPoint,
			},
		}

		geoSchema := &memstore.TableSchema{
			ColumnIDs: map[string]int{
				"geofence_uuid": 0,
				"shape":         1,
				"count":         2,
			},
			Schema: metaCom.Table{
				Name:        "geofences_configstore_udr_geofences",
				IsFactTable: false,
				Columns: []metaCom.Column{
					{Name: "geofence_uuid", Type: metaCom.UUID},
					{Name: "shape", Type: metaCom.GeoShape},
					{Name: "count", Type: metaCom.Uint32},
				},
				PrimaryKeyColumns: []int{0},
			},

			ValueTypeByColumn: []memCom.DataType{
				memCom.UUID,
				memCom.GeoShape,
				memCom.Uint32,
			},
		}

		store.On("GetSchemas").Return(map[string]*memstore.TableSchema{
			"orders":                              orderSchema,
			"merchant_info":                       merchantInfoSchema,
			"geofences_configstore_udr_geofences": geoSchema,
		})

		qc := AQLQueryContext{}
		query := &AQLQuery{
			Table: "orders",
			Measures: []Measure{
				{Expr: "count(*)"},
			},
			Joins: []Join{
				{
					Table: "merchant_info",
					Alias: "m",
					Conditions: []string{
						"orders.restaurant_uuid = m.uuid",
					},
				},
				{
					Alias: "g",
					Table: "geofences_configstore_udr_geofences",
					Conditions: []string{
						"geography_intersects(g.shape, m.location)",
					},
				},
			},
			Dimensions: []Dimension{{Expr: "request_at", TimeBucketizer: "m"}},
			Filters: []string{
				"g.geofence_uuid = 0x00000192F23D460DBE60400C32EA0667",
			},
			TimeFilter: TimeFilter{
				Column: "request_at",
				From:   "-1d",
			},
		}
		qc.Query = query
		qc.readSchema(store)
		qc.parseExprs()
		qc.resolveTypes()
		Ω(qc.Error).Should(BeNil())

		parsedQC := AQLQueryContext{
			Query: query,
			TableSchemaByName: map[string]*memstore.TableSchema{
				"orders":                              orderSchema,
				"merchant_info":                       merchantInfoSchema,
				"geofences_configstore_udr_geofences": geoSchema,
			},
			TableIDByAlias: map[string]int{
				"orders": 0,
				"m":      1,
				"g":      2,
			},
			TableScanners: []*TableScanner{
				{Schema: orderSchema, ColumnUsages: make(map[int]columnUsage)},
				{Schema: merchantInfoSchema, ColumnUsages: make(map[int]columnUsage)},
				{Schema: geoSchema, ColumnUsages: make(map[int]columnUsage)},
			},
			fromTime: qc.fromTime,
			toTime:   qc.toTime,
		}

		qc = parsedQC
		qc.resolveTypes()
		qc.processJoinConditions()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.OOPK.geoIntersection).ShouldNot(BeNil())
		Ω(*qc.OOPK.geoIntersection).Should(
			Equal(geoIntersection{shapeTableID: 2, shapeColumnID: 1, pointColumnID: 1, pointTableID: 1,
				shapeUUIDs: nil, shapeLatLongs: nullDevicePointer, shapeIndexs: nullDevicePointer, validShapeUUIDs: nil,
				dimIndex: -1, inOrOut: true}))
		qc.processFilters()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.OOPK.geoIntersection.shapeUUIDs).Should(Equal([]string{"00000192F23D460DBE60400C32EA0667"}))
		Ω(qc.TableScanners[1].ColumnUsages[1]).Should(Equal(columnUsedByAllBatches))
	})

	ginkgo.It("process geo intersection", func() {
		store := new(mocks.MemStore)
		store.On("RLock").Return()
		store.On("RUnlock").Return()

		tripsSchema := &memstore.TableSchema{
			ColumnIDs: map[string]int{
				"request_at":    0,
				"city_id":       1,
				"request_point": 2,
			},
			Schema: metaCom.Table{
				Name:        "trips",
				IsFactTable: true,
				Columns: []metaCom.Column{
					{Name: "request_at", Type: metaCom.Uint32},
					{Name: "city_id", Type: metaCom.Uint16},
					{Name: "request_point", Type: metaCom.GeoPoint},
				},
			},
			ValueTypeByColumn: []memCom.DataType{
				memCom.Uint32,
				memCom.Uint16,
				memCom.GeoPoint,
			},
		}

		apiCitySchema := &memstore.TableSchema{
			ColumnIDs: map[string]int{
				"id": 0,
			},
			Schema: metaCom.Table{
				Name:        "api_cities",
				IsFactTable: false,
				Columns: []metaCom.Column{
					{Name: "id", Type: metaCom.Uint32},
				},
				PrimaryKeyColumns: []int{0},
			},
			ValueTypeByColumn: []memCom.DataType{
				memCom.Uint32,
			},
		}

		geoSchema := &memstore.TableSchema{
			ColumnIDs: map[string]int{
				"geofence_uuid": 0,
				"shape":         1,
				"count":         2,
			},
			Schema: metaCom.Table{
				Name:        "geofences_configstore_udr_geofences",
				IsFactTable: false,
				Columns: []metaCom.Column{
					{Name: "geofence_uuid", Type: metaCom.UUID},
					{Name: "shape", Type: metaCom.GeoShape},
					{Name: "count", Type: metaCom.Uint32},
				},
				PrimaryKeyColumns: []int{0},
			},

			ValueTypeByColumn: []memCom.DataType{
				memCom.UUID,
				memCom.GeoShape,
				memCom.Uint32,
			},
		}

		store.On("GetSchemas").Return(map[string]*memstore.TableSchema{
			"trips":                               tripsSchema,
			"api_cities":                          apiCitySchema,
			"geofences_configstore_udr_geofences": geoSchema,
		})

		qc := AQLQueryContext{}
		query := &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count()"},
			},
			Joins: []Join{
				{
					Table: "api_cities",
					Alias: "c",
					Conditions: []string{
						"trips.city_id = c.id",
					},
				},
				{
					Alias: "g",
					Table: "geofences_configstore_udr_geofences",
					Conditions: []string{
						"geography_intersects(g.shape, request_point)",
					},
				},
			},
			Dimensions: []Dimension{{Expr: "request_at", TimeBucketizer: "m"}},
			Filters: []string{
				"g.geofence_uuid = 0x00000192F23D460DBE60400C32EA0667",
				"c.id>1",
			},
			TimeFilter: TimeFilter{
				Column: "request_at",
				From:   "-1d",
			},
		}
		qc.Query = query
		qc.readSchema(store)
		qc.parseExprs()
		qc.resolveTypes()
		Ω(qc.Error).Should(BeNil())

		parsedQC := AQLQueryContext{
			Query: query,
			TableSchemaByName: map[string]*memstore.TableSchema{
				"trips":                               tripsSchema,
				"api_cities":                          apiCitySchema,
				"geofences_configstore_udr_geofences": geoSchema,
			},
			TableIDByAlias: map[string]int{
				"trips": 0,
				"c":     1,
				"g":     2,
			},
			TableScanners: []*TableScanner{
				{Schema: tripsSchema, ColumnUsages: make(map[int]columnUsage)},
				{Schema: apiCitySchema, ColumnUsages: make(map[int]columnUsage)},
				{Schema: geoSchema, ColumnUsages: make(map[int]columnUsage)},
			},
			fromTime: qc.fromTime,
			toTime:   qc.toTime,
		}

		qc = parsedQC
		qc.resolveTypes()
		qc.processJoinConditions()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.OOPK.geoIntersection).ShouldNot(BeNil())
		Ω(*qc.OOPK.geoIntersection).Should(
			Equal(geoIntersection{shapeTableID: 2, shapeColumnID: 1, pointColumnID: 2,
				shapeUUIDs: nil, shapeLatLongs: nullDevicePointer, shapeIndexs: nullDevicePointer, validShapeUUIDs: nil,
				dimIndex: -1, inOrOut: true}))
		qc.processFilters()
		Ω(qc.Error).Should(BeNil())
		Ω(len(qc.OOPK.ForeignTableCommonFilters)).Should(Equal(1))
		Ω(qc.OOPK.ForeignTableCommonFilters[0].String()).Should(Equal("c.id > 1"))
		Ω(qc.OOPK.geoIntersection.shapeUUIDs).Should(Equal([]string{"00000192F23D460DBE60400C32EA0667"}))

		// Geo point on the left.
		query = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count()"},
			},
			Joins: []Join{
				{
					Table: "api_cities",
					Alias: "c",
					Conditions: []string{
						"trips.city_id = c.id",
					},
				},
				{
					Alias: "g",
					Table: "geofences_configstore_udr_geofences",
					Conditions: []string{
						"geography_intersects(request_point, g.shape)",
					},
				},
			},
			Dimensions: []Dimension{{Expr: "request_at", TimeBucketizer: "m"}},
			Filters: []string{
				"g.geofence_uuid = 0x00000192F23D460DBE60400C32EA0667",
				"c.id>1",
			},
			TimeFilter: TimeFilter{
				Column: "request_at",
				From:   "-1d",
			},
		}
		qc.Query = query
		qc.readSchema(store)
		qc.parseExprs()
		qc.resolveTypes()
		Ω(qc.Error).Should(BeNil())

		parsedQC = AQLQueryContext{
			Query: query,
			TableSchemaByName: map[string]*memstore.TableSchema{
				"trips":                               tripsSchema,
				"api_cities":                          apiCitySchema,
				"geofences_configstore_udr_geofences": geoSchema,
			},
			TableIDByAlias: map[string]int{
				"trips": 0,
				"c":     1,
				"g":     2,
			},
			TableScanners: []*TableScanner{
				{Schema: tripsSchema, ColumnUsages: make(map[int]columnUsage)},
				{Schema: apiCitySchema, ColumnUsages: make(map[int]columnUsage)},
				{Schema: geoSchema, ColumnUsages: make(map[int]columnUsage)},
			},
			fromTime: qc.fromTime,
			toTime:   qc.toTime,
		}

		qc = parsedQC
		qc.resolveTypes()
		qc.processJoinConditions()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.OOPK.geoIntersection).ShouldNot(BeNil())
		Ω(*qc.OOPK.geoIntersection).Should(
			Equal(geoIntersection{shapeTableID: 2, shapeColumnID: 1, pointColumnID: 2,
				shapeUUIDs: nil, shapeLatLongs: nullDevicePointer, shapeIndexs: nullDevicePointer, validShapeUUIDs: nil, dimIndex: -1, inOrOut: true}))
		qc.processFilters()
		Ω(qc.Error).Should(BeNil())
		Ω(len(qc.OOPK.ForeignTableCommonFilters)).Should(Equal(1))
		Ω(qc.OOPK.ForeignTableCommonFilters[0].String()).Should(Equal("c.id > 1"))
		Ω(qc.OOPK.geoIntersection.shapeUUIDs).Should(Equal([]string{"00000192F23D460DBE60400C32EA0667"}))

		// Two geo points in the argument of geography_intersects.
		query = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count()"},
			},
			Joins: []Join{
				{
					Table: "api_cities",
					Alias: "c",
					Conditions: []string{
						"trips.city_id = c.id",
					},
				},
				{
					Alias: "g",
					Table: "geofences_configstore_udr_geofences",
					Conditions: []string{
						"geography_intersects(request_point, request_point)",
					},
				},
			},
			Dimensions: []Dimension{{Expr: "request_at", TimeBucketizer: "m"}},
			Filters: []string{
				"g.geofence_uuid = 0x00000192F23D460DBE60400C32EA0667",
				"c.id>1",
			},
			TimeFilter: TimeFilter{
				Column: "request_at",
				From:   "-1d",
			},
		}
		qc.Query = query
		qc.readSchema(store)
		qc.parseExprs()
		qc.resolveTypes()
		Ω(qc.Error).ShouldNot(BeNil())
		Ω(qc.Error.Error()).Should(ContainSubstring(
			"expect exactly one geo shape column and one geo point column for " +
				"geography_intersects, got geography_intersects"))

		// Two geo shapes in the argument of geography_intersects.
		query = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count()"},
			},
			Joins: []Join{
				{
					Table: "api_cities",
					Alias: "c",
					Conditions: []string{
						"trips.city_id = c.id",
					},
				},
				{
					Alias: "g",
					Table: "geofences_configstore_udr_geofences",
					Conditions: []string{
						"geography_intersects(g.shape, g.shape)",
					},
				},
			},
			Dimensions: []Dimension{{Expr: "request_at", TimeBucketizer: "m"}},
			Filters: []string{
				"g.geofence_uuid = 0x00000192F23D460DBE60400C32EA0667",
				"c.id>1",
			},
			TimeFilter: TimeFilter{
				Column: "request_at",
				From:   "-1d",
			},
		}
		qc.Query = query
		qc.readSchema(store)
		qc.parseExprs()
		qc.resolveTypes()
		Ω(qc.Error).ShouldNot(BeNil())
		Ω(qc.Error.Error()).Should(ContainSubstring(
			"expect exactly one geo shape column and one geo point column for " +
				"geography_intersects, got geography_intersects"))

		// Match geofence_uuid in predicate.
		query = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count()"},
			},
			Joins: []Join{
				{
					Table: "api_cities",
					Alias: "c",
					Conditions: []string{
						"trips.city_id = c.id",
					},
				},
				{
					Alias: "g",
					Table: "geofences_configstore_udr_geofences",
					Conditions: []string{
						"geography_intersects(g.shape, request_point)",
					},
				},
			},
			Filters: []string{
				"g.geofence_uuid in (0x4c3226b27b1b11e8adc0fa7ae01bbebc,0x4c32295a7b1b11e8adc0fa7ae01bbebc)",
				"c.id>1",
			},
			TimeFilter: TimeFilter{
				Column: "request_at",
				From:   "-1d",
			},
		}

		qc = parsedQC
		qc.Error = nil
		qc.Query = query
		qc.parseExprs()
		Ω(qc.Error).Should(BeNil())
		qc.resolveTypes()
		qc.processJoinConditions()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.OOPK.geoIntersection).ShouldNot(BeNil())
		Ω(*qc.OOPK.geoIntersection).Should(
			Equal(geoIntersection{shapeTableID: 2, shapeColumnID: 1, pointColumnID: 2,
				shapeUUIDs: nil, shapeLatLongs: nullDevicePointer, shapeIndexs: nullDevicePointer, validShapeUUIDs: nil, dimIndex: -1, inOrOut: true}))
		qc.processFilters()
		Ω(qc.Error).Should(BeNil())
		Ω(len(qc.OOPK.ForeignTableCommonFilters)).Should(Equal(1))
		Ω(qc.OOPK.ForeignTableCommonFilters[0].String()).Should(Equal("c.id > 1"))
		Ω(qc.OOPK.geoIntersection.shapeUUIDs).Should(Equal([]string{"4C3226B27B1B11E8ADC0FA7AE01BBEBC",
			"4C32295A7B1B11E8ADC0FA7AE01BBEBC"}))

		// Return error if two geo filters
		qc = AQLQueryContext{}
		query = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count()"},
			},
			Joins: []Join{
				{
					Table: "api_cities",
					Alias: "c",
					Conditions: []string{
						"trips.city_id = c.id",
					},
				},
				{
					Alias: "g",
					Table: "geofences_configstore_udr_geofences",
					Conditions: []string{
						"geography_intersects(g.shape, request_point)",
					},
				},
			},
			Filters: []string{
				"g.geofence_uuid in (0x4c3226b27b1b11e8adc0fa7ae01bbebc,0x4c32295a7b1b11e8adc0fa7ae01bbebc)",
				"g.geofence_uuid in (0x4c3226b27b1b11e8adc0fa7ae01bbebc,0x4c32295a7b1b11e8adc0fa7ae01bbebc)",
				"c.id>1",
			},
			TimeFilter: TimeFilter{
				Column: "request_at",
				From:   "-1d",
			},
		}

		qc.Query = query
		qc.parseExprs()
		Ω(qc.Error).Should(BeNil())
		qc = parsedQC
		qc.Query = query
		qc.resolveTypes()
		qc.processJoinConditions()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.OOPK.geoIntersection).ShouldNot(BeNil())
		Ω(*qc.OOPK.geoIntersection).Should(
			Equal(geoIntersection{shapeTableID: 2, shapeColumnID: 1, pointColumnID: 2,
				shapeUUIDs: nil, shapeLatLongs: nullDevicePointer, shapeIndexs: nullDevicePointer, validShapeUUIDs: nil, dimIndex: -1, inOrOut: true}))
		qc.processFilters()
		Ω(qc.Error).ShouldNot(BeNil())
		Ω(qc.Error.Error()).Should(ContainSubstring("Only one geo filter is allowed"))

		// Return error if no geo filter
		qc = AQLQueryContext{}
		query = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count()"},
			},
			Joins: []Join{
				{
					Table: "api_cities",
					Alias: "c",
					Conditions: []string{
						"trips.city_id = c.id",
					},
				},
				{
					Alias: "g",
					Table: "geofences_configstore_udr_geofences",
					Conditions: []string{
						"geography_intersects(g.shape, request_point)",
					},
				},
			},
			Filters: []string{
				"c.id>1",
			},
			TimeFilter: TimeFilter{
				Column: "request_at",
				From:   "-1d",
			},
		}

		qc.Query = query
		qc.parseExprs()
		Ω(qc.Error).Should(BeNil())
		qc = parsedQC
		qc.Query = query
		qc.resolveTypes()
		qc.processJoinConditions()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.OOPK.geoIntersection).ShouldNot(BeNil())
		Ω(*qc.OOPK.geoIntersection).Should(
			Equal(geoIntersection{shapeTableID: 2, shapeColumnID: 1, pointColumnID: 2,
				shapeUUIDs: nil, shapeLatLongs: nullDevicePointer, shapeIndexs: nullDevicePointer, validShapeUUIDs: nil, dimIndex: -1, inOrOut: true}))
		qc.processFilters()
		Ω(qc.Error).ShouldNot(BeNil())
		Ω(qc.Error.Error()).Should(ContainSubstring("Exact one geo filter is needed if geo intersection is used during join"))

		// At most one join condition allowed per geo join.
		qc = AQLQueryContext{}
		query = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count()"},
			},
			Joins: []Join{
				{
					Table: "api_cities",
					Alias: "c",
					Conditions: []string{
						"trips.city_id = c.id",
					},
				},
				{
					Alias: "g",
					Table: "geofences_configstore_udr_geofences",
					Conditions: []string{
						"geography_intersects(g.shape, request_point)",
						"geography_intersects(g.shape, request_point)",
					},
				},
			},
			Filters: []string{
				"c.id>1",
				"g.geofence_uuid in (0x4c3226b27b1b11e8adc0fa7ae01bbebc,0x4c32295a7b1b11e8adc0fa7ae01bbebc)",
			},
			TimeFilter: TimeFilter{
				Column: "request_at",
				From:   "-1d",
			},
		}

		qc.Query = query
		qc.parseExprs()
		Ω(qc.Error).Should(BeNil())
		qc = parsedQC
		qc.Query = query
		qc.resolveTypes()
		qc.processJoinConditions()
		Ω(qc.Error).ShouldNot(BeNil())
		Ω(qc.Error.Error()).Should(ContainSubstring("At most one join condition allowed per geo join"))

		// Only dimension table is allowed in geo join
		geoSchema.Schema.IsFactTable = true
		qc = AQLQueryContext{}
		query = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count()"},
			},
			Joins: []Join{
				{
					Table: "api_cities",
					Alias: "c",
					Conditions: []string{
						"trips.city_id = c.id",
					},
				},
				{
					Alias: "g",
					Table: "geofences_configstore_udr_geofences",
					Conditions: []string{
						"geography_intersects(g.shape, request_point)",
					},
				},
			},
			Filters: []string{
				"c.id>1",
				"g.geofence_uuid in (0x4c3226b27b1b11e8adc0fa7ae01bbebc,0x4c32295a7b1b11e8adc0fa7ae01bbebc)",
			},
			TimeFilter: TimeFilter{
				Column: "request_at",
				From:   "-1d",
			},
		}
		qc.Query = query
		qc.parseExprs()
		Ω(qc.Error).Should(BeNil())
		qc = parsedQC
		qc.Query = query
		qc.resolveTypes()
		qc.processJoinConditions()
		Ω(qc.Error).ShouldNot(BeNil())
		Ω(qc.Error.Error()).Should(ContainSubstring("Only dimension table is allowed in geo join"))
		geoSchema.Schema.IsFactTable = false

		// Composite primary key for geo table is not allowed.
		geoSchema.Schema.PrimaryKeyColumns = []int{0, 1}
		qc = AQLQueryContext{}
		qc.Query = query
		qc.parseExprs()
		Ω(qc.Error).Should(BeNil())
		qc = parsedQC
		qc.resolveTypes()
		qc.processJoinConditions()
		Ω(qc.Error).ShouldNot(BeNil())
		Ω(qc.Error.Error()).Should(ContainSubstring("Composite primary key for geo table is not allowed"))

		// Geo filter column is not the primary key.
		geoSchema.Schema.PrimaryKeyColumns = []int{1}
		qc = AQLQueryContext{}
		qc.Query = query
		qc.parseExprs()
		Ω(qc.Error).Should(BeNil())
		qc = parsedQC
		qc.resolveTypes()
		qc.processJoinConditions()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.OOPK.geoIntersection).ShouldNot(BeNil())
		Ω(*qc.OOPK.geoIntersection).Should(
			Equal(geoIntersection{shapeTableID: 2, shapeColumnID: 1, pointColumnID: 2,
				shapeUUIDs: nil, shapeLatLongs: nullDevicePointer, shapeIndexs: nullDevicePointer, validShapeUUIDs: nil, dimIndex: -1, inOrOut: true}))
		qc.processFilters()
		Ω(qc.Error).ShouldNot(BeNil())
		Ω(qc.Error.Error()).Should(ContainSubstring("Geo filter column is not the primary key"))

		geoSchema.Schema.PrimaryKeyColumns = []int{0}

		query = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count()"},
			},
			Joins: []Join{
				{
					Table: "api_cities",
					Alias: "c",
					Conditions: []string{
						"trips.city_id = c.id",
					},
				},
				{
					Alias: "g",
					Table: "geofences_configstore_udr_geofences",
					Conditions: []string{
						"geography_intersects(g.shape, request_point)",
					},
				},
			},
			Filters: []string{
				"c.id>1",
				"g.geofence_uuid not in (trips.city_id)",
			},
			TimeFilter: TimeFilter{
				Column: "request_at",
				From:   "-1d",
			},
		}
		qc = parsedQC
		qc.Query = query
		qc.parseExprs()
		Ω(qc.Error).Should(BeNil())
		qc.resolveTypes()
		qc.processJoinConditions()
		qc.processFilters()
		Ω(qc.Error).ShouldNot(BeNil())
		Ω(qc.Error.Error()).Should(ContainSubstring("Unable to extract uuid from expression "))

		query = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count()"},
			},
			Joins: []Join{
				{
					Table: "api_cities",
					Alias: "c",
					Conditions: []string{
						"trips.city_id = c.id",
					},
				},
				{
					Alias: "g",
					Table: "geofences_configstore_udr_geofences",
					Conditions: []string{
						"geography_intersects(g.shape, request_point)",
					},
				},
			},
			Filters: []string{
				"c.id>1",
				"g.geofence_uuid not in (0x4c3226b27b1b11e8adc0fa7ae01bbebc)",
			},
			TimeFilter: TimeFilter{
				Column: "request_at",
				From:   "-1d",
			},
		}
		qc = parsedQC
		qc.Query = query
		qc.parseExprs()
		Ω(qc.Error).Should(BeNil())
		qc.resolveTypes()
		qc.processJoinConditions()
		qc.processFilters()
		Ω(qc.Error).ShouldNot(BeNil())
		Ω(qc.Error.Error()).Should(ContainSubstring("Only EQ and IN allowed for geo filters"))

		// Geo table column is not allowed to be used in measure.
		qc = AQLQueryContext{}
		query = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "sum(g.count)"},
			},
			Joins: []Join{
				{
					Table: "api_cities",
					Alias: "c",
					Conditions: []string{
						"trips.city_id = c.id",
					},
				},
				{
					Alias: "g",
					Table: "geofences_configstore_udr_geofences",
					Conditions: []string{
						"geography_intersects(g.shape, request_point)",
					},
				},
			},
			Filters: []string{
				"c.id>1",
				"g.geofence_uuid not in (trips.city_id)",
			},
			TimeFilter: TimeFilter{
				Column: "request_at",
				From:   "-1d",
			},
		}
		qc.Query = query
		qc.parseExprs()
		Ω(qc.Error).Should(BeNil())
		qc = parsedQC
		qc.Query = query
		qc.resolveTypes()
		qc.processJoinConditions()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.OOPK.geoIntersection).ShouldNot(BeNil())
		Ω(*qc.OOPK.geoIntersection).Should(
			Equal(geoIntersection{shapeTableID: 2, shapeColumnID: 1, pointColumnID: 2,
				shapeUUIDs: nil, shapeLatLongs: nullDevicePointer, shapeIndexs: nullDevicePointer, validShapeUUIDs: nil, dimIndex: -1, inOrOut: true}))
		qc.processMeasureAndDimensions()
		Ω(qc.Error).ShouldNot(BeNil())
		Ω(qc.Error.Error()).Should(ContainSubstring("Geo table column is not allowed to be used in measure"))

		// Only one geo dimension allowed.
		qc = AQLQueryContext{}
		query = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count(1)"},
			},
			Joins: []Join{
				{
					Table: "api_cities",
					Alias: "c",
					Conditions: []string{
						"trips.city_id = c.id",
					},
				},
				{
					Alias: "g",
					Table: "geofences_configstore_udr_geofences",
					Conditions: []string{
						"geography_intersects(g.shape, request_point)",
					},
				},
			},
			Dimensions: []Dimension{
				{
					Expr: "g.geofence_uuid",
				},
				{
					Expr: "g.geofence_uuid",
				},
			},
			Filters: []string{
				"c.id>1",
				"g.geofence_uuid not in (trips.city_id)",
			},
			TimeFilter: TimeFilter{
				Column: "request_at",
				From:   "-1d",
			},
		}
		qc.Query = query
		qc.parseExprs()
		Ω(qc.Error).Should(BeNil())
		qc = parsedQC
		qc.Query = query
		qc.resolveTypes()
		qc.processJoinConditions()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.OOPK.geoIntersection).ShouldNot(BeNil())
		Ω(*qc.OOPK.geoIntersection).Should(
			Equal(geoIntersection{shapeTableID: 2, shapeColumnID: 1, pointColumnID: 2,
				shapeUUIDs: nil, shapeLatLongs: nullDevicePointer, shapeIndexs: nullDevicePointer, validShapeUUIDs: nil, dimIndex: -1, inOrOut: true}))
		qc.processMeasureAndDimensions()
		Ω(qc.Error).ShouldNot(BeNil())
		Ω(qc.Error.Error()).Should(ContainSubstring("Only one geo dimension allowed"))

		// Only one geo uuid dimension allowed.
		qc = AQLQueryContext{}
		query = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count(1)"},
			},
			Joins: []Join{
				{
					Table: "api_cities",
					Alias: "c",
					Conditions: []string{
						"trips.city_id = c.id",
					},
				},
				{
					Alias: "g",
					Table: "geofences_configstore_udr_geofences",
					Conditions: []string{
						"geography_intersects(g.shape, request_point)",
					},
				},
			},
			Dimensions: []Dimension{
				{
					Expr: "g.shape",
				},
			},
			Filters: []string{
				"c.id>1",
				"g.geofence_uuid not in (trips.city_id)",
			},
			TimeFilter: TimeFilter{
				Column: "request_at",
				From:   "-1d",
			},
		}
		qc.Query = query
		qc.parseExprs()
		Ω(qc.Error).Should(BeNil())
		qc = parsedQC
		qc.Query = query
		qc.resolveTypes()
		qc.processJoinConditions()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.OOPK.geoIntersection).ShouldNot(BeNil())
		Ω(*qc.OOPK.geoIntersection).Should(
			Equal(geoIntersection{shapeTableID: 2, shapeColumnID: 1, pointColumnID: 2,
				shapeUUIDs: nil, shapeLatLongs: nullDevicePointer, shapeIndexs: nullDevicePointer, validShapeUUIDs: nil, dimIndex: -1, inOrOut: true}))
		qc.processMeasureAndDimensions()
		Ω(qc.Error).ShouldNot(BeNil())
		Ω(qc.Error.Error()).Should(ContainSubstring("Only geo uuid is allowed in dimensions"))

		// Only hex(uuid) or uuid supported.
		qc = AQLQueryContext{}
		query = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count(1)"},
			},
			Joins: []Join{
				{
					Table: "api_cities",
					Alias: "c",
					Conditions: []string{
						"trips.city_id = c.id",
					},
				},
				{
					Alias: "g",
					Table: "geofences_configstore_udr_geofences",
					Conditions: []string{
						"geography_intersects(g.shape, request_point)",
					},
				},
			},
			Dimensions: []Dimension{
				{
					Expr: "g.geofence_uuid+1",
				},
			},
			Filters: []string{
				"c.id>1",
				"g.geofence_uuid in (0x4c3226b27b1b11e8adc0fa7ae01bbebc,0x4c32295a7b1b11e8adc0fa7ae01bbebc)",
			},
			TimeFilter: TimeFilter{
				Column: "request_at",
				From:   "-1d",
			},
		}
		qc.Query = query
		qc.parseExprs()
		Ω(qc.Error).Should(BeNil())
		qc = parsedQC
		qc.Query = query
		qc.resolveTypes()
		qc.processJoinConditions()
		Ω(qc.Error).ShouldNot(BeNil())
		Ω(qc.Error.Error()).Should(ContainSubstring("numeric operations not supported for column over 4 bytes length"))

		// only hex function is supported on UUID type, but got count.
		qc = AQLQueryContext{}
		query = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count(1)"},
			},
			Joins: []Join{
				{
					Table: "api_cities",
					Alias: "c",
					Conditions: []string{
						"trips.city_id = c.id",
					},
				},
				{
					Alias: "g",
					Table: "geofences_configstore_udr_geofences",
					Conditions: []string{
						"geography_intersects(g.shape, request_point)",
					},
				},
			},
			Dimensions: []Dimension{
				{
					Expr: "count(g.geofence_uuid)",
				},
			},
			Filters: []string{
				"c.id>1",
				"g.geofence_uuid in (0x4c3226b27b1b11e8adc0fa7ae01bbebc,0x4c32295a7b1b11e8adc0fa7ae01bbebc)",
			},
			TimeFilter: TimeFilter{
				Column: "request_at",
				From:   "-1d",
			},
		}
		qc.Query = query
		qc.parseExprs()
		Ω(qc.Error).Should(BeNil())
		qc = parsedQC
		qc.Query = query
		qc.resolveTypes()
		qc.processJoinConditions()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.OOPK.geoIntersection).ShouldNot(BeNil())
		Ω(*qc.OOPK.geoIntersection).Should(
			Equal(geoIntersection{shapeTableID: 2, shapeColumnID: 1, pointColumnID: 2,
				shapeUUIDs: nil, shapeLatLongs: nullDevicePointer, shapeIndexs: nullDevicePointer, validShapeUUIDs: nil, dimIndex: -1, inOrOut: true}))
		qc.processMeasureAndDimensions()
		Ω(qc.Error).ShouldNot(BeNil())
		Ω(qc.Error.Error()).Should(ContainSubstring("Only hex function is supported on UUID type, but got count"))

		// Correct geo join query.
		qc = AQLQueryContext{}
		query = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count(1)"},
			},
			Joins: []Join{
				{
					Table: "api_cities",
					Alias: "c",
					Conditions: []string{
						"trips.city_id = c.id",
					},
				},
				{
					Alias: "g",
					Table: "geofences_configstore_udr_geofences",
					Conditions: []string{
						"geography_intersects(g.shape, request_point)",
					},
				},
			},
			Dimensions: []Dimension{
				{
					Expr: "hex(g.geofence_uuid)",
				},
			},
			Filters: []string{
				"c.id>1",
				"g.geofence_uuid in (0x4c3226b27b1b11e8adc0fa7ae01bbebc,0x4c32295a7b1b11e8adc0fa7ae01bbebc)",
			},
			TimeFilter: TimeFilter{
				Column: "request_at",
				From:   "-1d",
			},
		}
		qc.Query = query
		qc.parseExprs()
		Ω(qc.Error).Should(BeNil())
		qc = parsedQC
		qc.Query = query
		qc.resolveTypes()
		qc.processJoinConditions()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.OOPK.geoIntersection).ShouldNot(BeNil())
		Ω(*qc.OOPK.geoIntersection).Should(
			Equal(geoIntersection{shapeTableID: 2, shapeColumnID: 1, pointColumnID: 2,
				shapeUUIDs: nil, shapeLatLongs: nullDevicePointer, shapeIndexs: nullDevicePointer, validShapeUUIDs: nil, dimIndex: -1, inOrOut: true}))
		qc.processFilters()
		Ω(qc.Error).Should(BeNil())
		qc.processMeasureAndDimensions()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.OOPK.geoIntersection.shapeUUIDs).Should(
			Equal([]string{
				"4C3226B27B1B11E8ADC0FA7AE01BBEBC",
				"4C32295A7B1B11E8ADC0FA7AE01BBEBC",
			}))

		qc.OOPK.geoIntersection.shapeUUIDs = nil

		Ω(*qc.OOPK.geoIntersection).Should(
			Equal(geoIntersection{shapeTableID: 2, shapeColumnID: 1, pointColumnID: 2,
				shapeUUIDs: nil, shapeLatLongs: nullDevicePointer, shapeIndexs: nullDevicePointer, validShapeUUIDs: nil, dimIndex: 0, inOrOut: true}))
		dimExpr, ok := qc.OOPK.Dimensions[0].(*expr.VarRef)
		Ω(ok).Should(BeTrue())
		Ω(dimExpr.TableID).Should(BeEquivalentTo(2))
		Ω(dimExpr.ColumnID).Should(BeEquivalentTo(0))
		Ω(dimExpr.EnumReverseDict).Should(BeNil())
	})

	ginkgo.It("parseTimezoneColumnString should work", func() {
		inputs := []string{"timezone(city_id)", "region_timezone(city_id)", "foo", "tz(bla", "timezone)(foo)"}
		expectedSucesss := []bool{true, true, false, false, false}
		expectedColumns := []string{"timezone", "region_timezone", "", "", ""}
		expectedJoinKeys := []string{"city_id", "city_id", "", "", ""}
		for i := range inputs {
			column, key, ok := parseTimezoneColumnString(inputs[i])
			Ω(ok).Should(Equal(expectedSucesss[i]))
			Ω(key).Should(Equal(expectedJoinKeys[i]))
			Ω(column).Should(Equal(expectedColumns[i]))
		}
	})

	ginkgo.It("expandINOp should work", func() {
		query := &AQLQuery{
			Table:   "table1",
			Filters: []string{"id in (1, 2)"},
		}
		tableSchema := &memstore.TableSchema{
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
		qc := AQLQueryContext{
			Query: query,
			TableSchemaByName: map[string]*memstore.TableSchema{
				"table1": tableSchema,
			},
			TableIDByAlias: map[string]int{
				"table1": 0,
			},
			TableScanners: []*TableScanner{
				{Schema: tableSchema, ColumnUsages: make(map[int]columnUsage)},
			},
		}
		qc.parseExprs()
		Ω(qc.Error).Should(BeNil())
		qc.resolveTypes()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.Query.filters).Should(HaveLen(1))
		Ω(qc.Query.filters[0].String()).Should(Equal("id = 1 OR id = 2"))

		qc.Query.Filters[0] = "id in ()"
		qc.parseExprs()
		qc.resolveTypes()
		Ω(qc.Error).ShouldNot(BeNil())

		qc.Error = nil
		qc.Query.Filters[0] = "id in (1)"
		qc.parseExprs()
		qc.resolveTypes()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.Query.filters[0].String()).Should(Equal("id = 1"))

		qc.Error = nil
		qc.Query.Filters[0] = "id in ('1')"
		qc.parseExprs()
		qc.resolveTypes()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.Query.filters[0].String()).Should(Equal("id = '1'"))

		qc.Error = nil
		qc.Query.Filters[0] = "id in (1,2,3)"
		qc.parseExprs()
		qc.resolveTypes()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.Query.filters[0].String()).Should(Equal("id = 1 OR id = 2 OR id = 3"))

		qc.Error = nil
		qc.Query.Filters[0] = "id not in (1,2,3)"
		qc.parseExprs()
		qc.resolveTypes()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.Query.filters[0].String()).Should(Equal("NOT(id = 1 OR id = 2 OR id = 3)"))
	})

	ginkgo.It("dayofweek and hour should work", func() {
		query := &AQLQuery{
			Table:   "table1",
			Filters: []string{"dayofweek(table1.time_col) = 2", "hour(table1.time_col) = 21"},
		}
		tableSchema := &memstore.TableSchema{
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
		qc := AQLQueryContext{
			Query: query,
			TableSchemaByName: map[string]*memstore.TableSchema{
				"table1": tableSchema,
			},
			TableIDByAlias: map[string]int{
				"table1": 0,
			},
			TableScanners: []*TableScanner{
				{Schema: tableSchema, ColumnUsages: make(map[int]columnUsage)},
			},
		}
		qc.parseExprs()
		Ω(qc.Error).Should(BeNil())
		qc.resolveTypes()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.Query.filters).Should(HaveLen(2))
		Ω(qc.Query.filters[0].String()).Should(Equal("table1.time_col / 86400 + 4 % 7 + 1 = 2"))
		Ω(qc.Query.filters[1].String()).Should(Equal("table1.time_col % 86400 / 3600 = 21"))
	})

	ginkgo.It("convert_tz should work", func() {
		query := &AQLQuery{
			Table: "table1",
			Filters: []string{
				"convert_tz(table1.time_col, 'GMT', 'America/Phoenix') = 2",
				"convert_tz(from_unixtime(table1.time_col / 1000), 'GMT', 'America/Phoenix') = 2",
			},
		}
		tableSchema := &memstore.TableSchema{
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
		qc := AQLQueryContext{
			Query: query,
			TableSchemaByName: map[string]*memstore.TableSchema{
				"table1": tableSchema,
			},
			TableIDByAlias: map[string]int{
				"table1": 0,
			},
			TableScanners: []*TableScanner{
				{Schema: tableSchema, ColumnUsages: make(map[int]columnUsage)},
			},
		}
		qc.parseExprs()
		Ω(qc.Error).Should(BeNil())

		qc.resolveTypes()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.Query.filters).Should(HaveLen(2))
		Ω(qc.Query.filters[0].String()).Should(Equal("table1.time_col + -25200 = 2"))
		Ω(qc.Query.filters[1].String()).Should(Equal("table1.time_col + -25200 = 2"))

		qc.Query.Filters = []string{"convert_tz(from_unixtime(table1.time_col), 'GMT', 'America/Phoenix') = 2"}
		qc.parseExprs()
		qc.resolveTypes()
		Ω(qc.Error).ShouldNot(BeNil())
		Ω(qc.Error.Error()).Should(ContainSubstring("from_unixtime must be time column / 1000"))
	})

	ginkgo.It("parses point expressions", func() {
		q := &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{
					Expr: "count(*)",
				},
			},
		}

		qc := &AQLQueryContext{
			TableIDByAlias: map[string]int{
				"trips": 0,
			},
			TableScanners: []*TableScanner{
				{
					Schema: &memstore.TableSchema{
						ValueTypeByColumn: []memCom.DataType{
							memCom.Uint16,
							memCom.GeoPoint,
						},
						ColumnIDs: map[string]int{
							"city_id":       0,
							"request_point": 1,
						},
						Schema: metaCom.Table{
							Columns: []metaCom.Column{
								{Name: "city_id", Type: metaCom.Uint16},
								{Name: "request_point", Type: metaCom.GeoPoint},
							},
						},
					},
				},
			},
		}
		qc.Query = q
		qc.parseExprs()
		Ω(qc.Error).Should(BeNil())
		Ω(q.Measures[0].expr).Should(Equal(&expr.Call{
			Name: "count",
			Args: []expr.Expr{
				&expr.Wildcard{},
			},
		}))

		qc.Query = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{
					Expr: "count(*)",
				},
			},
			Filters: []string{
				"request_point = 'point(-122.386177 37.617994)'",
			},
		}
		qc.parseExprs()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.Query.Measures[0].expr).Should(Equal(&expr.Call{
			Name: "count",
			Args: []expr.Expr{
				&expr.Wildcard{},
			},
		}))
		Ω(qc.Query.filters[0]).Should(Equal(&expr.BinaryExpr{
			Op:  expr.EQ,
			LHS: &expr.VarRef{Val: "request_point"},
			RHS: &expr.StringLiteral{Val: "point(-122.386177 37.617994)"},
		}))

		qc.resolveTypes()

		Ω(qc.Query.filters[0]).Should(Equal(&expr.BinaryExpr{
			Op:       expr.EQ,
			LHS:      &expr.VarRef{Val: "request_point", ColumnID: 1, TableID: 0, ExprType: expr.GeoPoint, DataType: memCom.GeoPoint},
			RHS:      &expr.GeopointLiteral{Val: [2]float32{37.617994, -122.386177}},
			ExprType: expr.Boolean,
		}))

		qc.Query = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{
					Expr: "count(*)",
				},
			},
			Filters: []string{
				"request_point IN ('point(-12.386177 34.617994)', 'point(-56.386177 78.617994)', 'point(-1.386177 2.617994)')",
			},
		}
		qc.parseExprs()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.Query.Measures[0].expr).Should(Equal(&expr.Call{
			Name: "count",
			Args: []expr.Expr{
				&expr.Wildcard{},
			},
		}))
		Ω(qc.Query.filters[0]).Should(Equal(&expr.BinaryExpr{
			Op:  expr.IN,
			LHS: &expr.VarRef{Val: "request_point"},
			RHS: &expr.Call{
				Name: "",
				Args: []expr.Expr{
					&expr.StringLiteral{Val: "point(-12.386177 34.617994)"},
					&expr.StringLiteral{Val: "point(-56.386177 78.617994)"},
					&expr.StringLiteral{Val: "point(-1.386177 2.617994)"},
				},
				ExprType: 0,
			},
		}))

		qc.resolveTypes()

		Ω(qc.Query.filters[0]).Should(Equal(&expr.BinaryExpr{
			Op: expr.OR,
			LHS: &expr.BinaryExpr{
				Op: expr.OR,
				LHS: &expr.BinaryExpr{
					Op:       expr.EQ,
					LHS:      &expr.VarRef{Val: "request_point", ColumnID: 1, TableID: 0, ExprType: expr.GeoPoint, DataType: memCom.GeoPoint},
					RHS:      &expr.GeopointLiteral{Val: [2]float32{34.617994, -12.386177}},
					ExprType: expr.Boolean,
				},
				RHS: &expr.BinaryExpr{
					Op:       expr.EQ,
					LHS:      &expr.VarRef{Val: "request_point", ColumnID: 1, TableID: 0, ExprType: expr.GeoPoint, DataType: memCom.GeoPoint},
					RHS:      &expr.GeopointLiteral{Val: [2]float32{78.617994, -56.386177}},
					ExprType: expr.Boolean,
				},
			},
			RHS: &expr.BinaryExpr{
				Op:       expr.EQ,
				LHS:      &expr.VarRef{Val: "request_point", ColumnID: 1, TableID: 0, ExprType: expr.GeoPoint, DataType: memCom.GeoPoint},
				RHS:      &expr.GeopointLiteral{Val: [2]float32{2.617994, -1.386177}},
				ExprType: expr.Boolean,
			},
		}))
	})

	ginkgo.It("adjust filter to time filters", func() {
		schema := &memstore.TableSchema{
			ValueTypeByColumn: []memCom.DataType{
				memCom.Uint32,
				memCom.Uint16,
			},
			ColumnIDs: map[string]int{
				"request_at": 0,
				"id":         1,
			},
			Schema: metaCom.Table{
				IsFactTable: true,
				Columns: []metaCom.Column{
					{Name: "request_at", Type: metaCom.Uint32},
					{Name: "id", Type: metaCom.Uint16},
				},
			},
		}
		q := &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count()"},
			},
			Filters: []string{
				"id <= 1000",
				"request_at >= 1540399020000",
				"request_at < 1540399320000",
				"id > 100",
			},
		}

		qc := &AQLQueryContext{
			Query: q,
			TableIDByAlias: map[string]int{
				"trips": 0,
			},
			TableScanners: []*TableScanner{
				{Schema: schema, ColumnUsages: map[int]columnUsage{0: columnUsedByLiveBatches}},
			},
		}
		qc.processTimezone()
		qc.parseExprs()
		qc.resolveTypes()
		qc.processFilters()
		Ω(qc.Error).Should(BeNil())

		Ω(qc.OOPK.TimeFilters[1]).Should(Equal(&expr.BinaryExpr{
			ExprType: expr.Boolean,
			Op:       expr.LT,
			LHS: &expr.VarRef{
				Val:      "request_at",
				ExprType: expr.Unsigned,
				DataType: memCom.Uint32,
			},
			RHS: &expr.NumberLiteral{
				ExprType: expr.Unsigned,
				Int:      1540399320,
				Expr:     "1540399320",
			},
		}))
		Ω(qc.OOPK.TimeFilters[0]).Should(Equal(&expr.BinaryExpr{
			ExprType: expr.Boolean,
			Op:       expr.GTE,
			LHS: &expr.VarRef{
				Val:      "request_at",
				ExprType: expr.Unsigned,
				DataType: memCom.Uint32,
			},
			RHS: &expr.NumberLiteral{
				ExprType: expr.Unsigned,
				Int:      1540399020,
				Expr:     "1540399020",
			},
		}))
		Ω(len(qc.OOPK.MainTableCommonFilters)).Should(Equal(2))
		Ω(qc.OOPK.MainTableCommonFilters[1]).Should(Equal(&expr.BinaryExpr{
			ExprType: expr.Boolean,
			Op:       expr.GT,
			LHS: &expr.VarRef{
				Val:      "id",
				ColumnID: 1,
				ExprType: expr.Unsigned,
				DataType: memCom.Uint16,
			},
			RHS: &expr.NumberLiteral{
				ExprType: expr.Unsigned,
				Val:      100,
				Int:      100,
				Expr:     "100",
			},
		}))

		// case has no from filter
		q = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count()"},
			},
			Filters: []string{
				"request_at < 1540399320000",
				"id > 100",
			},
		}

		qc = &AQLQueryContext{
			Query: q,
			TableIDByAlias: map[string]int{
				"trips": 0,
			},
			TableScanners: []*TableScanner{
				{Schema: schema, ColumnUsages: map[int]columnUsage{0: columnUsedByLiveBatches}},
			},
		}
		qc.processTimezone()
		qc.parseExprs()
		qc.resolveTypes()
		qc.processFilters()
		Ω(qc.Error).ShouldNot(BeNil())

		// case has multiple from filter
		q = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count()"},
			},
			Filters: []string{
				"request_at >= 1540399020000",
				"request_at >= 1540399080000",
				"request_at < 1540399320000",
				"id > 100",
			},
		}

		qc = &AQLQueryContext{
			Query: q,
			TableIDByAlias: map[string]int{
				"trips": 0,
			},
			TableScanners: []*TableScanner{
				{Schema: schema, ColumnUsages: map[int]columnUsage{0: columnUsedByLiveBatches}},
			},
		}
		qc.processTimezone()
		qc.parseExprs()
		Ω(qc.Error).ShouldNot(BeNil())

		// case has timezone
		q = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count()"},
			},
			Filters: []string{
				"request_at >= 1540399140000",
				"request_at < 1540399320000",
				"id > 100",
			},
			Timezone: "Pacific/Auckland",
		}

		qc = &AQLQueryContext{
			Query: q,
			TableIDByAlias: map[string]int{
				"trips": 0,
			},
			TableScanners: []*TableScanner{
				{Schema: schema, ColumnUsages: map[int]columnUsage{0: columnUsedByLiveBatches}},
			},
		}
		qc.processTimezone()
		qc.parseExprs()
		qc.resolveTypes()
		qc.processFilters()
		Ω(qc.Error).Should(BeNil())

		// case using stringliteral in filter
		q = &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count()"},
			},
			Filters: []string{
				"request_at >= '-1d'",
				"request_at < 'now'",
				"id > 100",
			},
		}

		qc = &AQLQueryContext{
			Query: q,
			TableIDByAlias: map[string]int{
				"trips": 0,
			},
			TableScanners: []*TableScanner{
				{Schema: schema, ColumnUsages: map[int]columnUsage{0: columnUsedByLiveBatches}},
			},
		}
		qc.processTimezone()
		qc.parseExprs()
		Ω(qc.Error).Should(BeNil())

		qc.resolveTypes()
		qc.processFilters()
		Ω(qc.Error).Should(BeNil())
	})
})
