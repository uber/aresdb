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
	common2 "github.com/uber/aresdb/metastore/common"
	metaMocks "github.com/uber/aresdb/metastore/mocks"
	"github.com/uber/aresdb/query/common"
	"github.com/uber/aresdb/query/expr"
	"net/http/httptest"
)

var _ = ginkgo.Describe("query compiler", func() {
	ginkgo.It("should work happy path", func() {
		mockMutator := metaMocks.TableSchemaReader{}
		mockMutator.On("GetTable", "table1").Return(&common2.Table{
			Name: "table1",
			Columns: []common2.Column{
				{Name: "field1"},
				{Name: "field2"},
			},
		}, nil)

		qc := NewQueryContext(&common.AQLQuery{
			Table: "table1",
			Joins: nil,
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
			SQLQuery: "SELECT count(*) FROM table1 GROUP BY field1",
		}, httptest.NewRecorder())
		qc.Compile(&mockMutator)
		Ω(qc.Error).Should(BeNil())
		Ω(qc.AQLQuery).Should(Equal(&common.AQLQuery{
			Table: "table1",
			Joins: nil,
			Dimensions: []common.Dimension{
				{
					Expr: "field1",
				},
			},
			Measures: []common.Measure{
				{
					Expr:       "count(*)",
					ExprParsed: &expr.Call{Name: "count", Args: []expr.Expr{&expr.Wildcard{}}, ExprType: 0},
				},
			},
			SQLQuery: "SELECT count(*) FROM table1 GROUP BY field1",
		}))

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
			Limit:    nonAggregationQueryLimit,
			SQLQuery: "SELECT * FROM table1",
		}, httptest.NewRecorder())
		qc.Compile(&mockMutator)
		Ω(qc.Error).Should(BeNil())
		Ω(qc.AQLQuery).Should(Equal(&common.AQLQuery{
			Table: "table1",
			Joins: nil,
			Dimensions: []common.Dimension{
				{Expr: "field1"},
				{Expr: "field2"},
			},
			Measures: []common.Measure{
				{
					Expr:       "1",
					ExprParsed: &expr.NumberLiteral{Val: 1, Int: 1, Expr: "1", ExprType: 2},
				},
			},
			Limit:    nonAggregationQueryLimit,
			SQLQuery: "SELECT * FROM table1",
		}))
	})

	ginkgo.It("should fail invalid table names", func() {
		mockMutator := metaMocks.TableSchemaReader{}
		mockMutator.On("GetTable", "tableNonExist").Return(nil, errors.New("not found"))

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
		}, httptest.NewRecorder())
		qc.Compile(&mockMutator)
		Ω(qc.Error).ShouldNot(BeNil())

		mockMutator.On("GetTable", "table1").Return(nil, nil)
		mockMutator.On("GetTable", "foreignTableNonExsit").Return(nil, errors.New("no found"))
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
		}, httptest.NewRecorder())
		qc.Compile(&mockMutator)
		Ω(qc.Error).ShouldNot(BeNil())
	})

	ginkgo.It("should fail more than 1 measure", func() {
		mockMutator := metaMocks.TableSchemaReader{}
		mockMutator.On("GetTable", mock.Anything).Return(nil, nil)

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
		}, httptest.NewRecorder())
		qc.Compile(&mockMutator)
		Ω(qc.Error).ShouldNot(BeNil())
	})
})
