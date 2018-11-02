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
	"github.com/uber/aresdb/common"
	"github.com/uber/aresdb/memstore"
	"github.com/uber/aresdb/query/expr"
	"github.com/uber/aresdb/utils"
	"encoding/json"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber-go/tally"
	"time"
)

var _ = ginkgo.Describe("Time Bucketizer", func() {
	var qc *AQLQueryContext
	ginkgo.BeforeEach(func() {
		q := &AQLQuery{
			Table: "trips",
			Measures: []Measure{
				{Expr: "count()"},
			},
			TimeFilter: TimeFilter{
				From: "-1d",
				To:   "0d",
			},
			Dimensions: []Dimension{Dimension{Expr: "requested_at", TimeBucketizer: "hour"}},
		}
		qc = &AQLQueryContext{
			Query: q,
		}

		qc.parseExprs()
	})

	ginkgo.It("qc.buildTimeDimensionExpr for regular interval should work", func() {
		timeColumn := &expr.VarRef{
			Val: "request_at",
		}
		exp, err := qc.buildTimeDimensionExpr("Day", timeColumn)
		Ω(exp).ShouldNot(BeNil())
		Ω(exp.String()).Should(Equal("request_at FLOOR 86400"))
		Ω(err).Should(BeNil())

		exp, err = qc.buildTimeDimensionExpr("m", timeColumn)
		Ω(exp).ShouldNot(BeNil())
		Ω(exp.String()).Should(Equal("request_at FLOOR 60"))
		Ω(err).Should(BeNil())

		exp, err = qc.buildTimeDimensionExpr("3m", timeColumn)
		Ω(exp).ShouldNot(BeNil())
		Ω(exp.String()).Should(Equal("request_at FLOOR 180"))
		Ω(err).Should(BeNil())

		exp, err = qc.buildTimeDimensionExpr("3 minutes", timeColumn)
		Ω(exp).ShouldNot(BeNil())
		Ω(exp.String()).Should(Equal("request_at FLOOR 180"))
		Ω(err).Should(BeNil())

		exp, err = qc.buildTimeDimensionExpr("h", timeColumn)
		Ω(exp).ShouldNot(BeNil())
		Ω(exp.String()).Should(Equal("request_at FLOOR 3600"))
		Ω(err).Should(BeNil())

		exp, err = qc.buildTimeDimensionExpr("4h", timeColumn)
		Ω(exp).ShouldNot(BeNil())
		Ω(exp.String()).Should(Equal("request_at FLOOR 14400"))
		Ω(err).Should(BeNil())

		exp, err = qc.buildTimeDimensionExpr("4 hours", timeColumn)
		Ω(exp).ShouldNot(BeNil())
		Ω(exp.String()).Should(Equal("request_at FLOOR 14400"))
		Ω(err).Should(BeNil())

		exp, err = qc.buildTimeDimensionExpr("d", timeColumn)
		Ω(exp).ShouldNot(BeNil())
		Ω(exp.String()).Should(Equal("request_at FLOOR 86400"))
		Ω(err).Should(BeNil())

		exp, err = qc.buildTimeDimensionExpr("week", timeColumn)
		Ω(exp).ShouldNot(BeNil())
		Ω(exp.String()).Should(Equal("GET_WEEK_START(request_at)"))
		Ω(err).Should(BeNil())

		utils.Init(common.AresServerConfig{Query: common.QueryConfig{TimezoneTable: common.TimezoneConfig{
			TableName: "",
		}}}, common.NewLoggerFactory().GetDefaultLogger(), common.NewLoggerFactory().GetDefaultLogger(), tally.NewTestScope("test", nil))
		qc.timezoneTable.tableColumn = "timezone"
		qc.timezoneTable.tableAlias = defaultTimezoneTableAlias
		qc.TableIDByAlias = map[string]int{defaultTimezoneTableAlias: 0}
		qc.TableScanners = []*TableScanner{{Schema: &memstore.TableSchema{ColumnIDs: map[string]int{"timezone": 1}}}}
		exp, err = qc.buildTimeDimensionExpr("week", timeColumn)
		Ω(exp).ShouldNot(BeNil())
		Ω(exp.String()).Should(Equal("GET_WEEK_START(request_at CONVERT_TZ __timezone_lookup.timezone)"))
		Ω(err).Should(BeNil())

		qc.timezoneTable.tableColumn = ""
		qc.fixedTimezone = time.FixedZone("Foo", 2018)
		qc.fromTime = &alignedTime{Time: utils.Now().In(qc.fixedTimezone)}
		qc.toTime = &alignedTime{Time: utils.Now().In(qc.fixedTimezone)}
		exp, err = qc.buildTimeDimensionExpr("week", timeColumn)
		Ω(exp).ShouldNot(BeNil())
		Ω(exp.String()).Should(Equal("GET_WEEK_START(request_at CONVERT_TZ 2018)"))
		Ω(err).Should(BeNil())

		// bucket "Day-X" is illegal
		exp, err = qc.buildTimeDimensionExpr("Day-X", timeColumn)
		Ω(exp).Should(BeNil())
		Ω(err).ShouldNot(BeNil())

		// 7m can not align to start of hour
		exp, err = qc.buildTimeDimensionExpr("7m", timeColumn)
		Ω(exp).Should(BeNil())
		Ω(err).ShouldNot(BeNil())

		// 7h can not align to start of day
		exp, err = qc.buildTimeDimensionExpr("7h", timeColumn)
		Ω(exp).Should(BeNil())
		Ω(err).ShouldNot(BeNil())

		// 0m is not valid
		exp, err = qc.buildTimeDimensionExpr("7m", timeColumn)
		Ω(exp).Should(BeNil())
		Ω(err).ShouldNot(BeNil())

		// non minute/hour Unit with Size is not valid
		exp, err = qc.buildTimeDimensionExpr("1 centry", timeColumn)
		Ω(exp).Should(BeNil())
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("qc.buildTimeDimensionExpr for irregular interval should work", func() {
		timeColumn := &expr.VarRef{
			Val: "request_at",
		}
		exp, err := qc.buildTimeDimensionExpr("month", timeColumn)
		Ω(exp).ShouldNot(BeNil())
		Ω(exp.String()).Should(Equal("GET_MONTH_START(request_at)"))
		Ω(err).Should(BeNil())

		exp, err = qc.buildTimeDimensionExpr("quarter", timeColumn)
		Ω(exp).ShouldNot(BeNil())
		Ω(exp.String()).Should(Equal("GET_QUARTER_START(request_at)"))
		Ω(err).Should(BeNil())

		exp, err = qc.buildTimeDimensionExpr("year", timeColumn)
		Ω(exp).ShouldNot(BeNil())
		Ω(exp.String()).Should(Equal("GET_YEAR_START(request_at)"))
		Ω(err).Should(BeNil())
	})

	ginkgo.It("qc.buildTimeDimensionExpr for regular recurring time bucketizer should work", func() {
		timeColumn := &expr.VarRef{
			Val: "request_at",
		}
		exp, err := qc.buildTimeDimensionExpr("time of day", timeColumn)
		Ω(exp).ShouldNot(BeNil())
		Ω(exp.String()).Should(Equal("request_at % 86400"))
		Ω(err).Should(BeNil())

		exp, err = qc.buildTimeDimensionExpr("2 minutes of day", timeColumn)
		Ω(err).Should(BeNil())
		Ω(exp).ShouldNot(BeNil())
		Ω(exp.String()).Should(Equal("request_at % 86400 FLOOR 120"))

		exp, err = qc.buildTimeDimensionExpr("7 minutes of day", timeColumn)
		Ω(err).ShouldNot(BeNil())
		Ω(err.Error()).Should(ContainSubstring("Only {2,3,4,5,6,10,15,20,30} minutes of day are allowed : got 7 minutes of day"))

		exp, err = qc.buildTimeDimensionExpr("hour of day", timeColumn)
		Ω(exp).ShouldNot(BeNil())
		Ω(exp.String()).Should(Equal("request_at % 86400 FLOOR 3600"))
		Ω(err).Should(BeNil())

		exp, err = qc.buildTimeDimensionExpr("hour of week", timeColumn)
		Ω(exp).ShouldNot(BeNil())
		Ω(exp.String()).Should(Equal("request_at - 345600 % 604800 FLOOR 3600"))
		Ω(err).Should(BeNil())

		exp, err = qc.buildTimeDimensionExpr("day of week", timeColumn)
		Ω(exp).ShouldNot(BeNil())
		Ω(exp.String()).Should(Equal("request_at - 345600 % 604800 FLOOR 86400 / 86400.00"))
		Ω(err).Should(BeNil())
	})

	ginkgo.It("qc.buildTimeDimensionExpr for irregular recurring time bucketizer should work", func() {
		timeColumn := &expr.VarRef{
			Val: "request_at",
		}
		exp, err := qc.buildTimeDimensionExpr("day of month", timeColumn)
		Ω(exp).ShouldNot(BeNil())
		Ω(exp.String()).Should(Equal("GET_DAY_OF_MONTH(request_at)"))
		Ω(err).Should(BeNil())

		exp, err = qc.buildTimeDimensionExpr("day of year", timeColumn)
		Ω(exp).ShouldNot(BeNil())
		Ω(exp.String()).Should(Equal("GET_DAY_OF_YEAR(request_at)"))
		Ω(err).Should(BeNil())

		exp, err = qc.buildTimeDimensionExpr("month of year", timeColumn)
		Ω(exp).ShouldNot(BeNil())
		Ω(exp.String()).Should(Equal("GET_MONTH_OF_YEAR(request_at)"))
		Ω(err).Should(BeNil())

		exp, err = qc.buildTimeDimensionExpr("quarter of year", timeColumn)
		Ω(exp).ShouldNot(BeNil())
		Ω(exp.String()).Should(Equal("GET_QUARTER_OF_YEAR(request_at)"))
		Ω(err).Should(BeNil())
	})

	ginkgo.It("parses query with TimeSeriesBucketizer", func() {
		goodQuery := &AQLQuery{
			Table: "trips",
			Dimensions: []Dimension{
				{
					Expr:           "request_at",
					TimeBucketizer: "quarter-hour",
				},
			},
		}
		qc.Query = goodQuery
		qc.parseExprs()
		Ω(qc.Error).Should(BeNil())

		// missing time column
		badQuery := &AQLQuery{
			Table: "trips",
			Dimensions: []Dimension{
				{
					TimeBucketizer: "quarter-hour",
				},
			},
		}
		qc.Query = badQuery
		qc.parseExprs()
		Ω(qc.Error).ShouldNot(BeNil())
	})

	ginkgo.It("fixed timezone across DST switch timestamp", func() {
		qc = &AQLQueryContext{
			Query: &AQLQuery{
				Table: "trips",
				Measures: []Measure{
					{Expr: "count()"},
				},
				TimeFilter: TimeFilter{
					From: "1509772380",
					To:   "1509882360",
				},
				Dimensions: []Dimension{Dimension{Expr: "requested_at", TimeBucketizer: "hour"}},
				Timezone:   "America/Los_Angeles",
			},
		}
		qc.processTimezone()
		Ω(qc.Error).Should(BeNil())
		qc.parseExprs()
		Ω(qc.Error).Should(BeNil())
		Ω(qc.fromTime).ShouldNot(BeNil())
		Ω(qc.toTime).ShouldNot(BeNil())
		Ω(qc.fixedTimezone.String()).Should(Equal("America/Los_Angeles"))
		bb, _ := json.Marshal(qc.Query.Dimensions[0].expr)
		Ω(string(bb)).Should(Equal(`{"Op":"FLOOR","LHS":{"Op":"+","LHS":{"Val":"requested_at","ExprType":"Unknown","TableID":0,"ColumnID":0,"DataType":0},"RHS":{"Op":"+","LHS":{"Val":0,"Int":-25200,"Expr":"-25200","ExprType":"Signed"},"RHS":{"Op":"*","LHS":{"Val":0,"Int":3600,"Expr":"3600","ExprType":"Signed"},"RHS":{"Op":"\u003e=","LHS":{"Val":"requested_at","ExprType":"Unknown","TableID":0,"ColumnID":0,"DataType":0},"RHS":{"Val":0,"Int":1509872400,"Expr":"1509872400","ExprType":"Unknown"},"ExprType":"Boolean"},"ExprType":"Signed"},"ExprType":"Unknown"},"ExprType":"Unknown"},"RHS":{"Val":0,"Int":3600,"Expr":"3600","ExprType":"Unsigned"},"ExprType":"Unknown"}`))
	})
})
