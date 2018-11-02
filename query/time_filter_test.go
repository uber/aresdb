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
	"fmt"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber/aresdb/query/expr"
	"time"
)

type testCase struct {
	expectedFrom string
	expectedTo   string
	expectedUnit string
	expression   string
}

var _ = ginkgo.Describe("Test", func() {
	location, _ := time.LoadLocation("America/Los_Angeles")
	now := time.Date(2016, time.March, 15, 21, 24, 26, 0, location)

	ginkgo.It("Works on empty input", func() {
		from, to, err := parseTimeFilter(TimeFilter{}, time.UTC, now)
		Ω(err).Should(BeNil())
		Ω(from).Should(BeNil())
		Ω(to).Should(BeNil())
	})

	ginkgo.It("Works", func() {
		testCases := []testCase{
			{expectedFrom: "2016-03-16T00:24:26-04:00", expectedTo: "2016-03-16T00:24:26-04:00", expectedUnit: "s", expression: "now"},
			{expectedFrom: "2016-01-01T00:00:00-05:00", expectedTo: "2017-01-01T00:00:00-05:00", expectedUnit: "y", expression: "this year"},
			{expectedFrom: "2016-01-01T00:00:00-05:00", expectedTo: "2017-01-01T00:00:00-05:00", expectedUnit: "y", expression: "0y"},
			{expectedFrom: "2016-01-01T00:00:00-05:00", expectedTo: "2016-04-01T00:00:00-04:00", expectedUnit: "q", expression: "this quarter"}, // daylight saving begins
			{expectedFrom: "2016-01-01T00:00:00-05:00", expectedTo: "2016-04-01T00:00:00-04:00", expectedUnit: "q", expression: "0q"},           // daylight saving begins
			{expectedFrom: "2016-03-01T00:00:00-05:00", expectedTo: "2016-04-01T00:00:00-04:00", expectedUnit: "M", expression: "this month"},   // daylight saving begins
			{expectedFrom: "2016-03-01T00:00:00-05:00", expectedTo: "2016-04-01T00:00:00-04:00", expectedUnit: "M", expression: "0M"},           // daylight saving begins
			{expectedFrom: "2016-03-14T00:00:00-04:00", expectedTo: "2016-03-21T00:00:00-04:00", expectedUnit: "w", expression: "this week"},
			{expectedFrom: "2016-03-14T00:00:00-04:00", expectedTo: "2016-03-21T00:00:00-04:00", expectedUnit: "w", expression: "0w"},
			{expectedFrom: "2016-03-16T00:00:00-04:00", expectedTo: "2016-03-17T00:00:00-04:00", expectedUnit: "d", expression: "this day"},
			{expectedFrom: "2016-03-16T00:00:00-04:00", expectedTo: "2016-03-17T00:00:00-04:00", expectedUnit: "d", expression: "0d"},
			{expectedFrom: "2016-03-16T00:00:00-04:00", expectedTo: "2016-03-17T00:00:00-04:00", expectedUnit: "d", expression: "today"},
			{expectedFrom: "2016-03-16T00:00:00-04:00", expectedTo: "2016-03-16T01:00:00-04:00", expectedUnit: "h", expression: "this hour"},
			{expectedFrom: "2016-03-16T00:00:00-04:00", expectedTo: "2016-03-16T01:00:00-04:00", expectedUnit: "h", expression: "0h"},
			{expectedFrom: "2016-03-16T00:15:00-04:00", expectedTo: "2016-03-16T00:30:00-04:00", expectedUnit: "15m", expression: "this quarter-hour"},
			{expectedFrom: "2016-03-16T00:24:00-04:00", expectedTo: "2016-03-16T00:25:00-04:00", expectedUnit: "m", expression: "this minute"},
			{expectedFrom: "2016-03-16T00:24:00-04:00", expectedTo: "2016-03-16T00:25:00-04:00", expectedUnit: "m", expression: "0m"},
			{expectedFrom: "2015-01-01T00:00:00-05:00", expectedTo: "2016-01-01T00:00:00-05:00", expectedUnit: "y", expression: "last year"},
			{expectedFrom: "2015-01-01T00:00:00-05:00", expectedTo: "2016-01-01T00:00:00-05:00", expectedUnit: "y", expression: "-1y"},
			{expectedFrom: "2015-10-01T00:00:00-04:00", expectedTo: "2016-01-01T00:00:00-05:00", expectedUnit: "q", expression: "last quarter"}, // daylight saving ends
			{expectedFrom: "2015-10-01T00:00:00-04:00", expectedTo: "2016-01-01T00:00:00-05:00", expectedUnit: "q", expression: "-1q"},          // daylight saving ends
			{expectedFrom: "2016-02-01T00:00:00-05:00", expectedTo: "2016-03-01T00:00:00-05:00", expectedUnit: "M", expression: "last month"},
			{expectedFrom: "2016-02-01T00:00:00-05:00", expectedTo: "2016-03-01T00:00:00-05:00", expectedUnit: "M", expression: "-1M"},
			{expectedFrom: "2016-03-07T00:00:00-05:00", expectedTo: "2016-03-14T00:00:00-04:00", expectedUnit: "w", expression: "last week"}, // daylight saving begins
			{expectedFrom: "2016-03-07T00:00:00-05:00", expectedTo: "2016-03-14T00:00:00-04:00", expectedUnit: "w", expression: "-1w"},       // daylight saving begins
			{expectedFrom: "2016-03-15T00:00:00-04:00", expectedTo: "2016-03-16T00:00:00-04:00", expectedUnit: "d", expression: "last day"},
			{expectedFrom: "2016-03-15T00:00:00-04:00", expectedTo: "2016-03-16T00:00:00-04:00", expectedUnit: "d", expression: "-1d"},
			{expectedFrom: "2016-03-15T00:00:00-04:00", expectedTo: "2016-03-16T00:00:00-04:00", expectedUnit: "d", expression: "yesterday"},
			{expectedFrom: "2016-03-15T23:00:00-04:00", expectedTo: "2016-03-16T00:00:00-04:00", expectedUnit: "h", expression: "last hour"},
			{expectedFrom: "2016-03-15T23:00:00-04:00", expectedTo: "2016-03-16T00:00:00-04:00", expectedUnit: "h", expression: "-1h"},
			{expectedFrom: "2016-03-16T00:00:00-04:00", expectedTo: "2016-03-16T00:15:00-04:00", expectedUnit: "15m", expression: "last quarter-hour"},
			{expectedFrom: "2016-03-16T00:23:00-04:00", expectedTo: "2016-03-16T00:24:00-04:00", expectedUnit: "m", expression: "last minute"},
			{expectedFrom: "2016-03-16T00:23:00-04:00", expectedTo: "2016-03-16T00:24:00-04:00", expectedUnit: "m", expression: "-1m"},
			{expectedFrom: "2014-01-01T00:00:00-05:00", expectedTo: "2015-01-01T00:00:00-05:00", expectedUnit: "y", expression: "2 years ago"},
			{expectedFrom: "2014-01-01T00:00:00-05:00", expectedTo: "2015-01-01T00:00:00-05:00", expectedUnit: "y", expression: "-2y"},
			{expectedFrom: "2015-04-01T00:00:00-04:00", expectedTo: "2015-07-01T00:00:00-04:00", expectedUnit: "q", expression: "3 quarters ago"},
			{expectedFrom: "2015-04-01T00:00:00-04:00", expectedTo: "2015-07-01T00:00:00-04:00", expectedUnit: "q", expression: "-3q"},
			{expectedFrom: "2015-11-01T00:00:00-04:00", expectedTo: "2015-12-01T00:00:00-05:00", expectedUnit: "M", expression: "4 months ago"}, // daylight saving ends
			{expectedFrom: "2015-11-01T00:00:00-04:00", expectedTo: "2015-12-01T00:00:00-05:00", expectedUnit: "M", expression: "-4M"},          // daylight saving ends
			{expectedFrom: "2016-02-08T00:00:00-05:00", expectedTo: "2016-02-15T00:00:00-05:00", expectedUnit: "w", expression: "5 weeks ago"},
			{expectedFrom: "2016-02-08T00:00:00-05:00", expectedTo: "2016-02-15T00:00:00-05:00", expectedUnit: "w", expression: "-5w"},
			{expectedFrom: "2016-03-10T00:00:00-05:00", expectedTo: "2016-03-11T00:00:00-05:00", expectedUnit: "d", expression: "6 days ago"},
			{expectedFrom: "2016-03-10T00:00:00-05:00", expectedTo: "2016-03-11T00:00:00-05:00", expectedUnit: "d", expression: "-6d"},
			{expectedFrom: "2016-03-13T01:00:00-05:00", expectedTo: "2016-03-13T03:00:00-04:00", expectedUnit: "h", expression: "70 hours ago"}, // daylight saving begins
			{expectedFrom: "2016-03-13T01:00:00-05:00", expectedTo: "2016-03-13T03:00:00-04:00", expectedUnit: "h", expression: "-70h"},         // daylight saving begins
			{expectedFrom: "2016-03-15T23:00:00-04:00", expectedTo: "2016-03-15T23:15:00-04:00", expectedUnit: "15m", expression: "5 quarter-hours ago"},
			{expectedFrom: "2016-03-15T23:24:00-04:00", expectedTo: "2016-03-15T23:25:00-04:00", expectedUnit: "m", expression: "60 minutes ago"},
			{expectedFrom: "2016-03-15T23:24:00-04:00", expectedTo: "2016-03-15T23:25:00-04:00", expectedUnit: "m", expression: "-60m"},
			{expectedFrom: "2014-01-01T00:00:00-05:00", expectedTo: "2015-01-01T00:00:00-05:00", expectedUnit: "y", expression: "2014"},
			{expectedFrom: "2014-04-01T00:00:00-04:00", expectedTo: "2014-07-01T00:00:00-04:00", expectedUnit: "q", expression: "2014-Q2"},
			{expectedFrom: "2014-03-01T00:00:00-05:00", expectedTo: "2014-04-01T00:00:00-04:00", expectedUnit: "M", expression: "2014-03"},          // daylight saving begins
			{expectedFrom: "2016-03-13T00:00:00-05:00", expectedTo: "2016-03-14T00:00:00-04:00", expectedUnit: "d", expression: "2016-03-13"},       // daylight saving begins
			{expectedFrom: "2016-03-13T01:00:00-05:00", expectedTo: "2016-03-13T03:00:00-04:00", expectedUnit: "h", expression: "2016-03-13 01"},    // daylight saving begins
			{expectedFrom: "2016-03-13T02:00:00-04:00", expectedTo: "2016-03-13T03:00:00-04:00", expectedUnit: "h", expression: "2016-03-13 02"},    // same hour
			{expectedFrom: "2016-03-13T01:31:00-05:00", expectedTo: "2016-03-13T01:32:00-05:00", expectedUnit: "m", expression: "2016-03-13 01:31"}, // non-sense
			{expectedFrom: "2015-11-01T01:00:00-04:00", expectedTo: "2015-11-01T02:00:00-04:00", expectedUnit: "h", expression: "2015-11-01 01"},    // daylight saving ends, ambiguous
			{expectedFrom: "2015-11-01T02:00:00-05:00", expectedTo: "2015-11-01T03:00:00-05:00", expectedUnit: "h", expression: "2015-11-01 02"},    // daylight saving ends
			{expectedFrom: "2015-11-01T01:31:00-04:00", expectedTo: "2015-11-01T01:32:00-04:00", expectedUnit: "m", expression: "2015-11-01 01:31"}, // daylight saving ends, ambiguous
			{expectedFrom: "2016-06-01T22:00:00-04:00", expectedTo: "2016-06-01T22:00:00-04:00", expectedUnit: "m", expression: "1464832800"},
			{expectedFrom: "2016-06-01T22:00:01-04:00", expectedTo: "2016-06-01T22:00:01-04:00", expectedUnit: "s", expression: "1464832801"},
		}

		for _, testCase := range testCases {
			loc, _ := time.LoadLocation("America/New_York")
			from, to, err := parseTimeFilter(TimeFilter{
				Column: "request_at",
				From:   testCase.expression,
				To:     testCase.expression,
			}, loc, now)
			Ω(err).Should(BeNil())

			expectedFrom, err := time.Parse(time.RFC3339, testCase.expectedFrom)
			Ω(err).Should(BeNil())
			Ω(from.Time.UnixNano()).Should(Equal(expectedFrom.UnixNano()))
			Ω(from.Unit).Should(Equal(testCase.expectedUnit))

			expectedTo, err := time.Parse(time.RFC3339, testCase.expectedTo)
			Ω(err).Should(BeNil())
			Ω(to.Time.UnixNano()).Should(Equal(expectedTo.UnixNano()))
			Ω(to.Unit).Should(Equal(testCase.expectedUnit))

			fromExpr, toExpr := createTimeFilterExpr(&expr.VarRef{Val: "request_at"}, from, to)

			Ω(testCase.expression + ": " + fromExpr.String() + " AND " + toExpr.String()).Should(Equal(fmt.Sprintf(
				testCase.expression+": request_at >= %d AND request_at < %d",
				expectedFrom.Unix(), expectedTo.Unix())))
		}
	})

	ginkgo.It("Works on fixed timezone", func() {
		loc := time.FixedZone("fixed", -(7*60*60 + 30*60))
		from, to, err := parseTimeFilter(TimeFilter{
			Column: "request_at",
			From:   "this year",
			To:     "",
		}, loc, now)
		Ω(err).Should(BeNil())
		fromExpr, toExpr := createTimeFilterExpr(&expr.VarRef{Val: "request_at"}, from, to)
		Ω(fromExpr.String() + " AND " + toExpr.String()).Should(Equal("request_at >= 1451633400 AND request_at < 1458102266"))

		from, to, err = parseTimeFilter(TimeFilter{
			Column: "request_at",
			From:   "",
			To:     "last year",
		}, loc, now)
		Ω(err).Should(BeNil())
		fromExpr, toExpr = createTimeFilterExpr(&expr.VarRef{Val: "request_at"}, from, to)
		Ω(toExpr.String()).Should(Equal("request_at < 1451633400"))
	})

	ginkgo.It("Corrects America/Sao_Paulo daylight saving start issue", func() {
		loc, _ := time.LoadLocation("America/Sao_Paulo")
		t := time.Date(2016, 10, 16, 13, 23, 0, 0, loc)
		start, end, _ := applyTimeOffset(t, 0, "d")
		Ω(start.Day()).Should(Equal(16))
		Ω(start.Hour()).Should(Equal(1))
		Ω(end.Day()).Should(Equal(17))
		Ω(end.Hour()).Should(Equal(0))
	})

	ginkgo.It("Fails on error", func() {
		testCases := []TimeFilter{
			{Column: "request_at", From: "future", To: ""},
			{Column: "request_at", From: "", To: "future"},
			{Column: "request_at", From: "this", To: ""},
			{Column: "request_at", From: "last friday night", To: ""},
			{Column: "request_at", From: "years ago", To: ""},
			{Column: "request_at", From: "N years ago", To: ""},
			{Column: "request_at", From: "-Xd", To: ""},
			{Column: "request_at", From: "2014-01-01 00:00 GMT", To: ""},
			{Column: "request_at", From: "2014-01-01-Haha", To: ""},
			{Column: "request_at", From: "2014-QE", To: ""},
			{Column: "request_at", From: "2014-Q1-earnings", To: ""},
			{Column: "request_at", From: "2014-EF", To: ""},
			{Column: "request_at", From: "2014-03-de", To: ""},
			{Column: "request_at", From: "2014-03 11:23", To: ""},
			{Column: "request_at", From: "2014-03-04 11:23:45", To: ""},
			{Column: "request_at", From: "2014-03-04 hh", To: ""},
			{Column: "request_at", From: "2014-03-04 06:mm", To: ""},
			{Column: "request_at", From: "this century", To: ""},
		}

		for _, testCase := range testCases {
			_, _, err := parseTimeFilter(testCase, time.UTC, now)
			Ω(err).ShouldNot(BeNil())
		}
	})
})
