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
	"github.com/uber/aresdb/common"
	queryCom "github.com/uber/aresdb/query/common"
)

var _ = ginkgo.Describe("SQL Parser", func() {

	logger := &common.NoopLogger{}

	runTest := func(sqls []string, aql queryCom.AQLQuery, log common.Logger) {
		for _, sql := range sqls {
			actual, err := Parse(sql, logger)
			expected := aql
			expected.SQLQuery = sql
			Ω(err).Should(BeNil())
			Ω(*actual).Should(BeEquivalentTo(expected))
		}
	}

	ginkgo.It("parse row filters should work", func() {
		sqls := []string{
			`SELECT count(*) AS completed_trips, sum(fare)
			FROM trips
			WHERE status='completed' AND NOT status = 'cancelled' OR marketplace='agora'
			GROUP BY status`,
		}
		res := queryCom.AQLQuery{
			Table:      "trips",
			Measures:   []queryCom.Measure{{Alias: "completed_trips", Expr: "count(*)"}, {Expr: "sum(fare)"}},
			Filters:    []string{"status='completed' AND NOT status = 'cancelled' OR marketplace='agora'"},
			Dimensions: []queryCom.Dimension{{Expr: "status"}},
		}
		runTest(sqls, res, logger)
	})

	ginkgo.It("geography_intersects should work", func() {
		sqls := []string{
			`SELECT count(*) AS completed_trips
			FROM trips LEFT JOIN geo_table g ON geography_intersects(g.shape, request_location)
			WHERE status='completed' AND NOT status = 'cancelled' OR marketplace='agora' AND g.geofence_uuid IN (0x9EAE9256C1F547449E9BD3A2B64826B9)
			GROUP BY status, hex(g.geofence_uuid)`,
		}
		res := queryCom.AQLQuery{
			Table:      "trips",
			Measures:   []queryCom.Measure{{Alias: "completed_trips", Expr: "count(*)"}},
			Filters:    []string{"status='completed' AND NOT status = 'cancelled' OR marketplace='agora' AND g.geofence_uuid IN (0x9EAE9256C1F547449E9BD3A2B64826B9)"},
			Dimensions: []queryCom.Dimension{{Expr: "status"}, {Expr: "hex(g.geofence_uuid)"}},
			Joins: []queryCom.Join{
				{Table: "geo_table", Alias: "g", Conditions: []string{"geography_intersects(g.shape, request_location)"}},
			},
		}
		runTest(sqls, res, logger)
	})

	ginkgo.It("parse dimensions should work", func() {
		sqls := []string{
			`SELECT status AS trip_status, count(*) 
			FROM trips 
			GROUP BY trip_status;`,
		}
		res := queryCom.AQLQuery{
			Table:      "trips",
			Measures:   []queryCom.Measure{{Alias: "trip_status", Expr: "status"}, {Expr: "count(*)"}},
			Dimensions: []queryCom.Dimension{{Alias: "trip_status", Expr: "status"}},
		}
		runTest(sqls, res, logger)
	})

	ginkgo.It("parse non agg queryCom.AQLQuery should work", func() {
		sqls := []string{
			`SELECT field1, *
			FROM trips LIMIT 10;`,
		}
		res := queryCom.AQLQuery{
			Table:      "trips",
			Measures:   []queryCom.Measure{{Expr: "1"}},
			Dimensions: []queryCom.Dimension{{Expr: "field1"}, {Expr: "*"}},
			Limit:      10,
		}
		runTest(sqls, res, logger)
	})

	ginkgo.It("parse sort by should work", func() {
		sqls := []string{
			`SELECT field1
			FROM trips
			ORDER BY field1;`,
		}
		res := queryCom.AQLQuery{
			Table:      "trips",
			Measures:   []queryCom.Measure{{Expr: "1"}},
			Dimensions: []queryCom.Dimension{{Expr: "field1"}},
			Sorts: []queryCom.SortField{
				{Name: "field1", Order: "ASC"},
			},
		}
		runTest(sqls, res, logger)
	})

	ginkgo.It("parse time UDFs should work", func() {

		sqls := []string{
			// Precision truncation based bucketizer.
			`SELECT count(*) FROM trips
			GROUP BY aql_time_bucket_minute("request_at", "minute", "America/New_York");`,
			`SELECT count(*) FROM trips
			GROUP BY aql_time_bucket_hour("request_at", "minute", "America/New_York");`,
			`SELECT count(*) FROM trips
			GROUP BY aql_time_bucket_day("request_at", "minute", "America/New_York");`,
			`SELECT count(*) FROM trips
			GROUP BY aql_time_bucket_week("request_at", "minute", "America/New_York");`,
			`SELECT count(*) FROM trips
			GROUP BY aql_time_bucket_month("request_at", "minute", "America/New_York");`,
			`SELECT count(*) FROM trips
			GROUP BY aql_time_bucket_quarter("request_at", "minute", "America/New_York");`,
			`SELECT count(*) FROM trips
			GROUP BY aql_time_bucket_year("request_at", "minute", "America/New_York");`,
			// Component based bucketizer.
			`SELECT count(*) FROM trips
			GROUP BY aql_time_bucket_time_of_day("request_at", "minute", "America/New_York");`,
			`SELECT count(*) FROM trips
			GROUP BY aql_time_bucket_minutes_of_day("request_at", "minute", "America/New_York");`,
			`SELECT count(*) FROM trips
			GROUP BY aql_time_bucket_hour_of_day("request_at", "minute", "America/New_York");`,
			`SELECT count(*) FROM trips
			GROUP BY aql_time_bucket_hour_of_week("request_at", "minute", "America/New_York");`,
			`SELECT count(*) FROM trips
			GROUP BY aql_time_bucket_day_of_week("request_at", "minute", "America/New_York");`,
			`SELECT count(*) FROM trips
			GROUP BY aql_time_bucket_day_of_month("request_at", "minute", "America/New_York");`,
			`SELECT count(*) FROM trips
			GROUP BY aql_time_bucket_day_of_year("request_at", "minute", "America/New_York");`,
			`SELECT count(*) FROM trips
			GROUP BY aql_time_bucket_month_of_year("request_at", "minute", "America/New_York");`,
			`SELECT count(*) FROM trips
			GROUP BY aql_time_bucket_quarter_of_year("request_at", "minute", "America/New_York");`,
		}

		td := queryCom.Dimension{Expr: "request_at", TimeUnit: "minute"}
		tbs := []string{"minute", "hour", "day", "week", "month", "quarter", "year",
			"time of day", "minutes of day", "hour of day", "hour of week",
			"day of week", "day of month", "day of year", "month of year", "quarter of year"}
		res := queryCom.AQLQuery{
			Table:      "trips",
			Measures:   []queryCom.Measure{{Expr: "count(*)"}},
			Dimensions: make([]queryCom.Dimension, 1),
			Timezone:   "America/New_York",
		}
		for i, sql := range sqls {
			aql, err := Parse(sql, logger)
			Ω(err).Should(BeNil())
			td.TimeBucketizer = tbs[i]
			res.Dimensions[0] = td
			res.SQLQuery = sql
			Ω(*aql).Should(Equal(res))
		}
	})

	ginkgo.It("parse time filters, time dimension and timezone should work", func() {
		sqls := []string{
			`SELECT count(*)
			FROM trips 

			WHERE aql_time_filter(request_at, "96 quarter-hours ago", "1 quarter-hours ago", America/New_York)
			GROUP BY aql_time_bucket_minute(request_at, "minute", America/New_York);`,
		}
		res := queryCom.AQLQuery{
			Table:      "trips",
			Measures:   []queryCom.Measure{{Expr: "count(*)"}},
			TimeFilter: queryCom.TimeFilter{Column: "request_at", From: "96 quarter-hours ago", To: "1 quarter-hours ago"},
			Dimensions: []queryCom.Dimension{{Expr: "request_at", TimeBucketizer: "minute", TimeUnit: "minute"}},
			Timezone:   "America/New_York",
		}
		runTest(sqls, res, logger)
	})

	ginkgo.It("parse time filters, time dimension and timezone and row filters should work", func() {
		sqls := []string{
			`SELECT  count(*)
			FROM trips 
			WHERE aql_time_filter(request_at, "96 quarter-hours ago", "1 quarter-hours ago", America/New_York) AND marketplace="agora"
			GROUP BY aql_time_bucket_minutes(request_at, "minute", America/New_York);`,
		}
		res := queryCom.AQLQuery{
			Table:      "trips",
			Measures:   []queryCom.Measure{{Expr: "count(*)"}},
			TimeFilter: queryCom.TimeFilter{Column: "request_at", From: "96 quarter-hours ago", To: "1 quarter-hours ago"},
			Dimensions: []queryCom.Dimension{{Expr: "request_at", TimeBucketizer: "minutes", TimeUnit: "minute"}},
			Filters:    []string{`marketplace="agora"`},
			Timezone:   "America/New_York",
		}
		runTest(sqls, res, logger)
	})

	ginkgo.It("parse numeric bucketizer should work", func() {
		sqls := []string{
			`SELECT  population AS pop, count(*)
			FROM trips
			WHERE aql_time_filter(request_at, "96 quarter-hours ago", "1 quarter-hours ago", America/New_York) AND marketplace="agora"
			GROUP BY aql_time_bucket_hour(request_at, "minute", America/New_York), aql_numeric_bucket_logbase(pop, 2);`,
		}
		res := queryCom.AQLQuery{
			Table:      "trips",
			Measures:   []queryCom.Measure{{Alias: "pop", Expr: "population"}, {Expr: "count(*)"}},
			Dimensions: []queryCom.Dimension{{Expr: "request_at", TimeBucketizer: "hour", TimeUnit: "minute"}, {Expr: "pop", NumericBucketizer: queryCom.NumericBucketizerDef{LogBase: 2}}},
			TimeFilter: queryCom.TimeFilter{Column: "request_at", From: "96 quarter-hours ago", To: "1 quarter-hours ago"},
			Filters:    []string{`marketplace="agora"`},
			Timezone:   "America/New_York",
		}
		runTest(sqls, res, logger)
	})

	ginkgo.It("parse join should work", func() {
		sqls := []string{
			`SELECT  population AS pop, count(*)
			FROM trips
				LEFT JOIN trips AS rush_leg
					ON trips.workflow_uuid=rush_leg.workflow_uuid AND status='completed'
  				LEFT JOIN api_cities AS cities
					ON cities.id=city_id
			WHERE aql_time_filter(request_at, "96 quarter-hours ago", "1 quarter-hours ago", America/New_York) AND marketplace="agora"
			GROUP BY aql_time_bucket_hours(request_at, "minute", America/New_York), aql_numeric_bucket_logbase(pop, 2);`,
		}
		res := queryCom.AQLQuery{
			Table: "trips",
			Joins: []queryCom.Join{
				{Table: "trips", Alias: "rush_leg", Conditions: []string{"trips.workflow_uuid=rush_leg.workflow_uuid", "status='completed'"}},
				{Table: "api_cities", Alias: "cities", Conditions: []string{"cities.id=city_id"}},
			},
			Measures:   []queryCom.Measure{{Alias: "pop", Expr: "population"}, {Expr: "count(*)"}},
			Dimensions: []queryCom.Dimension{{Expr: "request_at", TimeBucketizer: "hours", TimeUnit: "minute"}, {Expr: "pop", NumericBucketizer: queryCom.NumericBucketizerDef{LogBase: 2}}},
			TimeFilter: queryCom.TimeFilter{Column: "request_at", From: "96 quarter-hours ago", To: "1 quarter-hours ago"},
			Filters:    []string{`marketplace="agora"`},
			Timezone:   "America/New_York",
		}
		runTest(sqls, res, logger)
	})

	ginkgo.It("parse composite measures should work", func() {
		sqls := []string{
			// test SubQuery
			`SELECT Completed, Requested, Completed/Requested
			FROM
			(SELECT count(*) AS Requested
			FROM trips
				LEFT JOIN trips AS rush_leg
    				ON trips.workflow_uuid=rush_leg.workflow_uuid AND status='completed'
				LEFT JOIN api_cities AS cities
    				ON cities.id=city_id
			WHERE aql_time_filter(request_at, "96 quarter-hours ago", "1 quarter-hours ago", America/New_York) AND marketplace="agora"
			GROUP BY aql_time_bucket_day(request_at, "minute", America/New_York), aql_numeric_bucket_logbase(pop, 2)) AS m1
			NATURAL LEFT JOIN
			(SELECT count(*) AS Completed
			FROM trips
  				LEFT JOIN trips AS rush_leg
    				ON trips.workflow_uuid=rush_leg.workflow_uuid AND status='completed'
  				LEFT JOIN api_cities AS cities
    				ON cities.id=city_id
			WHERE aql_time_filter(request_at, "96 quarter-hours ago", "1 quarter-hours ago", America/New_York) AND marketplace="agora" AND status='completed'
			GROUP BY aql_time_bucket_day(request_at, "minute", America/New_York), aql_numeric_bucket_logbase(pop, 2)) AS m2;`,
			// test WithQuery
			`WITH m1 (Requested) AS (SELECT count(*) AS Requested
			FROM trips
				LEFT JOIN trips AS rush_leg
					ON trips.workflow_uuid=rush_leg.workflow_uuid AND status='completed'
  				LEFT JOIN api_cities AS cities
					ON cities.id=city_id
			WHERE aql_time_filter(request_at, "96 quarter-hours ago", "1 quarter-hours ago", America/New_York) AND marketplace="agora"
			GROUP BY aql_time_bucket_day(request_at, "minute", America/New_York), aql_numeric_bucket_logbase(pop, 2)),
			m2 (Completed) AS
			(SELECT count(*) AS Completed
			FROM trips
				LEFT JOIN trips AS rush_leg
					ON trips.workflow_uuid=rush_leg.workflow_uuid AND status='completed'
  				LEFT JOIN api_cities AS cities
					ON cities.id=city_id
			WHERE aql_time_filter(request_at, "96 quarter-hours ago", "1 quarter-hours ago", America/New_York) AND marketplace="agora" AND status='completed'
			GROUP BY aql_time_bucket_day(request_at, "minute", America/New_York), aql_numeric_bucket_logbase(pop, 2))
			SELECT Completed, Requested, Completed/Requested
			FROM m1 NATURAL LEFT JOIN m2;`,
		}
		res := queryCom.AQLQuery{
			Table: "trips",
			Joins: []queryCom.Join{
				{Table: "trips", Alias: "rush_leg", Conditions: []string{"trips.workflow_uuid=rush_leg.workflow_uuid", "status='completed'"}},
				{Table: "api_cities", Alias: "cities", Conditions: []string{"cities.id=city_id"}},
			},
			Measures: []queryCom.Measure{
				{Alias: "Completed", Expr: "count(*)", Filters: []string{"marketplace=\"agora\"", "status='completed'"}},
				{Alias: "Requested", Expr: "count(*)", Filters: []string{"marketplace=\"agora\""}},
				{Expr: "Completed/Requested"},
			},
			Dimensions:           []queryCom.Dimension{{Expr: "request_at", TimeBucketizer: "day", TimeUnit: "minute"}, {Expr: "pop", NumericBucketizer: queryCom.NumericBucketizerDef{LogBase: 2}}},
			SupportingDimensions: []queryCom.Dimension{},
			SupportingMeasures:   []queryCom.Measure{},
			TimeFilter:           queryCom.TimeFilter{Column: "request_at", From: "96 quarter-hours ago", To: "1 quarter-hours ago"},
			Timezone:             "America/New_York",
		}
		runTest(sqls, res, logger)
	})

	ginkgo.It("parse supporting measures should work", func() {
		sqls := []string{
			// test Subquery
			`SELECT Completed/Requested
			FROM
			(SELECT count(*) AS Requested
			FROM trips
				LEFT JOIN trips AS rush_leg
					ON trips.workflow_uuid=rush_leg.workflow_uuid AND status='completed'
  				LEFT JOIN api_cities AS cities
					ON cities.id=city_id
			WHERE aql_time_filter(request_at, "96 quarter-hours ago", "1 quarter-hours ago", America/New_York) AND marketplace="agora"
			GROUP BY aql_time_bucket_day(request_at, "minute", America/New_York), aql_numeric_bucket_logbase(pop, 2)) AS m1
			NATURAL LEFT JOIN
			(SELECT count(*) AS Completed
			FROM trips
				LEFT JOIN trips AS rush_leg
					ON trips.workflow_uuid=rush_leg.workflow_uuid AND status='completed'
  				LEFT JOIN api_cities AS cities
					ON cities.id=city_id
			WHERE aql_time_filter(request_at, "96 quarter-hours ago", "1 quarter-hours ago", America/New_York) AND marketplace="agora" AND status='completed'
			GROUP BY aql_time_bucket_day(request_at, "minute", America/New_York), aql_numeric_bucket_logbase(pop, 2)) AS m2;`,
			// test WithQuery
			`WITH m1 (Requested) AS (SELECT count(*) AS Requested
			FROM trips
				LEFT JOIN trips AS rush_leg
					ON trips.workflow_uuid=rush_leg.workflow_uuid AND status='completed'
  				LEFT JOIN api_cities AS cities
					ON cities.id=city_id
			WHERE aql_time_filter(request_at, "96 quarter-hours ago", "1 quarter-hours ago", America/New_York) AND marketplace="agora"
			GROUP BY aql_time_bucket_day(request_at, "minute", America/New_York), aql_numeric_bucket_logbase(pop, 2)),
			m2 (Completed) AS
			(SELECT count(*) AS Completed
			FROM trips
				LEFT JOIN trips AS rush_leg
					ON trips.workflow_uuid=rush_leg.workflow_uuid AND status='completed'
  				LEFT JOIN api_cities AS cities
					ON cities.id=city_id
			WHERE aql_time_filter(request_at, "96 quarter-hours ago", "1 quarter-hours ago", America/New_York) AND marketplace="agora" AND status='completed'
			GROUP BY aql_time_bucket_day(request_at, "minute", America/New_York), aql_numeric_bucket_logbase(pop, 2))
			SELECT Completed/Requested
			FROM m1 NATURAL LEFT JOIN m2;`,
		}
		res := queryCom.AQLQuery{
			Table: "trips",
			Joins: []queryCom.Join{
				{Table: "trips", Alias: "rush_leg", Conditions: []string{"trips.workflow_uuid=rush_leg.workflow_uuid", "status='completed'"}},
				{Table: "api_cities", Alias: "cities", Conditions: []string{"cities.id=city_id"}},
			},
			Measures:             []queryCom.Measure{{Expr: "Completed/Requested"}},
			TimeFilter:           queryCom.TimeFilter{Column: "request_at", From: "96 quarter-hours ago", To: "1 quarter-hours ago"},
			Dimensions:           []queryCom.Dimension{{Expr: "request_at", TimeBucketizer: "day", TimeUnit: "minute"}, {Expr: "pop", NumericBucketizer: queryCom.NumericBucketizerDef{LogBase: 2}}},
			SupportingDimensions: []queryCom.Dimension{},
			SupportingMeasures: []queryCom.Measure{
				{Alias: "Requested", Expr: "count(*)", Filters: []string{"marketplace=\"agora\""}},
				{Alias: "Completed", Expr: "count(*)", Filters: []string{"marketplace=\"agora\"", "status='completed'"}},
			},
			Timezone: "America/New_York",
		}
		for _, sql := range sqls {
			actual, err := Parse(sql, logger)
			Ω(err).ShouldNot(BeNil())
			Ω(err.Error()).Should(Equal("sub query not supported yet"))
			res.SQLQuery = sql
			Ω(*actual).Should(BeEquivalentTo(res))
		}
	})

	ginkgo.It("With RECURSIVE is not allowed", func() {
		sqls := []string{
			`WITH RECURSIVE t(n) AS (
				VALUES (1)
			UNION ALL
    			SELECT n+1 FROM t WHERE n < 100
			)
			SELECT sum(n) FROM t;`,
		}
		for _, sql := range sqls {
			actual, err := Parse(sql, logger)
			Ω(err).ShouldNot(BeNil())
			Ω(err.Error()).Should(Equal("RECURSIVE not yet supported at (line:1, col:0)"))
			Ω(actual).Should(BeNil())
		}
	})

	ginkgo.It("Query in namedQuery should not contain With or queryNoWith", func() {
		sqls := []string{
			`WITH m1 (Requested) AS
				(With m (Requested) AS 
					SELECT count(*) AS Requested FROM trips
				SELECT Requested FROM m)
			SELECT Requested FROM m1;`,
		}
		for _, sql := range sqls {
			actual, err := Parse(sql, logger)
			Ω(err).ShouldNot(BeNil())
			Ω(err.Error()).Should(Equal("only support 1 level with query at (line:2, col:5)"))
			Ω(actual).Should(BeNil())
		}
	})

	ginkgo.It("Only main query allows NATURAL JOIN by using With/subqueryRelation identifier", func() {
		sqls := []string{
			`WITH m1 (Requested) AS (SELECT count(*) AS Requested FROM trips),
			m2 (Completed) AS (SELECT count(*) AS Completed	FROM trips 
				NATURAL LEFT JOIN m1)
			SELECT Completed, Requested, Completed/Requested
			FROM m1 NATURAL LEFT JOIN m2;`,
		}
		for _, sql := range sqls {
			actual, err := Parse(sql, logger)
			Ω(err).ShouldNot(BeNil())
			Ω(err.Error()).Should(Equal("natural join not supported at subquery/withQuery at (line:2, col:56)"))
			Ω(actual).Should(BeNil())
		}
	})

	ginkgo.It("Both left and right in joinRelation must be either tableName or With/subqueryRelation at the same time", func() {
		sqls := []string{
			`WITH m1 (f) AS (SELECT fare AS f FROM trips),
			SELECT f, driverUuid, riderUuid
			FROM m1 NATURAL LEFT JOIN trips;`,
			`WITH m2 (f) AS (SELECT fare AS f FROM trips),
			SELECT driverUuid, riderUuid, f
			FROM trips NATURAL LEFT JOIN m2;`,
		}
		for _, sql := range sqls {
			actual, err := Parse(sql, logger)
			Ω(err).ShouldNot(BeNil())
			Ω(err.Error()).Should(Equal("missing with query body at (line:2, col:3)"))
			Ω(actual).Should(BeNil())
		}
	})

	ginkgo.It("FROM, GROUP BY and ORDER BY clause are required to be same in the With/subqueryRelation", func() {
		sqls := []string{
			`WITH m1 (Requested) AS (SELECT count(*) AS Requested
			FROM base_trips
				LEFT JOIN trips AS rush_leg
					ON trips.workflow_uuid=rush_leg.workflow_uuid AND status='completed'
  				LEFT JOIN api_cities AS cities
					ON cities.id=city_id
			WHERE aql_time_filter(request_at, "96 quarter-hours ago", "1 quarter-hours ago", America/New_York) AND marketplace="agora"
			m2 (Completed) AS
			(SELECT count(*) AS Completed
			FROM workflow_trips
				LEFT JOIN trips AS rush_leg
					ON trips.workflow_uuid=rush_leg.workflow_uuid AND status='completed'
  				LEFT JOIN api_cities AS cities
					ON cities.id=city_id
			WHERE aql_time_filter(request_at, "96 quarter-hours ago", "1 quarter-hours ago", America/New_York) AND marketplace="agora" AND status='completed'
			SELECT Completed/Requested
			FROM m1 NATURAL LEFT JOIN m2;`,

			`WITH m1 (Requested) AS (SELECT count(*) AS Requested
			FROM trips
				LEFT JOIN trips AS rush_leg
					ON trips.workflow_uuid=rush_leg.workflow_uuid AND status='completed'
  				LEFT JOIN api_cities AS cities
					ON cities.id=city_id
			WHERE aql_time_filter(request_at, "96 quarter-hours ago", "1 quarter-hours ago", America/New_York) AND marketplace="agora"
			GROUP BY aql_time_bucket_hour(request_at, "minute", America/New_York), aql_numeric_bucket_logbase(pop, 2)),
			m2 (Completed) AS
			(SELECT count(*) AS Completed
			FROM trips
				LEFT JOIN trips AS rush_leg
					ON trips.workflow_uuid=rush_leg.workflow_uuid AND status='completed'
  				LEFT JOIN api_cities AS cities
					ON cities.id=city_id
			WHERE aql_time_filter(request_at, "96 quarter-hours ago", "1 quarter-hours ago", America/New_York) AND marketplace="agora" AND status='completed'
			GROUP BY aql_time_bucket_day(request_at, "minute", America/New_York), aql_numeric_bucket_logbase(pop, 2))
			SELECT Completed/Requested
			FROM m1 NATURAL LEFT JOIN m2;`,
		}
		for _, sql := range sqls {
			actual, err := Parse(sql, logger)
			Ω(err).ShouldNot(BeNil())
			Ω(actual).Should(BeNil())
		}
	})

	ginkgo.It("The identifier of With/subqueryRelation is not allowed in expression", func() {
		sqls := []string{
			`WITH m1 (avg_fare) AS 
				(SELECT avg(fare) AS avg_fare FROM trips)
			SELECT fare FROM trips 
			WHERE fare > m1.avg_fare;`,
		}
		for _, sql := range sqls {
			actual, err := Parse(sql, logger)
			Ω(err).ShouldNot(BeNil())
			Ω(err.Error()).Should(Equal("subquery/withQuery identifier in expression not supported yet. (line:4, col:16)"))
			Ω(actual).Should(BeNil())
		}
	})

	ginkgo.It("Empty query", func() {
		sqls := []string{
			``,
		}
		for _, sql := range sqls {
			actual, err := Parse(sql, logger)
			Ω(err).ShouldNot(BeNil())
			Ω(err.Error()).Should(Equal("missing queryNoWith body at (line:1, col:0)"))
			Ω(actual).Should(BeNil())
		}
	})

	ginkgo.It("In operator should work", func() {
		sqls := []string{
			`SELECT fare FROM trips 
			WHERE city_id in (1,2,3) fare > m1.avg_fare;`,
		}
		res := queryCom.AQLQuery{
			Table: "trips",
			Dimensions: []queryCom.Dimension{
				{
					Expr: "fare",
				},
			},
			Measures: []queryCom.Measure{{Expr: "1"}},
			Filters:  []string{"city_id in (1,2,3)"},
		}

		runTest(sqls, res, logger)

	})
})
