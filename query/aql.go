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
	queryCom "github.com/uber/aresdb/query/common"
	"github.com/uber/aresdb/query/expr"
)

// Ares only supports a subset of AQL; missing features are provided by Apollo.
// See https://github.com/uber/aresdb/wiki/aql#notice

// Dimension specifies a row level dimension for grouping by.
type Dimension struct {
	// The SQL expression for computing the dimension.
	// Expr can be empty when TimeBucketizer is specified, which implies the
	// designated time column from the main table is used as the expresssion.
	Expr string `json:"sqlExpression"`
	expr expr.Expr

	// Decides how to bucketize a timestamp Dimension before grouping by.
	// See https://github.com/uber/aresdb/wiki/aql#time_bucketizer
	TimeBucketizer string `json:"timeBucketizer,omitempty"`

	TimeUnit string `json:"timeUnit,omitempty"`

	// Bucketizes numeric dimensions for integers and floating point numbers.
	NumericBucketizer NumericBucketizerDef `json:"numericBucketizer,omitempty"`
}

// NumericBucketizerDef defines how numbers should be bucketized before being
// grouped by as a dimension. The returned dimension is a string in the format
// of `lower_bound`, representing `[lower_bound, uper_bound)`.
type NumericBucketizerDef struct {
	// Only one of the following field should be specified.

	// Generates equal-width buckets. BucketWidth should be positive.
	// The generated buckets are:
	// ... [-2w, -w), [-w, 0), [0, w), [w, 2w) ...
	BucketWidth float64 `json:"bucketWidth,omitempty"`

	// Generates exponential/log buckets. LogBase should be positive.
	// The generated buckets are:
	// ... [pow(b, -2), pow(b, -1)), [pow(b, -1), 1), [1, pow(b, 1)), [pow(b, 1), pow(b, 2)) ...
	LogBase float64 `json:"logBase,omitempty"`

	// Generates a fixed number of buckets using the specified partitions.
	// The numbers should be in sorted order. The generated buckets are:
	// [-inf, p0), [p0, p1), [p1, p2), ... [pn-1, inf)
	ManualPartitions []float64 `json:"manualPartitions,omitempty"`
}

// Measure specifies a group level aggregation measure.
type Measure struct {
	// The SQL expression for computing the measure.
	Expr string `json:"sqlExpression"`
	expr expr.Expr

	// Row level filters to apply for this measure.
	// The filters are ANDed togther.
	Filters []string `json:"rowFilters,omitempty"`
	filters []expr.Expr
}

// Join specifies a secondary table to be explicitly joined in the query.
type Join struct {
	// Name of the table to join against.
	Table string `json:"table"`

	// Alias for the table. Empty means the table name will be used as alias.
	Alias string `json:"alias"`

	// Condition expressions to be ANDed together for the join.
	Conditions []string `json:"conditions"`
	conditions []expr.Expr
}

// TimeFilter is a syntax sugar for specifying time range.
type TimeFilter struct {
	// A table time column in the format of column, or table_alias.column.
	// When empty, it defaults to the designated time column of the main table.
	Column string `json:"column"`

	// The time specified in from and to are both inclusive.
	// See https://github.com/uber/aresdb/wiki/aql#time_filter
	From string `json:"from"`
	To   string `json:"to"`
}

// AQLQuery specifies the query on top of tables.
type AQLQuery struct {
	// Name of the main table.
	Table string `json:"table"`

	// Foreign tables to be joined.
	Joins []Join `json:"joins,omitempty"`

	// Dimensions to group by on.
	Dimensions []Dimension `json:"dimensions,omitempty"`

	// Measures/metrics to report.
	Measures []Measure `json:"measures"`

	// Row level filters to apply for all measures. The filters are ANDed togther.
	Filters []string `json:"rowFilters,omitempty"`
	filters []expr.Expr

	// Syntax sugar for specifying a time based range filter.
	TimeFilter TimeFilter `json:"timeFilter,omitempty"`

	// Timezone to use when converting timestamp to calendar time, specified as:
	//   - -8:00
	//   - GMT
	//   - America/Los_Angeles
	//   - timezone(city_id)
	//   - region_timezone(city_id)
	//   - mega_region_timezone(city_id)
	//   - sub_region_timezone(city_id)
	//   - country_timezone(city_id)
	Timezone string `json:"timezone,omitempty"`

	// This overrides "now" (in seconds)
	Now int64 `json:"now,omitempty"`
}

// AQLRequest contains multiple of AQLQueries.
type AQLRequest struct {
	Queries []AQLQuery `json:"queries"`
}

// AQLResponse contains results for multiple AQLQueries.
type AQLResponse struct {
	Results      []queryCom.AQLTimeSeriesResult `json:"results"`
	Errors       []error                        `json:"errors,omitempty"`
	QueryContext []*AQLQueryContext             `json:"context,omitempty"`
}

func (d Dimension) isTimeDimension() bool {
	return d.TimeBucketizer != ""
}
