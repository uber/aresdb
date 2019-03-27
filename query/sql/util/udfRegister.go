package util

const (
	// NumericBucketTypeBucketWidth is a NumericBucketType
	NumericBucketTypeBucketWidth = "bucketWidth"
	// NumericBucketTypeLogBase is a NumericBucketType
	NumericBucketTypeLogBase = "logBase"
	// NumericBucketTypeManualPartitions is a NumericBucketType
	NumericBucketTypeManualPartitions = "manualPartitions"
)

type funcType int

const (
	// Timefilter is function type
	Timefilter funcType = iota
	// TimeNow is function type
	TimeNow
	// Timebucket is function type
	Timebucket
	// Numericbucket is function type
	Numericbucket
)

type funcDef struct {
	// Type is function type
	Type funcType
	// ArgsNum is #args
	ArgsNum int
	// Definition is the function's AQL expression
	Definition string
	// ArgsName is the args name
	ArgsName []string
}

// UdfTable is function registration table. key: function name; value: function definition.
var UdfTable = map[string]funcDef{
	// Timefilter function table. "functionName":#args
	"aql_time_filter": {Timefilter, 4, "timeFilter", []string{"column", "from", "to", "timezone"}},

	// now => AQL query field Now which will override to=now field
	"aql_now": {TimeNow, 2, "now", []string{"column", "now"}},

	// Timebucketizer function table. "functionName":#args
	// Supported timeUnit: "millisecond", "second", "minute", "hour"
	// ISO-8601 date formats is returned if timeunit is set as ''
	// AQL time_bucketizer detailed information can be found at https://code.uberinternal.com/w/projects/database/ares/aql/time_bucketizer/
	"aql_time_bucket_minute":          {Timebucket, 3, "minute", []string{"column", "timeunit", "timezone"}},
	"aql_time_bucket_minutes":         {Timebucket, 3, "minutes", []string{"column", "timeunit", "timezone"}},
	"aql_time_bucket_hour":            {Timebucket, 3, "hour", []string{"column", "timeunit", "timezone"}},
	"aql_time_bucket_hours":           {Timebucket, 3, "hours", []string{"column", "timeunit", "timezone"}},
	"aql_time_bucket_day":             {Timebucket, 3, "day", []string{"column", "timeunit", "timezone"}},
	"aql_time_bucket_week":            {Timebucket, 3, "week", []string{"column", "timeunit", "timezone"}},
	"aql_time_bucket_month":           {Timebucket, 3, "month", []string{"column", "timeunit", "timezone"}},
	"aql_time_bucket_quarter":         {Timebucket, 3, "quarter", []string{"column", "timeunit", "timezone"}},
	"aql_time_bucket_year":            {Timebucket, 3, "year", []string{"column", "timeunit", "timezone"}},
	"aql_time_bucket_time_of_day":     {Timebucket, 3, "time of day", []string{"column", "timeunit", "timezone"}},
	"aql_time_bucket_minutes_of_day":  {Timebucket, 3, "minutes of day", []string{"column", "timeunit", "timezone"}},
	"aql_time_bucket_hour_of_day":     {Timebucket, 3, "hour of day", []string{"column", "timeunit", "timezone"}},
	"aql_time_bucket_hour_of_week":    {Timebucket, 3, "hour of week", []string{"column", "timeunit", "timezone"}},
	"aql_time_bucket_day_of_week":     {Timebucket, 3, "day of week", []string{"column", "timeunit", "timezone"}},
	"aql_time_bucket_day_of_month":    {Timebucket, 3, "day of month", []string{"column", "timeunit", "timezone"}},
	"aql_time_bucket_day_of_year":     {Timebucket, 3, "day of year", []string{"column", "timeunit", "timezone"}},
	"aql_time_bucket_month_of_year":   {Timebucket, 3, "month of year", []string{"column", "timeunit", "timezone"}},
	"aql_time_bucket_quarter_of_year": {Timebucket, 3, "quarter of year", []string{"column", "timeunit", "timezone"}},

	// Numericbucketizer function table. "functionName":#args
	"aql_numeric_bucket_bucket_width":       {Numericbucket, 2, "bucketWidth", []string{"column", "expression"}},
	"aql_numeric_bucket_logbase":            {Numericbucket, 2, "logBase", []string{"column", "expression"}},
	"aql_numeric_bucket_mannual_partitions": {Numericbucket, 2, "mannualPartitions", []string{"column", "expression"}},
}

// AggregateFunctions is a set of call names that are aggregate functions
var AggregateFunctions = map[string]bool{
	"count": true,
	"sum":   true,
	"avg":   true,
	"max":   true,
	"min":   true,
	"hll":   true,
}
