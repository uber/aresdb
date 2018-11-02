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
	"code.uber.internal/data/ares/query/common"
	"code.uber.internal/data/ares/query/expr"
	"code.uber.internal/data/ares/utils"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// used to convert supported time units (string) to single char format
var bucketSizeToNormalized = map[string]string{
	"minutes": "m",
	"minute":  "m",
	"day":     "d",
	"hours":   "h",
	"hour":    "h",
}

// mapping from bucketizer to the functor to get the start of the bucketizer.
var irregularBucketizer2Functor = map[string]expr.Token{
	"month":   expr.GET_MONTH_START,
	"quarter": expr.GET_QUARTER_START,
	"year":    expr.GET_YEAR_START,
	"week":    expr.GET_WEEK_START,
}

// regularRecurringTimeBucketizer is in the format of "x of y" where y is a regular time interval of which number of
// seconds is fixed, e.g (week, day, hour). Y is called the bucket Size and x is called base Unit.
type regularRecurringTimeBucketizer struct {
	baseUnit   int
	bucketSize int
}

// mapping from regular recurring time bucketizer str to base Unit and bucket Size.
var tbStr2regularRecurringTimeBucketizer = map[string]regularRecurringTimeBucketizer{
	"time of day":  {baseUnit: 1, bucketSize: common.SecondsPerDay},
	"hour of day":  {baseUnit: common.SecondsPerHour, bucketSize: common.SecondsPerDay},
	"hour of week": {baseUnit: common.SecondsPerHour, bucketSize: common.SecondsPerWeek},
	"day of week":  {baseUnit: common.SecondsPerDay, bucketSize: common.SecondsPerWeek},
}

//
var irregularRecurringBucketizer2Functor = map[string]expr.Token{
	"day of month":    expr.GET_DAY_OF_MONTH,
	"day of year":     expr.GET_DAY_OF_YEAR,
	"month of year":   expr.GET_MONTH_OF_YEAR,
	"quarter of year": expr.GET_QUARTER_OF_YEAR,
}

// buildTimeDimensionExpr constructs sub ast based on several query params:
// the time bucketizer string and the timezone string.
// we parse time bucketizer into bucketInSeconds, for timezone string:
// if fixed (non-UTC) timezone is passed in, we extend the ast to `(timeColumn CONVERT_TZ fixed_timezone_offset) FLOOR bucketInSeconds`
// if timezoneColumn exists, we extend the ast to `(timeColumn CONVERT_TZ timezoneColumn) FLOOR bucketInSeconds`
func (qc *AQLQueryContext) buildTimeDimensionExpr(timeBucketizerString string, timeColumn expr.Expr) (expr.Expr, error) {
	var bucketizerExpr expr.Expr
	var err error
	timeColumnWithOffsetExpr := timeColumn

	// construct TimeSeriesBucketizer expr
	if qc.timezoneTable.tableColumn != "" {
		var timezoneTableID, timezoneColumnID int
		timezoneColumn := fmt.Sprintf("%s.%s", qc.timezoneTable.tableAlias, qc.timezoneTable.tableColumn)
		timezoneTableID = qc.TableIDByAlias[qc.timezoneTable.tableAlias]
		timezoneColumnID = qc.TableScanners[timezoneTableID].Schema.ColumnIDs[qc.timezoneTable.tableColumn]
		// expand ast by offsetting timezone column
		timeColumnWithOffsetExpr = &expr.BinaryExpr{
			Op:  expr.CONVERT_TZ,
			LHS: timeColumn,
			RHS: &expr.VarRef{
				Val:      timezoneColumn,
				TableID:  timezoneTableID,
				ColumnID: timezoneColumnID,
			},
		}
	} else if qc.fixedTimezone.String() != time.UTC.String() {
		_, fromOffset := qc.fromTime.Time.Zone()
		_, toOffset := qc.toTime.Time.Zone()
		if fromOffset != toOffset {
			offsetDiff := fromOffset - toOffset
			switchTs, err := utils.CalculateDSTSwitchTs(qc.fromTime.Time.Unix(), qc.toTime.Time.Unix(), qc.fixedTimezone)
			if err != nil {
				return nil, err
			}
			qc.dstswitch = switchTs
			// simulate IF statement. sub ast: timeCol + fromOffset + (timeCol > switchTs) * offsetDiff
			// where (timeCol > switchTs) will return 1 or 0
			timeColumnWithOffsetExpr = &expr.BinaryExpr{
				Op:  expr.ADD,
				LHS: timeColumn,
				RHS: &expr.BinaryExpr{
					Op: expr.ADD,
					LHS: &expr.NumberLiteral{
						Expr:     strconv.Itoa(fromOffset),
						Int:      fromOffset,
						ExprType: expr.Signed,
					},
					RHS: &expr.BinaryExpr{
						Op: expr.MUL,
						LHS: &expr.NumberLiteral{
							Expr:     strconv.Itoa(offsetDiff),
							Int:      offsetDiff,
							ExprType: expr.Signed,
						},
						RHS: &expr.BinaryExpr{
							Op:  expr.GTE,
							LHS: timeColumn,
							RHS: &expr.NumberLiteral{
								Expr: strconv.Itoa(int(switchTs)),
								Int:  int(switchTs),
							},
							ExprType: expr.Boolean,
						},
						ExprType: expr.Signed,
					},
				},
			}
		} else {
			timeColumnWithOffsetExpr = &expr.BinaryExpr{
				Op:  expr.CONVERT_TZ,
				LHS: timeColumn,
				RHS: &expr.NumberLiteral{
					Expr: strconv.Itoa(fromOffset),
					Int:  fromOffset,
				},
			}
		}

	}

	bucketizerExpr, err = parseRecurringTimeBucketizer(timeBucketizerString, timeColumnWithOffsetExpr)
	if err != nil || bucketizerExpr != nil {
		return bucketizerExpr, err
	}

	if bucketizerExpr = parseIrregularTimeBucketizer(timeBucketizerString, timeColumnWithOffsetExpr); bucketizerExpr != nil {
		return bucketizerExpr, nil
	}

	timeBucket, err := common.ParseRegularTimeBucketizer(timeBucketizerString)
	if err != nil {
		return nil, err
	}
	bucketInSeconds := timeBucket.Size * common.BucketSizeToseconds[timeBucket.Unit]

	bucketizerExpr = &expr.BinaryExpr{
		Op:  expr.FLOOR,
		LHS: timeColumnWithOffsetExpr,
		RHS: &expr.NumberLiteral{
			Expr:     strconv.Itoa(bucketInSeconds),
			Int:      bucketInSeconds,
			ExprType: expr.Unsigned,
		},
	}

	return bucketizerExpr, nil
}

// getRegularRecurringTimeBucketizer converts a time bucketizer string to a regularRecurringTimeBucketizer struct.
// Nil means it does not match.
func getRegularRecurringTimeBucketizer(tbStr string) (*regularRecurringTimeBucketizer, error) {
	if strings.HasSuffix(tbStr, "minutes of day") {
		comps := strings.Fields(tbStr)
		if len(comps) < 4 {
			return nil, utils.StackError(nil, "Must put number before minutes of day: got %s", tbStr)
		}
		n, err := strconv.Atoi(comps[0])
		if err != nil {
			return nil, utils.StackError(err, "Cannot parse the number before minutes of day: got %s", tbStr)
		}

		if n < 2 || n > 30 || 30%n != 0 {
			return nil, utils.StackError(err, "Only {2,3,4,5,6,10,15,20,30} minutes of day are allowed :"+
				" got %s", tbStr)
		}
		return &regularRecurringTimeBucketizer{baseUnit: 60 * n, bucketSize: common.SecondsPerDay}, nil
	}

	if tb, ok := tbStr2regularRecurringTimeBucketizer[tbStr]; ok {
		return &tb, nil
	}
	return nil, nil
}

// parseRecurringTimeBucketizer parses the time bucketizer string into a composite expression tree if it's a recurring
// time bucketizer. The tree will be like floor((timeColumn % bucketSize), unitSize) if it's a regular recurring
// time bucketizer, e.g.(each individual time duration contains the same amount of seconds). Otherwise it will
// return a AST of a special function call. E.g. getDayOfMonth.
func parseRecurringTimeBucketizer(timeBucketizerString string, timeColumnExpr expr.Expr) (expr.Expr, error) {
	tb, err := getRegularRecurringTimeBucketizer(timeBucketizerString)
	if err != nil {
		return nil, err
	}

	if tb != nil {
		var e expr.Expr
		if tb.baseUnit > 1 {
			adjustedTimeExpr := timeColumnExpr
			// if bucket size is equal to week, we need to adjust it to Monday by subtracting number of seconds per
			// 4 days (since 1970-01-01 is a Thursday).
			if tb.bucketSize == common.SecondsPerWeek {
				adjustedTimeExpr = &expr.BinaryExpr{
					Op:  expr.SUB,
					LHS: adjustedTimeExpr,
					RHS: &expr.NumberLiteral{
						Expr:     strconv.Itoa(common.SecondsPer4Day),
						Int:      common.SecondsPer4Day,
						ExprType: expr.Unsigned,
					},
				}
			}

			e = &expr.BinaryExpr{
				Op: expr.FLOOR,
				LHS: &expr.BinaryExpr{
					Op:  expr.MOD,
					LHS: adjustedTimeExpr,
					RHS: &expr.NumberLiteral{
						Expr:     strconv.Itoa(tb.bucketSize),
						Int:      tb.bucketSize,
						ExprType: expr.Unsigned,
					},
				},
				RHS: &expr.NumberLiteral{
					Expr:     strconv.Itoa(tb.baseUnit),
					Int:      tb.baseUnit,
					ExprType: expr.Unsigned,
				},
			}
		} else {
			e = &expr.BinaryExpr{
				Op:  expr.MOD,
				LHS: timeColumnExpr,
				RHS: &expr.NumberLiteral{
					Expr:     strconv.Itoa(tb.bucketSize),
					Int:      tb.bucketSize,
					ExprType: expr.Unsigned,
				},
			}
		}

		// if base unit >= day, we need to divide it by the base unit.
		if tb.baseUnit >= common.SecondsPerDay {
			// For division, everything is converted to float.
			val := float64(tb.baseUnit)
			e = &expr.BinaryExpr{
				Op:  expr.DIV,
				LHS: e,
				RHS: &expr.NumberLiteral{
					Expr:     strconv.FormatFloat(val, 'f', 2, 64),
					Val:      val,
					ExprType: expr.Float,
				},
			}
		}
		return e, nil
	}

	if functorToken, ok := irregularRecurringBucketizer2Functor[timeBucketizerString]; ok {
		return &expr.UnaryExpr{
			Op:       functorToken,
			Expr:     timeColumnExpr,
			ExprType: expr.Unsigned,
		}, nil
	}

	return nil, nil
}

// parseIrregularTimeBucketizer parses the time bucketizer into a UnaryExpr with the corresponding functor as the OP
// node and original time column expression as the call argument. Return nil if it is not a irregular time series
// bucketizer.
func parseIrregularTimeBucketizer(timeBucketizerString string, timeColumn expr.Expr) expr.Expr {
	if functorToken, ok := irregularBucketizer2Functor[timeBucketizerString]; ok {
		return &expr.UnaryExpr{
			Op:       functorToken,
			Expr:     timeColumn,
			ExprType: expr.Unsigned,
		}
	}
	return nil
}
