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

package common

import (
	"github.com/uber/aresdb/query/expr"
	"github.com/uber/aresdb/utils"
	"strconv"
	"strings"
	"time"
)

var timeUnitMap = map[string]string{
	"year":         "y",
	"quarter":      "q",
	"month":        "M",
	"week":         "w",
	"day":          "d",
	"hour":         "h",
	"quarter-hour": "15m",
	"minute":       "m",
	"second":       "s",
}

// AlignedTime  is time that is calendar aligned to the unit.
type AlignedTime struct {
	Time time.Time `json:"time"`
	// Values for unit: y, q, M, w, d, {12, 8, 6, 4, 3, 2}h, h, {30, 20, 15, 12, 10, 6, 5, 4, 3, 2}m, m
	Unit string `json:"unit"`
}

// adjustMidnight adjusts daylight saving anomalies in a few timezones
// that return day boundary/midnight as either 23:00 (of the previous day) or 01:00.
//
// Fix for America/Sao_Paulo daylight saving starts (2016-10-16):
// The midnight of 2016-10-16 does not exist and time.Date returns 23:00 of the previous day.
//
// Must check whether the 1 hour rewind still gives the same day:
// For Asia/Beirut, this is not true for 2017-03-26, and true for 2017-03-27 and beyond.
func adjustMidnight(t time.Time) time.Time {
	if t.Hour() == 23 {
		// Add one hour from 23:00 to 01:00 on the transition day;
		// and from 23:00 to 00:00 on non-transition days.
		return t.Add(time.Hour)
	} else if t.Hour() == 1 {
		t2 := t.Add(-time.Hour)
		if t2.Day() == t.Day() {
			// Must check whether the 1 hour rewind still gives the same day:
			// For Asia/Beirut, this is false for 2017-03-26 (transition day), and true for 2017-03-27.
			return t2
		}
	}
	return t
}

// ParseTimezone parses timezone
func ParseTimezone(timezone string) (*time.Location, error) {
	segments := strings.Split(timezone, ":")
	hours, err := strconv.Atoi(segments[0])
	if err == nil {
		minutes := 0
		if len(segments) > 1 {
			minutes, err = strconv.Atoi(segments[1])
		}
		if err == nil {
			if hours < 0 {
				minutes = -minutes
			}
			return time.FixedZone(timezone, hours*60*60+minutes*60), nil
		}
	}
	return time.LoadLocation(timezone)
}

// GetCurrentCalendarUnit returns the start and end of the calendar unit for base.
func GetCurrentCalendarUnit(base time.Time, unit string) (start, end time.Time, err error) {
	return applyTimeOffset(base, 0, unit)
}

// Returns the start and end of the calendar `unit` that is `amount` `unit`s later from `base`.
func applyTimeOffset(base time.Time, amount int, unit string) (start, end time.Time, err error) {
	monthStart := time.Date(base.Year(), base.Month(), 1, 0, 0, 0, 0, base.Location())
	monthStart = adjustMidnight(monthStart)
	dayStart := time.Date(base.Year(), base.Month(), base.Day(), 0, 0, 0, 0, base.Location())
	dayStart = adjustMidnight(dayStart)
	switch unit {
	case "y":
		start = time.Date(base.Year()+amount, time.January, 1, 0, 0, 0, 0, base.Location())
		end = time.Date(base.Year()+1+amount, time.January, 1, 0, 0, 0, 0, base.Location())
		start = adjustMidnight(start)
		end = adjustMidnight(end)
	case "q":
		start = monthStart.AddDate(0, (1-int(base.Month()))%3+3*amount, 0)
		end = start.AddDate(0, 3, 0)
		start = adjustMidnight(start)
		end = adjustMidnight(end)
	case "M":
		start = monthStart.AddDate(0, amount, 0)
		end = start.AddDate(0, 1, 0)
		start = adjustMidnight(start)
		end = adjustMidnight(end)
	case "w":
		start = dayStart.AddDate(0, 0, (-int(base.Weekday())-6)%7+7*amount)
		end = start.AddDate(0, 0, 7)
		start = adjustMidnight(start)
		end = adjustMidnight(end)
	case "d":
		start = dayStart.AddDate(0, 0, amount)
		end = start.AddDate(0, 0, 1)
		start = adjustMidnight(start)
		end = adjustMidnight(end)
	case "h":
		// Round to hour.
		base = time.Date(base.Year(), base.Month(), base.Day(), base.Hour(), 0, 0, 0, base.Location())
		// Apply the offset.
		start = base.Add(time.Duration(amount) * time.Hour)
		end = start.Add(time.Hour)
	case "15m":
		// Round to quarter-hour.
		base = time.Date(base.Year(), base.Month(), base.Day(), base.Hour(), base.Minute()-base.Minute()%15, 0, 0, base.Location())
		// Apply the offset.
		start = base.Add(time.Duration(amount) * time.Minute * 15)
		end = start.Add(time.Minute * 15)
	case "m":
		// Round to minute.
		base = time.Date(base.Year(), base.Month(), base.Day(), base.Hour(), base.Minute(), 0, 0, base.Location())
		// Apply the offset.
		start = base.Add(time.Duration(amount) * time.Minute)
		end = start.Add(time.Minute)
	default:
		err = utils.StackError(nil, "Unknown time filter unit: %s", unit)
	}
	return
}

// Returns the start and end of the absolute calendar unit specified in `dateExpr` and `timeExpr`.
func parseAbsoluteTime(dateExpr, timeExpr string, location *time.Location) (start, end time.Time, unit string, err error) {
	var year, quarter, hour, minute int
	month, day := time.January, 1

	segments := strings.Split(dateExpr, "-")
	if len(segments) > 3 {
		err = utils.StackError(nil, "Unknown time expression: %s %s", dateExpr, timeExpr)
		return
	}

	year, err = strconv.Atoi(segments[0])
	if err != nil {
		err = utils.StackError(err, "failed to parse %s as year", segments[0])
		return
	}
	unit = "y"

	if len(segments) >= 2 {
		if segments[1][0] == 'Q' {
			quarter, err = strconv.Atoi(segments[1][1:])
			if err != nil {
				err = utils.StackError(err, "failed to parse %s as quarter", segments[1][1:])
				return
			}
			if len(segments) == 3 {
				err = utils.StackError(nil, "Unknown time expression: %s %s", dateExpr, timeExpr)
				return
			}
			month = time.January + time.Month(quarter-1)*3
			unit = "q"
		} else {
			var monthNumber int
			monthNumber, err = strconv.Atoi(segments[1])
			if err != nil {
				err = utils.StackError(err, "failed to parse %s as month", segments[1])
				return
			}
			month = time.Month(monthNumber)
			unit = "M"
		}
	}

	if len(segments) == 3 {
		day, err = strconv.Atoi(segments[2])
		if err != nil {
			err = utils.StackError(err, "failed to parse %s as day", segments[2])
			return
		}
		unit = "d"
	} else if timeExpr != "" {
		err = utils.StackError(nil, "Unknown time expression: %s %s", dateExpr, timeExpr)
		return
	}

	if timeExpr != "" {
		segments = strings.Split(timeExpr, ":")
		if len(segments) > 2 {
			err = utils.StackError(nil, "Unknown time expression: %s %s", dateExpr, timeExpr)
			return
		}

		hour, err = strconv.Atoi(segments[0])
		if err != nil {
			err = utils.StackError(err, "failed to parse %s as hour", segments[0])
			return
		}
		unit = "h"

		if len(segments) == 2 {
			minute, err = strconv.Atoi(segments[1])
			if err != nil {
				err = utils.StackError(err, "failed to parse %s as minute", segments[1])
				return
			}
			unit = "m"

			// Temporary hack until summary switch to use relative time expression.
			if minute%15 == 0 {
				unit = "15m"
			}
		}
	}

	t := time.Date(year, month, day, hour, minute, 0, 0, location)
	if hour == 0 {
		t = adjustMidnight(t)
	}
	start, end, err = applyTimeOffset(t, 0, unit)
	return
}

// Returns the start and end of the calendar unit specified in `expression`.
func parseTimeFilterExpression(expression string, now time.Time) (start, end time.Time, unit string, err error) {
	start, end = now, now
	unit = "m"
	if expression == "now" {
		unit = "s"
		return
	}

	if expression == "today" {
		expression = "this day"
	} else if expression == "yesterday" {
		expression = "last day"
	}

	var amount int
	segments := strings.Split(expression, " ")
	if segments[0] == "this" {
		if len(segments) != 2 {
			err = utils.StackError(nil, "Unknown time filter expression: %s", expression)
			return
		}
		unit = timeUnitMap[segments[1]]
		if unit == "" {
			err = utils.StackError(nil, "Unknown time filter unit: %s", segments[1])
		}
		start, end, err = applyTimeOffset(now, 0, unit)
		return
	} else if segments[0] == "last" {
		if len(segments) != 2 {
			err = utils.StackError(nil, "Unknown time filter expression: %s", expression)
			return
		}
		unit = timeUnitMap[segments[1]]
		if unit == "" {
			err = utils.StackError(nil, "Unknown time filter unit: %s", segments[1])
		}
		start, end, err = applyTimeOffset(now, -1, unit)
		return
	} else if segments[len(segments)-1] == "ago" {
		if len(segments) != 3 {
			err = utils.StackError(nil, "Unknown time filter expression: %s", expression)
			return
		}
		amount, err = strconv.Atoi(segments[0])
		if err != nil {
			err = utils.StackError(err, "failed to parse %s as a number", segments[0])
			return
		}
		unit = timeUnitMap[segments[1][:len(segments[1])-1]]
		if unit == "" {
			err = utils.StackError(nil, "Unknown time filter unit: %s", segments[1])
		}
		start, end, err = applyTimeOffset(now, -amount, unit)
		return
	} else if len(segments) == 1 {
		amount, err = strconv.Atoi(expression[:len(expression)-1])
		if err == nil {
			unit = expression[len(expression)-1:]
			start, end, err = applyTimeOffset(now, amount, unit)
			if err == nil {
				return
			}
		}
	}

	dateExpr := segments[0]
	var timeExpr string
	if len(segments) == 2 {
		timeExpr = segments[1]
	} else if len(segments) > 2 {
		err = utils.StackError(nil, "Unknown time filter expression: %s", expression)
		return
	} else if len(segments) == 1 {
		var seconds int64
		seconds, err = strconv.ParseInt(segments[0], 10, 64)
		if seconds > 99999999999 {
			//we will assume data over 99999999999 will be timestamp in ms, and convert it to be in seconds
			seconds = seconds / 1000
		}
		// Numbers above 9999999 are treated as timestamps, otherwise the corresponding Time object (of year 10000 and beyond)
		// will fail JSON marshaling, and criples debugz.
		if err == nil && seconds > 9999999 {
			t := time.Unix(seconds, 0).In(now.Location())
			rounded := t.Round(time.Minute)
			if rounded.Equal(t) {
				start = rounded
				end = rounded
				unit = "m"
			} else {
				start, end = t, t
				unit = "s"
			}
			return
		}
	}
	start, end, unit, err = parseAbsoluteTime(dateExpr, timeExpr, now.Location())
	return
}

// ParseTimeFilter parses time filter
func ParseTimeFilter(filter TimeFilter, loc *time.Location, now time.Time) (from, to *AlignedTime, err error) {
	if loc == nil {
		loc = time.UTC
	}
	now = now.In(loc).Round(time.Second)

	if filter.From != "" {
		from = &AlignedTime{}
		from.Time, _, from.Unit, err = parseTimeFilterExpression(filter.From, now)
		if err != nil {
			err = utils.StackError(err, "failed to parse time filter `from` expression: %s", filter.From)
			return
		}
	}

	if filter.To != "" {
		to = &AlignedTime{}
		_, to.Time, to.Unit, err = parseTimeFilterExpression(filter.To, now)
		if err != nil {
			err = utils.StackError(err, "failed to parse time filter `to` expression: %s", filter.To)
			return
		}
	} else if from != nil {
		// Populate to with now if from is present.
		to = &AlignedTime{now, "s"}
	}
	return
}

// CreateTimeFilterExpr creates time filter expr
func CreateTimeFilterExpr(expression expr.Expr, from, to *AlignedTime) (fromExpr, toExpr expr.Expr) {
	if from != nil && from.Unit != "" {
		fromExpr = &expr.BinaryExpr{
			ExprType: expr.Boolean,
			Op:       expr.GTE,
			LHS:      expression,
			RHS: &expr.NumberLiteral{
				Int:      int(from.Time.Unix()),
				Expr:     strconv.FormatInt(from.Time.Unix(), 10),
				ExprType: expr.Unsigned,
			},
		}
	}
	if to != nil && to.Unit != "" {
		toExpr = &expr.BinaryExpr{
			ExprType: expr.Boolean,
			Op:       expr.LT,
			LHS:      expression,
			RHS: &expr.NumberLiteral{
				Int:      int(to.Time.Unix()),
				Expr:     strconv.FormatInt(to.Time.Unix(), 10),
				ExprType: expr.Unsigned,
			},
		}
	}
	return
}
