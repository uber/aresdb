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
	"fmt"
	"github.com/uber/aresdb/utils"
	"strconv"
	"strings"
	"time"
)

const (
	parseErrorString = "failed to parse time bucketizer: %s"
	// SecondsPerMinute is number of seconds per minute
	SecondsPerMinute = 60
	// SecondsPerHour is number of seconds per hour
	SecondsPerHour = SecondsPerMinute * 60
	// SecondsPerDay is number of secods per day
	SecondsPerDay = SecondsPerHour * 24
	// SecondsPer4Day is number of seconds per 4 days
	SecondsPer4Day = SecondsPerDay * 4
	// DaysPerWeek is number of days per week
	DaysPerWeek = 7
	// WeekdayOffset is to compensate 1970-01-01 being a Thursday
	WeekdayOffset = 4
	// SecondsPerWeek is number of seconds per week
	SecondsPerWeek = SecondsPerDay * DaysPerWeek
)

// BucketSizeToseconds is the map from normalized bucket unit to number of seconds
var BucketSizeToseconds = map[string]int{
	"m": SecondsPerMinute,
	"h": SecondsPerHour,
	"d": SecondsPerDay,
}

// TimeDimensionMeta is the aggregation of meta data needed to format time dimensions
type TimeDimensionMeta struct {
	TimeBucketizer  string
	TimeUnit        string
	IsTimezoneTable bool
	TimeZone        *time.Location
	DSTSwitchTs     int64
	FromOffset      int
	ToOffset        int
}

// TimeSeriesBucketizer is the helper struct to express parsed time bucketizer, see comment below
type TimeSeriesBucketizer struct {
	Size int
	Unit string
}

// used to convert supported time units (string) to single char format
var bucketSizeToNormalized = map[string]string{
	"minutes": "m",
	"minute":  "m",
	"day":     "d",
	"hours":   "h",
	"hour":    "h",
}

// ParseRegularTimeBucketizer tries to convert a regular time bucketizer(anything below month) input string to a (Size,
// Unit) pair, reports error if input is invalid/unsupported.
// e.g. "3m" -> (3, "m")  "4 hours" -> (4, "h")
func ParseRegularTimeBucketizer(timeBucketizerString string) (TimeSeriesBucketizer, error) {
	// hack to support quarter-hour
	if timeBucketizerString == "quarter-hour" {
		timeBucketizerString = "15m"
	}

	result := TimeSeriesBucketizer{}

	timeBucketizerString = strings.ToLower(timeBucketizerString)
	segments := strings.SplitN(timeBucketizerString, " ", 2)
	if len(segments) == 2 { // "N minutes" "N hours"
		if unit, ok := bucketSizeToNormalized[segments[1]]; ok {
			result.Unit = unit
			size, err := parseSize(segments[0], unit)
			if err != nil {
				return result, utils.StackError(err, fmt.Sprintf(parseErrorString, timeBucketizerString))
			}
			result.Size = size
		} else {
			return result, utils.StackError(nil, fmt.Sprintf(parseErrorString, timeBucketizerString))
		}
	} else {
		if normalized, ok := bucketSizeToNormalized[timeBucketizerString]; ok { // "day", "minute", "hour"
			timeBucketizerString = normalized
		}

		// "3m", "2h"
		unit := timeBucketizerString[len(timeBucketizerString)-1:]
		if _, ok := BucketSizeToseconds[unit]; !ok {
			return result, utils.StackError(nil, fmt.Sprintf(parseErrorString, timeBucketizerString))
		}
		result.Unit = unit

		if len(timeBucketizerString) > 1 {
			size, err := parseSize(timeBucketizerString[:len(timeBucketizerString)-1], unit)
			if err != nil {
				return result, utils.StackError(err, fmt.Sprintf(parseErrorString, timeBucketizerString))
			}
			result.Size = size
		} else {
			result.Size = 1
		}
	}
	return result, nil
}

// parseSize parses input string into integer time bucketizer Size, and validates it with given Unit
func parseSize(s, unit string) (int, error) {
	if unit == "m" || unit == "h" {
		size, err := strconv.Atoi(s)
		if err != nil {
			return 0, utils.StackError(err, fmt.Sprintf(parseErrorString, s))
		}
		if size > 0 && size < 60 && ((unit == "m" && (60%size) == 0) || (unit == "h" && (24%size) == 0)) {
			return size, nil
		}
	}
	return 0, utils.StackError(nil, fmt.Sprintf(parseErrorString, s), fmt.Sprintf("invalid bucket Size for %s", unit))
}
