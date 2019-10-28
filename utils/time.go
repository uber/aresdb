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

package utils

import (
	"sync/atomic"
	"time"
)

const (
	secondsInHour = 3600
)

// NowFunc type for function of getting current time
type NowFunc func() time.Time

// TimeIncrementer increment current time by configurable incremental
type TimeIncrementer struct {
	IncBySecond int64
	currentSec  int64
}

var nowFunc NowFunc

func init() {
	ResetClockImplementation()
}

// ResetClockImplementation resets implementation to use time.Now
func ResetClockImplementation() {
	nowFunc = time.Now
}

// SetClockImplementation sets implementation to use passed in nowFunc
func SetClockImplementation(f NowFunc) {
	nowFunc = f
}

// SetCurrentTime sets the clock implementation to the specified time,
func SetCurrentTime(t time.Time) {
	nowFunc = func() time.Time {
		return t
	}
}

// Now returns current time using nowFunc
func Now() time.Time {
	return nowFunc()
}

// FormatTimeStampToUTC formats a epoch timestamp to a time string in UTC time zone.
func FormatTimeStampToUTC(ts int64) string {
	return time.Unix(ts, 0).UTC().Format(time.RFC3339)
}

// TimeStampToUTC converts a timestamp to a Time struct in UTC time zone.
func TimeStampToUTC(ts int64) time.Time {
	return time.Unix(ts, 0).UTC()
}

// Now increment current time by one second at a time
func (r *TimeIncrementer) Now() time.Time {
	return time.Unix(atomic.AddInt64(&r.currentSec, r.IncBySecond), 0)
}

// CrossDST tells whether a time range crosses a DST switch time for given zone
func CrossDST(fromTs, toTs int64, loc *time.Location) bool {
	if fromTs >= toTs {
		return false
	}
	_, fromDiff := time.Unix(fromTs, 0).In(loc).Zone()
	_, toDiff := time.Unix(toTs, 0).In(loc).Zone()
	return fromDiff != toDiff

}

// CalculateDSTSwitchTs calculates DST switch timestamp given a time range and a timezone
// it returns 0 if given range doesn't contain a switch for that zone
// it assumes the range won't contain more than 1 switch timestamp, otherwise it will
// return one of them (which one to return is not determined)
func CalculateDSTSwitchTs(fromTs, toTs int64, loc *time.Location) (switchTs int64, err error) {
	cross := CrossDST(fromTs, toTs, loc)
	if !cross {
		return
	}
	for toTs-fromTs > secondsInHour {
		mid := fromTs + (toTs-fromTs)/2
		if CrossDST(fromTs, mid, loc) {
			toTs = mid
		} else {
			fromTs = mid
		}
	}
	return toTs - toTs%secondsInHour, nil
}

// AdjustOffset adjusts timestamp value with time range start or end ts basing on DST switch ts
func AdjustOffset(fromOffset, toOffset int, switchTs, ts int64) int64 {
	offset := fromOffset
	if switchTs > 0 && ts >= int64(int(switchTs)+toOffset) {
		offset = toOffset
	}
	return ts - int64(offset)
}
