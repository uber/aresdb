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
	memCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/utils"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"
	"unsafe"
)

// DimCountsPerDimWidth defines dimension counts per dimension width
// 16-byte 8-byte 4-byte 2-byte 1-byte
type DimCountsPerDimWidth [5]uint8

// ReadDimension reads a dimension value given the index and corresponding data type of node.
// tzRemedy is used to remedy the timezone offset
func ReadDimension(valueStart, nullStart unsafe.Pointer, index int, dataType memCom.DataType, enumReverseDict []string, meta TimeDimensionMeta, cache map[TimeDimensionMeta]map[int64]string) *string {
	isTimeDimension := meta.TimeBucketizer != ""
	// check for nulls
	if *(*uint8)(memAccess(nullStart, index)) == 0 {
		return nil
	}

	// determine value width in bytes
	valueBytes := memCom.DataTypeBytes(dataType)
	valuePtr := memAccess(valueStart, valueBytes*index)

	// read intValue; handle float and signed types
	var intValue int64
	var result string
	switch dataType {
	case memCom.Float32:
		// in case time dimension value was converted to float for division
		if isTimeDimension && meta.TimeUnit == "" {
			result = formatTimeDimension(int64(*(*float32)(valuePtr)), meta, cache)
			return &result
		}
		result = strconv.FormatFloat(float64(*(*float32)(valuePtr)), 'g', -1, 32)
		return &result
	case memCom.Int64, memCom.Int32, memCom.Int16, memCom.Int8, memCom.Bool:
		switch valueBytes {
		case 8:
			intValue = int64(*(*int64)(valuePtr))
		case 4:
			intValue = int64(*(*int32)(valuePtr))
		case 2:
			intValue = int64(*(*int16)(valuePtr))
		case 1:
			intValue = int64(*(*int8)(valuePtr))
		}
		result = strconv.FormatInt(intValue, 10)
		return &result
	case memCom.Uint32, memCom.Uint16, memCom.BigEnum, memCom.Uint8, memCom.SmallEnum:
		switch valueBytes {
		case 4:
			intValue = int64(*(*uint32)(valuePtr))
			if isTimeDimension && meta.TimeUnit != "" {
				intValue = utils.AdjustOffset(meta.FromOffset, meta.ToOffset, meta.DSTSwitchTs, intValue)
			}
		case 2:
			intValue = int64(*(*uint16)(valuePtr))
		case 1:
			intValue = int64(*(*uint8)(valuePtr))
		}
	case memCom.UUID:
		bys := *(*[16]byte)(valuePtr)
		uuidStr := hex.EncodeToString(bys[:])
		if len(uuidStr) == 32 {
			result = fmt.Sprintf("%s-%s-%s-%s-%s",
				uuidStr[:8],
				uuidStr[8:12],
				uuidStr[12:16],
				uuidStr[16:20],
				uuidStr[20:])
			return &result
		}
		return nil
	default:
		// Should never happen.
		return nil
	}

	// translate enum case back to string for unsigned types
	if intValue >= 0 && intValue < int64(len(enumReverseDict)) {
		result = enumReverseDict[int(intValue)]
	} else if isTimeDimension && meta.TimeUnit == "" {
		result = formatTimeDimension(intValue, meta, cache)
	} else {
		result = strconv.FormatInt(intValue, 10)
	}

	return &result
}

// GetDimensionStartOffsets calculates the value and null starting position for given dimension inside dimension vector
// dimIndex is the ordered index of given dimension inside the dimension vector
func GetDimensionStartOffsets(numDimsPerDimWidth DimCountsPerDimWidth, dimIndex int, length int) (valueOffset, nullOffset int) {
	startDim := 0
	dimBytes := 1 << uint(len(numDimsPerDimWidth)-1)
	for _, numDim := range numDimsPerDimWidth {
		// found which range this dimension vector belongs to
		if startDim+int(numDim) > dimIndex {
			valueOffset += (dimIndex - startDim) * length * dimBytes
			break
		}
		startDim += int(numDim)
		valueOffset += int(numDim) * length * dimBytes
		// dimBytes /= 2
		dimBytes >>= 1
	}

	valueBytes := 0
	for index, numDim := range numDimsPerDimWidth {
		valueBytes += (1 << uint(len(numDimsPerDimWidth)-index-1)) * int(numDim)
	}

	nullOffset = (valueBytes + dimIndex) * length
	return valueOffset, nullOffset
}

func formatTimeDimension(val int64, meta TimeDimensionMeta, cache map[TimeDimensionMeta]map[int64]string) (result string) {
	// skip timezone table dims TODO(shz): support timezone table dims
	if !meta.IsTimezoneTable {
		if cacheMap, ok := cache[meta]; ok {
			if result, exists := cacheMap[val]; exists {
				return result
			}
		} else if cache != nil {
			cache[meta] = make(map[int64]string)
		}
	}

	switch meta.TimeBucketizer {
	case "time of day":
		t := time.Unix(val, 0)
		return t.UTC().Format("15:04")
	case "hour of day":
		t := time.Unix(val-val%3600, 0)
		return t.UTC().Format("15:04")
	case "hour of week":
		t := time.Unix(val+SecondsPer4Day, 0)
		return t.UTC().Format("Monday 15:04")
	case "day of week":
		// 1970-01-01 was a Thursday
		t := time.Unix(((val+4)%7)*SecondsPerDay, 0)
		return t.UTC().Format("Monday")
	default:
		bucket, err := ParseRegularTimeBucketizer(meta.TimeBucketizer)
		if err != nil {
			return strconv.FormatInt(val, 10)
		}
		switch bucket.Unit {
		case "m":
			t := time.Unix(val, 0)
			return t.UTC().Format("2006-01-02 15:04")
		case "h":
			t := time.Unix(val-val%3600, 0)
			return t.UTC().Format("2006-01-02 15:00")
		case "d":
			t := time.Unix(val-val%(24*60*60), 0)
			return t.UTC().Format("2006-01-02")
		}

	}

	if !meta.IsTimezoneTable {
		cache[meta][val] = result
	}
	return
}
