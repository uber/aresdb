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

package sink

import (
	"fmt"
	"github.com/uber/aresdb/client"
	memCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/subscriber/common/rules"
	"github.com/uber/aresdb/utils"
	"math"
	"reflect"
	"strings"
	"unsafe"
)

// Sink is abstraction for interactions with downstream storage layer
type Sink interface {
	// Cluster returns the DB cluster name
	Cluster() string

	// Save will save the rows into underlying database
	Save(destination Destination, rows []client.Row) error

	// Shutdown will close the connections to the database
	Shutdown()
}

// Destination contains the table and columns that each job is storing data into
// also records the behavior when encountering key errors
type Destination struct {
	// Table is table name
	Table string
	// ColumnNames are the list of column names after sorted
	ColumnNames []string
	// PrimaryKeys maps primary key columnName to its columnID after sorted
	PrimaryKeys map[string]int
	// PrimaryKeysInSchema maps primary key columnName to its columnID defined in schema
	PrimaryKeysInSchema map[string]int
	// AresUpdateModes defines update modes
	AresUpdateModes []memCom.ColumnUpdateMode
	// NumShards is the number of shards in the aresDB cluster
	NumShards uint32
}

func Shard(rows []client.Row, destination Destination, jobConfig *rules.JobConfig) (map[uint32][]client.Row, int) {
	rowsIgnored := 0
	if destination.NumShards == 0 || destination.NumShards == 1 {
		// in this case, there is no sharding in this aresDB cluster
		return nil, rowsIgnored
	}

	shards := make(map[uint32][]client.Row)
	for i := uint32(0); i < destination.NumShards; i++ {
		shards[i] = make([]client.Row, 0, len(rows))
	}

	for _, row := range rows {
		// convert primaryKey to byte array
		pk, err := getPrimaryKeyBytes(row, destination, jobConfig, jobConfig.GetPrimaryKeyBytes())
		if err != nil {
			rowsIgnored++
			continue
		}

		// calculate shard
		shardID := shardFn(pk, destination.NumShards)
		shards[shardID] = append(shards[shardID], row)
	}
	return shards, rowsIgnored
}

func shardFn(key []byte, numShards uint32) uint32 {
	return utils.Murmur3Sum32(unsafe.Pointer(&key[0]), len(key), 0) / (math.MaxUint32 / numShards)
}

func getPrimaryKeyBytes(row client.Row, destination Destination, jobConfig *rules.JobConfig, keyLength int) ([]byte, error) {
	primaryKeyValues := make([]memCom.DataValue, len(destination.PrimaryKeys))
	var err error
	var key, strBytes []byte
	i := 0
	for columnName, columnID := range destination.PrimaryKeys {
		columnIDInSchema := destination.PrimaryKeysInSchema[columnName]
		if jobConfig.AresTableConfig.Table.Columns[columnIDInSchema].IsEnumColumn() {
			// convert the string to bytes if primaryKey value is string
			str := row[columnID].(string)
			if strBytes == nil {
				strBytes = make([]byte, 0, len(str))
			}

			if !jobConfig.AresTableConfig.Table.Columns[columnIDInSchema].CaseInsensitive {
				str = strings.ToLower(row[columnID].(string))
			}
			strBytes = append(strBytes, []byte(str)...)
		} else {
			primaryKeyValues[i], err = getDataValue(row[columnID], columnIDInSchema, jobConfig)
			if err != nil {
				return key, utils.StackError(err, "Failed to read primary key at row %d, col %d",
					row, columnID)
			}
			i++
		}
	}

	if key, err = memCom.GetPrimaryKeyBytes(primaryKeyValues, keyLength); err != nil {
		return key, err
	}
	if strBytes != nil {
		key = append(key, strBytes...)
	}
	return key, err
}

// getDataValue returns the DataValue for the given column value.
func getDataValue(col interface{}, columnIDInSchema int, jobConfig *rules.JobConfig) (memCom.DataValue, error) {

	dataType := memCom.DataTypeFromString(jobConfig.AresTableConfig.Table.Columns[columnIDInSchema].Type)
	if dataStr, ok := col.(string); ok {
		return memCom.ValueFromString(dataStr, dataType)
	}

	val := memCom.DataValue{
		DataType: dataType,
	}

	var ok bool
	switch dataType {
	case memCom.Bool:
		var b bool
		val.IsBool = true
		if b, ok = col.(bool); !ok {
			return val, fmt.Errorf("Invalid bool value, col:%d, val:%v, type:%s",
				columnIDInSchema, col, reflect.TypeOf(col))
		}
		val.Valid = true
		val.BoolVal = b
	case memCom.Int8:
		var i8 int8
		if i8, ok = col.(int8); !ok {
			t := reflect.TypeOf(i8)
			if reflect.TypeOf(col).ConvertibleTo(t) {
				i8 = int8(reflect.ValueOf(col).Convert(t).Int())
			} else {
				return val, fmt.Errorf("Invalid int8 value, col:%d, val:%v, type:%s",
					columnIDInSchema, col, reflect.TypeOf(col))
			}
		}
		val.Valid = true
		val.OtherVal = unsafe.Pointer(&i8)
	case memCom.Uint8, memCom.SmallEnum:
		var ui8 uint8
		if ui8, ok = col.(uint8); !ok {
			t := reflect.TypeOf(ui8)
			if reflect.TypeOf(col).ConvertibleTo(t) {
				ui8 = uint8(reflect.ValueOf(col).Convert(t).Uint())
			} else {
				return val, fmt.Errorf("Invalid uint8 value, col:%d, val:%v, type:%s",
					columnIDInSchema, col, reflect.TypeOf(col))
			}
		}
		val.Valid = true
		val.OtherVal = unsafe.Pointer(&ui8)
	case memCom.Int16:
		var i16 int16
		if i16, ok = col.(int16); !ok {
			t := reflect.TypeOf(i16)
			if reflect.TypeOf(col).ConvertibleTo(t) {
				i16 = int16(reflect.ValueOf(col).Convert(t).Int())
			} else {
				return val, fmt.Errorf("Invalid int16 value, col:%d, val:%v, type:%s",
					columnIDInSchema, col, reflect.TypeOf(col))
			}
		}
		val.Valid = true
		val.OtherVal = unsafe.Pointer(&i16)
	case memCom.Uint16, memCom.BigEnum:
		var ui16 uint16
		if ui16, ok = col.(uint16); !ok {
			t := reflect.TypeOf(ui16)
			if reflect.TypeOf(col).ConvertibleTo(t) {
				ui16 = uint16(reflect.ValueOf(col).Convert(t).Uint())
			} else {
				return val, fmt.Errorf("Invalid uint16 value, col:%d, val:%v, type:%s",
					columnIDInSchema, col, reflect.TypeOf(col))
			}
		}
		val.Valid = true
		val.OtherVal = unsafe.Pointer(&ui16)
	case memCom.Int32:
		var i32 int32
		if i32, ok = col.(int32); !ok {
			t := reflect.TypeOf(i32)
			if reflect.TypeOf(col).ConvertibleTo(t) {
				i32 = int32(reflect.ValueOf(col).Convert(t).Int())
			} else {
				return val, fmt.Errorf("Invalid int32 value, col:%d, val:%v, type:%s",
					columnIDInSchema, col, reflect.TypeOf(col))
			}
		}
		val.Valid = true
		val.OtherVal = unsafe.Pointer(&i32)
	case memCom.Uint32:
		var ui32 uint32
		if ui32, ok = col.(uint32); !ok {
			t := reflect.TypeOf(ui32)
			if reflect.TypeOf(col).ConvertibleTo(t) {
				ui32 = uint32(reflect.ValueOf(col).Convert(t).Uint())
			} else {
				return val, fmt.Errorf("Invalid uint32 value, col:%d, val:%v, type:%s",
					columnIDInSchema, col, reflect.TypeOf(col))
			}
		}
		val.Valid = true
		val.OtherVal = unsafe.Pointer(&ui32)
	case memCom.Int64:
		var i64 int64
		if i64, ok = col.(int64); !ok {
			t := reflect.TypeOf(i64)
			if reflect.TypeOf(col).ConvertibleTo(t) {
				i64 = int64(reflect.ValueOf(col).Convert(t).Int())
			} else {
				return val, fmt.Errorf("Invalid int64 value, col:%d, val:%v, type:%s",
					columnIDInSchema, col, reflect.TypeOf(col))
			}
		}
		val.Valid = true
		val.OtherVal = unsafe.Pointer(&i64)
	case memCom.Float32:
		var f32 float32
		if f32, ok = col.(float32); !ok {
			t := reflect.TypeOf(f32)
			if reflect.TypeOf(col).ConvertibleTo(t) {
				f32 = float32(reflect.ValueOf(col).Convert(t).Float())
			} else {
				return val, fmt.Errorf("Invalid float32 value, col:%d, val:%v, type:%s",
					columnIDInSchema, col, reflect.TypeOf(col))
			}
		}
		val.Valid = true
		val.OtherVal = unsafe.Pointer(&f32)
	default:
		return val, fmt.Errorf("Invalid data type, col:%d, val:%v, type:%d",
			columnIDInSchema, col, dataType)
	}
	return val, nil
}
