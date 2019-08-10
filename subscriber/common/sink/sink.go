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
			fmt.Printf("Failed to shard table: %s, err: %v\n", jobConfig.Name,
				utils.StackError(err, "Failed to convert primaryKey to byte array for row: %v", row))
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

// GetDataValue returns the DataValue for the given column value.
func getDataValue(col interface{}, columnIDInSchema int, jobConfig *rules.JobConfig) (memCom.DataValue, error) {
	var dataStr string
	var ok bool
	if dataStr, ok = col.(string); !ok {
		return memCom.DataValue{}, fmt.Errorf("Failed to convert %v to string", col)
	}

	dataType := memCom.DataTypeFromString(jobConfig.AresTableConfig.Table.Columns[columnIDInSchema].Type)
	dataVal, err := memCom.ValueFromString(dataStr, dataType)

	return dataVal, err
}
