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
	"github.com/uber/aresdb/memstore"
	memCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/subscriber/common/rules"
	"github.com/uber/aresdb/utils"
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

//NewAresDatabase(
//	serviceConfig config.ServiceConfig, jobConfig *rules.JobConfig, cluster string,
//	config client.ConnectorConfig) (Sink, error)
//NewKafkaPublisher(jobConfig *rules.JobConfig, serviceConfig config.ServiceConfig, cluster string, kpCfg config.KafkaProducerConfig) (Sink, error)
func Sharding(rows []client.Row, destination Destination, jobConfig *rules.JobConfig) map[uint32][]client.Row {
	if destination.NumShards == 0 || destination.NumShards == 1 {
		// in this case, there is no sharding in this aresDB cluster
		return nil
	}

	shards := make(map[uint32][]client.Row)
	for i := uint32(0); i < destination.NumShards; i++ {
		shards[i] = make([]client.Row, 0, len(rows))
	}

	for _, row := range rows {
		// convert primaryKey to byte array
		pk := make([]byte, jobConfig.GetPrimaryKeyBytes())
		err := GetPrimaryKeyBytes(row, destination, jobConfig, pk)
		if err != nil {
			utils.StackError(err, "Failed to convert primaryKey to byte array for row: %v", row)
			continue
		}

		// calculate shard
		shardID := shardFn(pk, destination.NumShards)
		shards[shardID] = append(shards[shardID], row)
	}
	return shards
}

func shardFn(key []byte, numShards uint32) uint32 {
	return utils.Murmur3Sum32(unsafe.Pointer(&key[0]), len(key), 0) % numShards
}

func GetPrimaryKeyBytes(row client.Row, destination Destination, jobConfig *rules.JobConfig, key []byte) error {
	primaryKeyValues := make([]memCom.DataValue, len(destination.PrimaryKeys))
	var err error
	i := 0
	for columnName, columnID := range destination.PrimaryKeys {
		columnIDInSchema := destination.PrimaryKeysInSchema[columnName]
		primaryKeyValues[i], err = GetDataValue(row[columnID], columnIDInSchema, jobConfig)
		if err != nil {
			return utils.StackError(err, "Failed to read primary key at row %d, col %d",
				row, columnID)
		}
		i++
	}

	if err := memstore.GetPrimaryKeyBytes(primaryKeyValues, key); err != nil {
		return err
	}
	return nil
}

// GetDataValue returns the DataValue for the given column value.
func GetDataValue(col interface{}, columnIDInSchema int, jobConfig *rules.JobConfig) (memCom.DataValue, error) {
	var dataStr string
	var ok bool
	if dataStr, ok = col.(string); !ok {
		return memCom.DataValue{}, fmt.Errorf("Failed to convert %v to string", col)
	}

	dataType := memCom.DataTypeFromString(jobConfig.AresTableConfig.Table.Columns[columnIDInSchema].Type)
	dataVal, err := memCom.ValueFromString(dataStr, dataType)

	return dataVal, err
}
