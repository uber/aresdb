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

package message

import (
	"fmt"
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/client"
	memcom "github.com/uber/aresdb/memstore/common"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/subscriber/common/rules"
	"github.com/uber/aresdb/subscriber/common/sink"
	"github.com/uber/aresdb/subscriber/config"
	"github.com/uber/aresdb/utils"
	"go.uber.org/zap"
	"runtime"
	"sort"
)

// Parser holds all resources needed to parse one message
// into one or multiple row objects with respect to different destinations
type Parser struct {
	// ServiceConfig is ares-subscriber configure
	ServiceConfig config.ServiceConfig
	// JobName is job name
	JobName string
	// Cluster is ares cluster name
	Cluster string
	// destinations each message will be parsed and written into
	Destination sink.Destination
	// Transformations are keyed on the output column name
	Transformations map[string]*rules.TransformationConfig
	scope           tally.Scope
}

// NewParser will create a Parser for given JobConfig
func NewParser(jobConfig *rules.JobConfig, serviceConfig config.ServiceConfig) *Parser {
	mp := &Parser{
		ServiceConfig:   serviceConfig,
		JobName:         jobConfig.Name,
		Cluster:         jobConfig.AresTableConfig.Cluster,
		Transformations: jobConfig.GetTranformations(),
		scope: serviceConfig.Scope.Tagged(map[string]string{
			"job":         jobConfig.Name,
			"aresCluster": jobConfig.AresTableConfig.Cluster,
		}),
	}
	mp.populateDestination(jobConfig)
	return mp
}

func (mp *Parser) populateDestination(jobConfig *rules.JobConfig) {
	columnNames := []string{}
	updateModes := []memcom.ColumnUpdateMode{}
	primaryKeys := make(map[string]int)
	primaryKeysInSchema := make(map[string]int)
	destinations := jobConfig.GetDestinations()

	for _, dstConfig := range destinations {
		columnNames = append(columnNames, dstConfig.Column)
	}

	// sort column names in destination for consistent query order
	sort.Strings(columnNames)

	for id, column := range columnNames {
		updateModes = append(updateModes, destinations[column].UpdateMode)
		if oid, ok := jobConfig.GetPrimaryKeys()[column]; ok {
			primaryKeysInSchema[column] = oid
			primaryKeys[column] = id
		}
	}

	mp.Destination = sink.Destination{
		Table:               jobConfig.AresTableConfig.Table.Name,
		ColumnNames:         columnNames,
		PrimaryKeys:         primaryKeys,
		PrimaryKeysInSchema: primaryKeysInSchema,
		AresUpdateModes:     updateModes,
		NumShards:           jobConfig.NumShards,
	}
}

// ParseMessage will parse given message to fit the destination
func (mp *Parser) ParseMessage(msg map[string]interface{}, destination sink.Destination) (client.Row, error) {
	mp.ServiceConfig.Logger.Debug("Parsing", zap.Any("msg", msg))
	var row client.Row
	for _, col := range destination.ColumnNames {
		transformation := mp.Transformations[col]
		fromValue := mp.extractSourceFieldValue(msg, col)
		toValue, err := transformation.Transform(fromValue)
		if err != nil {
			mp.ServiceConfig.Logger.Error("Tranformation error",
				zap.String("job", mp.JobName),
				zap.String("cluster", mp.Cluster),
				zap.String("field", col),
				zap.String("name", GetFuncName()),
				zap.Error(err))
		}
		row = append(row, toValue)
	}
	return row, nil
}

// IsMessageValid checks if the message is valid
func (mp *Parser) IsMessageValid(msg map[string]interface{}, destination sink.Destination) error {
	if len(destination.ColumnNames) == 0 {
		return utils.StackError(nil, "No column names specified")
	}

	if len(destination.AresUpdateModes) != len(destination.ColumnNames) {
		return utils.StackError(nil,
			"length of column update modes %d does not equal to number of columns %d",
			len(destination.AresUpdateModes), len(destination.ColumnNames))
	}

	return nil
}

// CheckPrimaryKeys returns error if the value of primary key column is nil
func (mp *Parser) CheckPrimaryKeys(destination sink.Destination, row client.Row) error {
	for columnName, columnID := range destination.PrimaryKeys {
		if row[columnID] == nil {
			return utils.StackError(nil, "Primary key column %s is nil", columnName)
		}
	}
	return nil
}

// CheckTimeColumnExistence checks if time column is missing for fact table
func (mp *Parser) CheckTimeColumnExistence(schema *metaCom.Table, columnDict map[string]int,
	destination sink.Destination, row client.Row) error {
	if !schema.IsFactTable || schema.Config.AllowMissingEventTime {
		return nil
	}

	for id, columnName := range destination.ColumnNames {

		columnID := columnDict[columnName]
		if columnID == 0 && row[id] != nil {
			return nil
		}
	}
	return utils.StackError(nil, "Missing time column")
}

func (mp *Parser) extractSourceFieldValue(msg map[string]interface{}, fieldName string) interface{} {
	value, err := mp.getValue(msg, fieldName)
	if err != nil {
		mp.ServiceConfig.Logger.Debug("Failed to get value for",
			zap.String("job", mp.JobName),
			zap.String("cluster", mp.Cluster),
			zap.String("field", fieldName),
			zap.String("name", GetFuncName()),
			zap.Error(err))
	}
	return value
}

func (mp *Parser) getValue(msg map[string]interface{}, fieldName string) (interface{}, error) {
	if value, found := msg[fieldName]; found {
		return value, nil
	}
	return nil,
		fmt.Errorf("Message does not contain key: %s, job: %s, cluster: %s", fieldName, mp.JobName, mp.Cluster)
}

//GetFuncName get the function name of the calling function
func GetFuncName() string {
	p, _, _, _ := runtime.Caller(1)
	fn := runtime.FuncForPC(p).Name()
	return fn
}
