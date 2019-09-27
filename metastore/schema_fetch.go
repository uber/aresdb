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

package metastore

import (
	"fmt"
	controllerCli "github.com/uber/aresdb/controller/client"
	controllerMutatorCom "github.com/uber/aresdb/controller/mutators/common"
	memCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/utils"
	"reflect"
	"time"
)

// SchemaFetchJob is a job that periodically pings ares-controller and updates table schemas if applicable
type SchemaFetchJob struct {
	clusterName       string
	hash              string
	intervalInSeconds int
	schemaMutator     common.TableSchemaMutator
	enumUpdater       memCom.EnumUpdater
	schemaValidator   TableSchemaValidator
	controllerClient  controllerCli.ControllerClient
	enumMutator       controllerMutatorCom.EnumMutator
	stopChan          chan struct{}
}

// NewSchemaFetchJob creates a new SchemaFetchJob
func NewSchemaFetchJob(intervalInSeconds int, schemaMutator common.TableSchemaMutator, enumUpdater memCom.EnumUpdater, schemaValidator TableSchemaValidator, controllerClient controllerCli.ControllerClient, enumMutator controllerMutatorCom.EnumMutator, clusterName, initialHash string) *SchemaFetchJob {
	return &SchemaFetchJob{
		clusterName:       clusterName,
		hash:              initialHash,
		intervalInSeconds: intervalInSeconds,
		schemaMutator:     schemaMutator,
		enumUpdater:       enumUpdater,
		schemaValidator:   schemaValidator,
		stopChan:          make(chan struct{}),
		controllerClient:  controllerClient,
		enumMutator:       enumMutator,
	}
}

// Run starts the scheduling
func (j *SchemaFetchJob) Run() {
	tickChan := time.NewTicker(time.Second * time.Duration(j.intervalInSeconds)).C

	for {
		select {
		case <-tickChan:
			j.FetchSchema()
			if j.enumUpdater != nil {
				j.FetchEnum()
			}
		case <-j.stopChan:
			return
		}
	}
}

// Stop stops the scheduling
func (j *SchemaFetchJob) Stop() {
	close(j.stopChan)
}

func (j *SchemaFetchJob) FetchSchema() {
	newHash, err := j.controllerClient.GetSchemaHash(j.clusterName)
	if err != nil {
		reportError(err, true, "hash")
		return
	}
	if newHash != j.hash {
		newSchemas, err := j.controllerClient.GetAllSchema(j.clusterName)
		if err != nil {
			reportError(err, true, "allSchema")
			return
		}
		err = j.applySchemaChange(newSchemas)
		if err != nil {
			// errors already reported, just return without updating hash
			return
		}
		j.hash = newHash
	}
	utils.GetLogger().Debug("Succeeded to run schema fetch job")
	utils.GetRootReporter().GetCounter(utils.SchemaFetchSuccess).Inc(1)
}

func (j *SchemaFetchJob) applySchemaChange(tables []common.Table) (err error) {
	oldTables, err := j.schemaMutator.ListTables()
	if err != nil {
		return
	}

	oldTablesMap := make(map[string]bool)
	for _, oldTableName := range oldTables {
		oldTablesMap[oldTableName] = true
	}

	for _, table := range tables {
		if _, exist := oldTablesMap[table.Name]; !exist {
			// found new table
			err = j.schemaMutator.CreateTable(&table)
			if err != nil {
				reportError(err, true, table.Name)
				continue
			}
			utils.GetRootReporter().GetCounter(utils.SchemaCreationCount).Inc(1)
			utils.GetLogger().With("table", table.Name).Info("added new table")
		} else {
			oldTablesMap[table.Name] = false
			var oldTable *common.Table
			oldTable, err = j.schemaMutator.GetTable(table.Name)
			if err != nil {
				reportError(err, true, table.Name)
				continue
			}
			if oldTable.Incarnation < table.Incarnation {
				// found new table incarnation, delete previous table and data
				// then create new table
				err := j.schemaMutator.DeleteTable(table.Name)
				if err != nil {
					reportError(err, true, table.Name)
					continue
				}
				utils.GetRootReporter().GetCounter(utils.SchemaDeletionCount).Inc(1)
				utils.GetLogger().With("table", table.Name).Info("deleted table")
				err = j.schemaMutator.CreateTable(&table)
				if err != nil {
					reportError(err, true, table.Name)
					continue
				}
				utils.GetRootReporter().GetCounter(utils.SchemaCreationCount).Inc(1)
				utils.GetLogger().With("table", table.Name).Info("recreated table")

			} else if oldTable.Incarnation == table.Incarnation && !reflect.DeepEqual(&table, oldTable) {
				// found table update
				j.schemaValidator.SetNewTable(table)
				j.schemaValidator.SetOldTable(*oldTable)
				err = j.schemaValidator.Validate()
				if err != nil {
					reportError(err, true, table.Name)
					continue
				}
				err = j.schemaMutator.UpdateTable(table)
				if err != nil {
					reportError(err, true, table.Name)
					continue
				}
				utils.GetRootReporter().GetCounter(utils.SchemaUpdateCount).Inc(1)
				utils.GetLogger().With("table", table.Name).Info("updated table")
			}
		}
	}

	for oldTableName, notAddressed := range oldTablesMap {
		if notAddressed {
			// found table deletion
			err = j.schemaMutator.DeleteTable(oldTableName)
			if err != nil {
				reportError(err, true, oldTableName)
				continue
			}
			utils.GetRootReporter().GetCounter(utils.SchemaDeletionCount).Inc(1)
		}
	}

	return
}

// FetchEnum updates all enums
func (j *SchemaFetchJob) FetchEnum() {
	var (
		tableNames []string
		table      *common.Table
		enumCases  []string
		err        error
	)
	tableNames, err = j.schemaMutator.ListTables()
	if err != nil {
		reportError(err, false, "failed to get tables when fetching enums")
		return
	}

	for _, tableName := range tableNames {
		table, err = j.schemaMutator.GetTable(tableName)
		if err != nil {
			reportError(err, false, fmt.Sprintf("failed to get table %s when fetching enums", tableName))
			continue
		}
		for _, column := range table.Columns {
			if !column.IsEnumColumn() {
				continue
			}
			enumCases, err = j.enumMutator.GetEnumCases(j.clusterName, tableName, column.Name)
			if err != nil {
				reportError(err, false, fmt.Sprintf("failed to get enums, table %s column %s", tableName, column.Name))
				continue
			}
			err = j.enumUpdater.UpdateEnum(tableName, column.Name, enumCases)
			if err != nil {
				reportError(err, false, fmt.Sprintf("failed to update enums, table %s column %s", tableName, column.Name))
				continue
			}
			utils.GetLogger().Debugf("Succeeded to fetch enums. table %s, column %s", tableName, column.Name)
		}
	}
}

func reportError(err error, isSchemaError bool, extraInfo string) {
	if isSchemaError {
		utils.GetRootReporter().GetCounter(utils.SchemaFetchFailure).Inc(1)
	} else {
		utils.GetRootReporter().GetCounter(utils.SchemaFetchFailureEnum).Inc(1)
	}
	utils.GetLogger().With("extraInfo", extraInfo).Error(utils.StackError(err, "err running schema fetch job"))
}
