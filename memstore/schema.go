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

package memstore

import (
	memCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/metastore"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/utils"
)

// FetchSchema fetches schema from metaStore and updates in-memory copy of table schema,
// and set up watch channels for metaStore schema changes, used for bootstrapping mem store.
func (m *memStoreImpl) FetchSchema() error {
	tables, err := m.metaStore.ListTables()
	if err != nil {
		return utils.StackError(err, "Failed to list tables from meta")
	}

	for _, tableName := range tables {
		err := m.fetchTable(tableName)
		if err != nil {
			return err
		}
	}

	// watch table addition/modification
	tableSchemaChangeEvents, done, err := m.metaStore.WatchTableSchemaEvents()
	if err != nil {
		return utils.StackError(err, "Failed to watch table list events")
	}
	go m.handleTableSchemaChange(tableSchemaChangeEvents, done)

	// watch table deletion
	tableListChangeEvents, done, err := m.metaStore.WatchTableListEvents()
	if err != nil {
		return utils.StackError(err, "Failed to watch table list events")
	}
	go m.handleTableListChange(tableListChangeEvents, done)

	// watch enum cases appending
	m.RLock()
	for _, tableSchema := range m.TableSchemas {
		for columnName, enumCases := range tableSchema.EnumDicts {
			err := m.watchEnumCases(tableSchema.Schema.Name, columnName, len(enumCases.ReverseDict))
			if err != nil {
				return err
			}
		}
	}
	m.RUnlock()

	return nil
}

func (m *memStoreImpl) fetchTable(tableName string) error {
	table, err := m.metaStore.GetTable(tableName)
	if err != nil {
		if err != metastore.ErrTableDoesNotExist {
			return utils.StackError(err, "Failed to get table schema for table %s from meta", tableName)
		}
	} else {
		tableSchema := memCom.NewTableSchema(table)
		for columnID, column := range table.Columns {
			if !column.Deleted {
				if column.IsEnumColumn() {
					enumCases, err := m.metaStore.GetEnumDict(tableName, column.Name)
					if err != nil {
						if err != metastore.ErrTableDoesNotExist && err != metastore.ErrColumnDoesNotExist {
							return utils.StackError(err, "Failed to fetch enum cases for table: %s, column: %s", tableName, column.Name)
						}
					} else {
						tableSchema.CreateEnumDict(column.Name, enumCases)
					}
				}
			}
			tableSchema.SetDefaultValue(columnID)
		}
		m.Lock()
		m.TableSchemas[tableName] = tableSchema
		m.Unlock()
	}
	return nil
}

// watch enumCases will setup watch channels for each enum column.
func (m *memStoreImpl) watchEnumCases(tableName, columnName string, startCase int) error {
	enumDictChangeEvents, done, err := m.metaStore.WatchEnumDictEvents(tableName, columnName, startCase)
	if err != nil {
		if err != metastore.ErrTableDoesNotExist && err != metastore.ErrColumnDoesNotExist {
			return utils.StackError(err, "Failed to watch enum case events")
		}
	} else {
		go m.handleEnumDictChange(tableName, columnName, enumDictChangeEvents, done)
	}
	return nil
}

// handleTableListChange handles table deletion events from metaStore.
func (m *memStoreImpl) handleTableListChange(tableListChangeEvents <-chan []string, done chan<- struct{}) {
	for newTableList := range tableListChangeEvents {
		m.applyTableList(newTableList)
		done <- struct{}{}
	}
	close(done)
}

func (m *memStoreImpl) applyTableList(newTableList []string) {
	m.Lock()
	for tableName, tableSchema := range m.TableSchemas {
		if utils.IndexOfStr(newTableList, tableName) < 0 {
			// detach shards and schema from map
			// to prevent new usage
			tableShards := m.TableShards[tableName]
			delete(m.TableSchemas, tableName)
			delete(m.TableShards, tableName)
			// only one table deletion at a time
			m.Unlock()
			for shardID, shard := range tableShards {
				shard.Destruct()
				m.diskStore.DeleteTableShard(tableName, shardID)
			}
			m.scheduler.DeleteTable(tableName, tableSchema.Schema.IsFactTable)
			return
		}
	}
	m.Unlock()
}

// handleTableSchemaChange handles table schema change event from metaStore including new table schema.
func (m *memStoreImpl) handleTableSchemaChange(tableSchemaChangeEvents <-chan *metaCom.Table, done chan<- struct{}) {
	for table := range tableSchemaChangeEvents {
		m.applyTableSchema(table)
		done <- struct{}{}
	}
	close(done)
}

func (m *memStoreImpl) applyTableSchema(newTable *metaCom.Table) {
	tableName := newTable.Name
	var newEnumColumns []string
	// default start watching from first enumCase
	startEnumID := 0
	defer func() {
		for _, column := range newEnumColumns {
			err := m.watchEnumCases(tableName, column, startEnumID)
			if err != nil {
				utils.GetLogger().With(
					"error", err.Error(),
					"table", tableName,
					"column", column).
					Panic("Failed to watch enum dict events")
			}
		}
	}()

	m.Lock()
	tableSchema, tableExist := m.TableSchemas[tableName]
	// new table
	if !tableExist {
		tableSchema = memCom.NewTableSchema(newTable)
		for columnID, column := range newTable.Columns {
			if !column.Deleted {
				if column.IsEnumColumn() {
					var enumCases []string
					if column.DefaultValue != nil {
						enumCases = append(enumCases, *column.DefaultValue)
						// default value is already appended, start watching from 1
						startEnumID = 1
					}
					tableSchema.CreateEnumDict(column.Name, enumCases)
					newEnumColumns = append(newEnumColumns, column.Name)
				}
			}
			tableSchema.SetDefaultValue(columnID)
		}
		m.TableSchemas[newTable.Name] = tableSchema
		m.Unlock()
		return
	}
	m.Unlock()

	var columnsToDelete []int

	tableSchema.Lock()
	oldColumns := tableSchema.Schema.Columns
	tableSchema.SetTable(newTable)

	for columnID, column := range newTable.Columns {
		if column.Deleted {
			tableSchema.SetDefaultValue(columnID)
			if columnID < len(oldColumns) && !oldColumns[columnID].Deleted { // new deletions only
				delete(tableSchema.EnumDicts, column.Name)
				columnsToDelete = append(columnsToDelete, columnID)
			}
		} else {
			if column.IsEnumColumn() {
				_, exist := tableSchema.EnumDicts[column.Name]
				if !exist {
					var enumCases []string
					if column.DefaultValue != nil {
						enumCases = append(enumCases, *column.DefaultValue)
						// default value is already appended, start watching from 1
						startEnumID = 1
					}
					tableSchema.CreateEnumDict(column.Name, enumCases)
					newEnumColumns = append(newEnumColumns, column.Name)
				}
			}
			// always set default value after enum map creation
			tableSchema.SetDefaultValue(columnID)
			var oldPreloadingDays int
			newPreloadingDays := column.Config.PreloadingDays
			// preloading will be triggered if
			// 1. this is a new column and PreloadingDays > 0
			// 2. this is a old column and PreloadingDays > oldPreloadingDays
			if columnID < len(oldColumns) {
				oldPreloadingDays = oldColumns[columnID].Config.PreloadingDays
			}
			m.HostMemManager.TriggerPreload(tableName, columnID, oldPreloadingDays, newPreloadingDays)
		}
	}
	tableSchema.Unlock()

	for _, columnID := range columnsToDelete {
		var shards []*TableShard
		m.RLock()
		for _, shard := range m.TableShards[tableName] {
			shard.Users.Add(1)
			shards = append(shards, shard)
		}
		m.RUnlock()

		for _, shard := range shards {
			// May block for extended amount of time during archiving
			shard.DeleteColumn(columnID)
			shard.Users.Done()
		}
	}
}

// handleEnumDictChange handles enum dict change event from metaStore for specific table and column.
func (m *memStoreImpl) handleEnumDictChange(tableName, columnName string, enumDictChangeEvents <-chan string, done chan<- struct{}) {
	for newEnumCase := range enumDictChangeEvents {
		m.applyEnumCase(tableName, columnName, newEnumCase)
	}
	close(done)
}

func (m *memStoreImpl) applyEnumCase(tableName, columnName string, newEnumCase string) {
	m.RLock()
	tableSchema, tableExist := m.TableSchemas[tableName]
	if !tableExist {
		m.RUnlock()
		return
	}

	tableSchema.Lock()
	m.RUnlock()
	enumDict, columnExist := tableSchema.EnumDicts[columnName]
	if !columnExist {
		tableSchema.Unlock()
		return
	}

	enumDict.Dict[newEnumCase] = len(enumDict.ReverseDict)
	enumDict.ReverseDict = append(enumDict.ReverseDict, newEnumCase)
	tableSchema.EnumDicts[columnName] = enumDict
	tableSchema.Unlock()
}
