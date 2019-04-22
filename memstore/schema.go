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
	"encoding/json"
	"sync"
	"unsafe"

	memCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/metastore"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/utils"
)

// TableSchema stores metadata of the table such as columns and primary keys.
// It also stores the dictionaries for enum columns.
type TableSchema struct {
	sync.RWMutex `json:"-"`
	// Main schema of the table. Mutable.
	Schema metaCom.Table `json:"schema"`
	// Maps from column names to their IDs. Mutable.
	ColumnIDs map[string]int `json:"columnIDs"`
	// Maps from enum column names to their case dictionaries. Mutable.
	EnumDicts map[string]EnumDict `json:"enumDicts"`
	// DataType for each column ordered by column ID. Mutable.
	ValueTypeByColumn []memCom.DataType `json:"valueTypeByColumn"`
	// Number of bytes in the primary key. Immutable.
	PrimaryKeyBytes int `json:"primaryKeyBytes"`
	// Types of each primary key column. Immutable.
	PrimaryKeyColumnTypes []memCom.DataType `json:"primaryKeyColumnTypes"`
	// Default values of each column. Mutable. Nil means default value is not set.
	DefaultValues []*memCom.DataValue `json:"-"`
}

// EnumDict contains mapping from and to enum strings to numbers.
type EnumDict struct {
	// Either 0x100 for small_enum, or 0x10000 for big_enum.
	Capacity    int            `json:"capacity"`
	Dict        map[string]int `json:"dict"`
	ReverseDict []string       `json:"reverseDict"`
}

// NewTableSchema creates a new table schema object from metaStore table object,
// this does not set enum cases.
func NewTableSchema(table *metaCom.Table) *TableSchema {
	tableSchema := &TableSchema{
		Schema:                *table,
		ColumnIDs:             make(map[string]int),
		EnumDicts:             make(map[string]EnumDict),
		ValueTypeByColumn:     make([]memCom.DataType, len(table.Columns)),
		PrimaryKeyColumnTypes: make([]memCom.DataType, len(table.PrimaryKeyColumns)),
		DefaultValues:         make([]*memCom.DataValue, len(table.Columns)),
	}

	for id, column := range table.Columns {
		if !column.Deleted {
			tableSchema.ColumnIDs[column.Name] = id
		}
		tableSchema.ValueTypeByColumn[id] = memCom.DataTypeForColumn(column)
	}

	for i, columnID := range table.PrimaryKeyColumns {
		columnType := tableSchema.ValueTypeByColumn[columnID]
		tableSchema.PrimaryKeyColumnTypes[i] = columnType

		dataBits := memCom.DataTypeBits(columnType)
		if dataBits < 8 {
			dataBits = 8
		}
		tableSchema.PrimaryKeyBytes += dataBits / 8
	}
	return tableSchema
}

// MarshalJSON marshals TableSchema into json.
func (t *TableSchema) MarshalJSON() ([]byte, error) {
	// Avoid loop json.Marshal calls.
	type alias TableSchema
	t.RLock()
	defer t.RUnlock()
	return json.Marshal((*alias)(t))
}

// SetTable sets a updated table and update TableSchema,
// should acquire lock before calling.
func (t *TableSchema) SetTable(table *metaCom.Table) {
	t.Schema = *table
	for id, column := range table.Columns {
		if !column.Deleted {
			t.ColumnIDs[column.Name] = id
		} else {
			delete(t.ColumnIDs, column.Name)
		}

		if id >= len(t.ValueTypeByColumn) {
			t.ValueTypeByColumn = append(t.ValueTypeByColumn, memCom.DataTypeForColumn(column))
		}

		if id >= len(t.DefaultValues) {
			t.DefaultValues = append(t.DefaultValues, nil)
		}
	}
}

// SetDefaultValue parses the default value string if present and sets to TableSchema.
// Schema lock should be acquired and release by caller and enum dict should already be
// created/update before this function.
func (t *TableSchema) SetDefaultValue(columnID int) {
	// Default values are already set.
	if t.DefaultValues[columnID] != nil {
		return
	}

	column := t.Schema.Columns[columnID]
	defStrVal := column.DefaultValue
	if defStrVal == nil || column.Deleted {
		t.DefaultValues[columnID] = &memCom.NullDataValue
		return
	}

	dataType := t.ValueTypeByColumn[columnID]
	dataTypeName := memCom.DataTypeName[dataType]
	val := memCom.DataValue{
		Valid:    true,
		DataType: dataType,
	}

	if dataType == memCom.SmallEnum || dataType == memCom.BigEnum {
		enumDict, ok := t.EnumDicts[column.Name]
		if !ok {
			// Should no happen since the enum dict should already be created.
			utils.GetLogger().With(
				"data_type", dataTypeName,
				"default_value", *defStrVal,
				"column", t.Schema.Columns[columnID].Name,
			).Panic("Cannot find EnumDict for column")
		}
		enumVal, ok := enumDict.Dict[*defStrVal]
		if !ok {
			// Should no happen since the enum value should already be created.
			utils.GetLogger().With(
				"data_type", dataTypeName,
				"default_value", *defStrVal,
				"column", t.Schema.Columns[columnID].Name,
			).Panic("Cannot find enum value for column")
		}

		if dataType == memCom.SmallEnum {
			enumValUint8 := uint8(enumVal)
			val.OtherVal = unsafe.Pointer(&enumValUint8)
		} else {
			enumValUint16 := uint16(enumVal)
			val.OtherVal = unsafe.Pointer(&enumValUint16)
		}
	} else {
		dataValue, err := memCom.ValueFromString(*defStrVal, dataType)
		if err != nil {
			// Should not happen since the string value is already validated by schema handler.
			utils.GetLogger().With(
				"data_type", dataTypeName,
				"default_value", *defStrVal,
				"column", t.Schema.Columns[columnID].Name,
			).Panic("Cannot parse default value")
		}

		if dataType == memCom.Bool {
			val.IsBool = true
			val.BoolVal = dataValue.BoolVal
		} else {
			val.OtherVal = dataValue.OtherVal
		}
	}

	val.CmpFunc = memCom.GetCompareFunc(dataType)
	t.DefaultValues[columnID] = &val
	return
}

// createEnumDict creates the enum dictionary for the specified column with the
// specified initial cases, and attaches it to TableSchema object.
// Caller should acquire the schema lock before calling this function.
func (t *TableSchema) createEnumDict(columnName string, enumCases []string) {
	columnID := t.ColumnIDs[columnName]
	dataType := t.ValueTypeByColumn[columnID]
	enumCapacity := 1 << uint(memCom.DataTypeBits(dataType))
	enumDict := map[string]int{}
	for id, enumCase := range enumCases {
		enumDict[enumCase] = id
	}
	t.EnumDicts[columnName] = EnumDict{
		Capacity:    enumCapacity,
		Dict:        enumDict,
		ReverseDict: enumCases,
	}
}

// GetValueTypeByColumn makes a copy of the ValueTypeByColumn so callers don't have to hold a read
// lock to access it.
func (t *TableSchema) GetValueTypeByColumn() []memCom.DataType {
	t.RLock()
	defer t.RUnlock()
	return t.ValueTypeByColumn
}

// GetPrimaryKeyColumns makes a copy of the Schema.PrimaryKeyColumns so callers don't have to hold
// a read lock to access it.
func (t *TableSchema) GetPrimaryKeyColumns() []int {
	t.RLock()
	defer t.RUnlock()
	return t.Schema.PrimaryKeyColumns
}

// GetColumnDeletions returns a boolean slice that indicates whether a column has been deleted. Callers
// need to hold a read lock.
func (t *TableSchema) GetColumnDeletions() []bool {
	deletedByColumn := make([]bool, len(t.Schema.Columns))
	for columnID, column := range t.Schema.Columns {
		deletedByColumn[columnID] = column.Deleted
	}
	return deletedByColumn
}

// GetColumnDeletions returns a boolean slice that indicates whether a column has non nil default value. Callers
// need to hold a read lock.
func (t *TableSchema) GetColumnIfNonNilDefault() []bool {
	nonNilDefaultByColumn := make([]bool, len(t.Schema.Columns))
	for columnID, column := range t.Schema.Columns {
		nonNilDefaultByColumn[columnID] = column.DefaultValue != nil
	}
	return nonNilDefaultByColumn
}

// GetArchivingSortColumns makes a copy of the Schema.ArchivingSortColumns so
// callers don't have to hold a read lock to access it.
func (t *TableSchema) GetArchivingSortColumns() []int {
	t.RLock()
	defer t.RUnlock()
	return t.Schema.ArchivingSortColumns
}

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
		tableSchema := NewTableSchema(table)
		for columnID, column := range table.Columns {
			if !column.Deleted {
				if column.IsEnumColumn() {
					enumCases, err := m.metaStore.GetEnumDict(tableName, column.Name)
					if err != nil {
						if err != metastore.ErrTableDoesNotExist && err != metastore.ErrColumnDoesNotExist {
							return utils.StackError(err, "Failed to fetch enum cases for table: %s, column: %s", tableName, column.Name)
						}
					} else {
						tableSchema.createEnumDict(column.Name, enumCases)
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
	newEnumColumns := []string{}
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
		tableSchema = NewTableSchema(newTable)
		for columnID, column := range newTable.Columns {
			if !column.Deleted {
				if column.IsEnumColumn() {
					var enumCases []string
					if column.DefaultValue != nil {
						enumCases = append(enumCases, *column.DefaultValue)
						// default value is already appended, start watching from 1
						startEnumID = 1
					}
					tableSchema.createEnumDict(column.Name, enumCases)
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
		tableSchema.SetDefaultValue(columnID)
		if column.Deleted {
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
					tableSchema.createEnumDict(column.Name, enumCases)
					newEnumColumns = append(newEnumColumns, column.Name)
				}
			}
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
