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

package broker

import (
	"errors"
	"fmt"
	memCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/metastore"
	"github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/utils"
	"sync"
)

// BrokerSchemaMutator implements metastore.TableSchemaMutator
// and memstore.TableSchemaReader, and memstore.EnumUpdater
type BrokerSchemaMutator struct {
	sync.RWMutex

	tables map[string]*memCom.TableSchema
}

func NewBrokerSchemaMutator() *BrokerSchemaMutator {
	return &BrokerSchemaMutator{
		tables: map[string]*memCom.TableSchema{},
	}
}

// ====  metastore/common.TableSchemaMutator ====
func (b *BrokerSchemaMutator) ListTables() (tables []string, err error) {
	tables = make([]string, len(b.tables))
	i := 0
	for _, table := range b.tables {
		tables[i] = table.Schema.Name
		i++
	}
	return
}

func (b *BrokerSchemaMutator) GetTable(name string) (table *common.Table, err error) {
	tableSchema, ok := b.tables[name]
	if !ok {
		err = metastore.ErrTableDoesNotExist
		return
	}
	table = &tableSchema.Schema
	return
}

func (b *BrokerSchemaMutator) CreateTable(table *common.Table) (err error) {
	b.tables[table.Name] = memCom.NewTableSchema(table)
	return
}
func (b *BrokerSchemaMutator) DeleteTable(name string) (err error) {
	delete(b.tables, name)
	return
}
func (b *BrokerSchemaMutator) UpdateTableConfig(table string, config common.TableConfig) (err error) {
	b.tables[table].Schema.Config = config
	return
}
func (b *BrokerSchemaMutator) UpdateTable(table common.Table) (err error) {
	b.tables[table.Name] = memCom.NewTableSchema(&table)
	return
}
func (b *BrokerSchemaMutator) AddColumn(table string, column common.Column, appendToArchivingSortOrder bool) (err error) {
	oldSchema := b.tables[table].Schema
	oldSchema.Columns = append(oldSchema.Columns, column)
	if appendToArchivingSortOrder {
		oldSchema.ArchivingSortColumns = append(oldSchema.ArchivingSortColumns, len(oldSchema.Columns)-1)
	}
	b.tables[table] = memCom.NewTableSchema(&oldSchema)
	return
}
func (b *BrokerSchemaMutator) UpdateColumn(table string, column string, config common.ColumnConfig) (err error) {
	oldSchema := b.tables[table].Schema
	target := -1
	for i, col := range oldSchema.Columns {
		if col.Name == column {
			target = i
			break
		}
	}
	if target == -1 {
		err = errors.New(fmt.Sprintf("column %s not found", column))
		return
	}
	oldSchema.Columns[target].Config = config
	b.tables[table] = memCom.NewTableSchema(&oldSchema)
	return
}

func (b *BrokerSchemaMutator) DeleteColumn(table string, column string) (err error) {
	oldSchema := b.tables[table].Schema
	target := -1
	for i, col := range oldSchema.Columns {
		if col.Name == column {
			target = i
			break
		}
	}
	if target == -1 {
		err = errors.New(fmt.Sprintf("column %s not found", column))
		return
	}
	oldSchema.Columns[target].Deleted = true
	b.tables[table] = memCom.NewTableSchema(&oldSchema)
	return
}

// ====  memstore/common.TableSchemaReader ====
func (b *BrokerSchemaMutator) GetSchema(table string) (*memCom.TableSchema, error) {
	if t, exists := b.tables[table]; exists {
		return t, nil
	}
	return nil, utils.StackError(nil, "Failed to get table schema for table %s", table)
}

func (b *BrokerSchemaMutator) GetSchemas() map[string]*memCom.TableSchema {
	return b.tables
}

func (b *BrokerSchemaMutator) UpdateEnum(table, column string, enumList []string) error {
	var (
		exists   bool
		t        *memCom.TableSchema
		columnID int
	)
	t, exists = b.tables[table]
	if !exists {
		return utils.StackError(nil, "Failed to find table %s", table)
	}
	columnID, exists = t.ColumnIDs[column]
	if !exists {
		return utils.StackError(nil, "Failed to find column %s from table %s", column, table)
	}

	col := t.Schema.Columns[columnID]
	if !col.IsEnumColumn() {
		return utils.StackError(nil, "Column is not enum column, table %s column %s", table, column)
	}

	t.CreateEnumDict(column, enumList)
	return nil
}

// === controller/common.TableSchameReader =====
