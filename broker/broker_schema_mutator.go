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
	memCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/metastore"
	"github.com/uber/aresdb/metastore/common"
	"sync"
)

// BrokerSchemaMutator implements 2 interfaces
//  - metastore.TableSchemaMutator needed by SchemaFetchJob
//  - memstore.TableSchemaReader needed by compiler
type BrokerSchemaMutator struct {
	sync.RWMutex

	tables map[string]*memCom.TableSchema
}

func NewBrokerSchemaMutator() *BrokerSchemaMutator {
	return &BrokerSchemaMutator{}
}

// ====  metastore/common.TableSchemaMutator ====
// TODO: implement. TableSchemaMutator should update b.tables
func (b *BrokerSchemaMutator) ListTables() (tables []string, err error) {
	return
}

func (b *BrokerSchemaMutator) GetTable(name string) (table *common.Table, err error) {
	tableSchema, ok := b.tables[name]
	if !ok {
		err = metastore.ErrTableDoesNotExist
	}
	table = &tableSchema.Schema
	return
}

func (b *BrokerSchemaMutator) CreateTable(table *common.Table) (err error) {
	return
}
func (b *BrokerSchemaMutator) DeleteTable(name string) (err error) {
	return
}
func (b *BrokerSchemaMutator) UpdateTableConfig(table string, config common.TableConfig) (err error) {
	return
}
func (b *BrokerSchemaMutator) UpdateTable(table common.Table) (err error) {
	return
}
func (b *BrokerSchemaMutator) AddColumn(table string, column common.Column, appendToArchivingSortOrder bool) (err error) {
	return
}
func (b *BrokerSchemaMutator) UpdateColumn(table string, column string, config common.ColumnConfig) (err error) {
	return
}

func (b *BrokerSchemaMutator) DeleteColumn(table string, column string) (err error) {
	return
}

// ====  memstore/common.TableSchemaReader ====
// TODO: implement. these are used by compiler
func (b *BrokerSchemaMutator) GetSchema(table string) (tableSchema *memCom.TableSchema, err error) {
	return
}

func (b *BrokerSchemaMutator) GetSchemas() (schemas map[string]*memCom.TableSchema) {
	return
}
