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
package etcd

import (
	"encoding/json"

	"github.com/m3db/m3/src/cluster/kv"
	pb "github.com/uber/aresdb/controller/generated/proto"
	"github.com/uber/aresdb/controller/mutators/common"
	"github.com/uber/aresdb/metastore"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/utils"
	"go.uber.org/zap"
)

// NewTableSchemaMutator returns a new TableSchemaMutator
func NewTableSchemaMutator(store kv.TxnStore, logger *zap.SugaredLogger) common.TableSchemaMutator {
	return &tableSchemaMutator{
		txnStore: store,
		logger:   logger,
	}
}

type tableSchemaMutator struct {
	txnStore kv.TxnStore
	logger   *zap.SugaredLogger
}

func (m *tableSchemaMutator) ListTables(namespace string) (tableNames []string, err error) {
	var tableListProto pb.EntityList
	tableListProto, _, err = readEntityList(m.txnStore, utils.SchemaListKey(namespace))
	if err != nil {
		return
	}

	for _, t := range tableListProto.Entities {
		if !t.Tomstoned {
			tableNames = append(tableNames, t.Name)
		}
	}

	return tableNames, nil
}

func (m *tableSchemaMutator) GetTable(namespace, name string) (table *metaCom.Table, err error) {
	var schemaProto pb.EntityConfig
	schemaProto, _, err = m.readSchema(namespace, name)
	if err != nil {
		return
	}

	if schemaProto.Tomstoned {
		return nil, metastore.ErrTableDoesNotExist
	}

	table = &metaCom.Table{}
	table.Config = metaCom.TableConfig{
		BatchSize:                metastore.DefaultBatchSize,
		ArchivingIntervalMinutes: metastore.DefaultArchivingIntervalMinutes,
		ArchivingDelayMinutes:    metastore.DefaultArchivingDelayMinutes,
		BackfillMaxBufferSize:    metastore.DefaultBackfillMaxBufferSize,
		BackfillIntervalMinutes:  metastore.DefaultBackfillIntervalMinutes,
		BackfillThresholdInBytes: metastore.DefaultBackfillThresholdInBytes,
		BackfillStoreBatchSize:   metastore.DefaultBackfillStoreBatchSize,
		RecordRetentionInDays:    metastore.DefaultRecordRetentionInDays,
		SnapshotIntervalMinutes:  metastore.DefaultSnapshotIntervalMinutes,
		SnapshotThreshold:        metastore.DefaultSnapshotThreshold,
		RedoLogRotationInterval:  metastore.DefaultRedologRotationInterval,
		MaxRedoLogFileSize:       metastore.DefaultMaxRedoLogSize,
	}

	err = json.Unmarshal(schemaProto.Config, &table)
	return table, err
}

func (m *tableSchemaMutator) CreateTable(namespace string, table *metaCom.Table, force bool) (err error) {
	if !force {
		validator := metastore.NewTableSchameValidator()
		validator.SetNewTable(*table)
		err = validator.Validate()
		if err != nil {
			return
		}
	}

	tableListProto, tableListVersion, err := readEntityList(m.txnStore, utils.SchemaListKey(namespace))
	if err != nil {
		return err
	}

	schemaProto := pb.EntityConfig{
		Name:      table.Name,
		Tomstoned: false,
	}
	schemaVersion := kv.UninitializedVersion

	tableListProto, incarnation, exist := addEntity(tableListProto, table.Name)
	if exist {
		return metastore.ErrTableAlreadyExist
	}
	table.Incarnation = incarnation

	// table get recreated
	if incarnation > 0 {
		schemaProto, schemaVersion, err = m.readSchema(namespace, table.Name)
		if err != nil {
			return
		}
		schemaProto.Tomstoned = false
	}

	schemaProto.Config, err = json.Marshal(table)
	if err != nil {
		return
	}

	txn := newTransaction().
		addKeyValue(utils.SchemaListKey(namespace), tableListVersion, &tableListProto).
		addKeyValue(utils.SchemaKey(namespace, table.Name), schemaVersion, &schemaProto)

	preCreateEnumNodes(txn, namespace, table, 0, len(table.Columns))

	err = txn.writeTo(m.txnStore)
	return
}

func (m *tableSchemaMutator) DeleteTable(namespace, name string) error {
	tableListProto, tableListVersion, err := readEntityList(m.txnStore, utils.SchemaListKey(namespace))
	if err != nil {
		return err
	}

	tableListProto, found := deleteEntity(tableListProto, name)
	if !found {
		return metastore.ErrTableDoesNotExist
	}

	// found table
	schemaProto, schemaVersion, err := m.readSchema(namespace, name)
	if err != nil {
		return err
	}

	if schemaProto.Tomstoned {
		return metastore.ErrTableDoesNotExist
	}
	schemaProto.Tomstoned = true

	var table metaCom.Table
	err = json.Unmarshal(schemaProto.Config, &table)
	if err != nil {
		return err
	}

	err = newTransaction().
		addKeyValue(utils.SchemaListKey(namespace), tableListVersion, &tableListProto).
		addKeyValue(utils.SchemaKey(namespace, name), schemaVersion, &schemaProto).
		writeTo(m.txnStore)
	if err != nil {
		return err
	}
	// delete enums in background
	go m.deleteEnum(namespace, &table)
	return nil
}

func (m *tableSchemaMutator) deleteEnum(namespace string, table *metaCom.Table) {
	logger := m.logger.With("namespace", namespace, "table", table.Name, "incarnation", table.Incarnation)
	for columnID, column := range table.Columns {
		if column.IsEnumColumn() {
			value, err := m.txnStore.Get(utils.EnumNodeListKey(namespace, table.Name, table.Incarnation, columnID))
			if err != nil {
				logger.With("column", column.Name, "columnID", columnID, "error", err.Error()).Error("failed to get enum node list")
				continue
			}
			var nodeList pb.EnumNodeList
			err = value.Unmarshal(&nodeList)
			if err != nil {
				logger.With("column", column.Name, "columnID", columnID, "error", err.Error()).Error("failed to get enum node list")
				continue
			}
			for nodeID := 0; nodeID < int(nodeList.NumEnumNodes); nodeID++ {
				_, err = m.txnStore.Delete(utils.EnumNodeKey(namespace, table.Name, table.Incarnation, columnID, nodeID))
				if err != nil {
					logger.With("column", column.Name, "columnID", columnID, "node", nodeID, "error", err.Error()).Error("failed to delete enum node")
					continue
				}
			}
			_, err = m.txnStore.Delete(utils.EnumNodeListKey(namespace, table.Name, table.Incarnation, columnID))
			if err != nil {
				logger.With("column", column.Name, "columnID", columnID, "error", err.Error()).Error("failed to delete enum node list")
			}
		}
	}
}

func (m *tableSchemaMutator) UpdateTable(namespace string, table metaCom.Table, force bool) (err error) {
	tableListProto, tableListVersion, err := readEntityList(m.txnStore, utils.SchemaListKey(namespace))
	if err != nil {
		return err
	}

	tableListProto, found := updateEntity(tableListProto, table.Name)
	if !found {
		m.logger.With(
			"table", table,
		).Info("table not found for update, creating new table")
		return m.CreateTable(namespace, &table, force)
	}

	schemaProto, schemaVersion, err := m.readSchema(namespace, table.Name)
	if err != nil {
		return err
	}
	if schemaProto.Tomstoned {
		return metastore.ErrTableDoesNotExist
	}

	var oldTable metaCom.Table
	err = json.Unmarshal(schemaProto.Config, &oldTable)
	if err != nil {
		return
	}

	// always use old table's incarnation for update table operation will not modify incarnation
	table.Incarnation = oldTable.Incarnation

	// merge existing table and column level configs if not specified in the input
	if (metaCom.TableConfig{}) == table.Config {
		table.Config = oldTable.Config
	}
	for columnID := range table.Columns {
		if columnID < len(oldTable.Columns) && (metaCom.ColumnConfig{}) == table.Columns[columnID].Config {
			table.Columns[columnID].Config = oldTable.Columns[columnID].Config
		}
	}

	// TODO: remove this when upstream supports archive sort columns
	if len(table.ArchivingSortColumns) == 0 {
		table.ArchivingSortColumns = oldTable.ArchivingSortColumns
	}

	if !force {
		validator := metastore.NewTableSchameValidator()
		validator.SetNewTable(table)
		validator.SetOldTable(oldTable)
		err = validator.Validate()
		if err != nil {
			return
		}
	}

	schemaProto.Tomstoned = false
	schemaProto.Config, err = json.Marshal(table)
	if err != nil {
		return
	}

	txn := newTransaction().
		addKeyValue(utils.SchemaListKey(namespace), tableListVersion, &tableListProto).
		addKeyValue(utils.SchemaKey(namespace, table.Name), schemaVersion, &schemaProto)

	// for new columns, pre-create enum nodes
	preCreateEnumNodes(txn, namespace, &table, len(oldTable.Columns), len(table.Columns))
	return txn.writeTo(m.txnStore)
}

func preCreateEnumNodes(txn *transaction, namespace string, table *metaCom.Table, startColumnID int, endColumnID int) {
	for columnID := startColumnID; columnID < endColumnID; columnID++ {
		if table.Columns[columnID].IsEnumColumn() {
			// enum node list
			txn.addKeyValue(utils.EnumNodeListKey(namespace, table.Name, table.Incarnation, columnID), kv.UninitializedVersion, &pb.EnumNodeList{NumEnumNodes: 1}).
				// first node for enum column
				addKeyValue(utils.EnumNodeKey(namespace, table.Name, table.Incarnation, columnID, 0), kv.UninitializedVersion, &pb.EnumCases{Cases: []string{}})
		}
	}
}

func (m *tableSchemaMutator) GetHash(namespace string) (hash string, err error) {
	return getHash(m.txnStore, utils.SchemaListKey(namespace))
}

func (m *tableSchemaMutator) readSchema(namespace string, name string) (schemaProto pb.EntityConfig, version int, err error) {
	version, err = readValue(m.txnStore, utils.SchemaKey(namespace, name), &schemaProto)
	if common.IsNonExist(err) {
		err = metastore.ErrTableDoesNotExist
	}
	return
}
