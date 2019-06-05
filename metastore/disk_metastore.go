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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/utils"
)

// meaningful defaults of table configurations.
const (
	DefaultBatchSize                      = 2097152
	DefaultArchivingDelayMinutes          = 1440
	DefaultArchivingIntervalMinutes       = 180
	DefaultBackfillIntervalMinutes        = 60
	DefaultBackfillMaxBufferSize    int64 = 4294967296
	DefaultBackfillThresholdInBytes int64 = 2097152
	DefaultBackfillStoreBatchSize         = 20000
	DefaultRecordRetentionInDays          = 90
	DefaultSnapshotIntervalMinutes        = 360                  // 6 hours
	DefaultSnapshotThreshold              = 3 * DefaultBatchSize // 3 batches
	DefaultRedologRotationInterval        = 10800                // 3 hours
	DefaultMaxRedoLogSize                 = 1 << 30              // 1 GB
)

// DefaultTableConfig represents default table config
var DefaultTableConfig = common.TableConfig{
	BatchSize:                DefaultBatchSize,
	ArchivingIntervalMinutes: DefaultArchivingIntervalMinutes,
	ArchivingDelayMinutes:    DefaultArchivingDelayMinutes,
	BackfillMaxBufferSize:    DefaultBackfillMaxBufferSize,
	BackfillIntervalMinutes:  DefaultBackfillIntervalMinutes,
	BackfillThresholdInBytes: DefaultBackfillThresholdInBytes,
	BackfillStoreBatchSize:   DefaultBackfillStoreBatchSize,
	RecordRetentionInDays:    DefaultRecordRetentionInDays,
	SnapshotIntervalMinutes:  DefaultSnapshotIntervalMinutes,
	SnapshotThreshold:        DefaultSnapshotThreshold,
	RedoLogRotationInterval:  DefaultRedologRotationInterval,
	MaxRedoLogFileSize:       DefaultMaxRedoLogSize,
}

// disk-based metastore implementation.
// all validation of user input (eg. table/column name and table/column struct) will be pushed to api layer,
// which is the earliest point of user input, all schemas inside system will be already valid,
// Note:
// There are four types of write calls to MetaStore, the handling of each is different:
// 1. Schema Changes
// 	synchronous, return after both writing to watcher channel and reading from done channel are done
// 2. Update EnumCases
// 	return after changes persisted in disk and writing to watcher channel; does not read from done channel
// 3. Adding Watchers
// 	3.1 for enum cases, create channels and push existing enum cases starting from start case to channel if any
//  3.2 for table list and table schema channels, create channels and return
// 4. Update configurations
//  configurations update including updates on archiving cutoff, snapshot version, archive batch version etc,
//  these changes does not need to be pushed to memstore.
// Operations involves writing to watcher channels (case 1 and 2), we need to enforce the order of changes pushed into channel,
// writeLock is introduced to enforce that.
// Other operations (case 3 and 4), we only need lock to protect internal data structure, a read write lock is used.
type diskMetaStore struct {
	sync.RWMutex
	utils.FileSystem

	// writeLock is to enforce single writer at a time
	// to make sure the same order of shema change when applied to
	// MemStore through watcher channel
	writeLock sync.Mutex

	// the base path for MetaStore in disk
	basePath string

	// tableListWatcher
	tableListWatcher chan<- []string
	// tableListDone is the channel for tracking whether watcher has
	// successfully got the table list change,
	// here we adopt a synchronous model for schema change.
	tableListDone <-chan struct{}

	// tableSchemaWatcher
	tableSchemaWatcher chan<- *common.Table
	// tableSchemaDone is the channel for tracking whether watcher has
	// successfully got the table schema change
	tableSchemaDone <-chan struct{}

	// enumDictWatchers
	// maps from tableName to columnName to watchers
	enumDictWatchers map[string]map[string]chan<- string
	// tableSchemaDone are the channels for tracking whether watcher has
	// successfully got the enum case change.
	enumDictDone map[string]map[string]<-chan struct{}

	// shardOwnershipWatcher
	shardOwnershipWatcher chan<- common.ShardOwnership
	// shardOwnershipDone is used for block waiting for the consumer to finish
	// processing each ownership change event.
	shardOwnershipDone <-chan struct{}
}

// ListTables list existing table names
func (dm *diskMetaStore) ListTables() ([]string, error) {
	return dm.listTables()
}

// GetTable return the table schema stored in metastore given tablename,
// return ErrTableDoesNotExist if table not exists.
func (dm *diskMetaStore) GetTable(name string) (*common.Table, error) {
	dm.RLock()
	defer dm.RUnlock()
	err := dm.tableExists(name)
	if err != nil {
		return nil, err
	}
	return dm.readSchemaFile(name)
}

// GetOwnedShards returns the list of shards that are owned by this instance.
func (dm *diskMetaStore) GetOwnedShards(table string) ([]int, error) {
	return []int{0}, nil
}

// GetEnumDict gets the enum cases for given tableName and columnName
func (dm *diskMetaStore) GetEnumDict(tableName, columnName string) ([]string, error) {
	dm.RLock()
	defer dm.RUnlock()
	if err := dm.enumColumnExists(tableName, columnName); err != nil {
		return nil, err
	}
	return dm.readEnumFile(tableName, columnName)
}

// GetArchivingCutoff gets the latest archiving cutoff for given table and shard.
func (dm *diskMetaStore) GetArchivingCutoff(tableName string, shard int) (uint32, error) {
	dm.RLock()
	defer dm.RUnlock()
	err := dm.shardExists(tableName, shard)
	if err != nil {
		return 0, err
	}
	file := dm.getShardVersionFilePath(tableName, shard)
	return dm.readVersion(file)
}

// GetSnapshotProgress gets the latest snapshot progress for given table and shard
func (dm *diskMetaStore) GetSnapshotProgress(tableName string, shard int) (int64, uint32, int32, uint32, error) {
	dm.RLock()
	defer dm.RUnlock()
	if err := dm.shardExists(tableName, shard); err != nil {
		return 0, 0, 0, 0, err
	}
	file := dm.getSnapshotRedoLogVersionAndOffsetFilePath(tableName, shard)
	return dm.readSnapshotRedoLogFileAndOffset(file)
}

// UpdateArchivingCutoff updates archiving cutoff for given table (fact table), shard
func (dm *diskMetaStore) UpdateArchivingCutoff(tableName string, shard int, cutoff uint32) error {
	dm.Lock()
	defer dm.Unlock()
	if err := dm.shardExists(tableName, shard); err != nil {
		return err
	}

	schema, err := dm.readSchemaFile(tableName)
	if err != nil {
		return err
	}

	if !schema.IsFactTable {
		return ErrNotFactTable
	}

	file := dm.getShardVersionFilePath(tableName, shard)
	return dm.writeArchivingCutoff(file, cutoff)
}

// UpdateSnapshotProgress update snapshot version for given table (dimension table), shard.
func (dm *diskMetaStore) UpdateSnapshotProgress(tableName string, shard int, redoLogFile int64, upsertBatchOffset uint32, lastReadBatchID int32, lastReadBatchOffset uint32) error {
	dm.Lock()
	defer dm.Unlock()
	if err := dm.shardExists(tableName, shard); err != nil {
		return err
	}

	schema, err := dm.readSchemaFile(tableName)
	if err != nil {
		return err
	}

	if schema.IsFactTable {
		return ErrNotDimensionTable
	}

	file := dm.getSnapshotRedoLogVersionAndOffsetFilePath(tableName, shard)
	return dm.writeSnapshotRedoLogVersionAndOffset(file, redoLogFile, upsertBatchOffset, lastReadBatchID, lastReadBatchOffset)
}

// Updates the latest redolog/offset that have been backfilled for the specified shard.
func (dm *diskMetaStore) UpdateBackfillProgress(table string, shard int, redoFile int64, offset uint32) error {
	utils.GetLogger().Debugf("Backfill checkpoint(table=%s shard=%d redoFile=%d offset=%d)", table, shard, redoFile, offset)

	dm.Lock()
	defer dm.Unlock()
	if err := dm.shardExists(table, shard); err != nil {
		return err
	}

	schema, err := dm.readSchemaFile(table)
	if err != nil {
		return err
	}

	if !schema.IsFactTable {
		return ErrNotFactTable
	}

	file := dm.getRedoLogVersionAndOffsetFilePath(table, shard)
	return dm.writeRedoLogVersionAndOffset(file, redoFile, offset)
}

// Retrieve the latest redolog/offset that have been backfilled for the specified shard.
func (dm *diskMetaStore) GetBackfillProgressInfo(table string, shard int) (int64, uint32, error) {
	dm.RLock()
	defer dm.RUnlock()
	if err := dm.shardExists(table, shard); err != nil {
		return 0, 0, err
	}

	file := dm.getRedoLogVersionAndOffsetFilePath(table, shard)
	return dm.readRedoLogFileAndOffset(file)
}

// Update ingestion commit offset, used for kafka like streaming ingestion
func (dm *diskMetaStore) UpdateRedoLogCommitOffset(table string, shard int, offset int64) error {
	dm.Lock()
	defer dm.Unlock()

	// No sanity check here for schema/fact table/directory, assuming all should be passed before calling this func
	file := dm.getIngestionCommitOffsetFilePath(table, shard)

	writer, err := dm.OpenFileForWrite(
		file,
		os.O_CREATE|os.O_TRUNC|os.O_WRONLY,
		0644,
	)

	if err != nil {
		return utils.StackError(err, "Failed to open ingestion commit offset file %s for write", file)
	}
	defer writer.Close()

	_, err = io.WriteString(writer, fmt.Sprintf("%d", offset))
	return err
}

// Get ingestion commit offset, used for kafka like streaming ingestion
func (dm *diskMetaStore) GetRedoLogCommitOffset(table string, shard int) (int64, error) {
	dm.RLock()
	defer dm.RUnlock()

	// No sanity check here for schema/fact table/directory, assuming all should be passed before calling this func
	file := dm.getIngestionCommitOffsetFilePath(table, shard)

	fileBytes, err := dm.ReadFile(file)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, utils.StackError(err, "Failed to open ingestion commit offset file %s", file)
	}

	var offset int64
	_, err = fmt.Fscanln(bytes.NewBuffer(fileBytes), &offset)
	if err != nil {
		return 0, utils.StackError(err, "Failed to read ingestion commit offset file %s", file)
	}
	return offset, nil
}

// Update ingestion checkpoint offset, used for kafka like streaming ingestion
func (dm *diskMetaStore) UpdateRedoLogCheckpointOffset(table string, shard int, offset int64) error {
	dm.Lock()
	defer dm.Unlock()

	// No sanity check here for schema/fact table/directory, assuming all should be passed before calling this func
	file := dm.getIngestionCheckpointOffsetFilePath(table, shard)

	writer, err := dm.OpenFileForWrite(
		file,
		os.O_CREATE|os.O_TRUNC|os.O_WRONLY,
		0644,
	)

	if err != nil {
		return utils.StackError(err, "Failed to open ingestion checkpoint offset file %s for write", file)
	}
	defer writer.Close()

	_, err = io.WriteString(writer, fmt.Sprintf("%d", offset))
	return err
}

// Get ingestion checkpoint offset, used for kafka like streaming ingestion
func (dm *diskMetaStore) GetRedoLogCheckpointOffset(table string, shard int) (int64, error) {
	dm.RLock()
	defer dm.RUnlock()

	// No sanity check here for schema/fact table/directory, assuming all should be passed before calling this func
	file := dm.getIngestionCheckpointOffsetFilePath(table, shard)

	fileBytes, err := dm.ReadFile(file)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, utils.StackError(err, "Failed to open ingestion checkpoint offset file %s", file)
	}

	var offset int64
	_, err = fmt.Fscanln(bytes.NewBuffer(fileBytes), &offset)
	if err != nil {
		return 0, utils.StackError(err, "Failed to read ingestion checkpoint offset file %s", file)
	}
	return offset, nil
}

// WatchTableListEvents register a watcher to table list change events,
// should only be called once,
// returns ErrWatcherAlreadyExist once watcher already exists
func (dm *diskMetaStore) WatchTableListEvents() (events <-chan []string, done chan<- struct{}, err error) {
	dm.Lock()
	defer dm.Unlock()
	if dm.tableListWatcher != nil {
		return nil, nil, ErrWatcherAlreadyExist
	}

	watcherChan, doneChan := make(chan []string), make(chan struct{})
	dm.tableListWatcher, dm.tableListDone = watcherChan, doneChan
	return watcherChan, doneChan, nil
}

// WatchTableSchemaEvents register a watcher to table schema change events,
// should be only called once,
// returns ErrWatcherAlreadyExist once watcher already exists
func (dm *diskMetaStore) WatchTableSchemaEvents() (events <-chan *common.Table, done chan<- struct{}, err error) {
	dm.Lock()
	defer dm.Unlock()
	if dm.tableSchemaWatcher != nil {
		return nil, nil, ErrWatcherAlreadyExist
	}

	watcherChan, doneChan := make(chan *common.Table), make(chan struct{})
	dm.tableSchemaWatcher, dm.tableSchemaDone = watcherChan, doneChan
	return watcherChan, doneChan, nil
}

// WatchEnumDictEvents register a watcher to enum cases change events for given table and column,
// returns
// 	ErrTableDoesNotExist, ErrColumnDoesNotExist, ErrNotEnumColumn, ErrWatcherAlreadyExist.
// if startCase is larger than the number of current existing enum cases, it will be just as if receiving from
// latest.
func (dm *diskMetaStore) WatchEnumDictEvents(table, column string, startCase int) (events <-chan string, done chan<- struct{}, err error) {
	dm.Lock()
	defer dm.Unlock()

	if err = dm.enumColumnExists(table, column); err != nil {
		return nil, nil, err
	}

	if _, exist := dm.enumDictWatchers[table]; !exist {
		dm.enumDictWatchers[table] = make(map[string]chan<- string)
		dm.enumDictDone[table] = make(map[string]<-chan struct{})
	}

	if _, exist := dm.enumDictWatchers[table][column]; exist {
		return nil, nil, ErrWatcherAlreadyExist
	}

	existingEnumCases, err := dm.readEnumFile(table, column)
	if err != nil {
		return nil, nil, err
	}

	channelCapacity := len(existingEnumCases) - startCase
	// if start is larger than length of existing enum cases
	// will treat as if sending from latest
	if channelCapacity <= 0 {
		channelCapacity = 1
	}

	watcherChan, doneChan := make(chan string, channelCapacity), make(chan struct{})
	dm.enumDictWatchers[table][column], dm.enumDictDone[table][column] = watcherChan, doneChan
	for start := startCase; start < len(existingEnumCases); start++ {
		watcherChan <- existingEnumCases[start]
	}
	return watcherChan, doneChan, nil
}

// WatchTableSchemaEvents register a watcher to table schema change events,
// should be only called once,
// returns ErrWatcherAlreadyExist once watcher already exists
func (dm *diskMetaStore) WatchShardOwnershipEvents() (events <-chan common.ShardOwnership, done chan<- struct{}, err error) {
	dm.Lock()
	defer dm.Unlock()
	if dm.shardOwnershipWatcher != nil {
		return nil, nil, ErrWatcherAlreadyExist
	}

	watcherChan, doneChan := make(chan common.ShardOwnership), make(chan struct{})
	dm.shardOwnershipWatcher, dm.shardOwnershipDone = watcherChan, doneChan
	return watcherChan, doneChan, nil
}

// CreateTable creates a new Table,
// returns
// 	ErrTableAlreadyExist if table already exists
func (dm *diskMetaStore) CreateTable(table *common.Table) (err error) {
	dm.writeLock.Lock()
	defer dm.writeLock.Unlock()

	var existingTables []string
	dm.Lock()
	defer func() {
		dm.Unlock()
		if err == nil {
			dm.pushSchemaChange(table)
			dm.pushShardOwnershipChange(table.Name)
		}
	}()

	existingTables, err = dm.listTables()
	if err != nil {
		return err
	}

	if utils.IndexOfStr(existingTables, table.Name) >= 0 {
		return ErrTableAlreadyExist
	}

	validator := NewTableSchameValidator()
	validator.SetNewTable(*table)
	err = validator.Validate()
	if err != nil {
		return err
	}

	if err = dm.MkdirAll(dm.getTableDirPath(table.Name), 0755); err != nil {
		return err
	}

	if err = dm.writeSchemaFile(table); err != nil {
		return err
	}

	// append enum case for enum column with default value
	for _, column := range table.Columns {
		if column.DefaultValue != nil && column.IsEnumColumn() {
			err = dm.writeEnumFile(table.Name, column.Name, []string{*column.DefaultValue})
			if err != nil {
				return err
			}
		}
	}

	// for single instance version, when creating table, create shard zero as well
	return dm.createShard(table.Name, table.IsFactTable, 0)
}

// UpdateTable update table configurations
// return
//  ErrTableDoesNotExist if table does not exist
func (dm *diskMetaStore) UpdateTableConfig(tableName string, config common.TableConfig) (err error) {
	dm.writeLock.Lock()
	defer dm.writeLock.Unlock()

	var table *common.Table
	dm.Lock()
	defer func() {
		dm.Unlock()
		if err == nil && table != nil {
			dm.pushSchemaChange(table)
		}
	}()

	if err = dm.tableExists(tableName); err != nil {
		return err
	}

	table, err = dm.readSchemaFile(tableName)
	if err != nil {
		return err
	}

	table.Config = config
	return dm.writeSchemaFile(table)
}

// UpdateTable updates table schema and config
// table passed in should have been validated against existing table schema
func (dm *diskMetaStore) UpdateTable(table common.Table) (err error) {
	dm.writeLock.Lock()
	defer dm.writeLock.Unlock()

	dm.Lock()
	defer func() {
		dm.Unlock()
		if err == nil {
			dm.pushSchemaChange(&table)
		}
	}()

	var existingTable *common.Table
	existingTable, err = dm.readSchemaFile(table.Name)
	if err != nil {
		return
	}

	if err = dm.writeSchemaFile(&table); err != nil {
		return err
	}

	// append enum case for enum column with default value for new columns
	for i := len(existingTable.Columns); i < len(table.Columns); i++ {
		column := table.Columns[i]
		if column.DefaultValue != nil && column.IsEnumColumn() {
			err = dm.writeEnumFile(table.Name, column.Name, []string{*column.DefaultValue})
			if err != nil {
				return
			}
		}
	}

	return
}

// DeleteTable deletes a table
// return
// 	ErrTableDoesNotExist if table does not exist
func (dm *diskMetaStore) DeleteTable(tableName string) (err error) {
	dm.writeLock.Lock()
	defer dm.writeLock.Unlock()

	var existingTables []string
	dm.Lock()
	defer func() {
		dm.Unlock()
		if dm.tableListWatcher != nil {
			dm.tableListWatcher <- existingTables
			<-dm.tableListDone
		}
	}()

	existingTables, err = dm.listTables()
	if err != nil {
		return utils.StackError(err, "Failed to list tables")
	}

	index := utils.IndexOfStr(existingTables, tableName)
	if index < 0 {
		return ErrTableDoesNotExist
	}

	if err = dm.removeTable(tableName); err != nil {
		return err
	}
	existingTables = append(existingTables[:index], existingTables[index+1:]...)
	return nil
}

// AddColumn adds a new column
// returns
// 	ErrTableDoesNotExist if table does not exist
// 	ErrColumnAlreadyExist if column already exists
func (dm *diskMetaStore) AddColumn(tableName string, column common.Column, appendToArchivingSortOrder bool) (err error) {
	dm.writeLock.Lock()
	defer dm.writeLock.Unlock()

	var table *common.Table
	dm.Lock()
	defer func() {
		dm.Unlock()
		if err == nil {
			dm.pushSchemaChange(table)
		}
	}()

	if err = dm.tableExists(tableName); err != nil {
		return err
	}

	if table, err = dm.readSchemaFile(tableName); err != nil {
		return err
	}
	return dm.addColumn(table, column, appendToArchivingSortOrder)
}

// UpdateColumn deletes a column.
// return
// 	ErrTableDoesNotExist if table does not exist.
// 	ErrColumnDoesNotExist if column does not exist.
func (dm *diskMetaStore) UpdateColumn(tableName string, columnName string, config common.ColumnConfig) (err error) {
	dm.writeLock.Lock()
	defer dm.writeLock.Unlock()

	var table *common.Table
	dm.Lock()
	defer func() {
		dm.Unlock()
		if err == nil {
			dm.pushSchemaChange(table)
		}
	}()

	if err = dm.tableExists(tableName); err != nil {
		return err
	}

	if table, err = dm.readSchemaFile(tableName); err != nil {
		return err
	}

	return dm.updateColumn(table, columnName, config)
}

// DeleteColumn deletes a column
// return
// 	ErrTableDoesNotExist if table not exist
// 	ErrColumnDoesNotExist if column not exist
func (dm *diskMetaStore) DeleteColumn(tableName string, columnName string) (err error) {
	dm.writeLock.Lock()
	defer dm.writeLock.Unlock()

	var table *common.Table
	dm.Lock()
	defer func() {
		dm.Unlock()
		if err == nil {
			dm.pushSchemaChange(table)
		}
	}()

	if err = dm.tableExists(tableName); err != nil {
		return err
	}

	if table, err = dm.readSchemaFile(tableName); err != nil {
		return err
	}

	return dm.removeColumn(table, columnName)
}

// ExtendEnumDict extends enum cases for given table column
func (dm *diskMetaStore) ExtendEnumDict(table, column string, enumCases []string) (enumIDs []int, err error) {
	dm.writeLock.Lock()
	defer dm.writeLock.Unlock()

	var existingCases []string
	newEnumCases := make([]string, 0, len(enumCases))

	dm.Lock()
	defer func() {
		dm.Unlock()
		if err == nil {
			if _, tableExist := dm.enumDictWatchers[table]; tableExist {
				if watcher, columnExist := dm.enumDictWatchers[table][column]; columnExist {
					for _, enumCase := range newEnumCases {
						watcher <- enumCase
					}
				}
			}
		}
	}()

	if err = dm.enumColumnExists(table, column); err != nil {
		return nil, err
	}

	existingCases, err = dm.readEnumFile(table, column)
	if err != nil {
		return nil, err
	}

	enumDict := make(map[string]int)
	for enumID, enumCase := range existingCases {
		enumDict[enumCase] = enumID
	}

	newEnumID := len(existingCases)

	enumIDs = make([]int, len(enumCases))
	for index, newCase := range enumCases {
		if enumID, exist := enumDict[newCase]; exist {
			enumIDs[index] = enumID
		} else {
			enumDict[newCase] = newEnumID
			newEnumCases = append(newEnumCases, newCase)
			enumIDs[index] = newEnumID
			newEnumID++
		}
	}

	if err = dm.writeEnumFile(table, column, newEnumCases); err != nil {
		return nil, err
	}

	utils.GetRootReporter().GetChildGauge(map[string]string{
		"table":      table,
		"columnName": column,
	}, utils.NumberOfEnumCasesPerColumn).Update(float64(newEnumID))

	return enumIDs, nil
}

// PurgeArchiveBatches deletes the archive batches' metadata with batchID within [batchIDStart, batchIDEnd)
func (dm *diskMetaStore) PurgeArchiveBatches(tableName string, shard, batchIDStart, batchIDEnd int) error {
	dm.Lock()
	defer dm.Unlock()

	if err := dm.shardExists(tableName, shard); err != nil {
		return err
	}

	batchFiles, err := dm.ReadDir(dm.getArchiveBatchDirPath(tableName, shard))
	if os.IsNotExist(err) {
		utils.GetLogger().Warnf("table %s shard %d does not exist", tableName, shard)
		return nil
	} else if err != nil {
		return utils.StackError(err, "failed to read batch dir, table: %s, shard: %d", tableName, shard)
	}

	for _, batchFile := range batchFiles {
		batchID, err := strconv.ParseInt(batchFile.Name(), 10, 32)
		if err != nil {
			return err
		}

		if batchID < int64(batchIDEnd) && batchID >= int64(batchIDStart) {
			path := dm.getArchiveBatchVersionFilePath(tableName, shard, int(batchID))
			err := dm.Remove(path)
			if os.IsNotExist(err) {
				utils.GetLogger().Warnf("batch %d of table %s, shard %d does not exist", batchID, tableName, shard)
			} else if err != nil {
				return utils.StackError(err, "failed to delete metadata, table: %s, shard: %d, batch: %d", tableName, shard, batchID)
			}
		}
	}

	return nil
}

// AddArchiveBatchVersion adds a new version to archive batch.
func (dm *diskMetaStore) AddArchiveBatchVersion(tableName string, shard, batchID int, version uint32, seqNum uint32, batchSize int) error {
	dm.Lock()
	defer dm.Unlock()

	if err := dm.shardExists(tableName, shard); err != nil {
		return err
	}

	path := dm.getArchiveBatchVersionFilePath(tableName, shard, batchID)

	if err := dm.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return utils.StackError(err, "Failed to create archive batch version directory")
	}

	writer, err := dm.OpenFileForWrite(
		path,
		os.O_WRONLY|os.O_APPEND|os.O_CREATE,
		0644,
	)

	if err != nil {
		return utils.StackError(
			err,
			"Failed to open archive batch version file, table: %s, shard: %d, batch: %d",
			tableName,
			shard,
			batchID,
		)
	}
	defer writer.Close()

	if seqNum > 0 {
		_, err = io.WriteString(writer, fmt.Sprintf("%d-%d,%d\n", version, seqNum, batchSize))
	} else {
		_, err = io.WriteString(writer, fmt.Sprintf("%d,%d\n", version, batchSize))
	}
	if err != nil {
		return utils.StackError(err, "Failed to write to batch version file, table: %s, shard: %d, batch: %d",
			tableName,
			shard,
			batchID,
		)
	}

	return nil
}

// GetArchiveBatchVersion gets the latest version <= given archiving/live cutoff
// all cutoff and batch versions are sorted in file per batch
// sample:
// 	/root_path/metastore/{$table}/shards/{$shard_id}/batches/{$batch_id}
//  version,size
//  1-0,10
//  2-0,20
//  2-1,26
//  4-0,20
//  5-0,20
//  5-1,25
//  5-2,38
// if given cutoff 6, returns 5-2,38
// if given cutoff 4, returns 4-0,20
// if given cutoff 0, returns 0-0, 0
func (dm *diskMetaStore) GetArchiveBatchVersion(table string, shard, batchID int, cutoff uint32) (uint32, uint32, int, error) {
	dm.RLock()
	defer dm.RUnlock()

	if err := dm.shardExists(table, shard); err != nil {
		return 0, 0, 0, err
	}

	batchVersionBytes, err := dm.ReadFile(dm.getArchiveBatchVersionFilePath(table, shard, batchID))
	if os.IsNotExist(err) {
		return 0, 0, 0, nil
	} else if err != nil {
		return 0, 0, 0, utils.StackError(err, "Failed to read batch")
	}

	batchVersionSizes := strings.Split(strings.TrimSuffix(string(batchVersionBytes), "\n"), "\n")

	var version uint64
	// do binary search to find the first cutoff that is larger than the specified cutoff
	firstIndex := sort.Search(len(batchVersionSizes), func(i int) bool {
		versionSizePair := strings.Split(batchVersionSizes[i], ",")

		// backward compatible: sequence number may not exist for old version
		if !strings.Contains(versionSizePair[0], "-") {
			version, err = strconv.ParseUint(versionSizePair[0], 10, 32)
		} else {
			versionSeqStr := strings.Split(versionSizePair[0], "-")
			version, err = strconv.ParseUint(versionSeqStr[0], 10, 32)
		}

		if err != nil {
			// this should never happen
			utils.GetLogger().With(
				"error", err.Error(),
				"table", table,
				"shard", shard,
				"batchID", batchID).
				Panic("Incorrect batch version")
		}
		return uint32(version) > cutoff
	})

	// all cutoffs larger than given cutoff
	if firstIndex == 0 {
		return 0, 0, 0, nil
	}

	versionSizePair := strings.Split(batchVersionSizes[firstIndex-1], ",")
	if len(versionSizePair) != 2 {
		return 0, 0, 0, utils.StackError(err, "Incorrect batch version and size pair, %s", batchVersionSizes[firstIndex-1])
	}

	var seqNum uint64
	if !strings.Contains(versionSizePair[0], "-") {
		version, err = strconv.ParseUint(versionSizePair[0], 10, 32)
		seqNum = 0
	} else {
		versionSeqStr := strings.Split(versionSizePair[0], "-")
		seqNum, err = strconv.ParseUint(versionSeqStr[1], 10, 32)
		if err != nil {
			return 0, 0, 0, utils.StackError(err, "Failed to parse batch sequence, %s", versionSizePair[0])
		}
		version, err = strconv.ParseUint(versionSeqStr[0], 10, 32)
	}

	if err != nil {
		return 0, 0, 0, utils.StackError(err, "Failed to parse batchVersion, %s", versionSizePair[0])
	}
	batchSize, err := strconv.ParseInt(versionSizePair[1], 10, 32)
	if err != nil {
		return 0, 0, 0, utils.StackError(err, "Failed to parse batchSize, %s", versionSizePair[1])
	}

	return uint32(version), uint32(seqNum), int(batchSize), nil
}

func (dm *diskMetaStore) pushSchemaChange(table *common.Table) {
	if dm.tableSchemaWatcher != nil {
		dm.tableSchemaWatcher <- table
		<-dm.tableSchemaDone
	}
}

func (dm *diskMetaStore) pushShardOwnershipChange(tableName string) {
	if dm.shardOwnershipWatcher != nil {
		dm.shardOwnershipWatcher <- common.ShardOwnership{
			TableName: tableName,
			Shard:     0,
			ShouldOwn: true}
		<-dm.shardOwnershipDone
	}
}

// listTable lists the table
func (dm *diskMetaStore) listTables() ([]string, error) {
	tableDirs, err := dm.ReadDir(dm.basePath)
	if err != nil {
		return nil, utils.StackError(err, "Failed to list tables")
	}
	tableNames := make([]string, len(tableDirs))
	for id, tableDir := range tableDirs {
		tableNames[id] = tableDir.Name()
	}
	return tableNames, nil
}

func (dm *diskMetaStore) removeTable(tableName string) error {
	if err := dm.RemoveAll(dm.getTableDirPath(tableName)); err != nil {
		return utils.StackError(err, "Failed to remove directory, table: %s", tableName)
	}

	// close all related enum dict watchers
	// make sure all producer have done producing and detach
	columnWatchers := dm.enumDictWatchers[tableName]
	doneWatchers := dm.enumDictDone[tableName]
	delete(dm.enumDictWatchers, tableName)
	delete(dm.enumDictDone, tableName)

	for columnName, watcher := range columnWatchers {
		close(watcher)
		// drain done channels for related enum watchers
		// to make sure all previous changes are done
		for range doneWatchers[columnName] {
		}
	}
	return nil
}

func (dm *diskMetaStore) addColumn(table *common.Table, column common.Column, appendToArchivingSortOrder bool) error {
	validator := NewTableSchameValidator()
	validator.SetOldTable(*table)

	newColumnID := len(table.Columns)
	table.Columns = append(table.Columns, column)
	if appendToArchivingSortOrder {
		table.ArchivingSortColumns = append(table.ArchivingSortColumns, newColumnID)
	}
	validator.SetNewTable(*table)
	err := validator.Validate()
	if err != nil {
		return err
	}

	if err := dm.writeSchemaFile(table); err != nil {
		return utils.StackError(err, "Failed to write schema file, table: %s", table.Name)
	}

	// if enum column, append a enum case for default value
	if column.DefaultValue != nil && column.IsEnumColumn() {
		return dm.writeEnumFile(table.Name, column.Name, []string{*column.DefaultValue})
	}

	return nil
}

func (dm *diskMetaStore) updateColumn(table *common.Table, columnName string, config common.ColumnConfig) (err error) {
	for id, column := range table.Columns {
		if column.Name == columnName {
			if column.Deleted {
				// continue looking since there could be reused column name
				// with different column id.
				continue
			}
			column.Config = config
			table.Columns[id] = column
			return dm.writeSchemaFile(table)
		}
	}
	return ErrColumnDoesNotExist
}

func (dm *diskMetaStore) removeColumn(table *common.Table, columnName string) error {
	for id, column := range table.Columns {
		if column.Name == columnName {
			if column.Deleted {
				// continue looking since there could be reused column name
				// with different column id
				continue
			}

			// trying to delete timestamp column from fact table
			if table.IsFactTable && id == 0 {
				return ErrDeleteTimeColumn
			}

			if utils.IndexOfInt(table.PrimaryKeyColumns, id) >= 0 {
				return ErrDeletePrimaryKeyColumn
			}

			column.Deleted = true
			table.Columns[id] = column
			if err := dm.writeSchemaFile(table); err != nil {
				return err
			}

			if column.IsEnumColumn() {
				dm.removeEnumColumn(table.Name, column.Name)
			}

			return nil
		}
	}
	return ErrColumnDoesNotExist
}

func (dm *diskMetaStore) getTableDirPath(tableName string) string {
	return filepath.Join(dm.basePath, tableName)
}

func (dm *diskMetaStore) getEnumDirPath(tableName string) string {
	return filepath.Join(dm.getTableDirPath(tableName), "enums")
}

func (dm *diskMetaStore) getEnumFilePath(tableName, columnName string) string {
	return filepath.Join(dm.getEnumDirPath(tableName), columnName)
}

func (dm *diskMetaStore) getSchemaFilePath(tableName string) string {
	return filepath.Join(dm.getTableDirPath(tableName), "schema")
}

func (dm *diskMetaStore) getShardsDirPath(tableName string) string {
	return filepath.Join(dm.getTableDirPath(tableName), "shards")
}

func (dm *diskMetaStore) getShardDirPath(tableName string, shard int) string {
	return filepath.Join(dm.getShardsDirPath(tableName), strconv.Itoa(shard))
}

func (dm *diskMetaStore) getShardVersionFilePath(tableName string, shard int) string {
	return filepath.Join(dm.getShardDirPath(tableName, shard), "version")
}

func (dm *diskMetaStore) getArchiveBatchVersionFilePath(tableName string, shard, batchID int) string {
	return filepath.Join(dm.getShardDirPath(tableName, shard), "batches", strconv.Itoa(batchID))
}

func (dm *diskMetaStore) getArchiveBatchDirPath(tableName string, shard int) string {
	return filepath.Join(dm.getShardDirPath(tableName, shard), "batches")
}

func (dm *diskMetaStore) getRedoLogVersionAndOffsetFilePath(tableName string, shard int) string {
	return filepath.Join(dm.getShardDirPath(tableName, shard), "redolog-offset")
}

func (dm *diskMetaStore) getSnapshotRedoLogVersionAndOffsetFilePath(tableName string, shard int) string {
	return filepath.Join(dm.getShardDirPath(tableName, shard), "snapshot")
}

// Get file path which stores the ingestion commit offset, mainly used for kafka or other streaming based ingestion
func (dm *diskMetaStore) getIngestionCommitOffsetFilePath(tableName string, shard int) string {
	return filepath.Join(dm.getShardDirPath(tableName, shard), "commit-offset")
}

// Get file path which stores the ingestion checkpoint offset, mainly used fo kafka or other streaming based ingestion
func (dm *diskMetaStore) getIngestionCheckpointOffsetFilePath(tableName string, shard int) string {
	return filepath.Join(dm.getShardDirPath(tableName, shard), "checkpoint-offset")
}

// readEnumFile reads the enum cases from file.
func (dm *diskMetaStore) readEnumFile(tableName, columnName string) ([]string, error) {
	enumBytes, err := dm.ReadFile(dm.getEnumFilePath(tableName, columnName))
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil
		}
		return nil,
			utils.StackError(err,
				"Failed to read enum file, table: %s, column: %s",
				tableName,
				columnName,
			)
	}
	return strings.Split(strings.TrimSuffix(string(enumBytes), common.EnumDelimiter), common.EnumDelimiter), nil
}

// writeEnumFile append enum cases to existing file
func (dm *diskMetaStore) writeEnumFile(tableName, columnName string, enumCases []string) error {
	if len(enumCases) == 0 {
		return nil
	}
	err := dm.MkdirAll(dm.getEnumDirPath(tableName), 0755)
	if err != nil {
		return utils.StackError(err, "Failed to create enums directory")
	}

	writer, err := dm.OpenFileForWrite(
		dm.getEnumFilePath(tableName, columnName),
		os.O_WRONLY|os.O_APPEND|os.O_CREATE,
		0644,
	)
	if err != nil {
		return utils.StackError(err, "Failed to open enum file, table: %s, column: %s", tableName, columnName)
	}
	defer writer.Close()

	_, err = io.WriteString(writer, fmt.Sprintf("%s%s", strings.Join(enumCases, common.EnumDelimiter), common.EnumDelimiter))
	if err != nil {
		return utils.StackError(err, "Failed to write enum cases, table: %s, column: %s", tableName, columnName)
	}

	return nil
}

// readSchemaFile reads the schema file for given table.
func (dm *diskMetaStore) readSchemaFile(tableName string) (*common.Table, error) {
	jsonBytes, err := dm.ReadFile(dm.getSchemaFilePath(tableName))
	if err != nil {
		return nil, utils.StackError(
			err,
			"Failed to read schema file, table: %s",
			tableName,
		)
	}
	var table common.Table
	table.Config = DefaultTableConfig

	err = json.Unmarshal(jsonBytes, &table)
	if err != nil {
		return nil, utils.StackError(
			err,
			"Failed to unmarshal table schema, table: %s",
			tableName,
		)
	}

	return &table, nil
}

// writeSchemaFile reads the schema file for given table.
func (dm *diskMetaStore) writeSchemaFile(table *common.Table) error {
	tableSchemaBytes, err := json.MarshalIndent(table, "", "  ")
	if err != nil {
		return utils.StackError(err, "Failed to marshal schema")
	}

	writer, err := dm.OpenFileForWrite(
		dm.getSchemaFilePath(table.Name),
		os.O_WRONLY|os.O_TRUNC|os.O_CREATE,
		0644,
	)

	if err != nil {
		return utils.StackError(
			err,
			"Failed to open schema file for write, table: %s",
			table.Name,
		)
	}

	defer writer.Close()
	_, err = writer.Write(tableSchemaBytes)
	return err
}

// readVersion reads the version from a given version file.
func (dm *diskMetaStore) readVersion(file string) (uint32, error) {
	fileBytes, err := dm.ReadFile(file)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, utils.StackError(err, "Failed to open version file %s", file)
	}

	var version uint32
	_, err = fmt.Fscanln(bytes.NewBuffer(fileBytes), &version)
	if err != nil {
		return 0, utils.StackError(err, "Failed to read version file %s", file)
	}
	return version, nil
}

// readRedoLogFileAndOffset reads the redo log file and offset from the file.
func (dm *diskMetaStore) readRedoLogFileAndOffset(filePath string) (int64, uint32, error) {
	bytes, err := dm.ReadFile(filePath)
	if os.IsNotExist(err) {
		return 0, 0, nil
	} else if err != nil {
		return 0, 0, utils.StackError(err, "Failed to read file:%s\n", filePath)
	}

	redoLogAndOffset := strings.Split(strings.TrimSuffix(string(bytes), "\n"), ",")

	if len(redoLogAndOffset) < 2 {
		return 0, 0, utils.StackError(nil, "Invalid redo log and offset file:%s:not enough strings\n", filePath)
	}

	var redoLogVersion int64
	redoLogVersion, err = strconv.ParseInt(redoLogAndOffset[0], 10, 64)
	if err != nil {
		return 0, 0, utils.StackError(err, "Invalid redo log and offset file:%s:invalid redo log file\n", filePath)
	}

	offset, err := strconv.ParseUint(redoLogAndOffset[1], 10, 32)
	if err != nil {
		return 0, 0, utils.StackError(err, "Invalid redo log and offset file:%s:invalid offset\n", filePath)
	}

	return redoLogVersion, uint32(offset), nil
}

// readSnapshotRedoLogFileAndOffset reads the redo log file and offset from the file.
func (dm *diskMetaStore) readSnapshotRedoLogFileAndOffset(filePath string) (int64, uint32, int32, uint32, error) {
	bytes, err := dm.ReadFile(filePath)
	if os.IsNotExist(err) {
		return 0, 0, 0, 0, nil
	} else if err != nil {
		return 0, 0, 0, 0, utils.StackError(err, "Failed to read file:%s\n", filePath)
	}

	snapshotInfo := strings.Split(strings.TrimSuffix(string(bytes), "\n"), ",")

	if len(snapshotInfo) < 4 {
		return 0, 0, 0, 0, utils.StackError(nil, "Invalid snapshot redolog file:%s:not enough strings\n", filePath)
	}

	var redoLogVersion int64
	redoLogVersion, err = strconv.ParseInt(snapshotInfo[0], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, utils.StackError(err, "Invalid sanshot redolog file:%s:invalid redo log file\n", filePath)
	}

	offset, err := strconv.ParseUint(snapshotInfo[1], 10, 32)
	if err != nil {
		return 0, 0, 0, 0, utils.StackError(err, "Invalid snapshot redolog file:%s:invalid offset\n", filePath)
	}

	batchID, err := strconv.ParseInt(snapshotInfo[2], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, utils.StackError(err, "Invalid snapshot redolog file:%s:invalid batch id\n", filePath)
	}

	index, err := strconv.ParseUint(snapshotInfo[3], 10, 32)
	if err != nil {
		return 0, 0, 0, 0, utils.StackError(err, "Invalid snapshot redolog file:%s:invalid index\n", filePath)
	}

	return redoLogVersion, uint32(offset), int32(batchID), uint32(index), nil
}

// writeArchivingCutoff writes the version to a given file.
func (dm *diskMetaStore) writeArchivingCutoff(file string, version uint32) error {
	if err := dm.MkdirAll(filepath.Dir(file), 0755); err != nil {
		return utils.StackError(err, "Failed to create version directory")
	}

	writer, err := dm.OpenFileForWrite(
		file,
		os.O_CREATE|os.O_TRUNC|os.O_WRONLY,
		0644,
	)

	if err != nil {
		return utils.StackError(err, "Failed to open version file %s for write", file)
	}
	defer writer.Close()

	_, err = io.WriteString(writer, fmt.Sprintf("%d", version))
	return err
}

// writeRedoLogVersionAndOffset writes redolog&offset to a given file.
func (dm *diskMetaStore) writeRedoLogVersionAndOffset(file string, redoLogFile int64, upsertBatchOffset uint32) error {
	if err := dm.MkdirAll(filepath.Dir(file), 0755); err != nil {
		return utils.StackError(err, "Failed to create redo log version and upsert batch offset directory")
	}

	writer, err := dm.OpenFileForWrite(
		file,
		os.O_CREATE|os.O_TRUNC|os.O_WRONLY,
		0644,
	)

	if err != nil {
		return utils.StackError(err, "Failed to open redo log version and upsert batch offset file %s for write", file)
	}
	defer writer.Close()

	_, err = io.WriteString(writer, fmt.Sprintf("%d,%d", redoLogFile, upsertBatchOffset))
	return err
}

// writeSnapshotRedoLogVersionAndOffset writes redolog&offset and last record position to a given file.
func (dm *diskMetaStore) writeSnapshotRedoLogVersionAndOffset(file string, redoLogFile int64, upsertBatchOffset uint32, lastReadBatchID int32, lastReadBatchOffset uint32) error {
	if err := dm.MkdirAll(filepath.Dir(file), 0755); err != nil {
		return utils.StackError(err, "Failed to create snapshot redo log version and upsert batch offset directory")
	}

	writer, err := dm.OpenFileForWrite(
		file,
		os.O_CREATE|os.O_TRUNC|os.O_WRONLY,
		0644,
	)

	if err != nil {
		return utils.StackError(err, "Failed to open snapshot redo log version and upsert batch offset file %s for write", file)
	}
	defer writer.Close()

	_, err = io.WriteString(writer, fmt.Sprintf("%d,%d,%d,%d", redoLogFile, upsertBatchOffset, lastReadBatchID, lastReadBatchOffset))
	return err
}

// closeEnumWatcher try to close enum watcher and delete enum file
func (dm *diskMetaStore) removeEnumColumn(tableName, columnName string) {
	if _, tableExist := dm.enumDictWatchers[tableName]; tableExist {
		watcher, watcherExist := dm.enumDictWatchers[tableName][columnName]
		if watcherExist {
			doneChan, _ := dm.enumDictDone[tableName][columnName]
			delete(dm.enumDictWatchers[tableName], columnName)
			delete(dm.enumDictDone[tableName], columnName)
			close(watcher)
			// drain up done channel for enum column
			// to make sure previous changes are processed
			for range doneChan {
			}
		}
	}

	if err := dm.Remove(dm.getEnumFilePath(tableName, columnName)); err != nil {
		//TODO: log an error and alert.
	}
}

// tableExists checks whether table exists,
// return ErrTableDoesNotExist.
func (dm *diskMetaStore) tableExists(tableName string) error {
	_, err := dm.Stat(dm.getSchemaFilePath(tableName))
	if os.IsNotExist(err) {
		return ErrTableDoesNotExist
	} else if err != nil {
		return utils.StackError(err, "Failed to read directory, table: %s", tableName)
	}
	return nil
}

// enumColumnExists checks whether column exists and it is a enum column,
// return ErrTableDoesNotExist, ErrColumnDoesNotExist, ErrNotEnumColumn.
func (dm *diskMetaStore) enumColumnExists(tableName string, columnName string) error {
	if err := dm.tableExists(tableName); err != nil {
		return err
	}

	table, err := dm.readSchemaFile(tableName)
	if err != nil {
		return err
	}

	for _, column := range table.Columns {
		if column.Name == columnName {
			if column.Deleted {
				// continue since column name can be reused
				// with different id
				continue
			}

			if !column.IsEnumColumn() {
				return ErrNotEnumColumn
			}

			return nil
		}
	}
	return ErrColumnDoesNotExist
}

// shardExists checks whether shard exists,
// return ErrShardDoesNotExist.
func (dm *diskMetaStore) shardExists(tableName string, shard int) error {
	if err := dm.tableExists(tableName); err != nil {
		return err
	}

	_, err := dm.Stat(dm.getShardDirPath(tableName, shard))
	if os.IsNotExist(err) {
		return ErrShardDoesNotExist
	} else if err != nil {
		return utils.StackError(err, "Failed to read directory, table: %s, shard: %d", tableName, shard)
	}

	return nil
}

// createShard assume table is created already
func (dm *diskMetaStore) createShard(tableName string, isFactTable bool, shard int) error {
	var err error
	if isFactTable {
		// only fact table have archive batches directory
		err = dm.MkdirAll(filepath.Join(dm.getShardDirPath(tableName, 0), "batches"), 0755)
	} else {
		err = dm.MkdirAll(dm.getShardDirPath(tableName, 0), 0755)
	}
	if err != nil {
		return utils.StackError(err, "Failed to create shard directory, table: %s, shard: %d", tableName, shard)
	}
	return nil
}

// NewDiskMetaStore creates a new disk based metastore
func NewDiskMetaStore(basePath string) (common.MetaStore, error) {
	metaStore := &diskMetaStore{
		FileSystem:       utils.OSFileSystem{},
		basePath:         basePath,
		writeLock:        sync.Mutex{},
		enumDictWatchers: make(map[string]map[string]chan<- string),
		enumDictDone:     make(map[string]map[string]<-chan struct{}),
	}
	err := metaStore.MkdirAll(basePath, 0755)
	if err != nil {
		return nil, utils.StackError(err, "Failed to make base directory for metastore, path: %s", basePath)
	}
	return metaStore, nil
}
