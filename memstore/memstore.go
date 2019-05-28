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
	"sync"

	"fmt"
	"github.com/uber/aresdb/diskstore"
	"github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/metastore"
	"github.com/uber/aresdb/redolog"
	"github.com/uber/aresdb/utils"
)

// TableShardMemoryUsage contains memory usage for column memory and primary key memory usage
type TableShardMemoryUsage struct {
	ColumnMemory     map[string]*common.ColumnMemoryUsage `json:"cols"`
	PrimaryKeyMemory uint                                 `json:"pk"`
}

// MemStore defines the interface for managing multiple table shards in memory. This is for mocking
// in unit tests
type MemStore interface {
	// GetMemoryUsageDetails
	GetMemoryUsageDetails() (map[string]TableShardMemoryUsage, error)
	// GetScheduler returns the scheduler for scheduling archiving and backfill jobs.
	GetScheduler() Scheduler
	// GetTableShard gets the data for a pinned table Shard. Caller needs to unpin after use.
	GetTableShard(table string, shardID int) (*TableShard, error)
	// GetSchema returns schema for a table.
	GetSchema(table string) (*common.TableSchema, error)
	// GetSchemas returns all table schemas.
	GetSchemas() map[string]*common.TableSchema
	// FetchSchema fetches schema from metaStore and updates in-memory copy of table schema,
	// and set up watch channels for metaStore schema changes, used for bootstrapping mem store.
	FetchSchema() error
	// InitShards loads/recovers data for shards initially owned by the current instance.
	InitShards(schedulerOff bool)
	// HandleIngestion logs an upsert batch and applies it to the in-memory store.
	HandleIngestion(table string, shardID int, upsertBatch *common.UpsertBatch) error
	// Archive is the process moving stable records in fact tables from live batches to archive
	// batches.
	Archive(table string, shardID int, cutoff uint32, reporter ArchiveJobDetailReporter) error

	// Backfill is the process of merging records with event time older than cutoff with
	// archive batches.
	Backfill(table string, shardID int, reporter BackfillJobDetailReporter) error

	// Snapshot is the process to write the current content of dimension table live store in memory to disk.
	Snapshot(table string, shardID int, reporter SnapshotJobDetailReporter) error

	// Purge is the process to purge out of retention archive batches
	Purge(table string, shardID, batchIDStart, batchIDEnd int, reporter PurgeJobDetailReporter) error

	// Provide exclusive access to read/write data protected by MemStore.
	utils.RWLocker
}

// memStoreImpl implements the MemStore interface.
type memStoreImpl struct {
	// memStoreImpl mutex is used to protect the TableShards and TableSchemas maps.
	//
	// For Shard access:
	//   Readers/writers must call TableShard.liveStore.Users.Add(1)
	//   before releasing this mutex, and call
	//   TableShard.liveStore.Users.Done() after their businesses.
	//
	//   Table Shard deleter must detach the Shard first, and then call
	//   TableShard.liveStore.Users.Wait() before deleting the Shard.
	//
	// For schema access:
	//   User should lock the TableSchema before releasing this mutex.
	//
	sync.RWMutex
	// Table name and Shard ID as the map keys.
	TableShards map[string]map[int]*TableShard
	// Schema for all tables in the system. Schemas are not deleted for simplicity
	TableSchemas map[string]*common.TableSchema

	HostMemManager common.HostMemoryManager

	// reference to metaStore for registering watchers,
	// fetch latest schema and store Shard versions.
	metaStore            metastore.MetaStore
	diskStore            diskstore.DiskStore
	redologManagerMaster *redolog.RedoLogManagerMaster

	// each MemStore should only have one scheduler instance.
	scheduler Scheduler
}

func getTableShardKey(tableName string, shardID int) string {
	return fmt.Sprintf("%s_%d", tableName, shardID)
}

// NewMemStore creates a MemStore from the specified MetaStore.
func NewMemStore(metaStore metastore.MetaStore, diskStore diskstore.DiskStore, redologManagerMaster *redolog.RedoLogManagerMaster) MemStore {
	memStore := &memStoreImpl{
		TableShards:          make(map[string]map[int]*TableShard),
		TableSchemas:         make(map[string]*common.TableSchema),
		metaStore:            metaStore,
		diskStore:            diskStore,
		redologManagerMaster: redologManagerMaster,
	}
	// Create HostMemoryManager
	memStore.HostMemManager = NewHostMemoryManager(memStore, utils.GetConfig().TotalMemorySize)
	memStore.scheduler = newScheduler(memStore)
	return memStore
}

func (m *memStoreImpl) GetMemoryUsageDetails() (map[string]TableShardMemoryUsage, error) {
	archiveMemoryUsageByTableShard, err := m.HostMemManager.GetArchiveMemoryUsageByTableShard()
	if err != nil {
		return nil, err
	}

	totalMemoryUsageByTableShard := map[string]TableShardMemoryUsage{}

	tableShardsSnapshot := map[string][]int{}
	m.RLock()
	for tableName, shards := range m.TableShards {
		tableShardsSnapshot[tableName] = []int{}
		for shardID := range shards {
			tableShardsSnapshot[tableName] = append(tableShardsSnapshot[tableName], shardID)
		}
	}
	m.RUnlock()

	for tableName, shardIDs := range tableShardsSnapshot {
		for _, shardID := range shardIDs {
			tableShardKey := getTableShardKey(tableName, shardID)
			shard, err := m.GetTableShard(tableName, shardID)
			if err != nil {
				return totalMemoryUsageByTableShard, err
			}

			tableShardMemoryUsage := TableShardMemoryUsage{}
			tableShardMemoryUsage.ColumnMemory = map[string]*common.ColumnMemoryUsage{}

			// primary key memory usage
			shard.LiveStore.WriterLock.RLock()
			tableShardMemoryUsage.PrimaryKeyMemory = shard.LiveStore.PrimaryKey.AllocatedBytes()
			shard.LiveStore.WriterLock.RUnlock()

			// archive memory usage
			if archiveMemoryUsage, ok := archiveMemoryUsageByTableShard[tableShardKey]; ok {
				tableShardMemoryUsage.ColumnMemory = archiveMemoryUsage
			}

			// live store memory usage
			shard.getLiveMemoryUsageByColumns(tableShardMemoryUsage.ColumnMemory)

			totalMemoryUsageByTableShard[tableShardKey] = tableShardMemoryUsage
			shard.Users.Done()
		}
	}
	return totalMemoryUsageByTableShard, nil
}

func (shard *TableShard) getLiveMemoryUsageByColumns(columnMemory map[string]*common.ColumnMemoryUsage) {
	shard.Schema.RLock()
	valueTypeByColumn := shard.Schema.GetValueTypeByColumn()
	columnIDs := map[string]int{}
	for columnName, columnID := range shard.Schema.ColumnIDs {
		columnIDs[columnName] = columnID
	}
	shard.Schema.RUnlock()

	for columnName, columnID := range columnIDs {
		valueType := valueTypeByColumn[columnID]
		liveStoreMemory := shard.LiveStore.GetMemoryUsageForColumn(valueType, columnID)
		if memoryUsage, ok := columnMemory[columnName]; ok {
			memoryUsage.Live = uint(liveStoreMemory)
		} else {
			columnMemory[columnName] = &common.ColumnMemoryUsage{
				Live: uint(liveStoreMemory),
			}
		}
	}
}

// GetTableShard gets the data for a pinned table Shard. Caller needs to unpin after use.
func (m *memStoreImpl) GetTableShard(table string, shardID int) (*TableShard, error) {
	m.RLock()
	defer m.RUnlock()
	tableShardMap, ok := m.TableShards[table]

	if !ok {
		return nil, utils.StackError(nil, "Failed to get table Shard map for table %s", table)
	}
	tableShard, ok := tableShardMap[shardID]
	if !ok {
		return nil, utils.StackError(nil, "Failed to get Shard %d for table %s", shardID, table)
	}
	tableShard.Users.Add(1)
	return tableShard, nil
}

// GetSchema returns schema for a table.
func (m *memStoreImpl) GetSchema(table string) (*common.TableSchema, error) {
	m.RLock()
	defer m.RUnlock()
	schema, ok := m.TableSchemas[table]
	if !ok {
		return nil, utils.StackError(nil, "Failed to get table schema for table %s", table)
	}
	return schema, nil
}

// GetSchemas returns all table schemas. Callers need to hold a reader lock to access this function.
func (m *memStoreImpl) GetSchemas() map[string]*common.TableSchema {
	return m.TableSchemas
}

// GetScheduler returns the scheduler instance bound to the MemStore.
func (m *memStoreImpl) GetScheduler() Scheduler {
	return m.scheduler
}

// TryEvictBatchColumn tries to evict a column from a given table/Shard/batchID.
// Return values are the check for column is deleted or not and error.
func (m *memStoreImpl) TryEvictBatchColumn(table string, shardID int, batchID int32, columnID int) (bool, error) {
	tableShard, err := m.GetTableShard(table, shardID)
	if err != nil {
		return false, utils.StackError(err, "Failed to delete batch %d from Shard %d for table %s", batchID, shardID, table)
	}
	defer tableShard.Users.Done()

	currentVersion := tableShard.ArchiveStore.GetCurrentVersion()
	defer currentVersion.Users.Done()

	currentVersion.RLock()
	archivingBatch, ok := currentVersion.Batches[batchID]
	currentVersion.RUnlock()
	if !ok {
		utils.GetLogger().Debugf("Batch already got removed from memstore: table %s, shardID %d, batchID %d, columnID %d", table, shardID, batchID, columnID)
		return true, nil
	}

	if evictedVP := archivingBatch.TryEvict(columnID); evictedVP == nil {
		return false, nil
	}

	utils.GetLogger().Debugf("Successfully evict batch from memstore: table %s, shardID %d, batchID %d, columnID %d", table, shardID, batchID, columnID)
	return true, nil
}
