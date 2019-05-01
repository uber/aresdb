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

	"math"
	"sort"

	memcom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/utils"
)

// ReplayRedoLogs loads data for the table Shard from disk store and recovers the Shard for serving.
func (shard *TableShard) ReplayRedoLogs() {
	timer := utils.GetReporter(shard.Schema.Schema.Name, shard.ShardID).GetTimer(utils.RecoveryLatency).Start()
	defer timer.Stop()

	utils.GetLogger().Infof("Replay redo logs for Shard %d of table %s",
		shard.ShardID, shard.Schema.Schema.Name)

	var redoLogFilePersisted int64
	var offsetPersisted uint32

	if backfillMgr := shard.LiveStore.BackfillManager; backfillMgr != nil {
		redoLogFilePersisted, offsetPersisted = backfillMgr.GetLatestRedoFileAndOffset()
	} else {
		redoLogFilePersisted, offsetPersisted, _, _ = shard.LiveStore.SnapshotManager.GetLastSnapshotInfo()
	}
	utils.GetLogger().Infof("Checkpointed redoLogFile=%d offset=%d", redoLogFilePersisted, offsetPersisted)

	// Replay redo logs to create LiveStore.
	nextUpsertBatch := shard.LiveStore.RedoLogManager.NextUpsertBatch()

	for {
		upsertBatch, redoLogFile, offset := nextUpsertBatch()

		if upsertBatch == nil {
			break
		}

		shard.LiveStore.WriterLock.Lock()

		// Put a 0 in maxEventTimePerFile in case this is redolog is full of backfill batches.
		shard.LiveStore.RedoLogManager.UpdateMaxEventTime(0, redoLogFile)

		// check if this batch has already been backfilled and persisted
		skipBackfillRows := redoLogFile < redoLogFilePersisted ||
			(redoLogFile == redoLogFilePersisted && offset <= offsetPersisted)

		_, err := shard.ApplyUpsertBatch(upsertBatch, redoLogFile, offset, skipBackfillRows)

		shard.LiveStore.WriterLock.Unlock()

		if err != nil {
			utils.GetLogger().With("err", err).Panic("Failed to apply upsert batch during recovery")
		}
	}

	// report redolog size after replay
	if fileRedologManager, ok := shard.LiveStore.RedoLogManager.(*fileRedologManager); ok {
		utils.GetReporter(shard.Schema.Schema.Name, shard.ShardID).GetGauge(utils.NumberOfRedologs).Update(float64(len(fileRedologManager.SizePerFile)))
		utils.GetReporter(shard.Schema.Schema.Name, shard.ShardID).GetGauge(utils.SizeOfRedologs).Update(float64(fileRedologManager.TotalRedoLogSize))
	}

	// proactively purge redo files
	if shard.LiveStore.BackfillManager != nil {
		shard.LiveStore.RedoLogManager.
			PurgeRedologFileAndData(shard.LiveStore.ArchivingCutoffHighWatermark, redoLogFilePersisted, offsetPersisted)
	}
}

func (shard *TableShard) cleanOldSnapshotAndLogs(redoLogFile int64, offset uint32) {
	tableName := shard.Schema.Schema.Name
	// snapshot won't care about the cutoff.
	if err := shard.LiveStore.RedoLogManager.PurgeRedologFileAndData(math.MaxUint32, redoLogFile, offset); err != nil {
		utils.GetLogger().With(
			"job", "snapshot_cleanup",
			"table", tableName).Errorf(
			"Purge redologs failed, shard: %d, error: %v", shard.ShardID, err)
	}
	// delete old snapshots
	if err := shard.diskStore.DeleteSnapshot(shard.Schema.Schema.Name, shard.ShardID, redoLogFile, offset); err != nil {
		utils.GetLogger().With(
			"job", "snapshot_cleanup",
			"table", tableName).Errorf(
			"Delete snapshots failed, shard: %d, error: %v", shard.ShardID, err)
	}
}

// LoadMetaData loads metadata for the table Shard from metastore.
func (shard *TableShard) LoadMetaData() {
	if shard.Schema.Schema.IsFactTable {
		cutoff, err := shard.metaStore.GetArchivingCutoff(shard.Schema.Schema.Name, shard.ShardID)
		if err != nil {
			utils.GetLogger().Panic(err)
		}

		shard.ArchiveStore.CurrentVersion = NewArchiveStoreVersion(cutoff, shard)

		// We set the archiving cutoff to the persisted value (CLW) in meta so recovery will apply
		// all items in redolog that have event time > CLW. The backfill job will ignore items in
		// the backfill that have associated CHW (cutoff high watermark) > persisted CHW.
		shard.LiveStore.ArchivingCutoffHighWatermark = cutoff
		shard.LiveStore.PrimaryKey.UpdateEventTimeCutoff(cutoff)

		// retrieve redoLog/offset checkpointed for backfill
		redoLog, offset, err := shard.metaStore.GetBackfillProgressInfo(shard.Schema.Schema.Name, shard.ShardID)
		if err != nil {
			utils.GetLogger().Panic(err)
		}

		shard.LiveStore.BackfillManager.LastRedoFile = redoLog
		shard.LiveStore.BackfillManager.LastBatchOffset = offset
	} else {
		redoLogFile, offset, batchID, lastRecord, err := shard.metaStore.GetSnapshotProgress(shard.Schema.Schema.Name, shard.ShardID)
		if err != nil {
			utils.GetLogger().Panic(err)
		}
		// retrieve latest snapshot info
		record := RecordID{BatchID: batchID, Index: lastRecord}
		shard.LiveStore.SnapshotManager.SetLastSnapshotInfo(redoLogFile, offset, record)
	}
}

// loadSnapshots load snapshots for dimension tables
func (m *memStoreImpl) loadSnapshots() {
	utils.GetLogger().Info("Start loading snapshots for all table shards")
	var wg sync.WaitGroup
	for table, tableSchema := range m.TableSchemas {
		if tableSchema.Schema.IsFactTable {
			continue
		}
		wg.Add(1)
		go func(tableName string) {
			tableShards := m.TableShards[tableName]
			for _, shard := range tableShards {
				utils.GetLogger().With(
					"job", "snapshot_load",
					"table", shard.Schema.Schema.Name,
					"shard", shard.ShardID).
					Info("Loading snapshots")
				if err := shard.LoadSnapshot(); err != nil {
					utils.GetLogger().With(
						"job", "snapshot_load",
						"table", shard.Schema.Schema.Name,
						"shard", shard.ShardID).Panic(err)
				}
				utils.GetLogger().With(
					"job", "snapshot_load",
					"table", shard.Schema.Schema.Name,
					"shard", shard.ShardID).
					Info("Loading snapshots done")
			}
			wg.Done()
		}(table)

	}
	wg.Wait()
	utils.GetLogger().Info("Finish loading snapshots for all table shards")
}

// replayRedoLogs replay redo logs for all tables in parallel.
func (m *memStoreImpl) replayRedoLogs() {
	utils.GetLogger().Info("Start replaying redo logs for all table shards")
	var wg sync.WaitGroup
	for table := range m.TableSchemas {
		wg.Add(1)
		go func(tableName string) {
			tableShards := m.TableShards[tableName]
			// Replay all redologs
			for _, shard := range tableShards {
				utils.GetLogger().With(
					"job", "replay_redo_logs",
					"table", shard.Schema.Schema.Name,
					"shard", shard.ShardID).
					Info("Replaying redo logs")
				shard.ReplayRedoLogs()
				utils.GetLogger().With(
					"job", "replay_redo_logs",
					"table", shard.Schema.Schema.Name,
					"shard", shard.ShardID).
					Info("Replaying redo logs done")
			}
			wg.Done()
		}(table)
	}
	wg.Wait()
	utils.GetLogger().Info("Finish replaying redo logs for all table shards")
}

// InitShards loads/recovers data for shards initially owned by the current instance.
// It also watches Shard ownership change events and handles them in a separate goroutine.
func (m *memStoreImpl) InitShards(schedulerOff bool) {
	for table, schema := range m.TableSchemas {
		shards, err := m.metaStore.GetOwnedShards(table)
		if err != nil {
			utils.GetLogger().Panic(err)
		}

		for _, shard := range shards {
			if err := m.LoadShard(schema, shard, false); err != nil {
				utils.GetLogger().Panic(err)
			}
		}
	}

	// tryPreload data according the column retention config and start the go routines
	// to do eviction and preloading.
	m.HostMemManager.Start()

	// load snapshot for dimension tables
	m.loadSnapshots()

	// start scheduler after we load all the metadata. This ensure we can start backfill job earlier to consume
	// the backfill queue.
	if !schedulerOff {
		// Start scheduler.
		utils.GetLogger().Infof("Starting archiving scheduler")
		m.GetScheduler().Start()
	} else {
		utils.GetLogger().Infof("Scheduler is off")
	}

	m.replayRedoLogs()

	// watch Shard ownership change
	shardOwnershipChangeEvents, done, err := m.metaStore.WatchShardOwnershipEvents()
	if err != nil {
		utils.GetLogger().Panic(utils.StackError(err, "Failed to watch Shard ownership change"))
	}

	// Shard ownership change handling
	go func() {
		for event := range shardOwnershipChangeEvents {
			if event.ShouldOwn {
				m.RLock()
				schema := m.TableSchemas[event.TableName]
				m.RUnlock()
				if schema == nil {
					utils.GetLogger().Panic(utils.StackError(nil, "Trying to load Shard %d of unknown table %s",
						event.Shard, event.TableName))
				}
				// This assumes that (certain) schema change must wait until the Shard
				// is fully loaded, which may take a while.
				err := m.LoadShard(schema, event.Shard, true)
				if err != nil {
					utils.GetLogger().Panic(err)
				}
			} else {
				// Unload the Shard.
				var shard *TableShard
				// Detach first.
				m.Lock()
				shards := m.TableShards[event.TableName]
				if shards != nil {
					shard = shards[event.Shard]
					delete(shards, event.Shard)
					utils.DeleteTableShardReporter(event.TableName, event.Shard)
				}
				m.Unlock()
				// Destruct.
				if shard != nil {
					shard.Destruct()
				}
				// Do not delete the file on diskstore.
			}
			done <- struct{}{}
		}
		close(done)
	}()
}

// LoadShard loads/recovers the specified Shard and attaches it to memStoreImpl for serving. If will load the metadata
// first and then replay redologs only if replayRedologs is true.
func (m *memStoreImpl) LoadShard(schema *TableSchema, shard int, replayRedologs bool) error {
	tableShard := NewTableShard(schema, m.metaStore, m.diskStore, m.HostMemManager, shard)
	tableShard.LoadMetaData()
	if replayRedologs {
		tableShard.ReplayRedoLogs()
	}

	m.Lock()
	defer m.Unlock()
	shardMap := m.TableShards[schema.Schema.Name]
	if shardMap == nil {
		shardMap = make(map[int]*TableShard)
		m.TableShards[schema.Schema.Name] = shardMap
	}
	if shardMap[shard] != nil {
		return utils.StackError(nil, "Shard %d of Table %s has already been loaded",
			shard, schema.Schema.Name)
	}
	shardMap[shard] = tableShard
	// Add reporter for current table and Shard.
	utils.AddTableShardReporter(schema.Schema.Name, shard)
	return nil
}

// LoadSnapshot load shard data from snapshot files
func (shard *TableShard) LoadSnapshot() error {
	loadTimer := utils.GetReporter(shard.Schema.Schema.Name, shard.ShardID).GetTimer(utils.SnapshotTimingLoad)
	start := utils.Now()
	defer func() {
		duration := utils.Now().Sub(start)
		loadTimer.Record(duration)
	}()

	redoLogFile, offset, _, lastReadRecord := shard.LiveStore.SnapshotManager.GetLastSnapshotInfo()
	if redoLogFile <= 0 {
		// no snapshot created yet
		return nil
	}
	tableName := shard.Schema.Schema.Name
	shardID := shard.ShardID

	utils.GetLogger().With(
		"job", "snapshot_load",
		"table", tableName,
		"shard", shardID).Info("Load data from snapshot")

	var batchIDs []int
	var err error
	if batchIDs, err = shard.diskStore.ListSnapshotBatches(tableName, shardID, redoLogFile, offset); err != nil {
		return err
	} else if len(batchIDs) == 0 {
		return utils.StackError(nil, "No snapshot file/directory found")
	}

	shard.LiveStore.WriterLock.Lock()
	defer shard.LiveStore.WriterLock.Unlock()
	for _, id := range batchIDs {
		batchID := int32(id)
		// find all columns in snapshot dir
		batchPos, err := shard.loadTableShardSnapshot(tableName, shardID, batchID, redoLogFile, offset)
		if err != nil {
			return err
		}
		if batchID == lastReadRecord.BatchID {
			batchPos = lastReadRecord.Index
		}
		shard.rebuildIndexForLiveStore(batchID, batchPos)
	}
	//reset back the read/write record position
	shard.LiveStore.Lock()
	shard.LiveStore.LastReadRecord = lastReadRecord
	shard.LiveStore.Unlock()
	shard.LiveStore.NextWriteRecord = lastReadRecord
	return nil
}

func (shard *TableShard) loadTableShardSnapshot(
	tableName string, shardID int,
	batchID int32, redoLogFile int64, offset uint32) (uint32, error) {

	shard.Schema.RLock()
	dataTypes := shard.Schema.ValueTypeByColumn
	defaultValues := shard.Schema.DefaultValues
	columns := shard.Schema.Schema.Columns
	shard.Schema.RUnlock()

	var err error
	var cols []int

	// find all columns in snapshot dir
	if cols, err = shard.diskStore.ListSnapshotVectorPartyFiles(tableName, shardID, redoLogFile, offset, int(batchID)); err != nil {
		return 0, err
	}
	var vp memcom.LiveVectorParty

	batch := shard.LiveStore.getOrCreateBatch(int32(batchID))
	defer batch.Unlock()
	for colID, column := range columns {
		utils.GetLogger().With(
			"job", "snapshot_load",
			"table", shard.Schema.Schema.Name,
			"shard", shardID,
			"batch", batchID,
			"column", colID).Info("Load snapshot column")

		index := sort.SearchInts(cols, colID)
		existing := index >= 0 && index < len(cols) && cols[index] == colID
		if column.Deleted || !existing {
			vp = nil
		} else {
			// found the column in snapshot, read from snapshot file
			vp = NewLiveVectorParty(batch.Capacity, dataTypes[colID], *defaultValues[colID], shard.HostMemoryManager)
			serializer := NewVectorPartySnapshotSerializer(shard, colID, int(batchID), 0, 0, redoLogFile, offset)
			if err := serializer.ReadVectorParty(vp); err != nil {
				return 0, err
			}
		}
		batch.Columns[colID] = vp
	}
	return uint32(batch.Capacity - 1), nil
}

func (shard *TableShard) rebuildIndexForLiveStore(batchID int32, lastRecord uint32) error {
	buildIndexTimer := utils.GetReporter(shard.Schema.Schema.Name, shard.ShardID).GetTimer(utils.SnapshotTimingBuildIndex)
	start := utils.Now()
	defer func() {
		duration := utils.Now().Sub(start)
		buildIndexTimer.Record(duration)
	}()

	utils.GetLogger().With(
		"job", "snapshot_load",
		"table", shard.Schema.Schema.Name).Info("Rebuilding index")

	batch := shard.LiveStore.Batches[batchID]
	primaryKeyBytes := shard.Schema.PrimaryKeyBytes
	primaryKeyColumns := shard.Schema.GetPrimaryKeyColumns()
	key := make([]byte, primaryKeyBytes)
	primaryKeyValues := make([]memcom.DataValue, len(primaryKeyColumns))

	var row uint32
	for row = 0; row <= lastRecord; row++ {
		for i, col := range primaryKeyColumns {
			primaryKeyValues[i] = batch.Columns[col].GetDataValue(int(row))
		}
		if err := GetPrimaryKeyBytes(primaryKeyValues, key); err != nil {
			return err
		}
		recordID := RecordID{
			BatchID: batchID,
			Index:   uint32(row),
		}
		found, _, err := shard.LiveStore.PrimaryKey.FindOrInsert(key, recordID, 0)
		if err != nil {
			return err
		} else if found {
			return utils.StackError(nil, "Duplicate primary key found during rebuild index")
		}
	}
	return nil
}
