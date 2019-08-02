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
	"github.com/uber/aresdb/memstore/vectors"
	"sort"

	"github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/utils"
	"time"

	memCom "github.com/uber/aresdb/memstore/common"
)

// Backfill is the process of merging records with event time older than cutoff with
// archive batches.
func (m *memStoreImpl) Backfill(table string, shardID int, reporter BackfillJobDetailReporter) error {
	backfillTimer := utils.GetReporter(table, shardID).GetTimer(utils.BackfillTimingTotal)
	start := utils.Now()
	jobKey := getIdentifier(table, shardID, memCom.BackfillJobType)

	defer func() {
		duration := utils.Now().Sub(start)
		backfillTimer.Record(duration)
		reporter(jobKey, func(status *BackfillJobDetail) {
			status.LastDuration = duration
		})
		utils.GetReporter(table, shardID).
			GetCounter(utils.BackfillCount).Inc(1)
	}()

	shard, err := m.GetTableShard(table, shardID)
	if err != nil {
		utils.GetLogger().With("table", table, "shard", shardID, "error", err).Warn("Failed to find shard, is it deleted?")
		return nil
	}

	defer shard.Users.Done()

	backfillMgr := shard.LiveStore.BackfillManager
	backfillBatches, currentRedoFile, currentBatchOffset := backfillMgr.StartBackfill()

	// no data to backfill: checkpoint if applicable
	if backfillBatches == nil {
		backfillMgr.Done(currentRedoFile, currentBatchOffset, shard.metaStore)
		reporter(jobKey, func(status *BackfillJobDetail) {
			status.RedologFile = currentRedoFile
			status.BatchOffset = currentBatchOffset
			status.Current = 0
			status.Total = 0
			status.NumAffectedDays = 0
			status.NumRecords = 0
		})
		return nil
	}

	backfillPatches, err := createBackfillPatches(backfillBatches, reporter, jobKey)
	if err != nil {
		return err
	}

	if err = shard.createNewArchiveStoreVersionForBackfill(
		backfillPatches, reporter, jobKey); err != nil {
		return err
	}

	// checkpoint backfill progress
	backfillMgr.Done(currentRedoFile, currentBatchOffset, shard.metaStore)

	// Wait for queries in other goroutines to prevent archiving from prematurely purging the old version.
	reporter(jobKey, func(status *BackfillJobDetail) {
		status.RedologFile = currentRedoFile
		status.BatchOffset = currentBatchOffset
		status.Stage = BackfillPurge
	})

	// Archiving cutoff won't change during backfill, so it's safe to use current version's cutoff.
	if err := shard.LiveStore.RedoLogManager.
		CheckpointRedolog(shard.ArchiveStore.CurrentVersion.ArchivingCutoff, backfillMgr.LastRedoFile,
			backfillMgr.LastBatchOffset); err != nil {
		return err
	}

	reporter(jobKey, func(status *BackfillJobDetail) {
		status.Stage = BackfillComplete
	})

	return nil
}

func (shard *TableShard) createNewArchiveStoreVersionForBackfill(
	backfillPatches map[int32]*backfillPatch, reporter BackfillJobDetailReporter, jobKey string) (err error) {
	// Block column deletion
	shard.columnDeletion.Lock()
	defer shard.columnDeletion.Unlock()

	// Snapshot schema
	shard.Schema.RLock()
	columnDeletions := shard.Schema.GetColumnDeletions()
	sortColumns := shard.Schema.Schema.ArchivingSortColumns
	primaryKeyColumns := shard.Schema.Schema.PrimaryKeyColumns
	dataTypes := shard.Schema.ValueTypeByColumn
	defaultValues := shard.Schema.DefaultValues
	numColumns := len(shard.Schema.ValueTypeByColumn)
	shard.Schema.RUnlock()

	var numAffectedDays int
	dayIdx := 1
	reporter(jobKey, func(status *BackfillJobDetail) {
		status.Stage = BackfillApplyPatch
		status.Current = 0
		status.Total = len(backfillPatches)
	})

	lockTimer := utils.GetReporter(shard.Schema.Schema.Name, shard.ShardID).
		GetTimer(utils.BackfillLockTiming)
	var totalLockDuration time.Duration
	defer func() {
		lockTimer.Record(totalLockDuration)
		reporter(jobKey, func(status *BackfillJobDetail) {
			status.LockDuration = totalLockDuration
		})
	}()

	// Only those batches that are affected and changed need to be cleaned.
	for day, patch := range backfillPatches {
		baseBatch := shard.ArchiveStore.CurrentVersion.RequestBatch(day)

		var requestedVPs []vectors.ArchiveVectorParty
		for columnID := 0; columnID < numColumns; columnID++ {
			requestedVP := baseBatch.RequestVectorParty(columnID)
			requestedVP.WaitForDiskLoad()
			requestedVPs = append(requestedVPs, requestedVP)
		}

		backfillCtx := newBackfillContext(baseBatch, patch, shard.Schema, columnDeletions, sortColumns,
			primaryKeyColumns, dataTypes, defaultValues, shard.HostMemoryManager)

		// Real backfill implementation.
		if err = backfillCtx.backfill(reporter, jobKey); err != nil {
			UnpinVectorParties(requestedVPs)
			backfillCtx.release()
			return
		}

		if backfillCtx.okForEarlyUnpin {
			UnpinVectorParties(requestedVPs)
		}
		backfillCtx.release()

		oldVersion := shard.ArchiveStore.CurrentVersion
		newVersion := NewArchiveStoreVersion(oldVersion.ArchivingCutoff, shard)

		var affected bool
		var purgeOldBatch bool
		if len(backfillCtx.columnsToPurge) == 0 {
			// Batch is clean, we can copy the old batch to new version directly.
			newVersion.Batches[day] = backfillCtx.base
			// clean pointer in cloned batch.
			backfillCtx.new.Columns = nil
		} else {
			affected = true
			purgeOldBatch = true
			numAffectedDays++
			newVersion.Batches[day] = backfillCtx.new
			if err = newVersion.Batches[day].WriteToDisk(); err != nil {
				return
			}
			if err = shard.metaStore.AddArchiveBatchVersion(
				shard.Schema.Schema.Name, shard.ShardID, int(day), newVersion.Batches[day].Version,
				newVersion.Batches[day].SeqNum, newVersion.Batches[day].Size); err != nil {
				return
			}
		}

		lockStart := utils.Now()
		// Copy other batches in old version to new version.
		oldVersion.RLock()
		for oldDay, oldBatch := range oldVersion.Batches {
			if oldDay != day {
				newVersion.Batches[oldDay] = oldBatch
			}
		}
		oldVersion.RUnlock()

		// switch to new version
		shard.ArchiveStore.Lock()
		shard.ArchiveStore.CurrentVersion = newVersion
		shard.ArchiveStore.Unlock()

		if !backfillCtx.okForEarlyUnpin {
			UnpinVectorParties(requestedVPs)
		}

		reporter(jobKey, func(status *BackfillJobDetail) {
			status.NumAffectedDays = numAffectedDays
			utils.GetReporter(shard.Schema.Schema.Name, shard.ShardID).GetGauge(utils.BackfillAffectedDays).
				Update(float64(status.NumAffectedDays))
		})

		oldVersion.Users.Wait()
		totalLockDuration += utils.Now().Sub(lockStart)

		oldBatch := backfillCtx.base
		// Purge batches on disk.
		if purgeOldBatch {
			if shard.options.bootstrapToken.AcquireToken(shard.Schema.Schema.Name, uint32(shard.ShardID)) {
				err = shard.diskStore.DeleteBatchVersions(shard.Schema.Schema.Name, shard.ShardID,
					int(oldBatch.BatchID), oldBatch.Version, oldBatch.SeqNum)
				shard.options.bootstrapToken.ReleaseToken(shard.Schema.Schema.Name, uint32(shard.ShardID))
				if err != nil {
					return
				}
			}
		}

		// Purge columns in memory.
		for _, column := range backfillCtx.columnsToPurge {
			column.SafeDestruct()
		}

		// Report memory usage.
		newVersion.Users.Add(1)
		if affected {
			newVersion.RLock()
			batch := newVersion.Batches[day]
			newVersion.RUnlock()
			batch.RLock()
			for columnID, column := range batch.Columns {
				// Do the nil check in case column is evicted.
				if column != nil {
					bytes := column.GetBytes()
					shard.HostMemoryManager.ReportManagedObject(
						shard.Schema.Schema.Name, shard.ShardID, int(day), columnID, bytes)
				}
			}
			batch.RUnlock()
		}
		newVersion.Users.Done()
		shard.HostMemoryManager.ReportUnmanagedSpaceUsageChange(-backfillCtx.unmanagedMemoryBytes)

		reporter(jobKey, func(status *BackfillJobDetail) {
			status.Current = dayIdx
		})
		dayIdx++
	}

	return
}

// patch stores records to be patched onto a archive batch.
// The records are identified by upsert batch idx and row number within
// the upsert batch
type backfillPatch struct {
	recordIDs []memCom.RecordID
	// For convenience.
	backfillBatches []*memCom.UpsertBatch
}

// createBackfillPatches groups records in upsert batches by day and put them into backfillPatches.
// Records in each backfillPatch are identified by RecordID where BatchID is the upsert batch index
// index is the row within the upsert batch.
func createBackfillPatches(backfillBatches []*memCom.UpsertBatch, reporter BackfillJobDetailReporter, jobKey string) (map[int32]*backfillPatch, error) {
	numBatches := len(backfillBatches)
	var numRecordsBackfilled int
	reporter(jobKey, func(status *BackfillJobDetail) {
		status.Stage = BackfillCreatePatch
		status.Current = 0
		status.Total = numBatches
	})

	backfillPatches := make(map[int32]*backfillPatch)

	for upsertBatchIdx, backfillBatch := range backfillBatches {
		eventColumnIndex := backfillBatch.GetEventColumnIndex()
		if eventColumnIndex == -1 {
			return nil, utils.StackError(nil, "Event column does not exist for backfill batch %v",
				backfillBatch)
		}

		for row := 0; row < backfillBatch.NumRows; row++ {
			value, valid, err := backfillBatch.GetValue(row, eventColumnIndex)
			if err != nil {
				return nil, utils.StackError(err, "Failed to get event time for row %d", row)
			}
			if !valid {
				return nil, utils.StackError(err, "Event time for row %d is null", row)
			}
			eventTime := *(*uint32)(value)

			day := int32(eventTime / 86400)
			patch, exists := backfillPatches[day]
			if !exists {
				backfillPatches[day] = &backfillPatch{
					backfillBatches: backfillBatches,
				}
				patch = backfillPatches[day]
			}
			patch.recordIDs = append(patch.recordIDs,
				memCom.RecordID{BatchID: int32(upsertBatchIdx), Index: uint32(row)})
			numRecordsBackfilled++
		}

		reporter(jobKey, func(status *BackfillJobDetail) {
			status.Current = upsertBatchIdx + 1
		})
	}

	reporter(jobKey, func(status *BackfillJobDetail) {
		status.NumRecords = numRecordsBackfilled
	})
	return backfillPatches, nil
}

// backfillContext carries all context information used during backfill for a single day.
type backfillContext struct {
	// temporary live store to hold data to be later on merged with archive batch.
	backfillStore *LiveStore
	base          *ArchiveBatch
	patch         *backfillPatch

	// new archive batch after backfill.
	new *ArchiveBatch

	// snapshot of table schema.
	columnDeletions   []bool
	sortColumns       []int
	primaryKeyColumns []int
	defaultValues     []*common.DataValue

	// keep track of which columns have been forked already.
	columnsForked []bool

	// keep track of which row in base batch has been deleted and added to backfill store.
	baseRowDeleted []int

	dataTypes []common.DataType

	// columns need to be purged under two cases:
	// 	1. old columns are forked for updating in place.
	//  2. old columns in base batch for merging.
	// if there are no columns to be purged, it means the base batch is clean and therefore we don't
	// need to advance the version for this batch.
	columnsToPurge []vectors.ArchiveVectorParty

	// keep track of how much unmanaged memory bytes this day uses.
	// this does not include the temp primary key.
	unmanagedMemoryBytes int64

	// If we invoked a merge process,we can early unpin it before writing to disk.
	okForEarlyUnpin bool
}

func newBackfillStore(tableSchema *memCom.TableSchema, hostMemoryManager common.HostMemoryManager, initBuckets int) *LiveStore {
	ls := &LiveStore{
		BatchSize:       tableSchema.Schema.Config.BackfillStoreBatchSize,
		Batches:         make(map[int32]*LiveBatch),
		tableSchema:     tableSchema,
		LastReadRecord:  memCom.RecordID{BatchID: BaseBatchID, Index: 0},
		NextWriteRecord: memCom.RecordID{BatchID: BaseBatchID, Index: 0},
		PrimaryKey: NewPrimaryKey(tableSchema.PrimaryKeyBytes,
			false, initBuckets, hostMemoryManager),
		HostMemoryManager: hostMemoryManager,
	}
	return ls
}

func newBackfillContext(baseBatch *ArchiveBatch, patch *backfillPatch, tableSchema *memCom.TableSchema, columnDeletions []bool,
	sortColumns []int, primaryKeyColumns []int, dataTypes []common.DataType, defaultValues []*common.DataValue,
	hostMemoryManager common.HostMemoryManager) backfillContext {
	initBuckets := (baseBatch.Size + len(patch.recordIDs)) / memCom.BucketSize
	// allocate more space for insertion.
	initBuckets += initBuckets / 8
	// column deletion will be blocked during backfill, so we are safe to get column deletions from schema without
	// lock.
	return backfillContext{
		backfillStore: newBackfillStore(tableSchema, hostMemoryManager, initBuckets),
		base:          baseBatch,
		// we can simply copy all the columns of base batch without lock since all columns already have been requested
		// and pinned.
		new:               baseBatch.Clone(),
		patch:             patch,
		columnDeletions:   columnDeletions,
		sortColumns:       sortColumns,
		primaryKeyColumns: primaryKeyColumns,
		columnsForked:     make([]bool, len(baseBatch.Columns)),
		dataTypes:         dataTypes,
		defaultValues:     defaultValues,
	}
}

// release releases the resource hold by the backfillContext.
func (ctx *backfillContext) release() {
	// release both the batch and primary key resources.
	ctx.backfillStore.Destruct()
}

// createArchivingPatch create an archiving patch for a single day. This assume all records in the snapshot is within
// the same calendar day bucket.
func (ss liveStoreSnapshot) createArchivingPatch(sortColumns []int) *archivingPatch {
	ap := &archivingPatch{
		data:        ss,
		sortColumns: sortColumns,
	}
	for batchIdx, batch := range ss.batches {
		numRecords := batch[0].GetLength()
		if batchIdx == len(ss.batches)-1 {
			numRecords = ss.numRecordsInLastBatch
		}

		for recordIdx := 0; recordIdx < numRecords; recordIdx++ {
			ap.recordIDs = append(ap.recordIDs,
				memCom.RecordID{BatchID: int32(batchIdx), Index: uint32(recordIdx)})
		}
	}
	return ap
}

func (ctx *backfillContext) backfill(reporter BackfillJobDetailReporter, jobKey string) error {
	// build index on archive batch.
	err := ctx.base.BuildIndex(ctx.sortColumns, ctx.primaryKeyColumns, ctx.backfillStore.PrimaryKey)
	if err != nil {
		return err
	}

	// reuse the space for primary primaryKeyValues of each row.
	var primaryKeyValues []byte
	tableName := ctx.base.Shard.Schema.Schema.Name
	shardID := ctx.base.Shard.ShardID

	// newRecords: records that does not exist in base
	// inplaceUpdateRecords: records that modifies unsortedColumns and can be updated inplace
	// deleteThenInsertRecords: records that modifies sortedColumns and needs to be deleted from base and inserted again into temp live store
	// noEffectRecords: records that does not modify any column
	var newRecords, inplaceUpdateRecords, deleteThenInsertRecords, noEffectRecords int64

	// We will do backfill row by row in patch.
	for _, patchRecordID := range ctx.patch.recordIDs {
		// record id to apply to temp live store.
		nextWriteRecord := ctx.backfillStore.NextWriteRecord

		upsertBatch := ctx.patch.backfillBatches[patchRecordID.BatchID]
		primaryKeyCols, err := upsertBatch.GetPrimaryKeyCols(ctx.primaryKeyColumns)
		if err != nil {
			return err
		}

		if primaryKeyValues, err = upsertBatch.GetPrimaryKeyBytes(int(patchRecordID.Index),
			primaryKeyCols, ctx.base.Shard.Schema.PrimaryKeyBytes); err != nil {
			return err
		}

		exists, recordID, err := ctx.backfillStore.PrimaryKey.FindOrInsert(primaryKeyValues, nextWriteRecord, 0)
		if err != nil {
			return utils.StackError(err, "Failed to find or insert patch record into primary key at row %d",
				patchRecordID.Index)
		}

		if !exists {
			// new row in live store!
			recordID = nextWriteRecord
			ctx.backfillStore.AdvanceNextWriteRecord()
		}

		// changedPatchRow converts patch values in upsert batch to a slice of data values. The length is the number
		// of columns in base batch. If the corresponding column does not exist in the upsert batch or the column is
		// deleted, it will be nil. This changed row is used in several places:
		//  1. when this patch row is a new row or an update on existing row in temp live store, we apply the row to
		// temp live store directly.
		//  2. when this patch row contains updates on sort column of base batch, we will first get the whole column
		// from base batch and apply changes from this patch changed row and then write into temp live store
		//  3. when this patch row only contains updates on unsort column of base batch, we will apply the changed patch
		// row to forked column.

		// get the data value from upsertBatch
		changedPatchRow, err := ctx.getChangedPatchRow(patchRecordID, upsertBatch)
		if err != nil {
			return err
		}

		if exists && recordID.BatchID >= 0 {
			// record is already in base batch.

			// first detect if there are any changes to sort columns or array columns.
			changedBaseRow := ctx.getChangedBaseRow(recordID, changedPatchRow)
			// we should write to live store.
			if changedBaseRow != nil {
				// sorted column or array column size changed
				deleteThenInsertRecords++
				recordID = nextWriteRecord
				ctx.backfillStore.AdvanceNextWriteRecord()
				// update the primary key pointing to new record id.
				ctx.backfillStore.PrimaryKey.Update(primaryKeyValues, recordID)
				ctx.applyChangedRowToLiveStore(recordID, changedBaseRow)
			} else {
				// only unsorted columns are changed, or array column has value change while size not changed
				if ctx.writePatchValueForUnsortedColumn(recordID, changedPatchRow) {
					inplaceUpdateRecords++
				} else {
					noEffectRecords++
				}
			}
		} else {
			newRecords++
			ctx.applyChangedRowToLiveStore(recordID, changedPatchRow)
		}
	}

	utils.GetReporter(tableName, shardID).GetCounter(utils.BackfillNewRecords).Inc(newRecords)
	utils.GetReporter(tableName, shardID).GetCounter(utils.BackfillNoEffectRecords).Inc(noEffectRecords)
	utils.GetReporter(tableName, shardID).GetCounter(utils.BackfillInplaceUpdateRecords).Inc(inplaceUpdateRecords)
	utils.GetReporter(tableName, shardID).GetCounter(utils.BackfillDeleteThenInsertRecords).Inc(deleteThenInsertRecords)

	// in case we fork the column but does not invoke the merge procedure (which also call column.Prune()).
	// column.Prune is idempotent so it's safe to call multiple times.
	for _, column := range ctx.new.Columns {
		column.(vectors.ArchiveVectorParty).Prune()
	}

	// original baseRowDeleted is not sorted.
	if ctx.baseRowDeleted != nil {
		sort.Ints(ctx.baseRowDeleted)
	}

	ctx.backfillStore.AdvanceLastReadRecord()
	if ctx.backfillStore.LastReadRecord.BatchID > BaseBatchID || ctx.backfillStore.LastReadRecord.Index > 0 {
		ctx.merge(reporter, jobKey)
		// We can early unpin it only if we invoked a merge process.
		ctx.okForEarlyUnpin = true
	}

	ctx.new.SeqNum++
	return nil
}

// merge merges the records in temp live store with the new batch.
func (ctx *backfillContext) merge(reporter BackfillJobDetailReporter, jobKey string) {
	snapshot := ctx.backfillStore.snapshot()
	ap := snapshot.createArchivingPatch(ctx.sortColumns)
	sort.Sort(ap)
	mergeCtx := newMergeContext(ctx.new, ap, ctx.columnDeletions, ctx.dataTypes,
		ctx.defaultValues, ctx.baseRowDeleted)
	mergeCtx.merge(ctx.new.Version, ctx.new.SeqNum)
	for _, column := range ctx.new.Columns {
		ctx.columnsToPurge = append(ctx.columnsToPurge, column.(vectors.ArchiveVectorParty))
	}
	ctx.new = mergeCtx.merged
	ctx.unmanagedMemoryBytes += mergeCtx.unmanagedMemoryBytes
}

// getChangedPatchRow get the upsert batch row as a slice of pointer of data value format to be consistent with changed
// base row. Note an upsert batch row may not have values for all columns so some of the data value may be nil.
func (ctx *backfillContext) getChangedPatchRow(patchRecordID memCom.RecordID, upsertBatch *memCom.UpsertBatch) ([]*common.DataValue, error) {
	changedRow := make([]*common.DataValue, len(ctx.new.Columns))
	for col := 0; col < upsertBatch.NumColumns; col++ {
		columnID, err := upsertBatch.GetColumnID(col)
		if err != nil {
			return nil, utils.StackError(err, "Failed to get column id for col %d", col)
		}

		if ctx.columnDeletions[columnID] {
			continue
		}

		value, err := upsertBatch.GetDataValue(int(patchRecordID.Index), col)
		if err != nil {
			return nil, utils.StackError(err, "Failed to get value at row %d, col %d",
				patchRecordID.Index, col)
		}

		if value.Valid {
			changedRow[columnID] = &value
		}
	}
	return changedRow, nil
}

// getChangedBaseRow get changed row from base batch if there are any changes to sort columns or array columns. It will fetch the whole
// row in base batch and apply patch value to it.
// for array columns, only when array size change will trigger the base batch change, value change while size not change will be covered in
// the unsorted column change
func (ctx *backfillContext) getChangedBaseRow(baseRecordID memCom.RecordID, changedPatchRow []*common.DataValue) []*common.DataValue {
	var changedBaseRow []*common.DataValue
	for columnID, patchValue := range changedPatchRow {
		// loop through sorted columns and array columns
		if patchValue != nil && (utils.IndexOfInt(ctx.sortColumns, columnID) >= 0 || ctx.new.Columns[columnID].IsList()) {
			baseDataValue := ctx.new.Columns[columnID].GetDataValueByRow(int(baseRecordID.Index))
			// there's change in sorted column or size change in array column
			if (ctx.new.Columns[columnID].IsList() && common.ArrayLengthCompare(&baseDataValue, patchValue) != 0) ||
				(!ctx.new.Columns[columnID].IsList() && baseDataValue.Compare(*patchValue) != 0) {
				changedBaseRow = make([]*common.DataValue, len(ctx.new.Columns))
				// Mark deletion for this row.
				ctx.baseRowDeleted = append(ctx.baseRowDeleted, int(baseRecordID.Index))
				// Copy the whole row in base batch to changed row and apply the change.
				for newColumnID := 0; newColumnID < len(ctx.new.Columns); newColumnID++ {
					if ctx.columnDeletions[newColumnID] {
						continue
					}
					if changedPatchRow[newColumnID] != nil {
						// column changed, get from patch
						changedBaseRow[newColumnID] = changedPatchRow[newColumnID]
					} else {
						// column unchanged, get from base
						existingValue := ctx.new.Columns[newColumnID].GetDataValueByRow(int(baseRecordID.Index))
						changedBaseRow[newColumnID] = &existingValue
					}
				}
				break
			}
		}
	}
	return changedBaseRow
}

// writePatchValueForUnsortColumn writes the patch value to forked columns if value changes.
// this function return false if no column update happens
func (ctx *backfillContext) writePatchValueForUnsortedColumn(baseRecordID memCom.RecordID, changedPatchRow []*common.DataValue) (updated bool) {
	for columnID, patchValue := range changedPatchRow {
		if patchValue != nil && utils.IndexOfInt(ctx.sortColumns, columnID) < 0 {
			baseDataValue := ctx.new.Columns[columnID].GetDataValueByRow(int(baseRecordID.Index))
			if baseDataValue.Compare(*patchValue) != 0 {
				// For updates to unsorted columns, if the value changes, fork the column, and update in place
				// in the forked copy, this will make sure that ongoing queries do not see this change.
				if !ctx.columnsForked[columnID] {
					ctx.columnsToPurge = append(ctx.columnsToPurge, ctx.base.Columns[columnID].(vectors.ArchiveVectorParty))
					// For the forked columns, we will always allocate space for value vector and null vector despite
					// of the mode of the original vector.
					var bytes int64
					if ctx.base.Columns[columnID].IsList() {
						// will use original size for array vp
						bytes = ctx.base.Columns[columnID].GetBytes()
					} else {
						bytes = int64(vectors.CalculateVectorPartyBytes(
							ctx.base.Columns[columnID].GetDataType(), ctx.base.Size, true, false))
					}
					ctx.unmanagedMemoryBytes += bytes
					// Report before allocation.
					ctx.backfillStore.HostMemoryManager.ReportUnmanagedSpaceUsageChange(bytes)
					ctx.new.Columns[columnID] = ctx.base.Columns[columnID].(vectors.ArchiveVectorParty).CopyOnWrite(ctx.base.Size)
					ctx.columnsForked[columnID] = true
				}
				ctx.new.Columns[columnID].SetDataValue(int(baseRecordID.Index), *patchValue, vectors.CheckExistingCount)
				updated = true
			}
		}
	}
	return
}

// applyChangedRowToLiveStore applies changes in changedRow to temp live store.
func (ctx backfillContext) applyChangedRowToLiveStore(recordID memCom.RecordID, changedRow []*common.DataValue) {
	backfillBatch := ctx.backfillStore.GetBatchForWrite(recordID.BatchID)
	defer backfillBatch.Unlock()
	for columnID, changedDataValue := range changedRow {
		if changedDataValue != nil {
			backfillStoreVP := backfillBatch.GetOrCreateVectorParty(columnID, true)
			backfillStoreVP.SetDataValue(int(recordID.Index), *changedDataValue, vectors.IgnoreCount)
		}
	}
}
