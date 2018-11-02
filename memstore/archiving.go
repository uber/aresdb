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
	"sort"

	"github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/utils"
)

// liveStoreSnapshot stores a snapshot of the LiveStore structure
// for fast archiving read without mutex locking and map accessing.
// The structure is snapshotted so that new batch/vector party creation by
// ingestion will not affect archiving. The underlying data is still shared with
// ingestion in parallel, with no read/write conflict, assuming that:
//  - archiving will not read beyond lastReadRecord for newly appended records
//  - archiving will only read records older than the cutoff, while ingestion
//    will not update any record older than that (backfill will be delayed until
//    ongoing archiving completes).
type liveStoreSnapshot struct {
	// Stores the structure as [RandomBatchIndex][ColumnID].
	batches [][]common.VectorParty
	// For purging live batch later.
	batchIDs              []int32
	numRecordsInLastBatch int
}

// snapshot creates a snapshot of the LiveStore structure for archiving and backfill fast read.
func (s *LiveStore) snapshot() (ss liveStoreSnapshot) {
	batchIDs, numRecordsInLastBatch := s.GetBatchIDs()
	ss.batchIDs = batchIDs
	ss.batches = make([][]common.VectorParty, len(batchIDs))
	ss.numRecordsInLastBatch = numRecordsInLastBatch
	for i, batchID := range batchIDs {
		batch := s.GetBatchForRead(batchID)
		// Live batches are purged by archiving so all batches returned here
		// should be valid.
		ss.batches[i] = make([]common.VectorParty, len(batch.Columns))
		copy(ss.batches[i], batch.Columns)
		batch.RUnlock()
	}
	return
}

// archivingPatch stores records to be patched onto a archive batch.
// The records are identified by recordIDs and stored in the snapshot.
// The records will be sorted according to the sortColumns.
type archivingPatch struct {
	// RecordID.BatchID here refers to the RandomBatchIndex in the snapshot.
	recordIDs   []RecordID
	sortColumns []int
	// Readonly. We won't change it during sorting archiving patch and merging
	// with archive batch.
	data liveStoreSnapshot
}

// createArchivingPatches creates an archiving patch per affected UTC day.
// The key of the returned map is the number of days since Unix Epoch.
func (ss liveStoreSnapshot) createArchivingPatches(
	cutoff uint32, oldCutoff uint32, sortColumns []int,
	reporter ArchiveJobDetailReporter, jobKey string, tableName string, shardID int,
) map[int32]*archivingPatch {
	patchByDay := make(map[int32]*archivingPatch)
	numBatches := len(ss.batches)

	reporter(jobKey, func(status *ArchiveJobDetail) {
		status.Stage = ArchivingCreatePatch
		status.Current = 0
		status.Total = numBatches
	})

	var numRecordsArchived int64
	var numRecordsIgnored int64

	for batchIdx, batch := range ss.batches {
		timeColumn := batch[0]
		numRecords := timeColumn.GetLength()
		minValue, _ := timeColumn.(common.LiveVectorParty).GetMinMaxValue()
		if batchIdx == len(ss.batches)-1 {
			numRecords = ss.numRecordsInLastBatch
		}

		if minValue < cutoff {
			for recordIdx := 0; recordIdx < numRecords; recordIdx++ {
				time := *(*uint32)(timeColumn.GetDataValue(recordIdx).OtherVal)
				if time < cutoff {
					if time >= oldCutoff {
						// Add the record for archiving
						day := int32(time / 86400)
						patch := patchByDay[day]
						if patch == nil {
							patch = &archivingPatch{
								data:        ss,
								sortColumns: sortColumns,
							}
							patchByDay[day] = patch
						}
						patch.recordIDs = append(patch.recordIDs,
							RecordID{int32(batchIdx), uint32(recordIdx)})
						numRecordsArchived++
					} else {
						numRecordsIgnored++
					}
				}
			}
		}
		reporter(jobKey, func(status *ArchiveJobDetail) {
			status.Current = batchIdx + 1
		})
	}

	utils.GetReporter(tableName, shardID).GetCounter(utils.ArchivingIgnoredRecords).Inc(numRecordsIgnored)
	utils.GetReporter(tableName, shardID).GetCounter(utils.ArchivingRecords).Inc(numRecordsArchived)

	reporter(jobKey, func(status *ArchiveJobDetail) {
		status.NumRecords = int(numRecordsArchived)
	})
	return patchByDay
}

// getBatchIDsToPurge returns list of batchIDs to purge in live store if its
// max event time is less than cutoff
// We do not purge the last batch if it's partially archived (ss.numRecordsInLastBatch
// != lastBatch.Size).
func (s *LiveStore) getBatchIDsToPurge(cutoff uint32) []int32 {
	var batchIDs []int32
	s.RLock()
	for batchID, batch := range s.Batches {
		if batchID >= s.LastReadRecord.BatchID {
			continue
		}

		if _, maxValue := batch.Columns[0].(common.LiveVectorParty).GetMinMaxValue(); maxValue < cutoff {
			batchIDs = append(batchIDs, batchID)
		}
	}
	s.RUnlock()
	return batchIDs
}

func (ap archivingPatch) Len() int {
	return len(ap.recordIDs)
}

func (ap archivingPatch) Swap(i, j int) {
	ap.recordIDs[i], ap.recordIDs[j] = ap.recordIDs[j], ap.recordIDs[i]
}

func (ap archivingPatch) Less(i, j int) bool {
	for _, columnID := range ap.sortColumns {
		iValue := ap.GetDataValue(i, columnID)
		jValue := ap.GetDataValue(j, columnID)
		res := iValue.Compare(jValue)
		if res != 0 {
			return res < 0
		}
		// Tie, move on to next sort column.
	}
	return false
}

// GetDataValue reads value from underlying columns after sorted.
func (ap *archivingPatch) GetDataValue(row, columnID int) common.DataValue {
	recordID := ap.recordIDs[row]
	batch := ap.data.batches[recordID.BatchID]

	if columnID >= len(batch) {
		return common.NullDataValue
	}

	vp := batch[columnID]
	if vp == nil {
		return common.NullDataValue
	}

	return vp.GetDataValue(int(recordID.Index))
}

// GetDataValue reads value from underlying columns after sorted. If it's missing, it will return
// passed value instead.
func (ap *archivingPatch) GetDataValueWithDefault(row, columnID int, defaultValue common.DataValue) common.DataValue {
	recordID := ap.recordIDs[row]
	batch := ap.data.batches[recordID.BatchID]

	if columnID >= len(batch) {
		return defaultValue
	}

	vp := batch[columnID]
	if vp == nil {
		return defaultValue
	}

	return vp.GetDataValue(int(recordID.Index))
}

// Archive is the process of periodically moving stable records in fact tables from live batches to archive batches,
// and converting them to a compressed format (run-length encoding). This is a blocking call so caller need to wait
// for archiving process to finish.
func (m *memStoreImpl) Archive(table string, shardID int, cutoff uint32, reporter ArchiveJobDetailReporter) error {
	archivingTimer := utils.GetReporter(table, shardID).GetTimer(utils.ArchivingTimingTotal)
	start := utils.Now()
	jobKey := getIdentifier(table, shardID, common.ArchivingJobType)
	// Emit duration metrics and report back to scheduler.
	defer func() {
		duration := utils.Now().Sub(start)
		archivingTimer.Record(duration)
		reporter(jobKey, func(status *ArchiveJobDetail) {
			status.LastDuration = duration
		})
	}()

	reporter(jobKey, func(status *ArchiveJobDetail) {
		status.RunningCutoff = cutoff
	})

	shard, err := m.GetTableShard(table, shardID)
	if err != nil {
		return err
	}
	defer shard.Users.Done()

	if cutoff <= shard.ArchiveStore.CurrentVersion.ArchivingCutoff {
		return utils.StackError(nil, "Cutoff %d is no greater than current cutoff %d",
			cutoff, shard.ArchiveStore.CurrentVersion.ArchivingCutoff)
	}

	// Update the archiving cutoff time high water mark so ingestion won't update records below
	// the new target archiving cutoff time.
	shard.LiveStore.WriterLock.Lock()
	shard.LiveStore.ArchivingCutoffHighWatermark = cutoff
	shard.LiveStore.PrimaryKey.UpdateEventTimeCutoff(cutoff)
	shard.LiveStore.WriterLock.Unlock()

	utils.GetReporter(table, shardID).GetGauge(utils.ArchivingHighWatermark).Update(float64(cutoff))

	// Create a new archive store version and switch to it.
	patchByDay, oldVersion, unmanagedMemoryBytes, err := shard.createNewArchiveStoreVersion(cutoff, reporter, jobKey)
	if err != nil {
		return err
	}

	// Wait for queries in other goroutines to prevent archiving from prematurely purging the old version.
	oldVersion.Users.Wait()

	// Purge redo log files on disk.
	reporter(jobKey, func(status *ArchiveJobDetail) {
		status.Stage = ArchivingPurge
	})

	backfillMgr := shard.LiveStore.BackfillManager
	if err := shard.LiveStore.RedoLogManager.
		PurgeRedologFileAndData(cutoff, backfillMgr.LastRedoFile,
			backfillMgr.LastBatchOffset); err != nil {
		return err
	}

	// Delete obsolete (merged base batch) vector parties in oldArchivedStore. We don't need any lock since all queries
	// should already finish processing the old version.
	for day := range patchByDay {
		// We don't need to check existence again since it should be already created if missing in merge stage.
		batch := oldVersion.Batches[day]
		// Purge archive batch on disk.
		if err := m.diskStore.DeleteBatchVersions(table, shardID, int(day), batch.Version, batch.SeqNum); err != nil {
			return err
		}
		// Purge archive batch in memory.
		batch.SafeDestruct()
	}

	// Report memory usage.
	newVersion := shard.ArchiveStore.GetCurrentVersion()
	defer newVersion.Users.Done()

	for day := range patchByDay {
		batch := newVersion.Batches[day]
		batch.RLock()
		for columnID, column := range batch.Columns {
			if column != nil {
				// The new merged batch is no longer unmanaged memory.
				bytes := column.GetBytes()
				shard.HostMemoryManager.ReportManagedObject(shard.Schema.Schema.Name, shardID, int(day), columnID, bytes)
			}
		}
		batch.RUnlock()
	}
	shard.HostMemoryManager.ReportUnmanagedSpaceUsageChange(-unmanagedMemoryBytes)

	// Purge live store in memory.
	batchIDsToPurge := shard.LiveStore.getBatchIDsToPurge(cutoff)
	shard.LiveStore.PurgeBatches(batchIDsToPurge)

	reporter(jobKey, func(status *ArchiveJobDetail) {
		status.Stage = ArchivingComplete
		status.LastCutoff = status.CurrentCutoff
		status.CurrentCutoff = cutoff
	})
	utils.GetReporter(table, shardID).GetGauge(utils.ArchivingLowWatermark).Update(float64(cutoff))
	return nil
}

func (shard *TableShard) createNewArchiveStoreVersion(cutoff uint32, reporter ArchiveJobDetailReporter, jobKey string) (
	patchByDay map[int32]*archivingPatch, oldVersion *ArchiveStoreVersion, unmanagedMemoryBytes int64, err error) {

	// Block column deletion
	shard.columnDeletion.Lock()
	defer shard.columnDeletion.Unlock()

	tableName := shard.Schema.Schema.Name
	shardID := shard.ShardID

	// Snapshot schema
	shard.Schema.RLock()
	sortColumns := shard.Schema.Schema.ArchivingSortColumns
	dataTypes := shard.Schema.ValueTypeByColumn
	defaultValues := shard.Schema.DefaultValues
	numColumns := len(dataTypes)
	columnDeletions := shard.Schema.GetColumnDeletions()
	shard.Schema.RUnlock()

	oldVersion = shard.ArchiveStore.CurrentVersion
	// Snapshot unsorted store structure, but not the data.
	ss := shard.LiveStore.snapshot()

	// Scan unsorted snapshot for stable records.
	patchByDay = ss.createArchivingPatches(cutoff, oldVersion.ArchivingCutoff, sortColumns,
		reporter, jobKey, tableName, shardID)
	newVersion := NewArchiveStoreVersion(cutoff, shard)

	// Begin of merge.
	dayIdx := 1
	numPatches := len(patchByDay)
	reporter(jobKey, func(status *ArchiveJobDetail) {
		status.Stage = ArchivingMerge
		status.Current = 0
		status.Total = numPatches
		status.NumAffectedDays = numPatches
	})

	for day, patch := range patchByDay {
		sort.Sort(patch)
		batchID := int(day)

		baseBatch := shard.ArchiveStore.CurrentVersion.RequestBatch(day)

		var requestedVPs []common.ArchiveVectorParty
		// We need to load all columns into memory for archiving.
		for columnID := 0; columnID < numColumns; columnID++ {
			requestedVP := baseBatch.RequestVectorParty(columnID)
			requestedVP.WaitForDiskLoad()
			requestedVPs = append(requestedVPs, requestedVP)
		}

		ctx := newMergeContext(baseBatch, patch, columnDeletions,
			dataTypes, defaultValues, nil)
		ctx.merge(cutoff, 0)
		unmanagedMemoryBytes += ctx.unmanagedMemoryBytes
		newVersion.Batches[day] = ctx.merged

		// Unpin columns requested in this batch to unblock eviction.
		UnpinVectorParties(requestedVPs)

		if err = newVersion.Batches[day].WriteToDisk(); err != nil {
			return
		}

		if err = shard.metaStore.AddArchiveBatchVersion(
			shard.Schema.Schema.Name, shard.ShardID, batchID, cutoff, uint32(0),
			newVersion.Batches[day].Size); err != nil {
			return
		}
		reporter(jobKey, func(status *ArchiveJobDetail) {
			status.Current = dayIdx
		})
		dayIdx++
	}
	oldVersion.RLock()

	// Copy unmerged base batch into new version.
	for day, batch := range oldVersion.Batches {
		if _, ok := patchByDay[day]; !ok {
			newVersion.Batches[day] = batch
		}
	}
	oldVersion.RUnlock()

	if err = shard.metaStore.UpdateArchivingCutoff(
		shard.Schema.Schema.Name, shard.ShardID, cutoff); err != nil {
		return
	}
	shard.ArchiveStore.Lock()
	shard.ArchiveStore.CurrentVersion = newVersion
	shard.ArchiveStore.Unlock()
	return
}
