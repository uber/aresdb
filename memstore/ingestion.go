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
	"github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/memstore/vectors"
	"github.com/uber/aresdb/utils"
	"math"
	"strconv"
)

// HandleIngestion logs an upsert batch and applies it to the in-memory store.
func (m *memStoreImpl) HandleIngestion(table string, shardID int, upsertBatch *common.UpsertBatch) error {
	if m.options.redoLogMaster.RedoLogConfig.DiskConfig.Disabled {
		return utils.StackError(nil, "Local redolog file not enabled")
	}
	shard, err := m.GetTableShard(table, shardID)
	if err != nil {
		return utils.StackError(nil, "Failed to get shard %d for table %s for upsert batch", shardID, table)
	}
	// Release the wait group that proctects the shard to be deleted.
	defer shard.Users.Done()

	return shard.saveUpsertBatch(upsertBatch, 0, 0, false, false)
}

// saveUpsertBatch handles data ingestion from both redolog and http
func (shard *TableShard) saveUpsertBatch(upsertBatch *common.UpsertBatch, redoLogFile int64, offset uint32, recovery, skipBackFillRows bool) error {
	tableName := shard.Schema.Schema.Name
	shardID := shard.ShardID
	shard.LiveStore.WriterLock.Lock()

	if recovery {
		utils.GetReporter(tableName, shardID).GetCounter(utils.IngestedRecoveryBatches).Inc(1)
		utils.GetReporter(tableName, shardID).GetGauge(utils.RecoveryUpsertBatchSize).Update(float64(len(upsertBatch.GetBuffer())))
		// Put a 0 in maxEventTimePerFile in case this is redolog is full of backfill batches.
		shard.LiveStore.RedoLogManager.UpdateMaxEventTime(0, redoLogFile)
	} else {
		utils.GetReporter(tableName, shardID).GetCounter(utils.IngestedUpsertBatches).Inc(1)
		utils.GetReporter(tableName, shardID).GetGauge(utils.UpsertBatchSize).Update(float64(len(upsertBatch.GetBuffer())))
		// for non-recovery and local file based redolog, need write the upsertbatch into redolog file
		if !shard.options.redoLogMaster.RedoLogConfig.DiskConfig.Disabled {
			// change original file/offset to be local redolog file/offset
			redoLogFile, offset = shard.LiveStore.RedoLogManager.AppendToRedoLog(upsertBatch)
		}
	}

	needToWaitForBackfillBuffer, err := shard.ApplyUpsertBatch(upsertBatch, redoLogFile, offset, skipBackFillRows)
	shard.LiveStore.WriterLock.Unlock()

	// return immediately if it does not need to wait for backfill buffer availability
	if recovery || !needToWaitForBackfillBuffer {
		return err
	}

	// otherwise: block until backfill buffer becomes available again
	shard.LiveStore.BackfillManager.WaitForBackfillBufferAvailability()
	return err
}

// ApplyUpsertBatch applies the upsert batch to the memstore shard.
// Returns true if caller needs to wait for availability of backfill buffer
func (shard *TableShard) ApplyUpsertBatch(upsertBatch *common.UpsertBatch, redoLogFile int64, offset uint32, skipBackfillRows bool) (bool, error) {
	shard.Schema.RLock()
	valueTypeByColumn := shard.Schema.ValueTypeByColumn
	columnDeletions := shard.Schema.GetColumnDeletions()
	allowMissingEventTime := shard.Schema.Schema.Config.AllowMissingEventTime
	shard.Schema.RUnlock()
	primaryKeyColumns := shard.Schema.GetPrimaryKeyColumns()
	// IsFactTable should be immutable.
	isFactTable := shard.Schema.Schema.IsFactTable

	// This is the upsertbatch column index that points to the first logic column (which is
	// event time for fact table).
	eventTimeColumnIndex := -1

	// Validate columns in upsert batch are valid.
	for i := 0; i < upsertBatch.NumColumns; i++ {
		columnID, _ := upsertBatch.GetColumnID(i)
		if columnID >= len(valueTypeByColumn) {
			return false, utils.StackError(nil, "Unrecognized column id %d in upsert batch", columnID)
		}

		columnType, _ := upsertBatch.GetColumnType(i)
		if valueTypeByColumn[columnID] != columnType {
			return false, utils.StackError(
				nil,
				"Mismatched data type (upsert batch: %d, schema %d) for table %s shard %d column %d", columnType, valueTypeByColumn[columnID], shard.Schema.Schema.Name, shard.ShardID, columnID)
		}

		if columnID == 0 && isFactTable {
			eventTimeColumnIndex = i
		}
	}

	// For fact table ingestion, we will need to get the event time from the first column. we don't
	// have to validate the column type in the upsertbatch because the loop above already handled it.
	if isFactTable && eventTimeColumnIndex < 0 && !allowMissingEventTime {
		utils.GetReporter(shard.Schema.Schema.Name, shard.ShardID).GetCounter(utils.TimeColumnMissing).Inc(1)
		return false, utils.StackError(nil, "Fact table's event time column (first column) is missing")
	}

	updateRecords, insertRecords, backfillUpsertBatch, err := shard.insertPrimaryKeys(primaryKeyColumns, eventTimeColumnIndex,
		redoLogFile, upsertBatch, skipBackfillRows)

	if err != nil {
		return false, err
	}

	// We write insert records first so records with the same primary key in a upsert batch
	// will be updated in order.
	for batchID, records := range insertRecords {
		if err := shard.writeBatchRecords(columnDeletions, upsertBatch, batchID, records, false); err != nil {
			return false, err
		}
	}
	for batchID, records := range updateRecords {
		if err := shard.writeBatchRecords(columnDeletions, upsertBatch, batchID, records, true); err != nil {
			return false, err
		}
	}

	shard.LiveStore.AdvanceLastReadRecord()
	numMutations := len(insertRecords) + len(updateRecords)
	return shard.postUpsertBatchApplication(upsertBatch, backfillUpsertBatch, redoLogFile, offset, numMutations), nil
}

func (shard *TableShard) postUpsertBatchApplication(upsertBatch, backfillUpsertBatch *common.UpsertBatch, redoLogFile int64,
	offset uint32, numMutations int) bool {
	if shard.Schema.Schema.IsFactTable {
		// add records to backfill queue if any.
		// TODO: currently we're relying on LiveStore.WriterLock to guarantee the ordering of backfill batches
		backfillMgr := shard.LiveStore.BackfillManager
		needWaitForBackfillBuffer := backfillMgr.Append(backfillUpsertBatch, redoLogFile, offset)
		// caller needs to wait WHEN backfill buffer is full and there're more than 10 rows or over 5% rows are for backfill
		if needWaitForBackfillBuffer &&
			(backfillUpsertBatch.NumRows > 10 || float32(backfillUpsertBatch.NumRows)/float32(upsertBatch.NumRows) > 0.05) {
			return true
		}
	} else {
		shard.LiveStore.SnapshotManager.ApplyUpsertBatch(redoLogFile, offset, numMutations, shard.LiveStore.LastReadRecord)
	}

	return false
}

// Per record instruction on how to read from upsert batch and write to memStore.
type recordInfo struct {
	// The row index of the record in the upsert batch.
	row int
	// The index of the to be inserted/updated record in a batch.
	index int
}

// Insert primary keys and return the records for update, insert grouped by batch.
// eventTimeColumnIndex will be used to extract the event time value per row if it >= 0.
func (shard *TableShard) insertPrimaryKeys(primaryKeyColumns []int, eventTimeColumnIndex int, redoLogFile int64,
	upsertBatch *common.UpsertBatch, skipBackfillRows bool) (
	map[int32][]recordInfo, map[int32][]recordInfo, *common.UpsertBatch, error) {
	// Get primary key column indices and calculate the primary key width.
	primaryKeyBytes := shard.Schema.PrimaryKeyBytes
	primaryKeyCols, err := upsertBatch.GetPrimaryKeyCols(primaryKeyColumns)
	if err != nil {
		utils.GetReporter(shard.Schema.Schema.Name, shard.ShardID).GetCounter(utils.PrimaryKeyMissing).Inc(1)
		return nil, nil, nil, err
	}

	shard.Schema.RLock()
	recordRetentionDays := shard.Schema.Schema.Config.RecordRetentionInDays
	allowMissingEventTime := shard.Schema.Schema.Config.AllowMissingEventTime
	isFactTable := shard.Schema.Schema.IsFactTable
	shard.Schema.RUnlock()

	var key []byte
	updateRecords := make(map[int32][]recordInfo)
	insertRecords := make(map[int32][]recordInfo)

	nextWriteRecord := shard.LiveStore.NextWriteRecord

	tableName := shard.Schema.Schema.Name
	shardID := shard.ShardID
	var eventTime uint32
	var isEventTimeValid bool
	var backfillRows = make([]int, 0)
	var numRecordsIngested int64
	var numRecordsAppended int64
	var numRecordsUpdated int64
	var numRecordsSkipped int64
	var maxUpsertBatchEventTime uint32
	for row := 0; row < upsertBatch.NumRows; row++ {
		// Get primary key bytes for each record.
		if key, err = upsertBatch.GetPrimaryKeyBytes(row, primaryKeyCols, primaryKeyBytes); err != nil {
			return nil, nil, nil, utils.StackError(err, "Failed to create primary key at row %d", row)
		}

		// For fact table we need to get the event time from the first column.
		if eventTimeColumnIndex >= 0 {
			value, validity, err := upsertBatch.GetValue(row, eventTimeColumnIndex)
			if err != nil {
				return nil, nil, nil, utils.StackError(err, "Failed to get event time for row %d", row)
			}

			isEventTimeValid = validity
			if isEventTimeValid {
				eventTime = *(*uint32)(value)
			}
		}

		var primaryKeyEventTime uint32
		if !isEventTimeValid {
			if isFactTable && !allowMissingEventTime {
				return nil, nil, nil, utils.StackError(err, "Event time for row %d is null", row)
			}
			// event with invalid event time will be ignored
			// once arrival time is older than archiving cutoff.
			if primaryKeyEventTime = upsertBatch.ArrivalTime; primaryKeyEventTime < shard.LiveStore.ArchivingCutoffHighWatermark {
				continue
			}
		} else {
			var nowInSeconds = uint32(utils.Now().Unix())
			var oldestRecordDays int
			if recordRetentionDays > 0 {
				oldestRecordDays = int(nowInSeconds/86400) - recordRetentionDays
			}

			primaryKeyEventTime = eventTime

			eventDay := int(eventTime / 86400)

			// Skip this record if it's out of retention
			if eventDay < oldestRecordDays {
				utils.GetReporter(tableName, shardID).GetCounter(utils.RecordsOutOfRetention).Inc(1)
				continue
			}

			// Skip this record if its event time is latter than current time
			if eventTime > nowInSeconds {
				utils.GetReporter(tableName, shardID).GetCounter(utils.RecordsFromFuture).Inc(1)
				continue
			}

			if eventTime > maxUpsertBatchEventTime {
				maxUpsertBatchEventTime = eventTime
			}

			// Update max event time so archiving won't purge redo log files that have records newer than
			// archiving cut off time.
			shard.LiveStore.RedoLogManager.UpdateMaxEventTime(eventTime, redoLogFile)

			// If we get a record that is older than archiving cutoff time (exclusive) that means
			// 1. during ingestion, the event should be put into a backfill queue
			// 2. during recovery, the event should be ignored, because it was already put into
			//    a backfill queue at ingestion time.
			if eventTime < shard.LiveStore.ArchivingCutoffHighWatermark {
				if !skipBackfillRows {
					// mark this row as backfill row
					backfillRows = append(backfillRows, row)
					timeDiff := float64(shard.LiveStore.ArchivingCutoffHighWatermark - eventTime)
					utils.GetReporter(tableName, shardID).
						GetGauge(utils.BackfillRecordsTimeDifference).Update(timeDiff)
				} else {
					numRecordsSkipped++
				}
				continue
			}
		}

		numRecordsIngested++
		existing, record, err := shard.LiveStore.PrimaryKey.FindOrInsert(key, nextWriteRecord, primaryKeyEventTime)
		if err != nil {
			return nil, nil, nil, utils.StackError(err, "Failed to insert key for row %d", row)
		}

		if !existing {
			nextWriteRecord = shard.LiveStore.AdvanceNextWriteRecord()
			numRecordsAppended++
		} else {
			numRecordsUpdated++
		}

		result := updateRecords
		if !existing {
			result = insertRecords
		}

		rows := result[record.BatchID]
		result[record.BatchID] = append(
			rows,
			recordInfo{
				row:   row,
				index: int(record.Index),
			})
	}

	var nowInSeconds = uint32(utils.Now().Unix())
	// Update max event time for each column in this upsert batch.
	if maxUpsertBatchEventTime > 0 {
		for col := 0; col < upsertBatch.NumColumns; col++ {
			columnID, err := upsertBatch.GetColumnID(col)
			if err != nil {
				return nil, nil, nil, utils.StackError(err, "Failed to get column id for col %d", col)
			}

			for columnID >= len(shard.LiveStore.lastModifiedTimePerColumn) {
				shard.LiveStore.lastModifiedTimePerColumn = append(shard.LiveStore.lastModifiedTimePerColumn, 0)
			}

			if maxUpsertBatchEventTime > shard.LiveStore.lastModifiedTimePerColumn[columnID] {
				shard.LiveStore.lastModifiedTimePerColumn[columnID] = maxUpsertBatchEventTime
				// We only do it on per upsert batch level so it should be acceptable to create the scope dynamically.
				// TODO: if there is any performance issue, cache the reporter at live store level.
				utils.GetReporter(tableName, shardID).GetChildGauge(map[string]string{
					"columnID": strconv.Itoa(columnID),
				}, utils.IngestionLagPerColumn).Update(math.Max(float64(nowInSeconds-maxUpsertBatchEventTime), 0))
			}
		}
	}

	utils.GetReporter(tableName, shardID).GetCounter(utils.IngestedRecords).Inc(numRecordsIngested)
	utils.GetReporter(tableName, shardID).GetCounter(utils.AppendedRecords).Inc(numRecordsAppended)
	utils.GetReporter(tableName, shardID).GetCounter(utils.UpdatedRecords).Inc(numRecordsUpdated)
	utils.GetReporter(tableName, shardID).GetCounter(utils.BackfillRecords).Inc(int64(len(backfillRows)))
	utils.GetReporter(tableName, shardID).GetCounter(utils.IngestSkippedRecords).Inc(numRecordsSkipped)

	// update ratio gauge of backfill rows/total rows
	if upsertBatch.NumRows > 0 {
		utils.GetReporter(tableName, shardID).GetGauge(utils.BackfillRecordsRatio).
			Update(float64(100.0 * len(backfillRows) / upsertBatch.NumRows))
	}

	// create backfill upsertBatch if applicable
	if len(backfillRows) == upsertBatch.NumRows {
		// all rows are for backfill
		return updateRecords, insertRecords, upsertBatch, nil
	}

	backfillBatch := upsertBatch.ExtractBackfillBatch(backfillRows)
	if backfillBatch != nil && backfillBatch.NumRows > 0 && backfillBatch.NumColumns < upsertBatch.NumColumns {
		// some columns get pruned due to inappropriate update functions
		utils.GetReporter(tableName, shardID).GetCounter(utils.BackfillRecordsColumnRemoved).Inc(int64(len(backfillRows)))

	}
	return updateRecords, insertRecords, backfillBatch, nil
}

// Read rows from a batch group and write to memStore. Batch id = 0 is for records to be inserted.
func (shard *TableShard) writeBatchRecords(columnDeletions []bool,
	upsertBatch *common.UpsertBatch, batchID int32, records []recordInfo, forUpdate bool) error {
	var batch *LiveBatch
	if forUpdate {
		// We need to lock the batch for update to achieve row level consistency.
		batch = shard.LiveStore.GetBatchForWrite(batchID)
		defer batch.Unlock()
	} else {
		// Make sure all columns are created.
		batch = shard.LiveStore.GetBatchForWrite(batchID)
		for i := 0; i < upsertBatch.NumColumns; i++ {
			columnID, _ := upsertBatch.GetColumnID(i)
			if columnDeletions[columnID] {
				continue
			}
			batch.GetOrCreateVectorParty(columnID, true)
		}
		batch.Unlock()

		batch.RLock()
		defer batch.RUnlock()
	}

	if batch.MaxArrivalTime < upsertBatch.ArrivalTime {
		batch.MaxArrivalTime = upsertBatch.ArrivalTime
	}

	// Instead of traversing row by row, we instead do column by column to avoid making checks on each row.
	for col := 0; col < upsertBatch.NumColumns; col++ {
		columnID, err := upsertBatch.GetColumnID(col)
		if err != nil {
			return utils.StackError(err, "Failed to get column id for col %d", col)
		}
		if columnDeletions[columnID] {
			continue
		}

		columnUpdateMode := upsertBatch.GetColumnUpdateMode(col)
		columnMode := upsertBatch.GetColumMode(col)

		// we will skip processing this column if
		// 1. columnMode is AllValuesDefault
		// 2. columnUpdateMode is UpdateOverwriteNotNull
		if columnMode == vectors.AllValuesDefault && columnUpdateMode == common.UpdateOverwriteNotNull {
			continue
		}

		if col >= upsertBatch.GetColumnLen() {
			return utils.StackError(nil, "Column index %d out of range %d", col, upsertBatch.GetColumnLen())
		}

		vectorParty := batch.GetOrCreateVectorParty(columnID, true)
		dataType, _ := upsertBatch.GetColumnType(col)
		cmpFunc := common.GetCompareFunc(dataType)

		// check whether the update mode is valid based on data type.
		forceWrite := false
		if forUpdate {
			switch columnUpdateMode {
			case common.UpdateForceOverwrite:
				// always update
				forceWrite = true
			case common.UpdateWithAddition:
				fallthrough
			case common.UpdateWithMin:
				fallthrough
			case common.UpdateWithMax:
				if !common.IsNumeric(dataType) {
					return utils.StackError(nil, "Unsupported data type %x for column update mode %x", dataType, columnUpdateMode)
				}
			}
		}

		for _, recordInfo := range records {
			if recordInfo.row >= upsertBatch.NumRows {
				return utils.StackError(nil, "Row index %d out of range %d", recordInfo.row, upsertBatch.NumRows)
			}

			// We explicitly treat different columns by checking whether they are
			// 1. Bool type
			// 2. Go types
			// 3. Other types
			// Via doing this, we save lots of stack space to storing all related fields for different cases.
			if dataType == common.Bool {
				val, valid, _ := upsertBatch.GetBool(recordInfo.row, col)
				if !valid && !forceWrite {
					continue
				}
				vectorParty.SetBool(recordInfo.index, val, valid)
			} else if common.IsGoType(dataType) {
				val := upsertBatch.ReadGoValue(recordInfo.row, col)
				valid := val != nil
				if !valid && !forceWrite {
					continue
				}
				vectorParty.SetGoValue(recordInfo.index, val, valid)
			} else {
				val, valid, _ := upsertBatch.GetValue(recordInfo.row, col)
				if !valid && !forceWrite {
					continue
				}

				// only read oldValue when mode is one of add, min, max.
				if columnUpdateMode >= common.UpdateWithAddition && columnUpdateMode <= common.UpdateWithMax {
					oldVal, oldValid := vectorParty.GetValue(recordInfo.index)
					// Only need to do calculation when old value is valid, otherwise we can directly
					// set what's in upsert batch.
					if oldValid {
						switch columnUpdateMode {
						case common.UpdateWithAddition:
							common.AdditionUpdate(oldVal, val, dataType)
						case common.UpdateWithMin:
							common.MinMaxUpdate(oldVal, val, dataType, cmpFunc, 1)
						case common.UpdateWithMax:
							common.MinMaxUpdate(oldVal, val, dataType, cmpFunc, -1)
						}
						continue
					}
				}

				// if the value is not updated, set the value directly using value from upsert batch.
				vectorParty.SetValue(recordInfo.index, val, valid)
			}
		}
	}
	return nil
}
