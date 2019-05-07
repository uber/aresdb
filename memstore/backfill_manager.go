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

	"encoding/json"

	"github.com/uber/aresdb/metastore"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/utils"
)

// BackfillManager manages the records that need to be put into a backfill queue and merged with
// sorted batches directly.
type BackfillManager struct {
	sync.RWMutex `json:"-"`
	// Name of the table.
	TableName string `json:"-"`

	// The shard id of the table.
	Shard int `json:"-"`

	// queue to hold UpsertBatches to backfill
	UpsertBatches []*UpsertBatch `json:"-"`

	// keep track of the number of records in backfill queue
	NumRecords int `json:"numRecords"`

	// keep track of the size of the buffer that holds batches to be backfilled
	CurrentBufferSize int64 `json:"currentBufferSize"`

	// keep track of the size of the buffer that holds batches being backfilled
	BackfillingBufferSize int64 `json:"backfillingBufferSize"`

	// max buffer size to hold backfill data
	MaxBufferSize int64 `json:"maxBufferSize"`

	// threshold to trigger archive
	BackfillThresholdInBytes int64 `json:"backfillThresholdInBytes"`

	// keep track of the redo log file of the last batch backfilled
	LastRedoFile int64 `json:"lastRedoFile"`

	// keep track of the offset of the last batch backfilled
	LastBatchOffset uint32 `json:"lastBatchOffset"`

	// keep track of the redo log file of the last batch queued
	CurrentRedoFile int64 `json:"currentRedoFile"`

	// keep track of the offset of the last batch being queued
	CurrentBatchOffset uint32 `json:"currentBatchOffset"`

	AppendCond *sync.Cond `json:"-"`
}

// NewBackfillManager creates a new BackfillManager instance.
func NewBackfillManager(tableName string, shard int, tableConfig metaCom.TableConfig) *BackfillManager {
	backfillManager := BackfillManager{
		TableName:                tableName,
		Shard:                    shard,
		MaxBufferSize:            tableConfig.BackfillMaxBufferSize,
		BackfillThresholdInBytes: tableConfig.BackfillThresholdInBytes,
	}
	backfillManager.AppendCond = sync.NewCond(&backfillManager.RWMutex)
	return &backfillManager
}

// WaitForBackfillBufferAvailability blocks until backfill buffer is available
func (r *BackfillManager) WaitForBackfillBufferAvailability() {
	r.Lock()
	defer r.Unlock()

	for r.CurrentBufferSize+r.BackfillingBufferSize >= r.MaxBufferSize {
		utils.GetLogger().Debugf("Waiting for slots of backfill manager to be available."+
			"Current buffer size=%d, backfilling buffer size=%d, max buffer size=%d",
			r.CurrentBufferSize, r.BackfillingBufferSize, r.MaxBufferSize)
		r.AppendCond.Wait()
	}
}

// Append appends an upsert batch into the backfill queue.
// Returns true if buffer limit has been reached and caller may need to wait
func (r *BackfillManager) Append(upsertBatch *UpsertBatch, redoFile int64, batchOffset uint32) bool {
	r.Lock()
	defer r.Unlock()
	r.CurrentRedoFile = redoFile
	r.CurrentBatchOffset = batchOffset

	// advance position even if data is not for backfill
	if upsertBatch == nil {
		return false
	}

	utils.GetLogger().Debugf("Table %s: Backfill batch of size %v, redoLog=%d offset=%d", r.TableName, len(upsertBatch.buffer)+upsertBatch.alternativeBytes,
		redoFile, batchOffset)

	r.UpsertBatches = append(r.UpsertBatches, upsertBatch)
	r.NumRecords += upsertBatch.NumRows
	r.CurrentBufferSize += (int64)(len(upsertBatch.buffer) + upsertBatch.alternativeBytes)
	utils.GetReporter(r.TableName, r.Shard).GetGauge(utils.BackfillBufferFillRatio).Update(float64(r.CurrentBufferSize+r.BackfillingBufferSize) / float64(r.MaxBufferSize))
	utils.GetReporter(r.TableName, r.Shard).GetGauge(utils.BackfillBufferSize).Update(float64(r.CurrentBufferSize + r.BackfillingBufferSize))
	utils.GetReporter(r.TableName, r.Shard).GetGauge(utils.BackfillBufferNumRecords).Update(float64(r.NumRecords))

	if r.CurrentBufferSize+r.BackfillingBufferSize >= int64(float64(r.MaxBufferSize)*0.95) {
		utils.GetLogger().With("size", r.CurrentBufferSize+r.BackfillingBufferSize).
			Warnf("Table %s Backfill buffer is full", r.TableName)
		return true
	}

	return false
}

// ReadUpsertBatch reads upsert batch in backfill queue, user should not lock schema
func (r *BackfillManager) ReadUpsertBatch(index, start, length int, schema *TableSchema) (data [][]interface{}, columnNames []string, err error) {
	r.RLock()
	defer r.RUnlock()

	if index < len(r.UpsertBatches) {
		upsertBatch := r.UpsertBatches[index]
		columnNames, err = upsertBatch.GetColumnNames(schema)
		if err != nil {
			return
		}

		data, err = upsertBatch.ReadData(start, length)
		if err != nil {
			return
		}
	}
	return
}

// StartBackfill gets a slice of UpsertBatches from backfill queue and returns the
// CurrentRedoFile and CurrentBatchOffset.
func (r *BackfillManager) StartBackfill() ([]*UpsertBatch, int64, uint32) {
	r.Lock()
	defer r.Unlock()

	utils.GetLogger().With("action", "Backfill", "table", r.TableName, "shard", r.Shard,
		"lastRedoFile", r.LastRedoFile, "lastOffset", r.LastBatchOffset, "newRedoFile", r.CurrentRedoFile,
		"newOffset", r.CurrentBatchOffset).Info("Start backfill")

	// no data to backfill
	// but CurrentRedoFile/CurrentBatchOffset may not be checkpointed yet(live batch)
	if r.CurrentBufferSize == 0 {
		return nil, r.CurrentRedoFile, r.CurrentBatchOffset
	}

	r.BackfillingBufferSize = r.CurrentBufferSize
	r.CurrentBufferSize = 0
	r.NumRecords = 0
	batches := r.UpsertBatches
	r.UpsertBatches = nil

	return batches, r.CurrentRedoFile, r.CurrentBatchOffset
}

// QualifyToTriggerBackfill decides if OK to trigger size-based backfill process
func (r *BackfillManager) QualifyToTriggerBackfill() bool {
	r.RLock()
	defer r.RUnlock()
	return r.CurrentBufferSize >= r.BackfillThresholdInBytes
}

// advanceOffset cleans up space and wakes up enqueue processes
func (r *BackfillManager) advanceOffset(redoFile int64, offset uint32) {
	r.BackfillingBufferSize = 0
	r.LastRedoFile = redoFile
	r.LastBatchOffset = offset
	r.AppendCond.Broadcast()
}

// GetLatestRedoFileAndOffset returns latest redofile and its batch offset
func (r *BackfillManager) GetLatestRedoFileAndOffset() (int64, uint32) {
	r.RLock()
	defer r.RUnlock()
	return r.LastRedoFile, r.LastBatchOffset
}

// MarshalJSON marshals a BackfillManager into json.
func (r *BackfillManager) MarshalJSON() ([]byte, error) {
	// Avoid json.Marshal loop calls.
	r.RLock()
	defer r.RUnlock()

	type alias BackfillManager
	marshalBackfillManager := struct {
		*alias
		NumUpsertBatches int `json:"numUpsertBatches"`
	}{
		alias:            (*alias)(r),
		NumUpsertBatches: len(r.UpsertBatches),
	}

	return json.Marshal(marshalBackfillManager)
}

// Destruct set the golang object references used by backfill manager to be nil to trigger gc ealier.
func (r *BackfillManager) Destruct() {
	r.UpsertBatches = nil
}

// Done updates the backfill progress both in memory and in metastore.
func (r *BackfillManager) Done(currentRedoFile int64, currentBatchOffset uint32,
	metaStore metastore.MetaStore) error {
	r.Lock()
	defer r.Unlock()
	if currentRedoFile > r.LastRedoFile ||
		currentRedoFile == r.LastRedoFile && currentBatchOffset > r.LastBatchOffset {
		if err := metaStore.UpdateBackfillProgress(r.TableName, r.Shard, currentRedoFile,
			currentBatchOffset); err != nil {
			return err
		}
		utils.GetLogger().With("action", "Backfill", "table", r.TableName, "shard", r.Shard,
			"lastRedoFile", r.LastRedoFile, "lastOffset", r.LastBatchOffset, "newRedoFile", currentRedoFile,
			"newOffset", currentBatchOffset).Info("Finish backfill")
		r.advanceOffset(currentRedoFile, currentBatchOffset)
	}

	return nil
}
