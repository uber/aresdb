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
	"math"
	"sync"
	"time"

	"encoding/json"

	"github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/memutils"
	"github.com/uber/aresdb/utils"
)

// BaseBatchID is the starting id of all batches.
const BaseBatchID = int32(math.MinInt32)

// LiveBatch represents a live batch.
type LiveBatch struct {
	// The common data structure holding column data.
	Batch

	// Capacity of the batch which is decided at the creation time.
	Capacity int

	// For convenience to access fields of live store.
	// Schema locks should be acquired after data locks.
	liveStore *LiveStore

	// maximum of arrival time
	MaxArrivalTime uint32
}

// LiveStore stores live batches of columnar data.
type LiveStore struct {
	sync.RWMutex

	// Following fields are protected by above mutex.

	// The batch id to batch map.
	Batches map[int32]*LiveBatch

	// Number of rows to create for new batches.
	BatchSize int

	// The upper bound of records (exclusive) that can be read by queries.
	LastReadRecord RecordID

	// This is the in memory archiving cutoff time high watermark that gets set by the archiving job
	// before each archiving run. Ingestion will not insert/update records that are older than
	// the archiving cutoff watermark.
	ArchivingCutoffHighWatermark uint32

	// Logs.
	RedoLogManager RedoLogManager

	// Manage backfill queue during ingestion.
	BackfillManager *BackfillManager

	// Manage snapshot related stats.
	SnapshotManager *SnapshotManager

	// For convenience. Schema locks should be acquired after data locks.
	tableSchema *TableSchema

	// The writer lock is to guarantee single writer to a Shard at all time. To ensure this, writers
	// (ingestion, archiving etc) need to hold this lock at all times. This lock
	// should be acquired before the VectorStore and Batch locks.
	// TODO: if spinning lock performance is a concern we may need to upgrade this
	// to a designated goroutine with a pc-queue channel.
	WriterLock sync.RWMutex

	// Following fields are protected by WriterLock.

	// Primary key table of the Shard.
	PrimaryKey PrimaryKey

	// The position of the next record to be used for writing. Only used by the ingester.
	NextWriteRecord RecordID

	// For convenience.
	HostMemoryManager common.HostMemoryManager `json:"-"`

	// Last modified time per column in live store and fact table only. Used to measure the data freshness for each column.
	// Protected by the writer lock of live store. If a column is never ingested, thhe last modified time will be zero.
	// Metrics will be emitted after each ingestion request.
	lastModifiedTimePerColumn []uint32
}

// NewLiveStore creates a new live batch.
func NewLiveStore(batchSize int, shard *TableShard) *LiveStore {
	schema := shard.Schema
	tableCfg := schema.Schema.Config
	ls := &LiveStore{
		BatchSize:       batchSize,
		Batches:         make(map[int32]*LiveBatch),
		tableSchema:     schema,
		LastReadRecord:  RecordID{BatchID: BaseBatchID, Index: 0},
		NextWriteRecord: RecordID{BatchID: BaseBatchID, Index: 0},
		PrimaryKey:      NewPrimaryKey(schema.PrimaryKeyBytes, schema.Schema.IsFactTable, schema.Schema.Config.InitialPrimaryKeyNumBuckets, shard.HostMemoryManager),
		// TODO: support table specific log rotation interval.
		RedoLogManager: NewRedoLogManager(int64(tableCfg.RedoLogRotationInterval), int64(tableCfg.MaxRedoLogFileSize),
			shard.diskStore, schema.Schema.Name, shard.ShardID),
		HostMemoryManager: shard.HostMemoryManager,
	}

	if schema.Schema.IsFactTable {
		ls.BackfillManager = NewBackfillManager(schema.Schema.Name, shard.ShardID, schema.Schema.Config)
		// reportBatch memory usage of backfill max buffer size.
		ls.HostMemoryManager.ReportUnmanagedSpaceUsageChange(
			int64(ls.BackfillManager.MaxBufferSize * utils.GolangMemoryFootprintFactor))
	} else {
		ls.SnapshotManager = NewSnapshotManager(shard)
	}
	return ls
}

// GetBatchIDs snapshots the batches and returns a list of batch ids for read
// with the number of records in batchIDs[len()-1].
func (s *LiveStore) GetBatchIDs() (batchIDs []int32, numRecordsInLastBatch int) {
	s.RLock()
	for key, batch := range s.Batches {
		if key < s.LastReadRecord.BatchID {
			batchIDs = append(batchIDs, key)
			numRecordsInLastBatch = batch.Capacity
		}
	}
	if s.LastReadRecord.Index > 0 {
		batchIDs = append(batchIDs, s.LastReadRecord.BatchID)
		numRecordsInLastBatch = int(s.LastReadRecord.Index)
	}
	s.RUnlock()
	return
}

// GetBatchForRead returns and read locks the batch with its ID for reads. Caller must explicitly
// RUnlock() the returned batch after all reads.
func (s *LiveStore) GetBatchForRead(id int32) *LiveBatch {
	s.RLock()
	batch := s.Batches[id]
	if batch != nil {
		batch.RLock()
	}
	s.RUnlock()
	return batch
}

// GetBatchForWrite returns and locks the batch with its ID for reads. Caller must explicitly
// Unlock() the returned batch after all reads.
func (s *LiveStore) GetBatchForWrite(id int32) *LiveBatch {
	s.RLock()
	batch := s.Batches[id]
	if batch != nil {
		batch.Lock()
	}
	s.RUnlock()
	return batch
}

// GetOrCreateBatch retrieve LiveBatch for specified batchID, append one if not exist
// this is only used during read snapshots, otherwise the batchID maybe cause conflict
// user should unlock the batch after use
func (s *LiveStore) getOrCreateBatch(batchID int32) *LiveBatch {
	batch := s.GetBatchForWrite(batchID)
	if batch == nil {
		batch = s.appendBatch(batchID)
		batch.Lock()
	}
	return batch
}

// AdvanceNextWriteRecord reserves space for a record that return the next available record position
// back to the caller.
func (s *LiveStore) AdvanceNextWriteRecord() RecordID {
	s.RLock()
	batch := s.Batches[s.NextWriteRecord.BatchID]
	s.RUnlock()

	if batch == nil {
		// We only create batch when the current NextWriteRecord is pointing to nil.
		batch = s.appendBatch(s.NextWriteRecord.BatchID)
	}

	if int(s.NextWriteRecord.Index)+1 == batch.Capacity {
		s.NextWriteRecord.BatchID++
		s.NextWriteRecord.Index = 0
	} else {
		s.NextWriteRecord.Index++
	}

	return s.NextWriteRecord
}

// AdvanceLastReadRecord advances the high watermark of the rows to the next write record.
func (s *LiveStore) AdvanceLastReadRecord() {
	s.Lock()
	s.LastReadRecord = s.NextWriteRecord
	s.Unlock()
}

// appendBatch appends a new batch. The batch is returned with its ID.
func (s *LiveStore) appendBatch(batchID int32) *LiveBatch {
	if s.Batches[batchID] != nil {
		return nil
	}
	valueTypeByColumn := s.tableSchema.GetValueTypeByColumn()
	numColumns := len(valueTypeByColumn)

	batch := &LiveBatch{
		Batch: Batch{
			Columns: make([]common.VectorParty, numColumns),
		},
		Capacity:  s.BatchSize,
		liveStore: s,
	}
	s.Lock()
	s.Batches[batchID] = batch
	s.Unlock()
	return batch
}

// PurgeBatch purges the specified batch.
func (s *LiveStore) PurgeBatch(id int32) {
	s.Lock()
	// Detach first.
	batch := s.Batches[id]
	delete(s.Batches, id)
	s.Unlock()

	if batch != nil {
		// Wait for readers to finish.
		batch.Lock()
		batch.Unlock()
		// SafeDestruct.
		for _, vp := range batch.Columns {
			if vp != nil {
				bytes := -vp.GetBytes()
				vp.SafeDestruct()
				s.HostMemoryManager.ReportUnmanagedSpaceUsageChange(bytes)
			}
		}
	}
}

// Destruct deletes all vectors allocated in C.
// Caller must detach the Shard first and wait until all users are finished.
func (s *LiveStore) Destruct() {
	s.PrimaryKey.Destruct()
	for batchID := range s.Batches {
		s.PurgeBatch(batchID)
	}

	// reportBatch free memory used by backfill manager.
	if s.BackfillManager != nil {
		s.BackfillManager.Destruct()

		go func() {
			// Delay 1 second to report memory change to wait for gc happens.
			timer := time.NewTimer(time.Second)
			<-timer.C
			s.HostMemoryManager.ReportUnmanagedSpaceUsageChange(
				-int64(s.BackfillManager.MaxBufferSize * utils.GolangMemoryFootprintFactor))
		}()
	}
	s.RedoLogManager.Close()
}

// PurgeBatches purges the specified batches.
func (s *LiveStore) PurgeBatches(ids []int32) {
	for _, id := range ids {
		s.PurgeBatch(id)
	}
}

// MarshalJSON marshals a LiveStore into json.
func (s *LiveStore) MarshalJSON() ([]byte, error) {
	// Avoid json.Marshal loop calls.
	jsonMap := make(map[string]interface{})

	// Following fields are protected by reader lock of liveStore.
	s.RLock()
	batchMapJSON, err := json.Marshal(s.Batches)
	if err != nil {
		s.RUnlock()
		return nil, err
	}
	jsonMap["batches"] = json.RawMessage(batchMapJSON)
	jsonMap["batchSize"] = s.BatchSize
	jsonMap["lastReadRecord"] = s.LastReadRecord
	jsonMap["lastModifiedTimePerColumn"] = s.lastModifiedTimePerColumn

	redologManagerJSON, err := json.Marshal(&s.RedoLogManager)
	if err != nil {
		s.RUnlock()
		return nil, err
	}
	jsonMap["redoLogManager"] = json.RawMessage(redologManagerJSON)

	backfillManagerJSON, err := json.Marshal(s.BackfillManager)
	if err != nil {
		s.RUnlock()
		return nil, err
	}
	jsonMap["backfillManager"] = json.RawMessage(backfillManagerJSON)

	snapshotManagerJSON, err := json.Marshal(s.SnapshotManager)
	if err != nil {
		s.RUnlock()
		return nil, err
	}
	jsonMap["snapshotManager"] = json.RawMessage(snapshotManagerJSON)

	s.RUnlock()

	// Following fields are protected by writer lock of liveStore.
	s.WriterLock.RLock()
	pkJSON, err := MarshalPrimaryKey(s.PrimaryKey)
	if err != nil {
		s.WriterLock.RUnlock()
		return nil, err
	}
	jsonMap["primaryKey"] = json.RawMessage(pkJSON)
	jsonMap["nextWriteRecord"] = s.NextWriteRecord

	s.WriterLock.RUnlock()
	return json.Marshal(jsonMap)
}

// LookupKey looks up the given key in primary key.
func (s *LiveStore) LookupKey(keyStrs []string) (RecordID, bool) {
	key := make([]byte, s.tableSchema.PrimaryKeyBytes)
	if len(s.tableSchema.PrimaryKeyColumnTypes) != len(keyStrs) {
		return RecordID{}, false
	}

	index := 0
	for colIndex, columnType := range s.tableSchema.PrimaryKeyColumnTypes {
		dataValue, err := common.ValueFromString(keyStrs[colIndex], columnType)
		if err != nil || !dataValue.Valid {
			return RecordID{}, false
		}

		if dataValue.IsBool {
			if dataValue.BoolVal {
				key[index] = 1
			} else {
				key[index] = 0
			}
			index++
		} else {
			for i := 0; i < common.DataTypeBits(columnType)/8; i++ {
				key[index] = *(*byte)(memutils.MemAccess(dataValue.OtherVal, i))
				index++
			}
		}
	}

	s.WriterLock.RLock()
	defer s.WriterLock.RUnlock()
	return s.PrimaryKey.Find(key)
}

// GetMemoryUsageForColumn get the live store memory usage for given data type
func (s *LiveStore) GetMemoryUsageForColumn(valueType common.DataType, columnID int) int {
	batchIDs, _ := s.GetBatchIDs()

	var liveStoreMemory int
	for _, batchID := range batchIDs {
		liveBatch := s.GetBatchForRead(batchID)
		if liveBatch == nil {
			continue
		}

		if columnID < len(liveBatch.Columns) && liveBatch.Columns[columnID] != nil {
			liveStoreMemory += int(liveBatch.Columns[columnID].GetBytes())
		}
		liveBatch.RUnlock()
	}

	return liveStoreMemory
}

// GetOrCreateVectorParty returns LiveVectorParty for the specified column from
// the live batch. locked specifies whether the batch has been locked.
// The lock will be left in the same state after the function returns.
func (b *LiveBatch) GetOrCreateVectorParty(columnID int, locked bool) common.LiveVectorParty {
	// Ensure that columnID is not out of bound.
	if columnID >= len(b.Columns) {
		if !locked {
			b.Lock()
		}
		for columnID >= len(b.Columns) {
			b.Columns = append(b.Columns, nil)
		}
		if !locked {
			b.Unlock()
		}
	}

	// Ensure that the VectorParty is allocated with values and nulls.
	if b.Columns[columnID] == nil {
		b.liveStore.tableSchema.RLock()
		dataType := b.liveStore.tableSchema.ValueTypeByColumn[columnID]
		defaultValue := *b.liveStore.tableSchema.DefaultValues[columnID]
		b.liveStore.tableSchema.RUnlock()

		bytes := CalculateVectorPartyBytes(dataType, b.Capacity, true, false)
		b.liveStore.HostMemoryManager.ReportUnmanagedSpaceUsageChange(int64(bytes))
		liveVP := NewLiveVectorParty(b.Capacity, dataType, defaultValue, b.liveStore.HostMemoryManager)
		liveVP.Allocate(false)

		if !locked {
			b.Lock()
		}
		b.Columns[columnID] = liveVP
		if !locked {
			b.Unlock()
		}
		return liveVP
	}
	return b.Columns[columnID].(common.LiveVectorParty)
}

// MarshalJSON marshals a LiveBatch into json.
func (b *LiveBatch) MarshalJSON() ([]byte, error) {
	b.RLock()
	defer b.RUnlock()
	return json.Marshal(map[string]interface{}{
		"numColumns": len(b.Columns),
		"capacity":   b.Capacity,
	})
}
