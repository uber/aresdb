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
	"encoding/json"
	"sync"

	"github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/utils"
	"github.com/uber-common/bark"
	"strconv"
)

// ArchiveBatch represents a archive batch.
type ArchiveBatch struct {
	Batch

	// Size of the batch (number of rows). Notice that compression changes the
	// length of some columns, does not change the size of the batch.
	Size int

	// Version for archive batches.
	Version uint32

	// SeqNum denotes backfill sequence number
	SeqNum uint32

	// For convenience.
	BatchID int32
	Shard   *TableShard
}

// ArchiveStoreVersion stores a version of archive batches of columnar data.
type ArchiveStoreVersion struct {
	// The mutex
	// protects the Batches map structure and the archiving cutoff field.
	// It does not protect contents within a batch. Before releasing
	// the VectorStore mutex, user should lock the batch level mutex if necessary
	// to ensure proper protection at batch level.
	sync.RWMutex `json:"-"`

	// Wait group used to prevent this ArchiveStore from being evicted
	Users sync.WaitGroup `json:"-"`

	// Each batch in the slice is identified by BaseBatchID+index.
	// Index out of bound and nil Batch for archive batches indicates that none of
	// the columns have been loaded into memory from disk.
	Batches map[int32]*ArchiveBatch `json:"batches"`

	// The archiving cutoff used for this version of the sorted store.
	ArchivingCutoff uint32 `json:"archivingCutoff"`

	// For convenience.
	shard *TableShard
}

// ArchiveStore manages archive stores versions.
// Archive store version evolves to a new version after archiving.
// Readers should follow the following locking protocol:
//   archiveStore.Users.Add(1)
//   // tableShard.ArchiveStore can no longer be accessed directly.
//   // continue reading from archiveStore
//   archiveStore.Users.Done()
type ArchiveStore struct {
	// The mutex protects the pointer pointing to the current version of archived vector version.
	sync.RWMutex

	PurgeManager *PurgeManager
	// Current version points to the most recent version of vector store version for queries to use.
	CurrentVersion *ArchiveStoreVersion
}

// NewArchiveStore creates a new archive store. Current version is just a place holder for test.
// It will be replaced during recovery.
func NewArchiveStore(shard *TableShard) ArchiveStore {
	return ArchiveStore{
		PurgeManager:   NewPurgeManager(shard),
		CurrentVersion: NewArchiveStoreVersion(0, shard),
	}
}

// NewArchiveStoreVersion creates a new empty archive store version given cutoff.
func NewArchiveStoreVersion(cutoff uint32, shard *TableShard) *ArchiveStoreVersion {
	return &ArchiveStoreVersion{
		Batches:         make(map[int32]*ArchiveBatch),
		ArchivingCutoff: cutoff,
		shard:           shard,
	}
}

// Destruct deletes all vectors allocated in C.
// Caller must detach the Shard first and wait until all users are finished.
func (s *ArchiveStore) Destruct() {
	if s.CurrentVersion == nil {
		return
	}
	shard := s.CurrentVersion.shard
	for _, batch := range s.CurrentVersion.Batches {
		for columnID, vp := range batch.Columns {
			if vp != nil {
				vp.SafeDestruct()
				shard.HostMemoryManager.ReportManagedObject(shard.Schema.Schema.Name, shard.ShardID,
					int(batch.BatchID), columnID, 0)
			}
		}
	}
}

// RequestBatch returns the requested archive batch from the archive store version.
func (v *ArchiveStoreVersion) RequestBatch(batchID int32) *ArchiveBatch {
	v.Lock()
	defer v.Unlock()

	batch, ok := v.Batches[batchID]
	if ok {
		return batch
	}

	// Read version and size from MetaStore.
	version, seqNum, size, err := v.shard.metaStore.GetArchiveBatchVersion(
		v.shard.Schema.Schema.Name, v.shard.ShardID, int(batchID), v.ArchivingCutoff)
	if err != nil {
		utils.GetLogger().WithFields(bark.Fields{
			"table":   v.shard.Schema.Schema.Name,
			"shard":   v.shard.ShardID,
			"batchID": batchID}).Panic(err)
	}
	batch = &ArchiveBatch{
		Version: version,
		SeqNum:  seqNum,
		Size:    size,
		BatchID: batchID,
		Shard:   v.shard,
	}
	v.Batches[batchID] = batch
	return batch
}

// WriteToDisk writes each column of a batch to disk. It happens on archiving
// stage for merged archive batch so there is no need to lock it.
func (b *ArchiveBatch) WriteToDisk() error {
	for columnID, column := range b.Columns {
		serializer := NewVectorPartyArchiveSerializer(
			b.Shard.HostMemoryManager, b.Shard.diskStore, b.Shard.Schema.Schema.Name, b.Shard.ShardID, columnID, int(b.BatchID), b.Version, b.SeqNum)
		if err := serializer.WriteVectorParty(column); err != nil {
			return err
		}
	}
	return nil
}

// GetCurrentVersion returns current SortedVectorStoreVersion and does proper locking. It'v used by
// query and data browsing. Users need to call version.Users.Done() after their work.
func (s *ArchiveStore) GetCurrentVersion() *ArchiveStoreVersion {
	s.RLock()
	version := s.CurrentVersion
	version.Users.Add(1)
	s.RUnlock()
	return version
}

// MarshalJSON marshals a ArchiveStore into json.
func (s *ArchiveStore) MarshalJSON() ([]byte, error) {
	currentVersion := s.GetCurrentVersion()
	defer currentVersion.Users.Done()
	return json.Marshal(map[string]interface{}{
		"currentVersion": currentVersion,
	})
}

// MarshalJSON marshals a ArchiveStoreVersion into json.
func (v *ArchiveStoreVersion) MarshalJSON() ([]byte, error) {
	// Avoid json.Marshal loop calls.
	type alias ArchiveStoreVersion
	v.RLock()
	defer v.RUnlock()
	return json.Marshal((*alias)(v))
}

// RequestVectorParty creates(optional), pins, and returns the requested vector party.
// On creation it also asynchronously loads a vector party from disk into memory.
//
// Caller must call vp.WaitForDiskLoad() before using it,
// and call vp.Release() afterwards.
func (b *ArchiveBatch) RequestVectorParty(columnID int) common.ArchiveVectorParty {
	b.Lock()
	defer b.Unlock()

	// This is a newly added column. We need to allocate more space to put this column.
	if columnID >= len(b.Columns) {
		b.Columns = append(b.Columns, make([]common.VectorParty, columnID-len(b.Columns)+1)...)
	}

	// archive batch always have archive batch
	vp := b.Columns[columnID]

	if vp != nil {
		// Release lock on batch and wait for vector party to be loaded
		archiveVP := vp.(common.ArchiveVectorParty)
		archiveVP.Pin()
		return archiveVP
	}

	// Set a placeholder to prevent repetitive loading,
	// columnID should always be smaller than len(ValueTypeByColumn).
	dataType := b.Shard.Schema.ValueTypeByColumn[columnID]
	defaultValue := b.Shard.Schema.DefaultValues[columnID]
	vp = newArchiveVectorParty(b.Size, dataType, *defaultValue, &b.RWMutex)
	b.Columns[columnID] = vp

	archiveVP := vp.(common.ArchiveVectorParty)
	archiveVP.Pin()
	archiveVP.LoadFromDisk(b.Shard.HostMemoryManager, b.Shard.diskStore, b.Shard.Schema.Schema.Name, b.Shard.ShardID, columnID, int(b.BatchID), b.Version, b.SeqNum)

	return archiveVP
}

// TryEvict attempts to evict and destruct the specified column from the archive
// batch. It will fail fast if the column is currently in use so that host
// memory manager can try evicting other VPs immediately.
// Returns vector party evicted if succeeded.
func (b *ArchiveBatch) TryEvict(columnID int) common.ArchiveVectorParty {
	return b.evict(columnID, false)
}

// BlockingDelete blocks until all users are finished with the specified column,
// and then deletes the column from the batch.
// Returns the vector party deleted if any.
func (b *ArchiveBatch) BlockingDelete(columnID int) common.ArchiveVectorParty {
	return b.evict(columnID, true)
}

// evict attempts to evict and destruct the specified column from the archive
// batch. It will block if the blocking is set to true, other wise it will fail
// fast.
func (b *ArchiveBatch) evict(columnID int, blocking bool) common.ArchiveVectorParty {
	b.Lock()
	defer b.Unlock()

	if columnID >= len(b.Columns) {
		return nil
	}

	rawVP := b.Columns[columnID]
	if rawVP == nil {
		return nil
	}

	vp := rawVP.(common.ArchiveVectorParty)
	if !vp.WaitForUsers(blocking) {
		return nil
	}

	b.Columns[columnID] = nil
	vp.SafeDestruct()
	b.Shard.HostMemoryManager.ReportManagedObject(
		b.Shard.Schema.Schema.Name, b.Shard.ShardID, int(b.BatchID), columnID, 0)
	return vp
}

// GetBatchForRead returns a archiveBatch for read,
// reader needs to unlock after use
func (v *ArchiveStoreVersion) GetBatchForRead(batchID int) *ArchiveBatch {
	batch := v.Batches[int32(batchID)]
	if batch != nil {
		batch.RLock()
	}
	return batch
}

// MarshalJSON marshals a ArchiveBatch into json.
func (b *ArchiveBatch) MarshalJSON() ([]byte, error) {
	b.RLock()
	defer b.RUnlock()
	return json.Marshal(map[string]interface{}{
		"numColumns": len(b.Columns),
		"size":       b.Size,
		"version":    b.Version,
	})
}

// BuildIndex builds an index over the primary key columns of this archive batch and inserts the records id into the
// given primary key.
func (b *ArchiveBatch) BuildIndex(sortColumns []int, primaryKeyColumns []int, pk PrimaryKey) error {
	if b == nil {
		return nil
	}

	key := make([]byte, b.Shard.Schema.PrimaryKeyBytes)
	primaryKeyValues := make([]common.DataValue, len(primaryKeyColumns))

	// we need to use sortedColumnIterator to advance sort column.
	sortedColumnIterators := make([]sortedColumnIterator, len(primaryKeyColumns))
	// create sort column iterators if the primary key column is also a sort column.
	for i, primaryKeyColumnID := range primaryKeyColumns {
		if utils.IndexOfInt(sortColumns, primaryKeyColumnID) >= 0 {
			sortedColumnIterators[i] = newArchiveBatchColumnIterator(b, primaryKeyColumnID, nil)
			sortedColumnIterators[i].setEndPosition(uint32(b.Size))
		}
	}

	numDuplicateRecords := 0
	for row := 0; row < b.Size; row++ {
		// Prepare primary key values.
		for i, primaryKeyColumnID := range primaryKeyColumns {
			if sortedColumnIterators[i] != nil {
				if uint32(row) >= sortedColumnIterators[i].nextPosition() {
					sortedColumnIterators[i].next()
				}
				primaryKeyValues[i] = sortedColumnIterators[i].value()
			} else {
				// Primary key column will not have any default values.
				primaryKeyValues[i] = b.GetDataValue(row, primaryKeyColumnID)
			}

			if !primaryKeyValues[i].Valid {
				return utils.StackError(nil, "Primary key col is null at row %d col %d "+
					"batchID %d batch %v",
					row, primaryKeyColumnID, b.BatchID, b)
			}
		}

		// Get primary key for each record.
		if err := GetPrimaryKeyBytes(primaryKeyValues, key); err != nil {
			return err
		}

		existing, _, err := pk.FindOrInsert(key, RecordID{BatchID: b.BatchID, Index: uint32(row)}, 0)

		if err != nil {
			return err
		}

		if existing {
			// Found duplicate record in backfill is a data correctness issue,
			// in which new update will only go to one of the records depending on the sort order.
			// we decide for now this rare case will proceed but trigger alert
			// so that user can adjust schema and backfill data when needed,
			// instead of crash the server completely.
			numDuplicateRecords++
		}
	}

	if numDuplicateRecords > 0 {
		utils.GetLogger().WithFields(bark.Fields{
			"table": b.Shard.Schema.Schema.Name,
			"shard": b.Shard.ShardID,
			"batch": b.BatchID,
			"error": "duplicate record",
		}).Errorf("duplicate record found when building index")
		utils.GetReporter(b.Shard.Schema.Schema.Name, b.Shard.ShardID).GetChildGauge(map[string]string{
			"batch": strconv.FormatInt(int64(b.BatchID), 10),
		}, utils.DuplicateRecordRatio).Update(float64(numDuplicateRecords) / float64(b.Size))
	}

	return nil
}

// Clone returns a copy of current batch including all references to underlying vector parties. Caller is responsible
// for holding the lock (if necessary).
func (b *ArchiveBatch) Clone() *ArchiveBatch {
	newBatch := &ArchiveBatch{
		Batch: Batch{
			Columns: make([]common.VectorParty, len(b.Columns)),
		},
		Version: b.Version,
		SeqNum:  b.SeqNum,
		Size:    b.Size,
		BatchID: b.BatchID,
		Shard:   b.Shard,
	}

	copy(newBatch.Columns, b.Columns)
	return newBatch
}

// UnpinVectorParties unpins all vector parties in the slice.
func UnpinVectorParties(requestedVPs []common.ArchiveVectorParty) {
	for _, vp := range requestedVPs {
		vp.Release()
	}
}
