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

	"github.com/uber/aresdb/utils"
	"encoding/json"
	"time"
)

// SnapshotManager manages the snapshot related stats and progress.
type SnapshotManager struct {
	sync.RWMutex `json:"-"`

	// Job Trigger Condition related fields

	// Number of mutations since last snapshot. Measured as number of rows mutated.
	NumMutations int `json:"numMutations"`

	// Last snapshot time.
	LastSnapshotTime time.Time `json:"'lastSnapshotTime'"`

	// Snapshot progress related fields.

	// keep track of the redo log file of the last batch snapshotted.
	LastRedoFile int64 `json:"lastRedoFile"`

	// keep track of the offset of the last batch snapshotted
	LastBatchOffset uint32 `json:"lastBatchOffset"`

	// keep track of the record position of the last batch snapshotted
	LastRecord RecordID

	// keep track of the redo log file of the last batch queued
	CurrentRedoFile int64 `json:"currentRedoFile"`

	// keep track of the offset of the last batch queued
	CurrentBatchOffset uint32 `json:"currentBatchOffset"`

	// keep track of the record position when last batch queued
	CurrentRecord RecordID

	// Configs
	SnapshotInterval time.Duration `json:"snapshotInterval"`

	SnapshotThreshold int `json:"snapshotThreshold"`

	// for convenience.
	shard *TableShard
}

// NewSnapshotManager creates a new SnapshotManager instance.
func NewSnapshotManager(shard *TableShard) *SnapshotManager {
	return &SnapshotManager{
		shard:             shard,
		SnapshotThreshold: shard.Schema.Schema.Config.SnapshotThreshold,
		SnapshotInterval:  time.Duration(shard.Schema.Schema.Config.SnapshotIntervalMinutes) * time.Minute,
		LastSnapshotTime:  utils.Now(),
	}
}

// StartSnapshot returns current redo log file ,offset
func (s *SnapshotManager) StartSnapshot() (int64, uint32, int, RecordID) {
	s.RLock()
	defer s.RUnlock()
	return s.CurrentRedoFile, s.CurrentBatchOffset, s.NumMutations, s.CurrentRecord
}

// ApplyUpsertBatch advances CurrentRedoLogFile and CurrentBatchOffset and increments NumMutations after applying
// an upsert batch to live store.
func (s *SnapshotManager) ApplyUpsertBatch(redoFile int64, offset uint32, numMutations int, currentRecord RecordID) {
	s.Lock()
	defer s.Unlock()
	s.CurrentRedoFile = redoFile
	s.CurrentBatchOffset = offset
	s.NumMutations += numMutations
	s.CurrentRecord = currentRecord
}

// QualifyForSnapshot tells whether we can trigger a snapshot job.
func (s *SnapshotManager) QualifyForSnapshot() bool {
	s.RLock()
	defer s.RUnlock()
	now := utils.Now()
	return s.NumMutations >= s.SnapshotThreshold || (now.Sub(s.LastSnapshotTime) >= s.SnapshotInterval && s.NumMutations > 0)
}

// updateSnapshotProgress updates snapshot progress in memory. It also subtracts NumMutations by lastNumMutations to reflect correct
// number of mutations since last snapshot.
func (s *SnapshotManager) updateSnapshotProgress(redoFile int64, offset uint32, lastNumMutations int, record RecordID) {
	s.LastRedoFile = redoFile
	s.LastBatchOffset = offset
	s.NumMutations -= lastNumMutations
	s.LastRecord = record
}

// Done updates the snapshot progress both in memory and in metastore and updates number of mutations
// accordingly.
func (s *SnapshotManager) Done(currentRedoFile int64, currentBatchOffset uint32, lastNumMutations int, currentRecord RecordID) error {
	s.Lock()
	defer s.Unlock()
	// we should always record last snapshot time
	s.LastSnapshotTime = utils.Now()
	if currentRedoFile > s.LastRedoFile ||
		currentRedoFile == s.LastRedoFile && currentBatchOffset > s.LastBatchOffset {
		if err := s.shard.metaStore.UpdateSnapshotProgress(s.shard.Schema.Schema.Name, s.shard.ShardID,
			currentRedoFile, currentBatchOffset, currentRecord.BatchID, currentRecord.Index); err != nil {
			return err
		}
		s.updateSnapshotProgress(currentRedoFile, currentBatchOffset, lastNumMutations, currentRecord)
	}
	return nil
}

// GetLastSnapshotInfo get last snapshot redolog file, offset and timestamp, lastRecord
func (s *SnapshotManager) GetLastSnapshotInfo() (int64, uint32, time.Time, RecordID) {
	s.Lock()
	defer s.Unlock()
	return s.LastRedoFile, s.LastBatchOffset, s.LastSnapshotTime, s.LastRecord
}

// SetLastSnapshotInfo update last snapshot redolog file, offset and timestamp, lastRecord
func (s *SnapshotManager) SetLastSnapshotInfo(redoLogFile int64, offset uint32, record RecordID) {
	s.Lock()
	defer s.Unlock()
	s.LastRedoFile = redoLogFile
	s.LastBatchOffset = offset
	s.LastRecord = record
}

// MarshalJSON marshals a BackfillManager into json.
func (s *SnapshotManager) MarshalJSON() ([]byte, error) {
	type alias SnapshotManager
	s.RLock()
	defer s.RUnlock()
	return json.Marshal((*alias)(s))
}
