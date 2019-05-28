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

package redolog

import "github.com/uber/aresdb/memstore/common"

// IteratorInfo contains
type NextUpsertBatchInfo struct {
	// upsertbatch
	Batch *common.UpsertBatch
	// Redo log File id used to save this batch
	RedoLogFile int64
	// offset of this batch in above redo log file
	BatchOffset uint32
	// If this batch is coming from recovery
	Recovery bool
}

// convenient function type
type NextUpsertFunc func() *NextUpsertBatchInfo

// RedologManager defines rodelog manager interface
type RedologManager interface {
	// Iterator will return a next function to iterate through all coming upsert batches
	Iterator() (NextUpsertFunc, error)
	// Block call to wait for recovery finish
	WaitForRecoveryDone()
	// UpdateMaxEventTime update max eventime of given redolog file
	UpdateMaxEventTime(eventTime uint32, redoFile int64)
	// CheckpointRedolog checkpoint event time cutoff (from archiving) and redologFileID and batchOffset (from backfill)
	// to redolog manager
	CheckpointRedolog(cutoff uint32, redoFileCheckpointed int64, batchOffset uint32) error
	// Append the upsertbatch into redolog
	AppendToRedoLog(upsertBatch *common.UpsertBatch) (int64, uint32)
	// Get total bytes of all redo log files
	GetTotalSize() int
	// Get total number of redo log files
	GetNumFiles() int
	// Get number of batch received after recovery
	GetBatchReceived() int
	// Get number of batch recovered for recovery
	GetBatchRecovered() int
	// Close free resources held by redolog manager
	Close()
}
