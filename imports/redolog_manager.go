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

package imports

import "github.com/uber/aresdb/memstore/common"

// RedologManager defines rodelog manager interface
type RedologManager interface {
	// Start recovery and import
	Start(redoFilePersisited int64, offsetPersisted uint32) error
	// Block call to wait for recovery finish
	WaitForRecoveryDone()
	// Support ingestion from HTTP
	WriteUpsertBatch(upsertBatch *common.UpsertBatch) error
	// Save the upsertbatch into redolog
	AppendToRedoLog(upsertBatch *common.UpsertBatch, offsetInSource int64) (int64, uint32)
	// UpdateMaxEventTime update max eventime of given redolog file
	UpdateMaxEventTime(eventTime uint32, redoFile int64)
	// CheckpointRedolog checkpoint event time cutoff (from archiving) and redologFileID and batchOffset (from backfill)
	// to redolog manager
	CheckpointRedolog(cutoff uint32, redoFileCheckpointed int64, batchOffset uint32) error
	// GetTotalSize returns the total size of all redologs tracked in redolog manager
	GetTotalSize() int
	// Get the number of files tracked in redolog manager
	GetNumFiles() int
	// Close free resources held by redolog manager
	Close()
}
