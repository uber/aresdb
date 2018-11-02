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

package diskstore

import (
	"io"

	"github.com/uber/aresdb/utils"
)

// DiskStore defines the interface for reading/writing redo logs, snapshot files, and archived vector party files.
type DiskStore interface {
	// Table shard level operation

	// Completely wipe out a table shard.
	DeleteTableShard(table string, shard int) error

	// Redo logs.

	// Returns the file creation unix time in second for each log file as a sorted slice.
	ListLogFiles(table string, shard int) ([]int64, error)
	// Opens the specified log file for replay.
	OpenLogFileForReplay(table string, shard int, creationTime int64) (utils.ReaderSeekerCloser, error)
	// Opens/creates the specified log file for append.
	OpenLogFileForAppend(table string, shard int, creationTime int64) (io.WriteCloser, error)
	// Deletes the specified log file.
	DeleteLogFile(table string, shard int, creationTime int64) error
	// Truncate Redolog to drop the last incomplete/corrupted upsert batch.
	TruncateLogFile(table string, shard int, creationTime int64, offset int64) error

	// Snapshot files.
	// Snapshots are stored in following format:
	// {root_path}/data/{table_name}_{shard_id}/snapshots/
	// 	 -- {redo_log1}_{offset1}
	//     -- {batchID1}
	//        -- {column1}.data
	//        -- {column2}.data
	//     -- {batchID2}
	//        -- {column1}.data
	//        -- {column2}.data
	//
	// 	 -- {redo_log2}_{offset2}
	//     -- {batchID1}
	//        -- {column1}.data
	//        -- {column2}.data
	//     -- {batchID2}
	//        -- {column1}.data
	//        -- {column2}.data
	// For all following snapshot methods, a {redo_log}_{offset} specifies the snapshot version.

	// Returns the batch directories under a specific snapshot directory.
	ListSnapshotBatches(table string, shard int,
		redoLogFile int64, offset uint32) ([]int, error)

	// Returns the vector party files under a specific snapshot batch directory.
	// the return value is sorted
	ListSnapshotVectorPartyFiles(table string, shard int,
		redoLogFile int64, offset uint32, batchID int) ([]int, error)

	// Opens the snapshot vector party file for read.
	OpenSnapshotVectorPartyFileForRead(table string, shard int,
		redoLogFile int64, offset uint32, batchID int, columnID int) (io.ReadCloser, error)
	// Creates/truncates the snapshot column file for write.

	OpenSnapshotVectorPartyFileForWrite(table string, shard int,
		redoLogFile int64, offset uint32, batchID int, columnID int) (io.WriteCloser, error)
	// Deletes snapshot files **older than** the specified version.
	DeleteSnapshot(table string, shard int, redoLogFile int64, offset uint32) error

	// Archived vector party files.

	// Opens the vector party file at the specified batchVersion for read.
	OpenVectorPartyFileForRead(table string, column, shard, batchID int, batchVersion uint32,
		seqNum uint32) (io.ReadCloser, error)
	// Creates/truncates the vector party file at the specified batchVersion for write.
	OpenVectorPartyFileForWrite(table string, column, shard, batchID int, batchVersion uint32,
		seqNum uint32) (io.WriteCloser, error)
	// Deletes all old batches with the specified batchID that have version lower than or equal to the specified batch
	// version. All columns of those batches will be deleted.
	DeleteBatchVersions(table string, shard, batchID int, batchVersion uint32, seqNum uint32) error
	// Deletes all batches within range [batchIDStart, batchIDEnd)
	DeleteBatches(table string, shard, batchIDStart, batchIDEnd int) (int, error)
	// Deletes all batches of the specified column.
	DeleteColumn(table string, column, shard int) error
}
