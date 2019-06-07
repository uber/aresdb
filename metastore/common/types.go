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

package common

// MetaStore defines interfaces of the external metastore,
// which can be implemented using file system, SQLite, Zookeeper etc.
type MetaStore interface {
	GetEnumDict(table, column string) ([]string, error)

	// Sets the watcher for the specified enum column.
	// Should only be called once for each enum column.
	// Returns a events channel that emits enum cases starting from startCase,
	// and a done channel for consumer to ack once the event is processed.
	WatchEnumDictEvents(table, column string, startCase int) (events <-chan string, done chan<- struct{}, err error)

	// shard level meta data does not have watcher interfaces.

	// Returns the latest archiving/live cutoff for the specified shard.
	GetArchivingCutoff(table string, shard int) (uint32, error)

	// PurgeArchiveBatches deletes the metadata related to the archive batch
	PurgeArchiveBatches(table string, shard, batchIDStart, batchIDEnd int) error
	// Returns the version to use for the specified archive batch and size of the batch with the
	// specified archiving/live cutoff.
	GetArchiveBatchVersion(table string, shard, batchID int, cutoff uint32) (uint32, uint32, int, error)
	// Returns the latest snapshot version for the specified shard.
	// the return value is: redoLogFile, offset, lastReadBatchID, lastReadBatchOffset
	GetSnapshotProgress(table string, shard int) (int64, uint32, int32, uint32, error)

	// shard ownership.
	GetOwnedShards(table string) ([]int, error)

	// Set the watcher for table shard ownership change events.
	// Should only be called once.
	// Returns an event channel that emits desired ownership states,
	// and a done channel for consumer to ack once the event is processed.
	WatchShardOwnershipEvents() (events <-chan ShardOwnership, done chan<- struct{}, err error)

	// A subset of newly added columns can be appended to the end of
	// ArchivingSortColumns by adding their index in columns to archivingSortColumns
	// Update column config.
	// Returns the assigned case IDs for each case string.
	ExtendEnumDict(table, column string, enumCases []string) ([]int, error)

	// List available archive batches
	GetArchiveBatches(table string, shard int, start, end int32) ([]int, error)

	// Adds a version and size for the specified archive batch.
	AddArchiveBatchVersion(table string, shard, batchID int, version uint32, seqNum uint32, batchSize int) error

	// Updates the archiving/live cutoff time for the specified shard. This is used
	// by the archiving job after each successful run.
	UpdateArchivingCutoff(table string, shard int, cutoff uint32) error

	// Updates the latest snapshot version for the specified shard.
	UpdateSnapshotProgress(table string, shard int, redoLogFile int64, upsertBatchOffset uint32, lastReadBatchID int32, lastReadBatchOffset uint32) error

	// Updates the latest redolog/offset that have been backfilled for the specified shard.
	UpdateBackfillProgress(table string, shard int, redoLogFile int64, offset uint32) error

	// Retrieve the latest redolog/offset that have been backfilled for the specified shard.
	GetBackfillProgressInfo(table string, shard int) (int64, uint32, error)

	// Update ingestion commit offset, used for kafka like streaming ingestion
	UpdateRedoLogCommitOffset(table string, shard int, offset int64) error

	// Get ingestion commit offset, used for kafka like streaming ingestion
	GetRedoLogCommitOffset(table string, shard int) (int64, error)

	// Update ingestion checkpoint offset, used for kafka like streaming ingestion
	UpdateRedoLogCheckpointOffset(table string, shard int, offset int64) error

	// Get ingestion checkpoint offset, used for kafka like streaming ingestion
	GetRedoLogCheckpointOffset(table string, shard int) (int64, error)

	TableSchemaWatchable
	TableSchemaMutator
}

// TableSchemaReader reads table schema
type TableSchemaReader interface {
	ListTables() ([]string, error)
	GetTable(name string) (*Table, error)
}

// TableSchemaWatchable watches table schema update events
type TableSchemaWatchable interface {
	// Sets the watcher for table list change (table deletion) events.
	// Should only be called once.
	// Returns a events channel that emits the entire table list on each table deletion event,
	// and a done channel for consumer to ack once the event is processed.
	WatchTableListEvents() (events <-chan []string, done chan<- struct{}, err error)
	// Sets the watcher for table modification/addition events.
	// Should only be called once.
	// Returns a events channel that emits the table schema on each change event for given table,
	// and a done channel for consumer to ack once the event is processed.
	WatchTableSchemaEvents() (events <-chan *Table, done chan<- struct{}, err error)
}

// TableSchemaMutator mutates table metadata
type TableSchemaMutator interface {
	TableSchemaReader
	CreateTable(table *Table) error
	DeleteTable(name string) error
	UpdateTableConfig(table string, config TableConfig) error
	UpdateTable(table Table) error

	// A subset of newly added columns can be appended to the end of
	// ArchivingSortColumns by adding their index in columns to archivingSortColumns
	AddColumn(table string, column Column, appendToArchivingSortOrder bool) error
	// Update column config.
	UpdateColumn(table string, column string, config ColumnConfig) error
	DeleteColumn(table string, column string) error
}
