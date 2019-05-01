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

// ColumnConfig defines the schema of a column config that can be mutated by
// UpdateColumn API call.
// swagger:model columnConfig
type ColumnConfig struct {
	// ColumnEvictionConfig : For column level in-memory eviction, it’s the best
	// effort TTL for in-memory data.
	// Column level eviction has nothing to do with data availability, but based
	// on how much data we pre-loaded, the major impact will be there for query
	// performance. Here we bring in two priorities configs: Preloading days and
	// Priority.
	//   - Preloading days is defined at each column level to indicate how many
	//     recent days data we want to preload to host memory. This is best effort
	//     operation.
	//   - Priority is defined at each column level to indicate the priority of
	//     each column. When data eviction happens, we will rely on column priority
	//     to decide which column will be evicted first.
	//     High number implies high priority.
	PreloadingDays int   `json:"preloadingDays,omitempty"`
	Priority       int64 `json:"priority,omitempty"`
}

// Column defines the schema of a column from MetaStore.
// swagger:model column
type Column struct {
	// Immutable, columns cannot be renamed.
	Name string `json:"name"`
	// Immutable, columns cannot have their types changed.
	Type string `json:"type"`
	// Deleted columns are kept as placeholders in Table.Columns.
	// read only: true
	Deleted bool `json:"deleted,omitempty"`
	// We store the default value as string here since it's from user input.
	// Nil means the default value is NULL. Actual default value of column data type
	// should be stored in memstore.
	DefaultValue *string `json:"defaultValue,omitempty"`

	// Whether to compare characters case insensitively for enum columns. It only matters
	// for ingestion client as it's the place to concert enum strings to enum values.
	CaseInsensitive bool `json:"caseInsensitive,omitempty"`

	// Whether disable enum cases auto expansion.
	DisableAutoExpand bool `json:"disableAutoExpand,omitempty"`

	// Mutable column configs.
	Config ColumnConfig `json:"config,omitempty"`

	// HLLEnabled determines whether a column is enabled for hll cardinality estimation
	// HLLConfig is immutable
	HLLConfig HLLConfig `json:"hllConfig,omitempty"`
}

// HLLConfig defines hll configuration
// swagger:model hllConfig
type HLLConfig struct {
	IsHLLColumn bool `json:"isHLLColumn,omitempty"`
}

// TableConfig defines the table configurations that can be changed
// swagger:model tableConfig
type TableConfig struct {
	// Common table configs

	// Initial setting of number of buckets for primary key
	// if equals to 0, default will be used
	InitialPrimaryKeyNumBuckets int `json:"initPrimaryKeyNumBuckets,omitempty"`

	// Size of each live batch, should be sufficiently large.
	BatchSize int `json:"batchSize,omitempty" validate:"min=1"`

	// Specifies how often to create a new redo log file.
	RedoLogRotationInterval int `json:"redoLogRotationInterval,omitempty" validate:"min=1"`

	// Specifies the size limit of a single redo log file.
	MaxRedoLogFileSize int `json:"maxRedoLogFileSize,omitempty" validate:"min=1"`

	// Fact table specific configs

	// Number of minutes after event time before a record can be archived.
	ArchivingDelayMinutes uint32 `json:"archivingDelayMinutes,omitempty" validate:"min=1"`
	// Specifies how often archiving runs.
	ArchivingIntervalMinutes uint32 `json:"archivingIntervalMinutes,omitempty" validate:"min=1"`

	// Specifies how often backfill runs.
	BackfillIntervalMinutes uint32 `json:"backfillIntervalMinutes,omitempty" validate:"min=1"`

	// Upper limit of current backfill buffer size + backfilling buffer size.
	BackfillMaxBufferSize int64 `json:"backfillMaxBufferSize,omitempty" validate:"min=1"`

	// Backfill buffer size in bytes that will trigger a backfill job.
	BackfillThresholdInBytes int64 `json:"backfillThresholdInBytes,omitempty" validate:"min=1"`

	// Size of each live batch used by backfill job.
	BackfillStoreBatchSize int `json:"backfillStoreBatchSize,omitempty" validate:"min=1"`

	// Records with timestamp older than now - RecordRetentionInDays will be skipped
	// during ingestion and backfill. 0 means unlimited days.
	RecordRetentionInDays int `json:"recordRetentionInDays,omitempty" validate:"min=1"`

	// Dimension table specific configs

	// Number of mutations to accumulate before creating a new snapshot.
	SnapshotThreshold int `json:"snapshotThreshold,omitempty" validate:"min=1"`

	// Specifies how often snapshot runs.
	SnapshotIntervalMinutes int `json:"snapshotIntervalMinutes,omitempty" validate:"min=1"`

	AllowMissingEventTime bool `json:"allowMissingEventTime,omitempty"`
}

// Table defines the schema and configurations of a table from MetaStore.
// swagger:model table
type Table struct {
	// Name of the table, immutable.
	Name string `json:"name"`
	// Index to Columns also serves as column IDs.
	Columns []Column `json:"columns"`
	// IDs of primary key columns. This field is immutable.
	PrimaryKeyColumns []int `json:"primaryKeyColumns"`
	// Whether this is a fact table.
	IsFactTable bool `json:"isFactTable"`

	// table configurations
	Config TableConfig `json:"config"`

	// Fact table only.
	// IDs of columns to sort based upon.
	ArchivingSortColumns []int `json:"archivingSortColumns,omitempty"`

	// Incarnation gets incremented every time an table name is reused
	// only used for controller managed schema in cluster setting
	Incarnation int `json:"incarnation"`
	// Version gets incremented every time when schema is updated
	// only used for controller managed schema in cluster setting
	Version int `json:"version"`
}

// IsEnumColumn checks whether a column is enum column
func (c *Column) IsEnumColumn() bool {
	return c.Type == BigEnum || c.Type == SmallEnum
}

// IsOverwriteOnlyDataType checks whether a column is overwrite only
func (c *Column) IsOverwriteOnlyDataType() bool {
	switch c.Type {
	case Uint8, Int8, Uint16, Int16, Uint32, Int32, Float32, Int64:
		return false
	default:
		return true
	}
}

// ShardOwnership defines an instruction on whether the receiving instance
// should start to own or disown the specified table shard.
type ShardOwnership struct {
	TableName string
	Shard     int
	ShouldOwn bool
}
