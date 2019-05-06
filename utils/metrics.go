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

package utils

import (
	"fmt"
	"github.com/uber-go/tally"
	"strconv"
	"sync"
)

// MetricName is the type of the metric.
type MetricName int

// List of supported metric names.
const (
	AllocatedDeviceMemory MetricName = iota
	ArchivingIgnoredRecords
	ArchivingRecords
	ArchivingTimingTotal
	ArchivingHighWatermark
	ArchivingLowWatermark
	BackfillTimingTotal
	BackfillLockTiming
	EstimatedDeviceMemory
	HTTPHandlerCall
	HTTPHandlerLatency
	IngestedRecords
	AppendedRecords
	UpdatedRecords
	IngestedUpsertBatches
	UpsertBatchSize
	PrimaryKeyMissing
	TimeColumnMissing
	BackfillRecords
	BackfillAffectedDays
	BackfillNewRecords
	BackfillNoEffectRecords
	BackfillInplaceUpdateRecords
	BackfillDeleteThenInsertRecords
	BackfillRecordsTimeDifference
	BackfillRecordsRatio
	BackfillRecordsColumnRemoved
	DuplicateRecordRatio
	RecoveryIgnoredRecordsTimeDifference
	RecoveryIgnoredRecords
	RecoveryLatency
	TotalMemorySize
	UnmanagedMemorySize
	ManagedMemorySize
	BackfillBufferFillRatio
	BackfillBufferSize
	BackfillBufferNumRecords
	IngestionLagPerColumn
	NumberOfEnumCasesPerColumn
	CurrentRedologCreationTime
	CurrentRedologSize
	NumberOfRedologs
	SizeOfRedologs
	QueryFailed
	QuerySucceeded
	QueryLatency
	QuerySQLParsingLatency
	QueryWaitForMemoryDuration
	QueryReceived
	QueryLiveRecordsProcessed
	QueryArchiveRecordsProcessed
	QueryLiveBatchProcessed
	QueryArchiveBatchProcessed
	QueryLiveBytesTransferred
	QueryArchiveBytesTransferred
	QueryRowsReturned
	RecordsOutOfRetention
	SnapshotTimingTotal
	SnapshotTimingLoad
	SnapshotTimingBuildIndex
	TimezoneLookupTableCreationTime
	RedoLogFileCorrupt
	MemoryOverflow
	PreloadingZoneEvicted
	PurgeTimingTotal
	PurgedBatches
	RecordsFromFuture
	BatchSize
	BatchSizeReportTime
	SchemaFetchSuccess
	SchemaFetchFailure
	SchemaUpdateCount
	SchemaDeletionCount
	SchemaCreationCount
	// Enum sentinel.
	NumMetricNames
)

// MetricType is the supported metric type.
type MetricType int

// MetricTypes which are supported.
const (
	Counter MetricType = iota
	Gauge
	Timer
)

// metricDefinition contains the definition for a metric.
type metricDefinition struct {
	// scope name for this definition
	name string
	// additional tags
	tags map[string]string
	// metric type
	metricType MetricType

	// cached tally counter
	counter tally.Counter

	// cached tally gauge
	gauge tally.Gauge

	// cached tally timer
	timer tally.Timer
}

// Scope names .
const (
	scopeNameRecoveryLatency                 = "recovery_latency"
	scopeNameAllocatedDeviceMemory           = "allocated_device_memory"
	scopeNameArchivingRecords                = "archiving_records"
	scopeNameArchivingHighWatermark          = "archiving_high_watermark"
	scopeNameArchivingLowWatermark           = "archiving_low_watermark"
	scopeNameBackfillRecords                 = "backfill_records"
	scopeNameBackfillNewRecords              = "backfill_new_records"
	scopeNameBackfillNoEffectRecords         = "backfill_no_effect_records"
	scopeNameBackfillInplaceUpdateRecords    = "backfill_inplace_records"
	scopeNameBackfillDeleteInsertRecords     = "backfill_delete_insert_records"
	scopeNameBackfillAffectedDays            = "backfill_affected_days"
	scopeNameBackfillRecordsTimeDifference   = "backfill_records_time_diff"
	scopeNameBackfillRecordsRatio            = "backfill_records_ratio_per_batch"
	scopeNameBackfillLockTiming              = "backfill_lock_timing"
	scopeNameBackfillRecordsColumnRemoved    = "backfill_records_column_removed"
	scopeNameDuplicateRecordRatio            = "duplicate_record_ratio"
	scopeNameEstimatedDeviceMemory           = "estimated_device_memory"
	scopeNameHTTPHandlerCall                 = "http.call"
	scopeNameHTTPHandlerLatency              = "http.latency"
	scopeNamePrimaryKeyMissing               = "primary_key_missing"
	scopeNameTimeColumnMissing               = "time_column_missing"
	scopeNameIngestedRecords                 = "ingested_records"
	scopeNameAppendedRecords                 = "appended_records"
	scopeNameUpdatedRecords                  = "updated_records"
	scopeNameIngestedUpsertBatches           = "ingested_upsert_batches"
	scopeNameUpsertBatchSize                 = "upsert_batch_size"
	scopeNameLoad                            = "load"
	scopeNameTotal                           = "total"
	scopeNameBuildIndex                      = "build_index"
	scopeNameTotalMemorySize                 = "total_memory_size"
	scopeNameUnmanagedMemorySize             = "unmanaged_memory_size"
	scopeNameManagedMemorySize               = "managed_memory_size"
	scopeNameBackfillBufferSize              = "backfill_buffer_size"
	scopeNameBackfillBufferNumRecords        = "backfill_buffer_num_records"
	scopeNameBackfillBufferFillRatio         = "backfill_buffer_fill_ratio"
	scopeNameIngestionLagPerColumn           = "ingestion_lag"
	scopeNameCurrentRedologCreationTime      = "current_redolog_creation_time"
	scopeNameCurrentRedologSize              = "current_redolog_size"
	scopeNameNumberOfRedologs                = "number_of_redologs"
	scopeNameSizeOfRedologs                  = "size_of_redologs"
	scopeNameNumberOfEnumCasesPerColumn      = "number_of_enum_cases"
	scopeNameQueryFailed                     = "query_failed"
	scopeNameQuerySucceeded                  = "query_succeeded"
	scopeNameQueryLatency                    = "query_latency"
	scopeNameQuerySQLParsingLatency			 = "sql_parsing_latency"
	scopeNameQueryWaitForMemoryDuration      = "query_wait_for_memory_duration"
	scopeNameQueryReceived                   = "query_received"
	scopeNameQueryRecordsProcessed           = "records_processed"
	scopeNameQueryBatchProcessed             = "batch_processed"
	scopeNameQueryBytesTransferred           = "bytes_transferred"
	scopeNameQueryRowsReturned               = "rows_returned"
	scopeNameRecordsOutOfRetention           = "records_out_of_retention"
	scopeNameTimezoneLookupTableCreationTime = "timezone_lookup_table_creation_time"
	scopeNameRedoLogFileCorrupt              = "redo_log_file_corrupt"
	scopeNameMemoryOverflow                  = "memory_overflow"
	scopeNamePreloadingZoneEvicted           = "preloading_zone_evicted"
	scopeNameBatchesPurged                   = "purged_batches"
	scopeNameFutureRecords                   = "records_from_future"
	scopeNameBatchSize                       = "batch_size"
	scopeNameBatchSizeReportTime             = "batch_size_report_time"
	scopeNameSchemaFetchSuccess              = "schema_fetch_success"
	scopeNameSchemaFetchFailure              = "schema_fetch_failure"
	scopeNameSchemaUpdateCount               = "schema_updates"
	scopeNameSchemaDeletionCount             = "schema_deletions"
	scopeNameSchemaCreationCount             = "schema_creations"
)

// Metric tag names
const (
	metricsTagComponent  = "component"
	metricsTagOperation  = "operation"
	metricsTagHandler    = "handler"
	metricsTagStatusCode = "status_code"
	metricsTagOrigin     = "origin"
	metricsTagTable      = "table"
	metricsTagShard      = "shard"
	metricsTagStore      = "store"
)

const (
	metricsStoreLive    = "live"
	metricsStoreArchive = "archive"
)

// Metric component tag values
const (
	metricsComponentMemStore  = "memstore"
	metricsComponentAPI       = "api"
	metricsComponentDiskStore = "diskstore"
	metricsComponentMetaStore = "metastore"
	metricsComponentQuery     = "query"
	metricsComponentStats     = "stats"
)

// Metric operation tag values
const (
	metricsOperationArchiving = "archiving"
	metricsOperationBackfill  = "backfill"
	metricsOperationIngestion = "ingestion"
	metricsOperationRecovery  = "recovery"
	metricsOperationSnapshot  = "snapshot"
	metricsOperationPurge     = "purge"
)

var metricsDefs = map[MetricName]metricDefinition{
	AllocatedDeviceMemory: {
		name:       scopeNameAllocatedDeviceMemory,
		metricType: Gauge,
		tags: map[string]string{
			metricsTagComponent: metricsComponentQuery,
		},
	},
	ArchivingIgnoredRecords: {
		name:       scopeNameBackfillRecords,
		metricType: Counter,
		tags: map[string]string{
			metricsTagOperation: metricsOperationArchiving,
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	ArchivingRecords: {
		name:       scopeNameArchivingRecords,
		metricType: Counter,
		tags: map[string]string{
			metricsTagOperation: metricsOperationArchiving,
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	ArchivingHighWatermark: {
		name:       scopeNameArchivingHighWatermark,
		metricType: Gauge,
		tags: map[string]string{
			metricsTagOperation: metricsOperationArchiving,
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	ArchivingLowWatermark: {
		name:       scopeNameArchivingLowWatermark,
		metricType: Gauge,
		tags: map[string]string{
			metricsTagOperation: metricsOperationArchiving,
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	ArchivingTimingTotal: {
		name:       scopeNameTotal,
		metricType: Timer,
		tags: map[string]string{
			metricsTagOperation: metricsOperationArchiving,
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	BackfillTimingTotal: {
		name:       scopeNameTotal,
		metricType: Timer,
		tags: map[string]string{
			metricsTagOperation: metricsOperationBackfill,
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	BackfillLockTiming: {
		name:       scopeNameBackfillLockTiming,
		metricType: Timer,
		tags: map[string]string{
			metricsTagOperation: metricsOperationBackfill,
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	EstimatedDeviceMemory: {
		name:       scopeNameEstimatedDeviceMemory,
		metricType: Gauge,
		tags: map[string]string{
			metricsTagComponent: metricsComponentQuery,
		},
	},
	HTTPHandlerCall: {
		name:       scopeNameHTTPHandlerCall,
		metricType: Counter,
		tags: map[string]string{
			metricsTagComponent: metricsComponentAPI,
		},
	},
	HTTPHandlerLatency: {
		name:       scopeNameHTTPHandlerLatency,
		metricType: Timer,
		tags: map[string]string{
			metricsTagComponent: metricsComponentAPI,
		},
	},
	IngestedRecords: {
		name:       scopeNameIngestedRecords,
		metricType: Counter,
		tags: map[string]string{
			metricsTagOperation: metricsOperationIngestion,
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	AppendedRecords: {
		name:       scopeNameAppendedRecords,
		metricType: Counter,
		tags: map[string]string{
			metricsTagOperation: metricsOperationIngestion,
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	UpdatedRecords: {
		name:       scopeNameUpdatedRecords,
		metricType: Counter,
		tags: map[string]string{
			metricsTagOperation: metricsOperationIngestion,
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	IngestedUpsertBatches: {
		name:       scopeNameIngestedUpsertBatches,
		metricType: Counter,
		tags: map[string]string{
			metricsTagOperation: metricsOperationIngestion,
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	UpsertBatchSize: {
		name:       scopeNameUpsertBatchSize,
		metricType: Gauge,
		tags: map[string]string{
			metricsTagOperation: metricsOperationIngestion,
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	PrimaryKeyMissing: {
		name:       scopeNamePrimaryKeyMissing,
		metricType: Counter,
		tags: map[string]string{
			metricsTagOperation: metricsOperationIngestion,
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	TimeColumnMissing: {
		name:       scopeNameTimeColumnMissing,
		metricType: Counter,
		tags: map[string]string{
			metricsTagOperation: metricsOperationIngestion,
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	DuplicateRecordRatio: {
		name:       scopeNameDuplicateRecordRatio,
		metricType: Gauge,
		tags: map[string]string{
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	BackfillRecords: {
		name:       scopeNameBackfillRecords,
		metricType: Counter,
		tags: map[string]string{
			metricsTagOperation: metricsOperationIngestion,
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	BackfillRecordsTimeDifference: {
		name:       scopeNameBackfillRecordsTimeDifference,
		metricType: Gauge,
		tags: map[string]string{
			metricsTagOperation: metricsOperationIngestion,
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	BackfillRecordsRatio: {
		name:       scopeNameBackfillRecordsRatio,
		metricType: Gauge,
		tags: map[string]string{
			metricsTagOperation: metricsOperationIngestion,
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	BackfillRecordsColumnRemoved: {
		name:       scopeNameBackfillRecordsColumnRemoved,
		metricType: Counter,
		tags: map[string]string{
			metricsTagOperation: metricsOperationIngestion,
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	BackfillAffectedDays: {
		name:       scopeNameBackfillAffectedDays,
		metricType: Gauge,
		tags: map[string]string{
			metricsTagOperation: metricsOperationBackfill,
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	BackfillNewRecords: {
		name:       scopeNameBackfillNewRecords,
		metricType: Counter,
		tags: map[string]string{
			metricsTagOperation: metricsOperationBackfill,
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	BackfillInplaceUpdateRecords: {
		name:       scopeNameBackfillInplaceUpdateRecords,
		metricType: Counter,
		tags: map[string]string{
			metricsTagOperation: metricsOperationBackfill,
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	BackfillDeleteThenInsertRecords: {
		name:       scopeNameBackfillDeleteInsertRecords,
		metricType: Counter,
		tags: map[string]string{
			metricsTagOperation: metricsOperationBackfill,
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	BackfillNoEffectRecords: {
		name:       scopeNameBackfillNoEffectRecords,
		metricType: Counter,
		tags: map[string]string{
			metricsTagOperation: metricsOperationBackfill,
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	RecoveryIgnoredRecords: {
		name:       scopeNameBackfillRecords,
		metricType: Counter,
		tags: map[string]string{
			metricsTagOperation: metricsOperationRecovery,
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	RecoveryIgnoredRecordsTimeDifference: {
		name:       scopeNameBackfillRecordsTimeDifference,
		metricType: Gauge,
		tags: map[string]string{
			metricsTagOperation: metricsOperationRecovery,
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	RecoveryLatency: {
		name:       scopeNameRecoveryLatency,
		metricType: Timer,
		tags: map[string]string{
			metricsTagOperation: metricsOperationRecovery,
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	TotalMemorySize: {
		name:       scopeNameTotalMemorySize,
		metricType: Gauge,
		tags: map[string]string{
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	UnmanagedMemorySize: {
		name:       scopeNameUnmanagedMemorySize,
		metricType: Gauge,
		tags: map[string]string{
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	ManagedMemorySize: {
		name:       scopeNameManagedMemorySize,
		metricType: Gauge,
		tags: map[string]string{
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	BackfillBufferFillRatio: {
		name:       scopeNameBackfillBufferFillRatio,
		metricType: Gauge,
		tags: map[string]string{
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	BackfillBufferSize: {
		name:       scopeNameBackfillBufferSize,
		metricType: Gauge,
		tags: map[string]string{
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	BackfillBufferNumRecords: {
		name:       scopeNameBackfillBufferNumRecords,
		metricType: Gauge,
		tags: map[string]string{
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	IngestionLagPerColumn: {
		name:       scopeNameIngestionLagPerColumn,
		metricType: Gauge,
		tags: map[string]string{
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	CurrentRedologCreationTime: {
		name:       scopeNameCurrentRedologCreationTime,
		metricType: Gauge,
		tags: map[string]string{
			metricsTagComponent: metricsComponentDiskStore,
		},
	},
	CurrentRedologSize: {
		name:       scopeNameCurrentRedologSize,
		metricType: Gauge,
		tags: map[string]string{
			metricsTagComponent: metricsComponentDiskStore,
		},
	},
	NumberOfRedologs: {
		name:       scopeNameNumberOfRedologs,
		metricType: Gauge,
		tags: map[string]string{
			metricsTagComponent: metricsComponentDiskStore,
		},
	},
	SizeOfRedologs: {
		name:       scopeNameSizeOfRedologs,
		metricType: Gauge,
		tags: map[string]string{
			metricsTagComponent: metricsComponentDiskStore,
		},
	},
	NumberOfEnumCasesPerColumn: {
		name:       scopeNameNumberOfEnumCasesPerColumn,
		metricType: Gauge,
		tags: map[string]string{
			metricsTagComponent: metricsComponentMetaStore,
		},
	},
	QueryFailed: {
		name:       scopeNameQueryFailed,
		metricType: Counter,
		tags: map[string]string{
			metricsTagComponent: metricsComponentQuery,
		},
	},
	QuerySucceeded: {
		name:       scopeNameQuerySucceeded,
		metricType: Counter,
		tags: map[string]string{
			metricsTagComponent: metricsComponentQuery,
		},
	},
	QueryLatency: {
		name:       scopeNameQueryLatency,
		metricType: Timer,
		tags: map[string]string{
			metricsTagComponent: metricsComponentQuery,
		},
	},
	QuerySQLParsingLatency: {
		name:       scopeNameQuerySQLParsingLatency,
		metricType: Timer,
		tags: map[string]string{
			metricsTagComponent: metricsComponentQuery,
		},
	},
	QueryWaitForMemoryDuration: {
		name:       scopeNameQueryWaitForMemoryDuration,
		metricType: Timer,
		tags: map[string]string{
			metricsTagComponent: metricsComponentQuery,
		},
	},
	QueryReceived: {
		name:       scopeNameQueryReceived,
		metricType: Counter,
		tags: map[string]string{
			metricsTagComponent: metricsComponentQuery,
		},
	},
	QueryLiveRecordsProcessed: {
		name:       scopeNameQueryRecordsProcessed,
		metricType: Counter,
		tags: map[string]string{
			metricsTagComponent: metricsComponentQuery,
			metricsTagStore:     metricsStoreLive,
		},
	},
	QueryArchiveRecordsProcessed: {
		name:       scopeNameQueryRecordsProcessed,
		metricType: Counter,
		tags: map[string]string{
			metricsTagComponent: metricsComponentQuery,
			metricsTagStore:     metricsStoreArchive,
		},
	},
	QueryLiveBatchProcessed: {
		name:       scopeNameQueryBatchProcessed,
		metricType: Counter,
		tags: map[string]string{
			metricsTagComponent: metricsComponentQuery,
			metricsTagStore:     metricsStoreLive,
		},
	},
	QueryArchiveBatchProcessed: {
		name:       scopeNameQueryBatchProcessed,
		metricType: Counter,
		tags: map[string]string{
			metricsTagComponent: metricsComponentQuery,
			metricsTagStore:     metricsStoreArchive,
		},
	},
	QueryLiveBytesTransferred: {
		name:       scopeNameQueryBytesTransferred,
		metricType: Counter,
		tags: map[string]string{
			metricsTagComponent: metricsComponentQuery,
			metricsTagStore:     metricsStoreLive,
		},
	},
	QueryArchiveBytesTransferred: {
		name:       scopeNameQueryBytesTransferred,
		metricType: Counter,
		tags: map[string]string{
			metricsTagComponent: metricsComponentQuery,
			metricsTagStore:     metricsStoreArchive,
		},
	},
	QueryRowsReturned: {
		name:       scopeNameQueryRowsReturned,
		metricType: Counter,
		tags: map[string]string{
			metricsTagComponent: metricsComponentQuery,
		},
	},
	RecordsOutOfRetention: {
		name:       scopeNameRecordsOutOfRetention,
		metricType: Counter,
		tags: map[string]string{
			metricsTagOperation: metricsOperationIngestion,
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	SnapshotTimingTotal: {
		name:       scopeNameTotal,
		metricType: Timer,
		tags: map[string]string{
			metricsTagOperation: metricsOperationSnapshot,
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	SnapshotTimingLoad: {
		name:       scopeNameLoad,
		metricType: Timer,
		tags: map[string]string{
			metricsTagOperation: metricsOperationSnapshot,
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	SnapshotTimingBuildIndex: {
		name:       scopeNameBuildIndex,
		metricType: Timer,
		tags: map[string]string{
			metricsTagOperation: metricsOperationSnapshot,
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	TimezoneLookupTableCreationTime: {
		name:       scopeNameTimezoneLookupTableCreationTime,
		metricType: Timer,
		tags: map[string]string{
			metricsTagComponent: metricsComponentQuery,
		},
	},
	RedoLogFileCorrupt: {
		name:       scopeNameRedoLogFileCorrupt,
		metricType: Counter,
		tags: map[string]string{
			metricsTagComponent: metricsComponentDiskStore,
		},
	},
	MemoryOverflow: {
		name:       scopeNameMemoryOverflow,
		metricType: Counter,
		tags: map[string]string{
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	PreloadingZoneEvicted: {
		name:       scopeNamePreloadingZoneEvicted,
		metricType: Counter,
		tags: map[string]string{
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	PurgeTimingTotal: {
		name:       scopeNameTotal,
		metricType: Timer,
		tags: map[string]string{
			metricsTagOperation: metricsOperationPurge,
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	PurgedBatches: {
		name:       scopeNameBatchesPurged,
		metricType: Counter,
		tags: map[string]string{
			metricsTagOperation: metricsOperationPurge,
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	RecordsFromFuture: {
		name:       scopeNameFutureRecords,
		metricType: Counter,
		tags: map[string]string{
			metricsTagOperation: metricsOperationIngestion,
			metricsTagComponent: metricsComponentMemStore,
		},
	},
	BatchSize: {
		name:       scopeNameBatchSize,
		metricType: Gauge,
		tags: map[string]string{
			metricsTagComponent: metricsComponentStats,
		},
	},
	BatchSizeReportTime: {
		name:       scopeNameBatchSizeReportTime,
		metricType: Timer,
		tags: map[string]string{
			metricsTagComponent: metricsComponentStats,
		},
	},
	SchemaFetchSuccess: {
		name:       scopeNameSchemaFetchSuccess,
		metricType: Counter,
		tags: map[string]string{
			metricsTagComponent: metricsComponentMetaStore,
		},
	},
	SchemaFetchFailure: {
		name:       scopeNameSchemaFetchFailure,
		metricType: Counter,
		tags: map[string]string{
			metricsTagComponent: metricsComponentMetaStore,
		},
	},
	SchemaUpdateCount: {
		name:       scopeNameSchemaUpdateCount,
		metricType: Counter,
		tags: map[string]string{
			metricsTagComponent: metricsComponentMetaStore,
		},
	},
	SchemaDeletionCount: {
		name:       scopeNameSchemaDeletionCount,
		metricType: Counter,
		tags: map[string]string{
			metricsTagComponent: metricsComponentMetaStore,
		},
	},
	SchemaCreationCount: {
		name:       scopeNameSchemaCreationCount,
		metricType: Counter,
		tags: map[string]string{
			metricsTagComponent: metricsComponentMetaStore,
		},
	},
}

func (def *metricDefinition) init(rootScope tally.Scope) {
	switch def.metricType {
	case Counter:
		def.counter = rootScope.Tagged(def.tags).Counter(def.name)
	case Gauge:
		def.gauge = rootScope.Tagged(def.tags).Gauge(def.name)
	case Timer:
		def.timer = rootScope.Tagged(def.tags).Timer(def.name)
	}
}

// ReporterFactory manages reporters for different table and shards.
// If the corresponding metrics are not associated with any table
// or shard. It can use the root reporter.
type ReporterFactory struct {
	sync.RWMutex
	rootReporter *Reporter
	reporters    map[string]*Reporter
}

// NewReporterFactory returns a new report factory.
func NewReporterFactory(rootScope tally.Scope) *ReporterFactory {
	return &ReporterFactory{
		rootReporter: NewReporter(rootScope),
		reporters:    make(map[string]*Reporter),
	}
}

// AddTableShard adds a reporter for the given table and shards. It should
// be called when bootstrap the table shards or shard ownership changes.
func (f *ReporterFactory) AddTableShard(tableName string, shardID int) {
	f.Lock()
	defer f.Unlock()
	key := fmt.Sprintf("%s_%d", tableName, shardID)
	_, ok := f.reporters[key]
	if !ok {
		f.reporters[key] = NewReporter(f.rootReporter.GetRootScope().Tagged(map[string]string{
			metricsTagTable: tableName,
			metricsTagShard: strconv.Itoa(shardID),
		}))
	}
}

// DeleteTableShard deletes the reporter for the given table and shards. It should
// be called when the table shard no longer belongs to current node.
func (f *ReporterFactory) DeleteTableShard(tableName string, shardID int) {
	f.Lock()
	defer f.Unlock()
	key := fmt.Sprintf("%s_%d", tableName, shardID)
	delete(f.reporters, key)
}

// GetReporter returns reporter given tableName and shardID. If the corresponding
// reporter cannot be found. It will return the root scope.
func (f *ReporterFactory) GetReporter(tableName string, shardID int) *Reporter {
	f.RLock()
	defer f.RUnlock()
	key := fmt.Sprintf("%s_%d", tableName, shardID)
	reporter, ok := f.reporters[key]
	if ok {
		return reporter
	}
	return f.rootReporter
}

// GetRootReporter returns the root reporter.
func (f *ReporterFactory) GetRootReporter() *Reporter {
	return f.rootReporter
}

// Reporter is the the interface used to report stats,
type Reporter struct {
	rootScope         tally.Scope
	cachedDefinitions []metricDefinition
}

// NewReporter returns a new reporter with supplied root scope.
func NewReporter(rootScope tally.Scope) *Reporter {
	defs := make([]metricDefinition, NumMetricNames)
	for key, metricDefinition := range metricsDefs {
		metricDefinition.init(rootScope)
		defs[key] = metricDefinition
	}
	return &Reporter{rootScope: rootScope, cachedDefinitions: defs}
}

// GetCounter returns the tally counter with corresponding tags.
func (r *Reporter) GetCounter(n MetricName) tally.Counter {
	def := r.cachedDefinitions[n]
	if def.metricType == Counter {
		return def.counter
	}
	GetLogger().Panicf("Cannot get counter given %d", n)
	return nil
}

// GetGauge returns the tally gauge with corresponding tags.
func (r *Reporter) GetGauge(n MetricName) tally.Gauge {
	def := r.cachedDefinitions[n]
	if def.metricType == Gauge {
		return def.gauge
	}
	GetLogger().Panicf("Cannot get gauge given %d", n)
	return nil
}

// GetTimer returns the tally timer with corresponding tags.
func (r *Reporter) GetTimer(n MetricName) tally.Timer {
	def := r.cachedDefinitions[n]
	if def.metricType == Timer {
		return def.timer
	}
	GetLogger().Panicf("Cannot get timer given %d", n)
	return nil
}

// GetChildCounter create tagged child counter from reporter
func (r *Reporter) GetChildCounter(tags map[string]string, n MetricName) tally.Counter {
	childScope := r.rootScope.Tagged(tags)
	def := r.cachedDefinitions[n]
	if def.metricType == Counter {
		return childScope.Tagged(def.tags).Counter(def.name)
	}
	GetLogger().Panicf("Cannot get child counter given %d", n)
	return nil
}

// GetChildGauge create tagged child gauge from reporter
func (r *Reporter) GetChildGauge(tags map[string]string, n MetricName) tally.Gauge {
	childScope := r.rootScope.Tagged(tags)
	def := r.cachedDefinitions[n]
	if def.metricType == Gauge {
		return childScope.Tagged(def.tags).Gauge(def.name)
	}
	GetLogger().Panicf("Cannot get child gauge given %d", n)
	return nil
}

// GetChildTimer create tagged child timer from reporter
func (r *Reporter) GetChildTimer(tags map[string]string, n MetricName) tally.Timer {
	childScope := r.rootScope.Tagged(tags)
	def := r.cachedDefinitions[n]
	if def.metricType == Timer {
		return childScope.Tagged(def.tags).Timer(def.name)
	}
	GetLogger().Panicf("Cannot get child timer given %d", n)
	return nil
}

// GetRootScope returns the root scope wrapped by this reporter.
func (r *Reporter) GetRootScope() tally.Scope {
	return r.rootScope
}
