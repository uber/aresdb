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

package api

// ShardRequest is the common request struct for all shard related operations.
type ShardRequest struct {
	TableName string `path:"table" json:"table"`
	ShardID   int    `path:"shard" json:"shard"`
}

// ShowBatchRequest represents request to show a batch.
type ShowBatchRequest struct {
	ShardRequest
	BatchID  int `path:"batch" json:"batch"`
	StartRow int `query:"startRow,optional" json:"startRow"`
	NumRows  int `query:"numRows,optional" json:"numRows"`
}

// LookupPrimaryKeyRequest represents primary key lookup request
type LookupPrimaryKeyRequest struct {
	ShardRequest
	// comma delimited string
	Key string `query:"key" json:"key"`
}

// ShowShardMetaRequest represents request to show metadata for a shard.
type ShowShardMetaRequest struct {
	ShardRequest
}

// ArchiveRequest represents request to start an on demand archiving.
type ArchiveRequest struct {
	ShardRequest
	Body struct {
		Cutoff uint32 `json:"cutoff"`
	} `body:""`
}

// BackfillRequest represents request to start an on demand backfill.
type BackfillRequest struct {
	ShardRequest
}

// SnapshotRequest represents request to start an on demand snapshot.
type SnapshotRequest struct {
	ShardRequest
}

// PurgeRequest represents request to purge a batch.
type PurgeRequest struct {
	ShardRequest
	Body struct {
		BatchIDStart int  `json:"batchIDStart"`
		BatchIDEnd   int  `json:"batchIDEnd"`
		SafePurge    bool `json:"safePurge"`
	} `body:""`
}

// LoadVectorPartyRequest represents a load request for vector party
type LoadVectorPartyRequest struct {
	ShardRequest
	BatchID    int    `path:"batch" json:"batch"`
	ColumnName string `path:"column" json:"column"`
}

// EvictVectorPartyRequest represents a evict request for vector party
type EvictVectorPartyRequest struct {
	LoadVectorPartyRequest
}

// ListRedoLogsRequest represents the request to list all redo log files for a given shard.
type ListRedoLogsRequest struct {
	ShardRequest
}

// ListUpsertBatchesRequest represents the request to list offsets of upsert batches in a redo
// log file.
type ListUpsertBatchesRequest struct {
	ShardRequest
	// CreationTime of the redolog file.
	CreationTime int64 `path:"creationTime"`
}

// ReadUpsertBatchRequest represents the request to show one page of current upsert batch
type ReadUpsertBatchRequest struct {
	// Offset of upsert batch.
	Offset int64 `path:"offset"`
	// Start of records in upsert batch in this page.
	Start int `query:"start,optional"`
	// Number of records to show in this page. If length is 0, it means caller only want to
	// get another metadata of this upsert batch.
	Length int `query:"length,optional"`
	// Draw is the  counter that this object is a response to.
	Draw int `query:"draw,optional"`
}

// ReadRedologUpsertBatchRequest represents the request to show one page of current upsert batch
// of a given redolog file.
type ReadRedologUpsertBatchRequest struct {
	ListUpsertBatchesRequest
	ReadUpsertBatchRequest
}

// ReadBackfillQueueUpsertBatchRequest represents the request to show one page of current upsert batch
// of backfill queue
type ReadBackfillQueueUpsertBatchRequest struct {
	ShardRequest
	ReadUpsertBatchRequest
}

// ShowJobStatusRequest represents the request to show job statuses for a given job type.
type ShowJobStatusRequest struct {
	JobType string `path:"jobType" json:"jobType"`
}

// HealthSwitchRequest represents the request to  turn on/off the health check.
type HealthSwitchRequest struct {
	OnOrOff string `path:"onOrOff" json:"onOrOff"`
}
