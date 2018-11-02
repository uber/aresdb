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

import "time"

// ArchivingStage represents different stages of a running archive job.
type ArchivingStage string

// List of ArchivingStages.
const (
	ArchivingCreatePatch ArchivingStage = "create patch"
	ArchivingMerge       ArchivingStage = "merge"
	ArchivingPurge       ArchivingStage = "purge"
	ArchivingComplete    ArchivingStage = "complete"
)

// BackfillStage represents different stages of a running backfill job.
type BackfillStage string

// List of BackfillStages.
const (
	BackfillCreatePatch BackfillStage = "create patch"
	BackfillApplyPatch  BackfillStage = "apply patch"
	BackfillPurge       BackfillStage = "purge"
	BackfillComplete    BackfillStage = "complete"
)

// JobStatus represents the job status of a given table Shard.
type JobStatus string

// List of JobStatus.
const (
	JobWaiting   JobStatus = "waiting"
	JobReady     JobStatus = "ready"
	JobRunning   JobStatus = "running"
	JobSucceeded JobStatus = "succeeded"
	JobFailed    JobStatus = "failed"
)

// SnapshotStage represents different stages of a running snapshot job.
type SnapshotStage string

// List of SnapshotStages
const (
	SnapshotSnapshot SnapshotStage = "snapshot"
	SnapshotCleanup  SnapshotStage = "cleanup"
	SnapshotComplete SnapshotStage = "complete"
)

// PurgeStage represents different stages of a running purge job.
type PurgeStage string

// List of purge stages
const (
	PurgeMetaData PurgeStage = "purge metadata"
	PurgeDataFile PurgeStage = "purge data file"
	PurgeMemory   PurgeStage = "purge memory"
	PurgeComplete PurgeStage = "complete"
)

// ArchiveJobDetailMutator is the mutator functor to change ArchiveJobDetail.
type ArchiveJobDetailMutator func(jobDetail *ArchiveJobDetail)

// ArchiveJobDetailReporter is the functor to apply mutator changes to corresponding JobDetail.
type ArchiveJobDetailReporter func(key string, mutator ArchiveJobDetailMutator)

// BackfillJobDetailMutator is the mutator functor to change BackfillJobDetail.
type BackfillJobDetailMutator func(jobDetail *BackfillJobDetail)

// BackfillJobDetailReporter is the functor to apply mutator changes to corresponding JobDetail.
type BackfillJobDetailReporter func(key string, mutator BackfillJobDetailMutator)

// SnapshotJobDetailMutator is the mutator functor to change SnapshotJobDetail.
type SnapshotJobDetailMutator func(jobDetail *SnapshotJobDetail)

// PurgeJobDetailMutator is the mutator functor to change PurgeJobDetail.
type PurgeJobDetailMutator func(jobDetail *PurgeJobDetail)

// SnapshotJobDetailReporter is the functor to apply mutator changes to corresponding JobDetail.
type SnapshotJobDetailReporter func(key string, mutator SnapshotJobDetailMutator)

// PurgeJobDetailReporter is the functor to apply mutator changes to corresponding JobDetail.
type PurgeJobDetailReporter func(key string, mutator PurgeJobDetailMutator)

// jobDetailMutator is the functor that change JobDetail.
type jobDetailMutator func(jobDetail *JobDetail)

// JobDetail represents common job status of a table Shard.
type JobDetail struct {
	// Status of archiving for current table Shard.
	Status JobStatus `json:"status"`

	// Time of next archiving job
	NextRun time.Time `json:"nextRun,omitempty"`

	// Time when the last backfill job finishes
	LastRun time.Time `json:"lastRun,omitempty"`

	// Error of last run if failed.
	LastError error `json:"lastError,omitempty"`
	// Start time of last archiving job.
	LastStartTime time.Time `json:"lastStartTime,omitempty"`
	// Duration of last archiving job.
	LastDuration time.Duration `json:"lastDuration,omitempty"`

	// Number of records processed.
	NumRecords int `json:"numRecords,omitempty"`
	// Number of days affected.
	NumAffectedDays int `json:"numAffectedDays,omitempty"`

	// Total amount of work
	// ie, archiving merge: number of days
	//     archiving snapshot: number of records
	Total int `json:"total,omitempty"`
	// Current finished work.
	Current int `json:"current,omitempty"`

	// Duration for waiting for lock.
	LockDuration time.Duration `json:"lockDuration,omitempty"`
}

// ArchiveJobDetail represents archiving job status of a table Shard.
type ArchiveJobDetail struct {
	JobDetail

	// Current cutoff.
	CurrentCutoff uint32 `json:"currentCutoff"`
	// Stage of the job is running.
	Stage ArchivingStage `json:"stage"`
	// New Cutoff.
	RunningCutoff uint32 `json:"runningCutoff"`
	// Cutoff of last completed archiving job.
	LastCutoff uint32 `json:"lastCutoff"`
}

// BackfillJobDetail represents backfill job status of a table Shard.
type BackfillJobDetail struct {
	JobDetail
	// Stage of the job is running.
	Stage BackfillStage `json:"stage"`
	// Current redolog file that's being backfilled.
	RedologFile int64 `json:"redologFile"`
	// Batch offset within the RedologFile.
	BatchOffset uint32 `json:"batchOffset"`
}

// SnapshotJobDetail represents snapshot job status of a table shard.
type SnapshotJobDetail struct {
	JobDetail
	// Number of mutations in this snapshot.
	NumMutations int `json:"numMutations"`
	// Number of batches written in this snapshot.
	NumBatches int `json:"numBatches"`
	// Current redolog file that's being backfilled.
	RedologFile int64 `json:"redologFile"`
	// Batch offset within the RedologFile.
	BatchOffset uint32 `json:"batchOffset"`
	// Stage of the job is running.
	Stage SnapshotStage `json:"stage"`
}

// PurgeJobDetail represents purge job status of a table shard.
type PurgeJobDetail struct {
	JobDetail
	// Stage of the job is running.
	Stage PurgeStage `json:"stage"`
	// Number of batches purged
	NumBatches   int `json:"numBatches"`
	BatchIDStart int `json:"batchIDStart"`
	BatchIDEnd   int `json:"batchIDEnd"`
}
