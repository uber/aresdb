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
	"fmt"
	"github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/utils"
	"strings"
	"sync"
)

// JobManager is responsible for generating new jobs to run and manages job related stats.
type JobManager interface {
	Enable(enable bool)
	IsEnabled() bool
	generateJobs() []Job
	getJobDetails() interface{}
	deleteTable(table string)
	// mutator is guaranteed to be a functor by caller(scheduler).
	reportJobDetail(key string, mutator jobDetailMutator)
}

// Enabler contains enabled variable to enable/disable job manager
type enabler struct {
	sync.RWMutex
	enabled bool
}

// Enable to enable/disable job manager
func (e *enabler) Enable(enable bool) {
	e.Lock()
	e.enabled = enable
	e.Unlock()
}

// IsEnabled to check if the job manager is enabled
func (e *enabler) IsEnabled() bool {
	e.RLock()
	defer e.RUnlock()
	return e.enabled
}

type archiveJobManager struct {
	enabler
	// archiveJobDetails for different tables, shard. Key is {tableName}|{shardID}|archiving,
	// value is the job details.
	jobDetails map[string]*ArchiveJobDetail
	// For accessing meta data like archiving delay and interval
	memStore  *memStoreImpl
	scheduler *schedulerImpl
}

// newArchiveJobManager creates a new jobManager to manage archive jobs.
func newArchiveJobManager(scheduler *schedulerImpl) JobManager {
	return &archiveJobManager{
		jobDetails: make(map[string]*ArchiveJobDetail),
		memStore:   scheduler.memStore,
		scheduler:  scheduler,
		enabler:    enabler{enabled: false},
	}
}

// generateJobs iterates each table shard from memStore and prepare list of archive jobs
// to run. A job should start to run only when newCutoff - cutoff > interval, where
// newCutoff = now - delay.
func (m *archiveJobManager) generateJobs() []Job {
	m.memStore.RLock()
	defer m.memStore.RUnlock()

	now := uint32(utils.Now().Unix())
	var jobs []Job
	if !m.enabled {
		return jobs
	}
	for tableName, shardMap := range m.memStore.TableShards {
		for shardID, tableShard := range shardMap {
			tableShard.Schema.RLock()
			if tableShard.Schema.Schema.IsFactTable {
				interval := tableShard.Schema.Schema.Config.ArchivingIntervalMinutes * 60
				delay := tableShard.Schema.Schema.Config.ArchivingDelayMinutes * 60
				currentCutoff := tableShard.ArchiveStore.CurrentVersion.ArchivingCutoff
				newCutoff := now - delay

				key := getIdentifier(tableName, shardID, common.ArchivingJobType)
				if newCutoff > currentCutoff+interval {
					job := m.scheduler.NewArchivingJob(tableName, shardID, newCutoff)
					jobs = append(jobs, job)
					m.reportArchiveJobDetail(key, func(jobDetail *ArchiveJobDetail) {
						jobDetail.Status = JobReady
						jobDetail.CurrentCutoff = currentCutoff
					})
				} else {
					m.reportArchiveJobDetail(key, func(jobDetail *ArchiveJobDetail) {
						jobDetail.Status = JobWaiting
						jobDetail.CurrentCutoff = currentCutoff
						jobDetail.NextRun = utils.TimeStampToUTC(int64(currentCutoff + delay + interval))
					})
				}
			}
			tableShard.Schema.RUnlock()
		}
	}
	return jobs
}

func (m *archiveJobManager) getJobDetails() interface{} {
	m.RLock()
	defer m.RUnlock()
	return m.jobDetails
}

func (m *archiveJobManager) reportJobDetail(key string, jobMutator jobDetailMutator) {
	m.Lock()
	defer m.Unlock()
	archiveJobDetail := m.getJobDetail(key)
	jobDetail := &archiveJobDetail.JobDetail
	jobMutator(jobDetail)
}

func (m *archiveJobManager) reportArchiveJobDetail(key string, jobMutator ArchiveJobDetailMutator) {
	m.Lock()
	defer m.Unlock()
	jobMutator(m.getJobDetail(key))
}

// caller needs to hold the write lock.
func (m *archiveJobManager) getJobDetail(key string) *ArchiveJobDetail {
	jobDetail, found := m.jobDetails[key]
	if !found {
		jobDetail = &ArchiveJobDetail{}
		m.jobDetails[key] = jobDetail
	}
	return jobDetail
}

// deleteTable deletes metadata for the table in archiveJobManager.
func (m *archiveJobManager) deleteTable(table string) {
	m.Lock()
	defer m.Unlock()
	for key := range m.jobDetails {
		if strings.HasPrefix(key, table) {
			delete(m.jobDetails, key)
		}
	}
}

// ArchivingJob defines the structure that an archiving job needs.
type ArchivingJob struct {
	// table to archive
	tableName string
	// shard to archive
	shardID int
	// new cut off
	cutoff uint32
	// for calling archiving function in memStore
	memStore MemStore
	// for reporting job detail changes
	reporter ArchiveJobDetailReporter
}

// Run starts the archiving process and wait for it to finish.
func (job *ArchivingJob) Run() error {
	return job.memStore.Archive(job.tableName, job.shardID, job.cutoff, job.reporter)
}

// GetIdentifier returns a unique identifier of this job.
func (job *ArchivingJob) GetIdentifier() string {
	return getIdentifier(job.tableName, job.shardID, common.ArchivingJobType)
}

// String gives meaningful string representation for this job
func (job *ArchivingJob) String() string {
	return fmt.Sprintf("ArchivingJob<Table: %s, ShardID: %d, Cutoff: %d>",
		job.tableName, job.shardID, job.cutoff)
}

type backfillJobManager struct {
	enabler
	// backfillJobDetails for different tables, shard. Key is {tableName}|{shardID}|backfill,
	jobDetails map[string]*BackfillJobDetail
	// For accessing meta data like archiving delay and interval
	memStore  *memStoreImpl
	scheduler *schedulerImpl
}

// newBackfillJobManager creates a new jobManager to manage backfill jobs.
func newBackfillJobManager(scheduler *schedulerImpl) JobManager {
	return &backfillJobManager{
		jobDetails: make(map[string]*BackfillJobDetail),
		memStore:   scheduler.memStore,
		scheduler:  scheduler,
		enabler:    enabler{enabled: true},
	}
}

// generateJobs iterates each table shard from memStore and prepare list of backfill jobs
// to run.
func (m *backfillJobManager) generateJobs() []Job {
	m.memStore.RLock()
	defer m.memStore.RUnlock()

	now := uint32(utils.Now().Unix())
	var jobs []Job
	if !m.enabled {
		return jobs
	}
	for tableName, shardMap := range m.memStore.TableShards {
		for shardID, tableShard := range shardMap {
			tableShard.Schema.RLock()
			if tableShard.Schema.Schema.IsFactTable {
				key := getIdentifier(tableName, shardID, common.BackfillJobType)
				backfillMgr := tableShard.LiveStore.BackfillManager
				if backfillMgr.QualifyToTriggerBackfill() {
					// size based strategy
					job := m.scheduler.NewBackfillJob(tableName, shardID)
					jobs = append(jobs, job)
					m.scheduler.reportJob(key, func(jobDetail *JobDetail) {
						jobDetail.Status = JobReady
					})
				} else {
					// timer based strategy
					interval := tableShard.Schema.Schema.Config.BackfillIntervalMinutes * 60
					m.Lock()
					jobDetail := m.getJobDetail(key)
					m.Unlock()

					// the job detail has just been initialized.
					if jobDetail.LastRun.Unix() <= 0 {
						m.scheduler.reportJob(key, func(jobDetail *JobDetail) {
							jobDetail.Status = JobWaiting
							jobDetail.LastRun = utils.TimeStampToUTC(int64(now))
						})
					} else if int64(now) >= jobDetail.LastRun.Unix()+int64(interval) {
						// enqueue backfill job
						job := m.scheduler.NewBackfillJob(tableName, shardID)
						jobs = append(jobs, job)
						m.scheduler.reportJob(key, func(jobDetail *JobDetail) {
							jobDetail.Status = JobReady
						})
					}
				}
			}
			tableShard.Schema.RUnlock()
		}
	}
	return jobs
}

func (m *backfillJobManager) getJobDetails() interface{} {
	m.RLock()
	defer m.RUnlock()
	return m.jobDetails
}

func (m *backfillJobManager) reportJobDetail(key string, jobMutator jobDetailMutator) {
	m.Lock()
	defer m.Unlock()
	backfillJobDetail := m.getJobDetail(key)
	jobDetail := &backfillJobDetail.JobDetail
	jobMutator(jobDetail)
}

func (m *backfillJobManager) reportBackfillJobDetail(key string, jobMutator BackfillJobDetailMutator) {
	m.Lock()
	defer m.Unlock()
	jobMutator(m.getJobDetail(key))
}

// caller needs to hold the write lock.
func (m *backfillJobManager) getJobDetail(key string) *BackfillJobDetail {
	jobDetail, found := m.jobDetails[key]
	if !found {
		jobDetail = &BackfillJobDetail{}
		m.jobDetails[key] = jobDetail
	}
	return jobDetail
}

// deleteTable deletes metadata for the table in backfillJobManager.
func (m *backfillJobManager) deleteTable(table string) {
	m.Lock()
	defer m.Unlock()
	for key := range m.jobDetails {
		if strings.HasPrefix(key, table) {
			delete(m.jobDetails, key)
		}
	}
}

// BackfillJob defines the structure that a backfill job needs.
type BackfillJob struct {
	// table to backfill
	tableName string
	// shard to backfill
	shardID int
	// for calling backfill function in memStore
	memStore MemStore
	// for reporting JobDetail changes.
	reporter BackfillJobDetailReporter
}

// Run starts the backfill process and wait for it to finish.
func (job *BackfillJob) Run() error {
	return job.memStore.Backfill(job.tableName, job.shardID, job.reporter)
}

// GetIdentifier returns a unique identifier of this job.
func (job *BackfillJob) GetIdentifier() string {
	return getIdentifier(job.tableName, job.shardID, common.BackfillJobType)
}

// String gives meaningful string representation for this job
func (job *BackfillJob) String() string {
	return fmt.Sprintf("BackfillJob<Table: %s, ShardID: %d>",
		job.tableName, job.shardID)
}

type snapshotJobManager struct {
	enabler
	// snapshotJobDetails for different tables, shard. Key is {tableName}|{shardID}|snapshot,
	jobDetails map[string]*SnapshotJobDetail
	// For accessing meta data like archiving delay and interval
	memStore  *memStoreImpl
	scheduler *schedulerImpl
}

// newSnapshotJobManager creates a new jobManager to manage snapshot jobs.
func newSnapshotJobManager(scheduler *schedulerImpl) JobManager {
	return &snapshotJobManager{
		jobDetails: make(map[string]*SnapshotJobDetail),
		memStore:   scheduler.memStore,
		scheduler:  scheduler,
		enabler:    enabler{enabled: true},
	}
}

// generateJobs iterates each table shard from memStore and prepare list of snapshot jobs
// to run.
func (m *snapshotJobManager) generateJobs() []Job {
	m.memStore.RLock()
	defer m.memStore.RUnlock()
	var jobs []Job
	if !m.enabled {
		return jobs
	}
	for tableName, shardMap := range m.memStore.TableShards {
		for shardID, tableShard := range shardMap {
			tableShard.Schema.RLock()
			if !tableShard.Schema.Schema.IsFactTable {
				key := getIdentifier(tableName, shardID, common.SnapshotJobType)

				snapshotManager := tableShard.LiveStore.SnapshotManager
				if snapshotManager.QualifyForSnapshot() {
					job := m.scheduler.NewSnapshotJob(tableName, shardID)
					jobs = append(jobs, job)
					m.scheduler.reportJob(key, func(jobDetail *JobDetail) {
						jobDetail.Status = JobReady
					})
				} else {
					jobDetail := m.getJobDetail(key)
					// the job detail has just been initialized.
					if jobDetail.LastRun.Unix() == 0 {
						m.scheduler.reportJob(key, func(jobDetail *JobDetail) {
							jobDetail.Status = JobWaiting
						})
					}
				}
			}
			tableShard.Schema.RUnlock()
		}
	}
	return jobs
}

func (m *snapshotJobManager) getJobDetails() interface{} {
	m.RLock()
	defer m.RUnlock()
	return m.jobDetails
}

// deleteTable deletes metadata for the table in snapshotJobManager.
func (m *snapshotJobManager) deleteTable(table string) {
	m.Lock()
	defer m.Unlock()
	for key := range m.jobDetails {
		if strings.HasPrefix(key, table) {
			delete(m.jobDetails, key)
		}
	}
}

func (m *snapshotJobManager) reportJobDetail(key string, jobMutator jobDetailMutator) {
	m.Lock()
	defer m.Unlock()
	snapshotJobDetail := m.getJobDetail(key)
	jobDetail := &snapshotJobDetail.JobDetail
	jobMutator(jobDetail)
}

func (m *snapshotJobManager) reportSnapshotJobDetail(key string, jobMutator SnapshotJobDetailMutator) {
	m.Lock()
	defer m.Unlock()
	jobMutator(m.getJobDetail(key))
}

// caller needs to hold the write lock.
func (m *snapshotJobManager) getJobDetail(key string) *SnapshotJobDetail {
	jobDetail, found := m.jobDetails[key]
	if !found {
		jobDetail = &SnapshotJobDetail{}
		m.jobDetails[key] = jobDetail
	}
	return jobDetail
}

// SnapshotJob defines the structure that a snapshot job needs.
type SnapshotJob struct {
	// table to snapshot
	tableName string
	// shard to snapshot
	shardID int
	// for calling snapshot function in memStore
	memStore MemStore
	// for reporting snapshot JobDetail changes
	reporter SnapshotJobDetailReporter
}

// Run starts the snapshot process and wait for it to finish.
func (job *SnapshotJob) Run() error {
	return job.memStore.Snapshot(job.tableName, job.shardID, job.reporter)
}

// GetIdentifier returns a unique identifier of this job.
func (job *SnapshotJob) GetIdentifier() string {
	return getIdentifier(job.tableName, job.shardID, common.SnapshotJobType)
}

// String gives meaningful string representation for this job
func (job *SnapshotJob) String() string {
	return fmt.Sprintf("SnapshotJob<Table: %s, ShardID: %d>",
		job.tableName, job.shardID)
}

type purgeJobManager struct {
	enabler
	// purge job details for different tables, shard. Key is {tableName}|{shardID}|purge,
	jobDetails map[string]*PurgeJobDetail
	memStore   *memStoreImpl
	scheduler  *schedulerImpl
}

// newPurgeJobManager creates a new jobManager to manage purge jobs.
func newPurgeJobManager(scheduler *schedulerImpl) JobManager {
	return &purgeJobManager{
		jobDetails: make(map[string]*PurgeJobDetail),
		memStore:   scheduler.memStore,
		scheduler:  scheduler,
		enabler:    enabler{enabled: true},
	}
}

// generateJobs iterates each table shard from memStore and prepare list of purge jobs
// to run.
func (m *purgeJobManager) generateJobs() []Job {
	m.memStore.RLock()
	defer m.memStore.RUnlock()

	nowInDay := int(utils.Now().Unix() / 86400)
	var jobs []Job
	if !m.enabled {
		return jobs
	}
	for tableName, shardMap := range m.memStore.TableShards {
		for shardID, tableShard := range shardMap {
			retentionDays := tableShard.Schema.Schema.Config.RecordRetentionInDays
			key := getIdentifier(tableName, shardID, common.PurgeJobType)
			if tableShard.ArchiveStore.PurgeManager.QualifyForPurge() &&
				tableShard.Schema.Schema.IsFactTable && retentionDays > 0 {
				batchCutOff := nowInDay - retentionDays
				jobs = append(jobs, m.scheduler.NewPurgeJob(tableName, shardID, 0, nowInDay-retentionDays))
				m.reportPurgeJobDetail(key, func(jobDetail *PurgeJobDetail) {
					jobDetail.Status = JobReady
					jobDetail.BatchIDStart = 0
					jobDetail.BatchIDEnd = batchCutOff
				})
			}
		}
	}

	return jobs
}

func (m *purgeJobManager) getJobDetails() interface{} {
	m.RLock()
	defer m.RUnlock()
	return m.jobDetails
}

func (m *purgeJobManager) getJobDetail(key string) *PurgeJobDetail {
	jobDetail, found := m.jobDetails[key]
	if !found {
		jobDetail = &PurgeJobDetail{}
		m.jobDetails[key] = jobDetail
	}
	return jobDetail
}

func (m *purgeJobManager) reportJobDetail(key string, jobMutator jobDetailMutator) {
	m.Lock()
	defer m.Unlock()
	purgeJobDetail := m.getJobDetail(key)
	jobDetail := &purgeJobDetail.JobDetail
	jobMutator(jobDetail)
}

// deleteTable deletes metadata for the table in purgeJobManager.
func (m *purgeJobManager) deleteTable(table string) {
	m.Lock()
	defer m.Unlock()
	for key := range m.jobDetails {
		if strings.HasPrefix(key, table) {
			delete(m.jobDetails, key)
		}
	}
}

func (m *purgeJobManager) reportPurgeJobDetail(key string, jobMutator PurgeJobDetailMutator) {
	m.Lock()
	defer m.Unlock()
	jobMutator(m.getJobDetail(key))
}

// PurgeJob defines the structure that a purge job needs.
type PurgeJob struct {
	tableName string
	shardID   int
	// max batch id to purge
	batchIDStart int
	batchIDEnd   int
	memStore     MemStore
	reporter     PurgeJobDetailReporter
}

// Run starts the purge process and wait for it to finish.
func (job *PurgeJob) Run() error {
	return job.memStore.Purge(job.tableName, job.shardID, job.batchIDStart, job.batchIDEnd, job.reporter)
}

// GetIdentifier returns a unique identifier of this job.
func (job *PurgeJob) GetIdentifier() string {
	return getIdentifier(job.tableName, job.shardID, common.PurgeJobType)
}

// String gives meaningful string representation for this job
func (job *PurgeJob) String() string {
	return fmt.Sprintf("PurgeJob<Table: %s, ShardID: %d>",
		job.tableName, job.shardID)
}
