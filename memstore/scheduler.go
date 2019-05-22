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
	"sync"
	"time"

	"strings"

	"github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/utils"
)

const (
	// interval for scheduler
	schedulerInterval = time.Minute
)

// jobBundle binds a result channel together with the job.
// Listening on the result channel will block until job finishes.
// Nil error indicates job runs successfully.
type jobBundle struct {
	Job
	resChan chan error
}

// Scheduler is for scheduling archiving jobs (and later backfill jobs) for table shards
// in memStore. It scans through all tables and shards to generate list of eligible jobs
// to run.
type Scheduler interface {
	Start()
	Stop()
	SubmitJob(job Job) (error, chan error)
	DeleteTable(table string, isFactTable bool)
	GetJobDetails(jobType common.JobType) interface{}
	NewBackfillJob(tableName string, shardID int) Job
	NewArchivingJob(tableName string, shardID int, cutoff uint32) Job
	NewSnapshotJob(tableName string, shardID int) Job
	NewPurgeJob(tableName string, shardID int, batchIDStart int, batchIDEnd int) Job
	EnableJobType(jobType common.JobType, enable bool)
	IsJobTypeEnabled(jobType common.JobType) bool
	utils.RWLocker
}

// newScheduler returns a new Scheduler.
func newScheduler(m *memStoreImpl) *schedulerImpl {
	s := &schedulerImpl{
		memStore:          m,
		schedulerStopChan: make(chan struct{}),
		jobBundleChan:     make(chan jobBundle),
		executorStopChan:  make(chan struct{}),
		jobManagers:       make(map[common.JobType]jobManager),
		jobEnableFlags:    make(map[common.JobType]bool),
	}
	s.jobManagers[common.ArchivingJobType] = newArchiveJobManager(s)
	s.jobManagers[common.BackfillJobType] = newBackfillJobManager(s)
	s.jobManagers[common.SnapshotJobType] = newSnapshotJobManager(s)
	s.jobManagers[common.PurgeJobType] = newPurgeJobManager(s)
	return s
}

// schedulerImpl is the implementation of Scheduler interface.
type schedulerImpl struct {
	// Protecting JobRunning.
	sync.RWMutex
	// For accessing meta data like archiving delay and interval
	memStore *memStoreImpl
	// Stop main scheduler loop.
	schedulerStopChan chan struct{}
	// Channel for executing job.
	jobBundleChan chan jobBundle
	// Stop executor loop.
	executorStopChan chan struct{}
	jobManagers      map[common.JobType]jobManager
	jobEnableFlags   map[common.JobType]bool
	archivingStarted bool
}

func (scheduler *schedulerImpl) EnableJobType(jobType common.JobType, enable bool) {
	scheduler.Lock()
	scheduler.jobEnableFlags[jobType] = enable
	scheduler.Unlock()
}

func (scheduler *schedulerImpl) IsJobTypeEnabled(jobType common.JobType) bool {
	scheduler.RLock()
	defer scheduler.RUnlock()
	enabled, ok := scheduler.jobEnableFlags[jobType]
	return !ok || enabled
}

func (scheduler *schedulerImpl) reportJob(key string, mutator jobDetailMutator) {
	scheduler.Lock()
	defer scheduler.Unlock()
	comps := strings.SplitN(key, "|", 3)
	if len(comps) < 3 {
		return
	}

	if jobManager, ok := scheduler.jobManagers[common.JobType(comps[2])]; ok {
		jobManager.reportJobDetail(key, mutator)
	}
}

// getIdentifier returns a unique identifier from table, shard and job type.
func getIdentifier(tableName string, shardID int, jobType common.JobType) string {
	return fmt.Sprintf("%s|%d|%s", tableName, shardID, jobType)
}

// GetJobDetails returns corresponding job details for given job type.
func (scheduler *schedulerImpl) GetJobDetails(jobType common.JobType) interface{} {
	if jobManager, ok := scheduler.jobManagers[jobType]; ok {
		return jobManager.getJobDetails()
	}
	return nil
}

// DeleteTable deletes the job details of a table given its name and whether it's a fact table.
func (scheduler *schedulerImpl) DeleteTable(table string, isFactTable bool) {
	if isFactTable {
		scheduler.jobManagers[common.ArchivingJobType].deleteTable(table)
		scheduler.jobManagers[common.BackfillJobType].deleteTable(table)
		scheduler.jobManagers[common.PurgeJobType].deleteTable(table)
		return
	}
	scheduler.jobManagers[common.SnapshotJobType].deleteTable(table)
}

// GetJobManager retrieve the JobManager according to job type
func (scheduler *schedulerImpl) GetJobManager(jobType common.JobType) jobManager {
	return scheduler.jobManagers[jobType]
}

// NewArchivingJob returns a new ArchivingJob.
func (scheduler *schedulerImpl) NewArchivingJob(tableName string, shardID int, cutoff uint32) Job {
	return &ArchivingJob{
		tableName: tableName,
		shardID:   shardID,
		cutoff:    cutoff,
		memStore:  scheduler.memStore,
		reporter:  scheduler.jobManagers[common.ArchivingJobType].(*archiveJobManager).reportArchiveJobDetail,
	}
}

// NewBackfillJob returns a new BackfillJob.
func (scheduler *schedulerImpl) NewBackfillJob(tableName string, shardID int) Job {
	return &BackfillJob{
		tableName: tableName,
		shardID:   shardID,
		memStore:  scheduler.memStore,
		reporter:  scheduler.jobManagers[common.BackfillJobType].(*backfillJobManager).reportBackfillJobDetail,
	}
}

// NewSnapshotJob returns a new SnapshotJob.
func (scheduler *schedulerImpl) NewSnapshotJob(tableName string, shardID int) Job {
	return &SnapshotJob{
		tableName: tableName,
		shardID:   shardID,
		memStore:  scheduler.memStore,
		reporter:  scheduler.jobManagers[common.SnapshotJobType].(*snapshotJobManager).reportSnapshotJobDetail,
	}
}

// NewPurgeJob returns a new PurgeJob
func (scheduler *schedulerImpl) NewPurgeJob(tableName string, shardID, batchIDStart, batchIDEnd int) Job {
	return &PurgeJob{
		tableName:    tableName,
		shardID:      shardID,
		batchIDStart: batchIDStart,
		batchIDEnd:   batchIDEnd,
		memStore:     scheduler.memStore,
		reporter:     scheduler.jobManagers[common.PurgeJobType].(*purgeJobManager).reportPurgeJobDetail,
	}
}

// Start starts the scheduler. It creates a new time.Timer every time to wait
// at least schedulerInterval time instead of running at every tick so that we
// will skip the tick if a single round takes more than one minute. This prevents
// accessing memStore (and lock) too many times during a short period.
func (scheduler *schedulerImpl) Start() {
	timer := time.NewTimer(schedulerInterval)

	// Scheduler loop.
	go func() {
		for {
			select {
			case <-timer.C:
				scheduler.run()
				// Since we already receive the event from channel,
				// there is no need to stop it and we can directly reset the timer.
				timer.Reset(schedulerInterval)
			case <-scheduler.schedulerStopChan:
				// It will block on waiting for executor to stop.
				scheduler.executorStopChan <- struct{}{}
				return
			}
		}
	}()

	// Executor loop.
	go func() {
		for {
			select {
			case jobBundle := <-scheduler.jobBundleChan:
				job := jobBundle.Job
				utils.GetLogger().With("job", job).Info("Recieved job")
				scheduler.executeJob(&jobBundle)
			case <-scheduler.executorStopChan:
				return
			}
		}
	}()
}

func (scheduler *schedulerImpl) executeJob(jb *jobBundle) {
	defer func() {
		if r := recover(); r != nil {
			utils.GetRootReporter().GetCounter(utils.JobFailuresCount).Inc(1)
			utils.GetLogger().With("error", r, "job", jb.Job).Error("Job failed after panic")
		}
	}()

	job := jb.Job
	utils.GetLogger().With("job", job).Info("Running job")
	scheduler.reportJob(job.GetIdentifier(), func(jobDetail *JobDetail) {
		jobDetail.Status = JobRunning
		jobDetail.LastStartTime = utils.Now().UTC()
	})
	err := jb.Run()

	// Set job status according to the result.
	now := uint32(utils.Now().Unix())
	if err != nil {
		utils.GetLogger().With("error", err, "job", job).Error("Failed to run job due to error")
		scheduler.reportJob(job.GetIdentifier(), func(jobDetail *JobDetail) {
			jobDetail.LastError = err
			jobDetail.Status = JobFailed
			jobDetail.LastRun = utils.TimeStampToUTC(int64(now))
		})
	} else {
		utils.GetLogger().With("job", job).Info("Succeeded to run job")
		scheduler.reportJob(job.GetIdentifier(), func(jobDetail *JobDetail) {
			jobDetail.LastError = nil
			jobDetail.Status = JobSucceeded
			jobDetail.LastRun = utils.TimeStampToUTC(int64(now))
		})
	}

	// This is a non-blocking channel sending.
	jb.resChan <- err
}

// Stop stops the scheduler.
func (scheduler *schedulerImpl) Stop() {
	scheduler.schedulerStopChan <- struct{}{}
}

// SubmitJob will submit a job to executor and block until it starts.
// Job submitter can decide whether to wait for job to finish and get
// the result.
func (scheduler *schedulerImpl) SubmitJob(job Job) (error, chan error) {
	if !scheduler.IsJobTypeEnabled(job.JobType()) {
		// this check is to block request from debug handler
		return fmt.Errorf("JobType %s disabled", job.JobType()), nil
	}

	jb := jobBundle{job, make(chan error, 1)}
	scheduler.jobBundleChan <- jb
	utils.GetLogger().With("job", job).Info("Submitted job")
	return nil, jb.resChan
}

// run runs at every tick. It first generates a list of jobs to run based on current condition,
// then it runs every job sequentially in the same process.
func (scheduler *schedulerImpl) run() {
	for jobType, jobManager := range scheduler.jobManagers {
		if !scheduler.IsJobTypeEnabled(jobType) {
			continue
		}
		for _, job := range jobManager.generateJobs() {
			// Waiting for job to finish.
			err, errChan := scheduler.SubmitJob(job)
			if err == nil {
				if err := <-errChan; err != nil {
					utils.GetLogger().With("job", job).Panic("Panic due to failure to run job")
				}
			} else {
				utils.GetLogger().With("job", job).Error("Fail to submit job")
			}
		}
	}
}

// Job defines the common interface for BackfillJob, ArchivingJob and SnapshotJob
type Job interface {
	JobType() common.JobType
	Run() error
	GetIdentifier() string
	String() string
}
