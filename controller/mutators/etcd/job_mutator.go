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
package etcd

import (
	"encoding/json"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/uber/aresdb/controller/cluster"
	pb "github.com/uber/aresdb/controller/generated/proto"
	"github.com/uber/aresdb/controller/models"
	"github.com/uber/aresdb/controller/mutators/common"
	"github.com/uber/aresdb/utils"
	"go.uber.org/zap"
)

// NewJobMutator creates new JobMutator
func NewJobMutator(etcdStore kv.TxnStore, logger *zap.SugaredLogger) common.JobMutator {
	return &jobMutatorImpl{
		logger:    logger,
		etcdStore: etcdStore,
	}
}

type jobMutatorImpl struct {
	logger    *zap.SugaredLogger
	etcdStore kv.TxnStore
}

// GetJob gets job config by name
func (j *jobMutatorImpl) GetJob(namespace, name string) (job models.JobConfig, err error) {
	var jobProto pb.EntityConfig
	jobProto, _, err = j.readJob(namespace, name)
	if err != nil {
		return job, err
	}

	if jobProto.Tomstoned {
		return job, common.ErrJobConfigDoesNotExist
	}

	job.StreamingConfig = models.KafkaConfig{
		File:            models.KafkaClusterFile,
		TopicType:            models.KafkaTopicType,
		LatestOffset:          models.KafkaLatestOffset,
		ErrorThreshold:  models.KafkaErrorThreshold,
		StatusCheckInterval:      models.KafkaStatusCheckInterval,
		ARThreshold:     models.KafkaAutoRecoveryThreshold,
		ProcessorCount:  models.KafkaProcessorCount,
		BatchSize:       models.KafkaBatchSize,
		MaxBatchDelayMS:    models.KafkaMaxBatchDelayMS,
		MegaBytePerSec:      models.KafkaMegaBytePerSec,
		RestartOnFailure:         models.KafkaRestartOnFailure,
		RestartInterval: models.KafkaRestartInterval,
		FailureHandler: models.FailureHandler{
			Type: models.FailureHandlerType,
			Config: models.FailureHandlerConfig{
				InitRetryIntervalInSeconds: models.FailureHandlerInitRetryIntervalInSeconds,
				Multiplier:                 models.FailureHandlerMultiplier,
				MaxRetryMinutes:            models.FailureHandlerMaxRetryMinutes,
			},
		},
	}

	err = json.Unmarshal(jobProto.Config, &job)
	return
}

// GetJobs returns all jobs config
func (j *jobMutatorImpl) GetJobs(namespace string) ([]models.JobConfig, error) {
	var jobList pb.EntityList
	jobList, _, err := readEntityList(j.etcdStore, utils.JobListKey(namespace))
	if err != nil {
		return nil, err
	}

	jobs := make([]models.JobConfig, 0)
	for _, jobName := range jobList.Entities {
		var job models.JobConfig
		job, err := j.GetJob(namespace, jobName.Name)
		if common.IsNonExist(err) {
			continue
		}
		if err != nil {
			return jobs, err
		}
		jobs = append(jobs, job)
	}
	return jobs, err
}

// DeleteJob deletes a job
func (j *jobMutatorImpl) DeleteJob(namespace, name string) error {
	jobList, jobListVersion, err := readEntityList(j.etcdStore, utils.JobListKey(namespace))
	if err != nil {
		return err
	}

	jobList, found := deleteEntity(jobList, name)
	if !found {
		return common.ErrJobConfigDoesNotExist
	}

	// found
	jobConfig, jobConfigVersion, err := j.readJob(namespace, name)
	if err != nil {
		return err
	}

	if jobConfig.Tomstoned {
		return common.ErrJobConfigDoesNotExist
	}
	jobConfig.Tomstoned = true

	return cluster.NewTransaction().
		AddKeyValue(utils.JobListKey(namespace), jobListVersion, &jobList).
		AddKeyValue(utils.JobKey(namespace, name), jobConfigVersion, &jobConfig).
		WriteTo(j.etcdStore)
}

// UpdateJob updates job config
func (j *jobMutatorImpl) UpdateJob(namespace string, job models.JobConfig) (err error) {
	jobListProto, jobListVersion, err := readEntityList(j.etcdStore, utils.JobListKey(namespace))
	if err != nil {
		return err
	}

	jobListProto, found := updateEntity(jobListProto, job.Name)
	if !found {
		j.logger.With(
			"job", job,
		).Info("job not found for update, creating new job")
		return j.AddJob(namespace, job)
	}

	jobProto, jobVersion, err := j.readJob(namespace, job.Name)
	if err != nil {
		return err
	}
	if jobProto.Tomstoned {
		return common.ErrJobConfigDoesNotExist
	}
	jobProto.Tomstoned = false
	jobProto.Config, err = json.Marshal(job)
	if err != nil {
		return
	}

	return cluster.NewTransaction().
		AddKeyValue(utils.JobListKey(namespace), jobListVersion, &jobListProto).
		AddKeyValue(utils.JobKey(namespace, job.Name), jobVersion, &jobProto).
		WriteTo(j.etcdStore)
}

// AddJob adds a new job
func (j *jobMutatorImpl) AddJob(namespace string, job models.JobConfig) error {
	err := common.Validate(&job, nil)
	if err != nil {
		return err
	}

	jobListProto, jobListVersion, err := readEntityList(j.etcdStore, utils.JobListKey(namespace))
	if err != nil {
		return err
	}

	jobProto := pb.EntityConfig{
		Name:      job.Name,
		Tomstoned: false,
	}
	jobVersion := kv.UninitializedVersion
	jobProto.Config, err = json.Marshal(job)
	if err != nil {
		return err
	}

	jobListProto, incarnation, exist := addEntity(jobListProto, jobProto.Name)
	if exist {
		return common.ErrJobConfigAlreadyExist
	}

	if incarnation > 0 {
		jobProto, jobVersion, err = j.readJob(namespace, job.Name)
		if err != nil {
			return err
		}
		jobProto.Tomstoned = false
	}

	return cluster.NewTransaction().
		AddKeyValue(utils.JobListKey(namespace), jobListVersion, &jobListProto).
		AddKeyValue(utils.JobKey(namespace, job.Name), jobVersion, &jobProto).
		WriteTo(j.etcdStore)
}

// GetHash returns hash that will be different if any job changed
func (j *jobMutatorImpl) GetHash(namespace string) (string, error) {
	return getHash(j.etcdStore, utils.JobListKey(namespace))
}

func (j *jobMutatorImpl) readJob(namespace string, name string) (jobConfig pb.EntityConfig, version int, err error) {
	version, err = readValue(j.etcdStore, utils.JobKey(namespace, name), &jobConfig)
	if common.IsNonExist(err) {
		err = common.ErrJobConfigDoesNotExist
	}
	return
}
