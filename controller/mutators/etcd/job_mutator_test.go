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
	"github.com/uber/aresdb/controller/models"
	"github.com/uber/aresdb/controller/mutators/common"
	"testing"

	"github.com/stretchr/testify/assert"
	pb "github.com/uber/aresdb/controller/generated/proto"
	"github.com/uber/aresdb/utils"
	"go.uber.org/zap"
)

func TestJobMutator(t *testing.T) {
	testConfig1 := models.JobConfig{
		Name:    "demand",
		Version: 0,
		AresTableConfig: models.TableConfig{
			Name: "rta_table1",
		},
		StreamingConfig: models.KafkaConfig{
			Topic:   "demand_topic1",
			Cluster: "demand_cluster",
		},
	}

	testConfig2 := models.JobConfig{
		Name:    "demand",
		Version: 1,
		AresTableConfig: models.TableConfig{
			Name: "rta_table2",
		},
		StreamingConfig: models.KafkaConfig{
			Topic:   "demand_topic2",
			Cluster: "demand_cluster",
		},
	}

	aresSubscriberPayload := models.JobConfig{
		Name:    "demand",
		Version: 0,
		AresTableConfig: models.TableConfig{
			Name: "rta_table1",
		},
		StreamingConfig: models.KafkaConfig{
			Topic:           "demand_topic1",
			Cluster:         "demand_cluster",
			File:            "/etc/uber/kafka8/clusters.yaml",
			TopicType:            "heatpipe",
			LatestOffset:          true,
			ErrorThreshold:  10,
			StatusCheckInterval:      60,
			ARThreshold:     8,
			ProcessorCount:  1,
			BatchSize:       32768,
			MaxBatchDelayMS:    10000,
			MegaBytePerSec:      600,
			RestartOnFailure:         true,
			RestartInterval: 300,
			FailureHandler: models.FailureHandler{
				Type: "retry",
				Config: models.FailureHandlerConfig{
					InitRetryIntervalInSeconds: 60,
					Multiplier:                 1,
					MaxRetryMinutes:            525600,
				},
			},
		},
	}

	logger, _ := zap.NewDevelopment()
	sugaredLogger := logger.Sugar()

	t.Run("CRUD should work", func(t *testing.T) {
		// test setup
		cleanUp, port := utils.SetUpEtcdTestServer(t)
		defer cleanUp()

		client := utils.SetUpEtcdTestClient(t, port)
		txnStore, err := client.Txn()
		assert.NoError(t, err)

		_, err = txnStore.Set(utils.JobListKey("ns1"), &pb.EntityList{})
		assert.NoError(t, err)

		// test
		jobMutator := NewJobMutator(txnStore, sugaredLogger)

		jobs, err := jobMutator.GetJobs("ns1")
		assert.NoError(t, err)
		assert.Equal(t, 0, len(jobs))

		err = jobMutator.AddJob("ns1", testConfig1)
		assert.NoError(t, err)

		hash1, err := jobMutator.GetHash("ns1")
		assert.NoError(t, err)

		job1, err := jobMutator.GetJob("ns1", "demand")
		assert.NoError(t, err)
		assert.Equal(t, aresSubscriberPayload, job1)

		jobs, err = jobMutator.GetJobs("ns1")
		assert.NoError(t, err)
		assert.Equal(t, []models.JobConfig{aresSubscriberPayload}, jobs)

		err = jobMutator.UpdateJob("ns1", testConfig2)
		assert.NoError(t, err)

		hash2, err := jobMutator.GetHash("ns1")
		assert.NoError(t, err)
		assert.NotEqual(t, hash2, hash1)

		job2, err := jobMutator.GetJob("ns1", "demand")
		assert.NoError(t, err)
		assert.Equal(t, testConfig2.Name, job2.Name)

		err = jobMutator.DeleteJob("ns1", "demand")
		assert.NoError(t, err)

		hash3, err := jobMutator.GetHash("ns1")
		assert.NoError(t, err)
		assert.NotEqual(t, hash3, hash2)

		jobs, err = jobMutator.GetJobs("ns1")
		assert.NoError(t, err)
		assert.Equal(t, 0, len(jobs))
		assert.Equal(t, []models.JobConfig{}, jobs)
	})

	t.Run("CRUD should fail", func(t *testing.T) {
		cleanUp, port := utils.SetUpEtcdTestServer(t)
		defer cleanUp()

		client := utils.SetUpEtcdTestClient(t, port)
		txnStore, err := client.Txn()
		assert.NoError(t, err)

		jobMutator := NewJobMutator(txnStore, sugaredLogger)

		// add fail if job config already exists
		_, err = txnStore.Set(utils.JobListKey("ns"), &pb.EntityList{
			Entities: []*pb.EntityName{
				{
					Name:      "demand",
					Tomstoned: false,
				},
			},
		})
		assert.NoError(t, err)
		_, err = txnStore.Set(utils.JobKey("ns", "demand"), &pb.EntityConfig{})
		assert.NoError(t, err)

		err = jobMutator.AddJob("ns", testConfig1)
		assert.EqualError(t, err, common.ErrJobConfigAlreadyExist.Error())

		// add fail if namespace not exists
		err = jobMutator.AddJob("ns2", testConfig1)
		assert.EqualError(t, err, common.ErrNamespaceDoesNotExist.Error())
		// get fail if namespace not exists
		_, err = jobMutator.GetJobs("ns2")
		assert.EqualError(t, err, common.ErrNamespaceDoesNotExist.Error())
		// get hash fail if namespace not exists
		_, err = jobMutator.GetHash("ns2")
		assert.EqualError(t, err, common.ErrNamespaceDoesNotExist.Error())
		// get fail if job or namespace not exists
		_, err = jobMutator.GetJob("ns2", "job0")
		assert.EqualError(t, err, common.ErrJobConfigDoesNotExist.Error())

		// delete fail if job not exists
		err = jobMutator.DeleteJob("ns1", "job1")
		assert.EqualError(t, err, common.ErrNamespaceDoesNotExist.Error())
		// update fail if job not exists
		err = jobMutator.UpdateJob("ns1", testConfig1)
		assert.EqualError(t, err, common.ErrNamespaceDoesNotExist.Error())

		// test cleanup
	})
}
