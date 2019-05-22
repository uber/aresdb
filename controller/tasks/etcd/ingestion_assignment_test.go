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
	"github.com/uber/aresdb/controller/models"
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/services"
	xwatch "github.com/m3db/m3x/watch"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
	pb "github.com/uber/aresdb/controller/generated/proto"
	mutators "github.com/uber/aresdb/controller/mutators/etcd"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/utils"
	"go.uber.org/zap"
)

func TestIngestionAssignmentTask(t *testing.T) {

	subInstance1 := placement.NewInstance().SetID("sub1")
	subInstance2 := placement.NewInstance().SetID("sub2")
	subInstance3 := placement.NewInstance().SetID("sub3")

	job1 := models.JobConfig{
		Name:  "job1",
		AresTableConfig: models.TableConfig{Name: "table1"},
		StreamingConfig: models.KafkaConfig{
			File:            models.KafkaClusterFile,
			TopicType:            models.KafkaTopicType,
			LatestOffset:          models.KafkaLatestOffset,
			ErrorThreshold:  models.KafkaErrorThreshold,
			StatusCheckInterval:      models.KafkaStatusCheckInterval,
			ARThreshold:     models.KafkaAutoRecoveryThreshold,
			ProcessorCount:  2,
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
		},
	}
	job1Bytes, _ := json.Marshal(job1)
	table1 := metaCom.Table{
		Name: "table1",
	}
	table1Bytes, _ := json.Marshal(table1)

	as1 := models.IngestionAssignment{
		Subscriber: "sub1",
		Jobs:       []models.JobConfig{job1},
	}
	asx := models.IngestionAssignment{
		Subscriber: "subx",
		Jobs:       []models.JobConfig{job1},
	}

	t.Run("suite", func(t *testing.T) {
		// test setup
		cleanUp, port := utils.SetUpEtcdTestServer(t)
		defer cleanUp()

		testClient0 := utils.SetUpEtcdTestClient(t, port)
		txnStore, err := testClient0.Txn()
		assert.NoError(t, err)

		clusterServices0, err := testClient0.Services(nil)
		assert.NoError(t, err)

		subscriberServiceID := services.NewServiceID().
			SetZone("test").
			SetEnvironment("test").
			SetName(utils.SubscriberServiceName("ns1"))

		err = clusterServices0.SetMetadata(subscriberServiceID, services.
			NewMetadata().
			SetHeartbeatInterval(1*time.Second).
			SetLivenessInterval(2*time.Second))
		assert.NoError(t, err)

		err = clusterServices0.Advertise(services.NewAdvertisement().SetPlacementInstance(subInstance1).SetServiceID(subscriberServiceID))
		assert.NoError(t, err)
		err = clusterServices0.Advertise(services.NewAdvertisement().SetPlacementInstance(subInstance2).SetServiceID(subscriberServiceID))
		assert.NoError(t, err)
		err = clusterServices0.Advertise(services.NewAdvertisement().SetPlacementInstance(subInstance3).SetServiceID(subscriberServiceID))
		assert.NoError(t, err)

		_, err = txnStore.Set(utils.NamespaceListKey(), &pb.EntityList{
			Entities: []*pb.EntityName{
				{
					Name: "ns1",
				},
			},
		})
		assert.NoError(t, err)

		_, err = txnStore.Set(utils.InstanceListKey("ns1"), &pb.EntityList{})
		assert.NoError(t, err)

		_, err = txnStore.Set(utils.JobListKey("ns1"), &pb.EntityList{
			Entities: []*pb.EntityName{
				{
					Name: "job1",
				},
			},
		})
		assert.NoError(t, err)

		_, err = txnStore.Set(utils.JobKey("ns1", "job1"), &pb.EntityConfig{
			Name:   "job1",
			Config: job1Bytes,
		})
		assert.NoError(t, err)

		_, err = txnStore.Set(utils.SchemaListKey("ns1"), &pb.EntityList{
			Entities: []*pb.EntityName{
				{
					Name: "table1",
				},
			},
		})
		assert.NoError(t, err)

		_, err = txnStore.Set(utils.SchemaKey("ns1", "table1"), &pb.EntityConfig{
			Name:   "table1",
			Config: table1Bytes,
		})
		assert.NoError(t, err)

		_, err = txnStore.Set(utils.JobAssignmentsListKey("ns1"), &pb.EntityList{})
		assert.NoError(t, err)

		logger, _ := zap.NewDevelopment()
		sugaredLogger := logger.Sugar()

		schemaMutator := mutators.NewTableSchemaMutator(txnStore, sugaredLogger)
		namespaceMutator := mutators.NewNamespaceMutator(txnStore)
		jobMutator := mutators.NewJobMutator(txnStore, sugaredLogger)
		assignmentMutator := mutators.NewIngestionAssignmentMutator(txnStore)
		err = assignmentMutator.AddIngestionAssignment("ns1", asx)
		assert.NoError(t, err)
		err = assignmentMutator.AddIngestionAssignment("ns1", as1)
		assert.NoError(t, err)

		controllerServiceID := services.NewServiceID().
			SetZone("test").
			SetEnvironment("test").
			SetName("controller")

		// task1
		testClient1 := utils.SetUpEtcdTestClient(t, port)
		assert.NoError(t, err)
		clusterServices1, err := testClient1.Services(nil)
		assert.NoError(t, err)
		leaderService1, err := clusterServices1.LeaderService(controllerServiceID, nil)
		assert.NoError(t, err)

		task1 := ingestionAssignmentTask{
			intervalSeconds:    10,
			logger:             sugaredLogger,
			scope:              tally.NoopScope,
			stopChan:           make(chan struct{}, 1),
			namespaceMutator:   namespaceMutator,
			jobMutator:         jobMutator,
			schemaMutator:      schemaMutator,
			assignmentsMutator: assignmentMutator,

			zone:           "test",
			environment:    "test",
			etcdServices:   clusterServices1,
			leaderElection: NewLeaderElector(leaderService1),
			watchManager: NewWatchManager(func(namespace string) (xwatch.Updatable, error) {
				serviceID := services.NewServiceID().
					SetZone("test").
					SetEnvironment("test").
					SetName(utils.SubscriberServiceName(namespace))
				hbSvc, err := clusterServices1.HeartbeatService(serviceID)
				if err != nil {
					return nil, err
				}
				return hbSvc.Watch()
			}),

			configHashes: make(map[string]configHash),
		}

		// client2
		testClient2 := utils.SetUpEtcdTestClient(t, port)
		assert.NoError(t, err)
		clusterServices2, err := testClient2.Services(nil)
		assert.NoError(t, err)
		leaderService2, err := clusterServices2.LeaderService(controllerServiceID, nil)
		assert.NoError(t, err)

		task2 := ingestionAssignmentTask{
			intervalSeconds:    10,
			logger:             sugaredLogger,
			scope:              tally.NoopScope,
			stopChan:           make(chan struct{}, 1),
			namespaceMutator:   namespaceMutator,
			jobMutator:         jobMutator,
			schemaMutator:      schemaMutator,
			assignmentsMutator: assignmentMutator,

			zone:           "test",
			environment:    "test",
			etcdServices:   clusterServices2,
			leaderElection: NewLeaderElector(leaderService2),
			watchManager: NewWatchManager(func(namespace string) (xwatch.Updatable, error) {
				serviceID := services.NewServiceID().
					SetZone("test").
					SetEnvironment("test").
					SetName(utils.SubscriberServiceName(namespace))
				hbSvc, err := clusterServices2.HeartbeatService(serviceID)
				if err != nil {
					return nil, err
				}
				return hbSvc.Watch()
			}),
			configHashes: make(map[string]configHash),
		}
		// test

		// only 1 task should win election
		go task1.Run()
		go task2.Run()

		// existing:
		//jobs: job1
		//subscribers: sub1, sub2, sub3
		//assignments: as1, asx

		// should have only one calculation
		// triggered by new subscriber event
		time.Sleep(5 * time.Second)
		assignments, _ := assignmentMutator.GetIngestionAssignments("ns1")
		sort.Slice(assignments, func(i, j int) bool {
			return assignments[i].Subscriber < assignments[j].Subscriber
		})
		assert.Len(t, assignments, 3)

		as2, _ := assignmentMutator.GetIngestionAssignment("ns1", assignments[0].Subscriber)
		job1Assigned := job1
		job1Assigned.StreamingConfig.ProcessorCount = 1
		assert.Equal(t, []models.JobConfig{job1Assigned}, as2.Jobs)
		assert.Len(t, assignments[1].Jobs, 1)
		assert.Len(t, assignments[2].Jobs, 0)
		task1.Done()
		task2.Done()
		time.Sleep(time.Duration(1) * time.Second)
	})
}
