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
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/uber/aresdb/controller/tasks/common"
	"go.uber.org/config"

	"github.com/uber/aresdb/cluster/kvstore"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3/src/cluster/kv/mem"
	"github.com/m3db/m3/src/cluster/services/leader/campaign"

	"github.com/uber/aresdb/controller/models"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/services"
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

	configProvider, err := config.NewYAML(config.Source(strings.NewReader("ingestionAssignmentTask: {intervalInSeconds: 1}")))
	assert.NoError(t, err)

	job1 := models.JobConfig{
		Name:            "job1",
		Version:         1,
		AresTableConfig: models.TableConfig{Name: "table1"},
		StreamingConfig: models.KafkaConfig{
			TopicType:           models.KafkaTopicType,
			LatestOffset:        models.KafkaLatestOffset,
			ErrorThreshold:      models.KafkaErrorThreshold,
			StatusCheckInterval: models.KafkaStatusCheckInterval,
			ARThreshold:         models.KafkaAutoRecoveryThreshold,
			ProcessorCount:      2,
			BatchSize:           models.KafkaBatchSize,
			MaxBatchDelayMS:     models.KafkaMaxBatchDelayMS,
			MegaBytePerSec:      models.KafkaMegaBytePerSec,
			RestartOnFailure:    models.KafkaRestartOnFailure,
			RestartInterval:     models.KafkaRestartInterval,
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
		txnStore := mem.NewStore()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		placementService := placement.NewMockService(ctrl)
		mockPlacement := placement.NewMockPlacement(ctrl)
		placementService.EXPECT().Placement().Return(mockPlacement, nil)
		mockPlacement.EXPECT().NumShards().Return(8)

		subscribers := []string{subInstance1.ID(), subInstance2.ID(), subInstance3.ID()}

		heartBeatService1 := services.NewMockHeartbeatService(ctrl)
		heartBeatService1.EXPECT().Get().Return(subscribers, nil).AnyTimes()

		heartBeatService2 := services.NewMockHeartbeatService(ctrl)
		heartBeatService1.EXPECT().Get().Return(subscribers, nil).AnyTimes()

		_, err := txnStore.Set(utils.NamespaceListKey(), &pb.EntityList{
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

		// task 1
		clusterServices1 := services.NewMockServices(ctrl)
		leaderService1 := services.NewMockLeaderService(ctrl)
		clusterServices1.EXPECT().LeaderService(gomock.Any(), gomock.Any()).Return(leaderService1, nil).AnyTimes()
		clusterServices1.EXPECT().HeartbeatService(gomock.Any()).Return(heartBeatService1, nil).AnyTimes()
		clusterServices1.EXPECT().PlacementService(gomock.Any(), gomock.Any()).Return(placementService, nil).AnyTimes()
		statusChan1 := make(chan campaign.Status, 1)
		statusChan1 <- campaign.NewStatus(campaign.Leader)
		leaderService1.EXPECT().Campaign("", gomock.Any()).Return(statusChan1, nil)
		leaderService1.EXPECT().Close().Return(nil).AnyTimes()

		etcdClient1 := &kvstore.EtcdClient{
			Zone:        "test",
			Environment: "test",
			ServiceName: "ares-controller",
			TxnStore:    mem.NewStore(),
			Services:    clusterServices1,
		}

		param1 := common.IngestionAssignmentTaskParams{
			ConfigProvider:     configProvider,
			Logger:             sugaredLogger,
			Scope:              tally.NoopScope,
			EtcdClient:         etcdClient1,
			NamespaceMutator:   namespaceMutator,
			JobMutator:         jobMutator,
			SchemaMutator:      schemaMutator,
			AssignmentsMutator: assignmentMutator,
			SubscriberMutator:  mutators.NewSubscriberMutator(etcdClient1),
		}
		task1 := NewIngestionAssignmentTask(param1)

		// task 2
		clusterServices2 := services.NewMockServices(ctrl)
		leaderService2 := services.NewMockLeaderService(ctrl)
		clusterServices2.EXPECT().LeaderService(gomock.Any(), gomock.Any()).Return(leaderService2, nil).AnyTimes()
		clusterServices2.EXPECT().HeartbeatService(gomock.Any()).Return(heartBeatService2, nil).AnyTimes()
		clusterServices2.EXPECT().PlacementService(gomock.Any(), gomock.Any()).Return(placementService, nil).AnyTimes()
		statusChan2 := make(chan campaign.Status, 1)
		statusChan2 <- campaign.NewStatus(campaign.Follower)
		leaderService2.EXPECT().Campaign("", gomock.Any()).Return(statusChan2, nil)
		leaderService2.EXPECT().Close().Return(nil).AnyTimes()

		etcdClient2 := &kvstore.EtcdClient{
			Zone:        "test",
			Environment: "test",
			ServiceName: "ares-controller",
			TxnStore:    mem.NewStore(),
			Services:    clusterServices2,
		}

		param2 := common.IngestionAssignmentTaskParams{
			ConfigProvider:     configProvider,
			Logger:             sugaredLogger,
			Scope:              tally.NoopScope,
			EtcdClient:         etcdClient2,
			NamespaceMutator:   namespaceMutator,
			JobMutator:         jobMutator,
			SchemaMutator:      schemaMutator,
			AssignmentsMutator: assignmentMutator,
			SubscriberMutator:  mutators.NewSubscriberMutator(etcdClient2),
		}
		task2 := NewIngestionAssignmentTask(param2)

		// only 1 task should win election
		go task1.Run()
		go task2.Run()

		//existing:
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
		assert.Len(t, as2.Jobs, 1)
		expectedAssignments := `
[
  {
    "job": "job1",
    "version": 1,
    "numShards": 8,
    "aresTableConfig": {
      "name": "table1",
      "cluster": "",
      "schema": {
        "name": "table1",
        "columns": null,
        "primaryKeyColumns": null,
        "isFactTable": false,
        "config": {
          "batchSize": 2097152,
          "redoLogRotationInterval": 10800,
          "maxRedoLogFileSize": 1073741824,
          "archivingDelayMinutes": 1440,
          "archivingIntervalMinutes": 180,
          "backfillIntervalMinutes": 60,
          "backfillMaxBufferSize": 4294967296,
          "backfillThresholdInBytes": 2097152,
          "backfillStoreBatchSize": 20000,
          "recordRetentionInDays": 90,
          "snapshotThreshold": 6291456,
          "snapshotIntervalMinutes": 360
        },
        "incarnation": 0,
        "version": 0
      }
    },
    "streamConfig": {
      "topic": "",
      "kafkaClusterName": "",
      "kafkaVersion": "",
      "topicType": "json",
      "latestOffset": true,
      "errorThreshold": 10,
      "statusCheckInterval": 60,
      "autoRecoveryThreshold": 8,
      "processorCount": 1,
      "batchSize": 32768,
      "maxBatchDelayMS": 10000,
      "megaBytePerSec": 600,
      "restartOnFailure": true,
      "restartInterval": 300,
      "failureHandler": {
        "type": "retry",
        "config": {
          "initRetryIntervalInSeconds": 60,
          "multiplier": 1,
          "maxRetryMinutes": 525600
        }
      },
      "kafkaBroker": "",
      "maxPollIntervalMs": 0,
      "sessionTimeoutNs": 0,
      "channelBufferSize": 0
    }
  }
]`
		actualJsonBytes, err := json.Marshal(as2.Jobs)
		assert.NoError(t, err)
		assert.JSONEq(t, expectedAssignments, string(actualJsonBytes))
		assert.Len(t, assignments[1].Jobs, 1)
		assert.Len(t, assignments[2].Jobs, 0)
		task1.Done()
		task2.Done()
		time.Sleep(time.Duration(1) * time.Second)
	})
}
