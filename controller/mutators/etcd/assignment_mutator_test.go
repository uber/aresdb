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
	"testing"

	"github.com/stretchr/testify/assert"
	pb "github.com/uber/aresdb/controller/generated/proto"
	"github.com/uber/aresdb/controller/models"
	"github.com/uber/aresdb/controller/mutators/common"
	"github.com/uber/aresdb/utils"
)

func TestIngestionAssignmentMutator(t *testing.T) {

	testConfig1 := models.JobConfig{
		Name:    "demand",
		Version: 0,
		Table: models.TableConfig{
			Name: "rta_table1",
		},
		Kafka: models.KafkaConfig{
			Topic:   "demand_topic1",
			Cluster: "demand_cluster",
		},
	}

	testConfig2 := models.JobConfig{
		Name:    "demand",
		Version: 1,
		Table: models.TableConfig{
			Name: "rta_table2",
		},
		Kafka: models.KafkaConfig{
			Topic:   "demand_topic2",
			Cluster: "demand_cluster",
		},
	}

	testAssignment1 := models.IngestionAssignment{
		Subscriber: "sub1",
		Jobs: []models.JobConfig{
			testConfig1, testConfig2,
		},
	}

	testAssignment2 := models.IngestionAssignment{
		Subscriber: "sub1",
		Jobs: []models.JobConfig{
			testConfig1,
		},
	}

	t.Run("CRUD should work", func(t *testing.T) {
		// test setup
		cleanUp, port := utils.SetUpEtcdTestServer(t)
		defer cleanUp()

		client := utils.SetUpEtcdTestClient(t, port)
		etcdStore, err := client.Txn()
		assert.NoError(t, err)
		_, err = etcdStore.Set(utils.JobAssignmentsListKey("ns1"), &pb.EntityList{})
		assert.NoError(t, err)

		// test
		ingestionAssignmentMutator := NewIngestionAssignmentMutator(etcdStore)

		ingestionAssignments, err := ingestionAssignmentMutator.GetIngestionAssignments("ns1")
		assert.NoError(t, err)
		assert.Equal(t, 0, len(ingestionAssignments))

		err = ingestionAssignmentMutator.AddIngestionAssignment("ns1", testAssignment1)
		assert.NoError(t, err)

		_, err = ingestionAssignmentMutator.GetHash("ns1", testAssignment1.Subscriber)
		assert.NoError(t, err)

		IngestionAssignment1, err := ingestionAssignmentMutator.GetIngestionAssignment("ns1", "sub1")
		assert.NoError(t, err)
		assert.Equal(t, testAssignment1, IngestionAssignment1)

		ingestionAssignments, err = ingestionAssignmentMutator.GetIngestionAssignments("ns1")
		assert.NoError(t, err)
		assert.Equal(t, []models.IngestionAssignment{testAssignment1}, ingestionAssignments)

		err = ingestionAssignmentMutator.UpdateIngestionAssignment("ns1", testAssignment2)
		assert.NoError(t, err)

		IngestionAssignment2, err := ingestionAssignmentMutator.GetIngestionAssignment("ns1", "sub1")
		assert.NoError(t, err)
		assert.Equal(t, testAssignment2, IngestionAssignment2)

		err = ingestionAssignmentMutator.DeleteIngestionAssignment("ns1", "sub1")
		assert.NoError(t, err)

		ingestionAssignments, err = ingestionAssignmentMutator.GetIngestionAssignments("ns1")
		assert.NoError(t, err)
		assert.Equal(t, 0, len(ingestionAssignments))
		assert.Equal(t, []models.IngestionAssignment{}, ingestionAssignments)
	})

	t.Run("CRUD should fail", func(t *testing.T) {
		// test setup
		cleanUp, port := utils.SetUpEtcdTestServer(t)
		defer cleanUp()

		client := utils.SetUpEtcdTestClient(t, port)
		etcdStore, err := client.Txn()
		assert.NoError(t, err)

		// test
		ingestionAssignmentMutator := NewIngestionAssignmentMutator(etcdStore)
		// add fail if IngestionAssignment config already exists
		_, err = etcdStore.Set(utils.JobAssignmentsListKey("ns"), &pb.EntityList{
			Entities: []*pb.EntityName{
				{
					Name: "demand",
				},
			},
		})
		assert.NoError(t, err)

		// add fail if namespace not exists
		err = ingestionAssignmentMutator.AddIngestionAssignment("ns2", testAssignment1)
		assert.EqualError(t, err, common.ErrNamespaceDoesNotExist.Error())
		// get fail if namespace not exists
		_, err = ingestionAssignmentMutator.GetIngestionAssignments("ns2")
		assert.EqualError(t, err, common.ErrNamespaceDoesNotExist.Error())
		// get hash fail if namespace not exists
		_, err = ingestionAssignmentMutator.GetHash("ns2", "foo")
		assert.EqualError(t, err, common.ErrNamespaceDoesNotExist.Error())
		// get fail if IngestionAssignment or namespace not exists
		_, err = ingestionAssignmentMutator.GetIngestionAssignment("ns2", "IngestionAssignment0")
		assert.EqualError(t, err, common.ErrIngestionAssignmentDoesNotExist.Error())

		// delete fail if IngestionAssignment not exists
		err = ingestionAssignmentMutator.DeleteIngestionAssignment("ns", "IngestionAssignment1")
		assert.EqualError(t, err, common.ErrIngestionAssignmentDoesNotExist.Error())
		// update fail if IngestionAssignment not exists
		err = ingestionAssignmentMutator.UpdateIngestionAssignment("ns", testAssignment1)
		assert.EqualError(t, err, common.ErrIngestionAssignmentDoesNotExist.Error())
	})
}
