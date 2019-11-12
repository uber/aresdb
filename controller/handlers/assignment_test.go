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

package handlers

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/uber/aresdb/controller/models"
	mutatorMocks "github.com/uber/aresdb/controller/mutators/mocks"
	metaCom "github.com/uber/aresdb/metastore/common"
	"go.uber.org/zap"
)

func TestAssignmentHandler(t *testing.T) {
	var testServer *httptest.Server

	testJob := models.JobConfig{
		Name:    "demand",
		Version: 0,
		AresTableConfig: models.TableConfig{
			Name: "demand_table",
		},
		StreamingConfig: models.KafkaConfig{
			Topic:   "demand_topic1",
			Cluster: "demand_cluster",
		},
	}

	testSchema := metaCom.Table{
		Name: "demand_table",
		Columns: []metaCom.Column{
			{
				Name: "col1",
				Type: "Int32",
			},
		},
	}

	testInstance := models.Instance{
		Name: "ins1",
	}

	testAssignment := models.IngestionAssignment{
		Subscriber: "sub1",
		Jobs:       []models.JobConfig{testJob},
	}

	logger, _ := zap.NewDevelopment()
	sugaredLogger := logger.Sugar()

	t.Run("Should work for GetAssignment endpoint", func(t *testing.T) {
		mockAssignmentMutator := mutatorMocks.IngestionAssignmentMutator{}
		mockSchemaMutator := mutatorMocks.TableSchemaMutator{}
		mockMembershipMutator := mutatorMocks.MembershipMutator{}

		assignmentHandler := NewAssignmentHandler(sugaredLogger, &mockAssignmentMutator, &mockSchemaMutator, &mockMembershipMutator)
		testRouter := mux.NewRouter()
		assignmentHandler.Register(testRouter.PathPrefix("/assignment").Subrouter())
		testServer = httptest.NewUnstartedServer(testRouter)
		testServer.Start()

		mockAssignmentMutator.On("GetIngestionAssignment", "ns1", "sub1").Return(testAssignment, nil)
		mockSchemaMutator.On("GetTable", "ns1", "demand_table").Return(&testSchema, nil)
		mockMembershipMutator.On("GetInstances", "ns1").Return([]models.Instance{testInstance}, nil)
		hostPort := testServer.Listener.Addr().String()
		resp, err := http.Get(fmt.Sprintf("http://%s/assignment/ns1/assignments/sub1", hostPort))
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		testServer.Close()
	})

	t.Run("Should work for GetAssignments request", func(t *testing.T) {
		mockAssignmentMutator := mutatorMocks.IngestionAssignmentMutator{}
		mockSchemaMutator := mutatorMocks.TableSchemaMutator{}
		mockMembershipMutator := mutatorMocks.MembershipMutator{}

		assignmentHandler := NewAssignmentHandler(sugaredLogger, &mockAssignmentMutator, &mockSchemaMutator, &mockMembershipMutator)
		testRouter := mux.NewRouter()
		assignmentHandler.Register(testRouter.PathPrefix("/assignment").Subrouter())
		testServer = httptest.NewUnstartedServer(testRouter)
		testServer.Start()

		mockAssignmentMutator.On("GetIngestionAssignments", "ns1").Return([]models.IngestionAssignment{testAssignment}, nil)
		mockSchemaMutator.On("GetTable", "ns1", "demand_table").Return(&testSchema, nil)
		mockMembershipMutator.On("GetInstances", "ns1").Return([]models.Instance{testInstance}, nil)
		hostPort := testServer.Listener.Addr().String()
		resp, err := http.Get(fmt.Sprintf("http://%s/assignment/ns1/assignments", hostPort))
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		testServer.Close()
	})

	t.Run("Should work for GetHash request", func(t *testing.T) {
		mockAssignmentMutator := mutatorMocks.IngestionAssignmentMutator{}
		mockSchemaMutator := mutatorMocks.TableSchemaMutator{}
		mockMembershipMutator := mutatorMocks.MembershipMutator{}

		assignmentHandler := NewAssignmentHandler(sugaredLogger, &mockAssignmentMutator, &mockSchemaMutator, &mockMembershipMutator)
		testRouter := mux.NewRouter()
		assignmentHandler.Register(testRouter.PathPrefix("/assignment").Subrouter())
		testServer = httptest.NewUnstartedServer(testRouter)
		testServer.Start()
		mockAssignmentMutator.On("GetHash", "ns1", "sub1").Return("someHash", nil)
		hostPort := testServer.Listener.Addr().String()
		resp, err := http.Get(fmt.Sprintf("http://%s/assignment/ns1/hash/sub1", hostPort))
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		testServer.Close()
	})
}
