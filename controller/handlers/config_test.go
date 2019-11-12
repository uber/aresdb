package handler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/uber/aresdb/controller/models"
	mutatorMocks "github.com/uber/aresdb/controller/mutators/mocks"
	metaCom "github.com/uber/aresdb/metastore/common"
	"go.uber.org/zap"
)

func TestConfigHandler(t *testing.T) {
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
		Name: "",
		Columns: []metaCom.Column{
			{
				Name: "col1",
				Type: "Int32",
			},
		},
	}
	logger, _ := zap.NewDevelopment()
	sugaredLogger := logger.Sugar()

	t.Run("Should work for GetJob endpoint", func(t *testing.T) {
		mockJobMutator := mutatorMocks.JobMutator{}
		mockSchemaMutator := mutatorMocks.TableSchemaMutator{}
		p := ConfigHandlerParams{
			Logger:        sugaredLogger,
			JobMutator:    &mockJobMutator,
			SchemaMutator: &mockSchemaMutator,
		}

		jobHandler := NewConfigHandler(p)
		testRouter := mux.NewRouter()
		jobHandler.Register(testRouter.PathPrefix("/config").Subrouter())
		testServer = httptest.NewUnstartedServer(testRouter)
		testServer.Start()

		mockJobMutator.On("GetJob", "ns1", "job1").Return(testJob, nil)
		mockSchemaMutator.On("GetTable", "ns1", "demand_table").Return(&testSchema, nil)
		hostPort := testServer.Listener.Addr().String()
		resp, err := http.Get(fmt.Sprintf("http://%s/config/ns1/jobs/job1", hostPort))
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		testServer.Close()
	})

	t.Run("Should work for GetJobs request", func(t *testing.T) {
		mockJobMutator := mutatorMocks.JobMutator{}
		mockSchemaMutator := mutatorMocks.TableSchemaMutator{}
		p := ConfigHandlerParams{
			Logger:        sugaredLogger,
			JobMutator:    &mockJobMutator,
			SchemaMutator: &mockSchemaMutator,
		}

		jobHandler := NewConfigHandler(p)
		testRouter := mux.NewRouter()
		jobHandler.Register(testRouter.PathPrefix("/config").Subrouter())
		testServer = httptest.NewUnstartedServer(testRouter)
		testServer.Start()

		mockJobMutator.On("GetJobs", "ns1").Return([]models.JobConfig{testJob}, nil)
		mockSchemaMutator.On("GetTable", "ns1", "demand_table").Return(&testSchema, nil)
		hostPort := testServer.Listener.Addr().String()
		resp, err := http.Get(fmt.Sprintf("http://%s/config/ns1/jobs", hostPort))
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		testServer.Close()
	})

	t.Run("Should work for DeletaJob request", func(t *testing.T) {
		mockJobMutator := mutatorMocks.JobMutator{}
		p := ConfigHandlerParams{
			Logger:     sugaredLogger,
			JobMutator: &mockJobMutator,
		}

		jobHandler := NewConfigHandler(p)
		testRouter := mux.NewRouter()
		jobHandler.Register(testRouter.PathPrefix("/config").Subrouter())
		testServer = httptest.NewUnstartedServer(testRouter)
		testServer.Start()
		mockJobMutator.On("DeleteJob", "ns1", "job1").Return(nil)
		hostPort := testServer.Listener.Addr().String()
		req, _ := http.NewRequest(http.MethodDelete, fmt.Sprintf("http://%s/config/ns1/jobs/%s", hostPort, "job1"), &bytes.Buffer{})
		resp, err := http.DefaultClient.Do(req)
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		testServer.Close()
	})

	t.Run("Should work for UpdateJob request", func(t *testing.T) {
		mockJobMutator := mutatorMocks.JobMutator{}
		p := ConfigHandlerParams{
			Logger:     sugaredLogger,
			JobMutator: &mockJobMutator,
		}

		jobHandler := NewConfigHandler(p)
		testRouter := mux.NewRouter()
		jobHandler.Register(testRouter.PathPrefix("/config").Subrouter())
		testServer = httptest.NewUnstartedServer(testRouter)
		testServer.Start()
		testJobBytes, _ := json.Marshal(testJob)

		mockJobMutator.On("UpdateJob", "ns1", mock.Anything).Return(nil)
		hostPort := testServer.Listener.Addr().String()
		req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("http://%s/config/ns1/jobs/%s", hostPort, "job1"), bytes.NewBuffer(testJobBytes))
		resp, err := http.DefaultClient.Do(req)
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		testServer.Close()
	})

	t.Run("Should work for AddJob request", func(t *testing.T) {
		mockJobMutator := mutatorMocks.JobMutator{}
		p := ConfigHandlerParams{
			Logger:     sugaredLogger,
			JobMutator: &mockJobMutator,
		}

		jobHandler := NewConfigHandler(p)
		testRouter := mux.NewRouter()
		jobHandler.Register(testRouter.PathPrefix("/config").Subrouter())
		testServer = httptest.NewUnstartedServer(testRouter)
		testServer.Start()
		jobBytes, _ := json.Marshal(testJob)

		mockJobMutator.On("AddJob", "ns1", mock.Anything).Return(nil)
		hostPort := testServer.Listener.Addr().String()
		resp, err := http.Post(fmt.Sprintf("http://%s/config/ns1/jobs", hostPort), "application/json", bytes.NewBuffer(jobBytes))
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		testServer.Close()
	})

	t.Run("Should work for GetHash request", func(t *testing.T) {
		mockJobMutator := mutatorMocks.JobMutator{}
		p := ConfigHandlerParams{
			Logger:     sugaredLogger,
			JobMutator: &mockJobMutator,
		}

		jobHandler := NewConfigHandler(p)
		testRouter := mux.NewRouter()
		jobHandler.Register(testRouter.PathPrefix("/config").Subrouter())
		testServer = httptest.NewUnstartedServer(testRouter)
		testServer.Start()
		mockJobMutator.On("GetHash", "ns1").Return("someHash", nil)
		hostPort := testServer.Listener.Addr().String()
		resp, err := http.Get(fmt.Sprintf("http://%s/config/ns1/hash", hostPort))
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		testServer.Close()
	})
}
