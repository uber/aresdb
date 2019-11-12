package handler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/uber/aresdb/controller/models"
	mutatorMocks "github.com/uber/aresdb/controller/mutators/mocks"
	"go.uber.org/zap"
)

func TestMembershipHandler(t *testing.T) {

	testInstance := models.Instance{
		Name: "instance1",
		Host: "bar",
		Port: 123,
	}
	logger, _ := zap.NewDevelopment()
	sugaredLogger := logger.Sugar()

	t.Run("Should work for Join request", func(t *testing.T) {
		mockMembershipMutator := &mutatorMocks.MembershipMutator{}
		p := MembershipHandlerParams{
			Logger:            sugaredLogger,
			MembershipMutator: mockMembershipMutator,
		}
		MembershipHandler := NewMembershipHandler(p)
		testRouter := mux.NewRouter()
		MembershipHandler.Register(testRouter.PathPrefix("/membership").Subrouter())
		testServer := httptest.NewUnstartedServer(testRouter)
		testServer.Start()

		instanceBytes, _ := json.Marshal(testInstance)

		mockMembershipMutator.On("Join", "ns1", mock.Anything).Return(nil)
		hostPort := testServer.Listener.Addr().String()
		resp, err := http.Post(fmt.Sprintf("http://%s/membership/ns1/instances", hostPort), "application/json", bytes.NewBuffer(instanceBytes))
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		testServer.Close()
	})

	t.Run("Should work for GetInstance request", func(t *testing.T) {
		mockMembershipMutator := &mutatorMocks.MembershipMutator{}
		p := MembershipHandlerParams{
			Logger:            sugaredLogger,
			MembershipMutator: mockMembershipMutator,
		}
		MembershipHandler := NewMembershipHandler(p)
		testRouter := mux.NewRouter()
		MembershipHandler.Register(testRouter.PathPrefix("/membership").Subrouter())
		testServer := httptest.NewUnstartedServer(testRouter)
		testServer.Start()

		mockMembershipMutator.On("GetInstance", "ns1", "instance1").Return(testInstance, nil)
		hostPort := testServer.Listener.Addr().String()
		resp, err := http.Get(fmt.Sprintf("http://%s/membership/ns1/instances/instance1", hostPort))
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		b, _ := ioutil.ReadAll(resp.Body)
		var response GetInstanceResponse
		json.Unmarshal(b, &response)
		assert.Equal(t, "bar:123", response.Instance.Address)
		testServer.Close()
	})

	t.Run("Should work for GetInstances request", func(t *testing.T) {
		mockMembershipMutator := &mutatorMocks.MembershipMutator{}
		p := MembershipHandlerParams{
			Logger:            sugaredLogger,
			MembershipMutator: mockMembershipMutator,
		}
		MembershipHandler := NewMembershipHandler(p)
		testRouter := mux.NewRouter()
		MembershipHandler.Register(testRouter.PathPrefix("/membership").Subrouter())
		testServer := httptest.NewUnstartedServer(testRouter)
		testServer.Start()

		mockMembershipMutator.On("GetInstances", "ns1").Return([]models.Instance{testInstance}, nil)
		hostPort := testServer.Listener.Addr().String()
		resp, err := http.Get(fmt.Sprintf("http://%s/membership/ns1/instances", hostPort))
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		b, _ := ioutil.ReadAll(resp.Body)
		var response GetInstancesResponse
		json.Unmarshal(b, &response)
		assert.Equal(t, "bar:123", response.Instances["instance1"].Address)
		testServer.Close()
	})

	t.Run("Should work for Leave request", func(t *testing.T) {
		mockMembershipMutator := &mutatorMocks.MembershipMutator{}
		p := MembershipHandlerParams{
			Logger:            sugaredLogger,
			MembershipMutator: mockMembershipMutator,
		}
		MembershipHandler := NewMembershipHandler(p)
		testRouter := mux.NewRouter()
		MembershipHandler.Register(testRouter.PathPrefix("/membership").Subrouter())
		testServer := httptest.NewUnstartedServer(testRouter)
		testServer.Start()

		mockMembershipMutator.On("Leave", "ns1", "instance1").Return(nil)
		hostPort := testServer.Listener.Addr().String()
		req, _ := http.NewRequest(http.MethodDelete, fmt.Sprintf("http://%s/membership/ns1/instances/%s", hostPort, "instance1"), &bytes.Buffer{})
		resp, err := http.DefaultClient.Do(req)
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		testServer.Close()
	})

	t.Run("Should work for GetHash request", func(t *testing.T) {
		mockMembershipMutator := &mutatorMocks.MembershipMutator{}
		p := MembershipHandlerParams{
			Logger:            sugaredLogger,
			MembershipMutator: mockMembershipMutator,
		}
		MembershipHandler := NewMembershipHandler(p)
		testRouter := mux.NewRouter()
		MembershipHandler.Register(testRouter.PathPrefix("/membership").Subrouter())
		testServer := httptest.NewUnstartedServer(testRouter)
		testServer.Start()

		mockMembershipMutator.On("GetHash", "ns1").Return("someHash", nil)
		hostPort := testServer.Listener.Addr().String()
		resp, err := http.Get(fmt.Sprintf("http://%s/membership/ns1/hash", hostPort))
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		testServer.Close()
	})
}
