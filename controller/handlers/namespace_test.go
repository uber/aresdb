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
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	mutatorMocks "github.com/uber/aresdb/controller/mutators/mocks"
	"go.uber.org/zap"
)

func TestNamespaceHandler(t *testing.T) {

	logger, _ := zap.NewDevelopment()
	sugaredLogger := logger.Sugar()

	t.Run("Should work for ListNamespaces", func(t *testing.T) {
		mockNamespaceMutator := &mutatorMocks.NamespaceMutator{}
		p := NamespaceHandlerParams{
			Logger:           sugaredLogger,
			NamespaceMutator: mockNamespaceMutator,
		}
		namespaceHandler := NewNamespaceHandler(p)
		testRouter := mux.NewRouter()
		namespaceHandler.Register(testRouter)
		testServer := httptest.NewUnstartedServer(testRouter)
		testServer.Start()

		mockNamespaceMutator.On("ListNamespaces").Return([]string{"ns1"}, nil)
		hostPort := testServer.Listener.Addr().String()
		resp, err := http.Get(fmt.Sprintf("http://%s/namespaces", hostPort))
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		testServer.Close()
	})

	t.Run("Should work for CreateNamespace", func(t *testing.T) {
		mockNamespaceMutator := &mutatorMocks.NamespaceMutator{}
		p := NamespaceHandlerParams{
			Logger:           sugaredLogger,
			NamespaceMutator: mockNamespaceMutator,
		}
		namespaceHandler := NewNamespaceHandler(p)
		testRouter := mux.NewRouter()
		namespaceHandler.Register(testRouter)
		testServer := httptest.NewUnstartedServer(testRouter)
		testServer.Start()
		mockNamespaceMutator.On("CreateNamespace", "ns1").Return(nil)
		hostPort := testServer.Listener.Addr().String()

		nsBytes, _ := json.Marshal(struct {
			Namespace string
		}{"ns1"})
		resp, err := http.Post(fmt.Sprintf("http://%s/namespaces", hostPort), "application/json", bytes.NewBuffer(nsBytes))
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		testServer.Close()
	})

}
