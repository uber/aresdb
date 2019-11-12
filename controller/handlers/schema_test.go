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
	"github.com/stretchr/testify/mock"
	mutatorMock "github.com/uber/aresdb/controller/mutators/mocks"
	metaCom "github.com/uber/aresdb/metastore/common"
	"go.uber.org/zap"
)

func TestSchemaHandler(t *testing.T) {

	logger, _ := zap.NewDevelopment()
	sl := logger.Sugar()
	var testTable = metaCom.Table{
		Name: "testTable",
		Columns: []metaCom.Column{
			{
				Name: "col1",
				Type: "Int32",
			},
		},
	}

	t.Run("Should work for AddTable request", func(t *testing.T) {
		mockTableSchemaMutator := &mutatorMock.TableSchemaMutator{}
		p := SchemaHandlerParams{
			TableSchemaMutator: mockTableSchemaMutator,
			Logger:             sl,
		}
		schemaHandler := NewSchemaHandler(p)
		testRouter := mux.NewRouter()
		schemaHandler.Register(testRouter.PathPrefix("/schema").Subrouter())
		testServer := httptest.NewUnstartedServer(testRouter)
		testServer.Start()
		tableSchemaBytes, _ := json.Marshal(testTable)

		mockTableSchemaMutator.On("CreateTable", "ns1", mock.Anything, mock.Anything).Return(nil)
		hostPort := testServer.Listener.Addr().String()
		resp, err := http.Post(fmt.Sprintf("http://%s/schema/ns1/tables", hostPort), "application/json", bytes.NewBuffer(tableSchemaBytes))
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		testServer.Close()
	})

	t.Run("Should work for GetTable request", func(t *testing.T) {
		mockTableSchemaMutator := &mutatorMock.TableSchemaMutator{}
		p := SchemaHandlerParams{
			TableSchemaMutator: mockTableSchemaMutator,
			Logger:             sl,
		}
		schemaHandler := NewSchemaHandler(p)
		testRouter := mux.NewRouter()
		schemaHandler.Register(testRouter.PathPrefix("/schema").Subrouter())
		testServer := httptest.NewUnstartedServer(testRouter)
		testServer.Start()
		mockTableSchemaMutator.On("GetTable", "ns1", "table1").Return(&testTable, nil)
		hostPort := testServer.Listener.Addr().String()
		resp, err := http.Get(fmt.Sprintf("http://%s/schema/ns1/tables/table1", hostPort))
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		testServer.Close()
	})

	t.Run("Should work for GetTables request", func(t *testing.T) {
		mockTableSchemaMutator := &mutatorMock.TableSchemaMutator{}
		p := SchemaHandlerParams{
			TableSchemaMutator: mockTableSchemaMutator,
			Logger:             sl,
		}
		schemaHandler := NewSchemaHandler(p)
		testRouter := mux.NewRouter()
		schemaHandler.Register(testRouter.PathPrefix("/schema").Subrouter())
		testServer := httptest.NewUnstartedServer(testRouter)
		testServer.Start()
		mockTableSchemaMutator.On("ListTables", "ns1").Return([]string{"table1"}, nil)
		mockTableSchemaMutator.On("GetTable", "ns1", "table1").Return(&testTable, nil)
		hostPort := testServer.Listener.Addr().String()
		resp, err := http.Get(fmt.Sprintf("http://%s/schema/ns1/tables", hostPort))
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		testServer.Close()
	})

	t.Run("Should work for DeletaTable request", func(t *testing.T) {
		mockTableSchemaMutator := &mutatorMock.TableSchemaMutator{}
		p := SchemaHandlerParams{
			TableSchemaMutator: mockTableSchemaMutator,
			Logger:             sl,
		}
		schemaHandler := NewSchemaHandler(p)
		testRouter := mux.NewRouter()
		schemaHandler.Register(testRouter.PathPrefix("/schema").Subrouter())
		testServer := httptest.NewUnstartedServer(testRouter)
		testServer.Start()
		mockTableSchemaMutator.On("DeleteTable", "ns1", "table1").Return(nil)
		hostPort := testServer.Listener.Addr().String()
		req, _ := http.NewRequest(http.MethodDelete, fmt.Sprintf("http://%s/schema/ns1/tables/%s", hostPort, "table1"), &bytes.Buffer{})
		resp, err := http.DefaultClient.Do(req)
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		testServer.Close()
	})

	t.Run("Should work for UpdateTable request", func(t *testing.T) {
		mockTableSchemaMutator := &mutatorMock.TableSchemaMutator{}
		p := SchemaHandlerParams{
			TableSchemaMutator: mockTableSchemaMutator,
			Logger:             sl,
		}
		schemaHandler := NewSchemaHandler(p)
		testRouter := mux.NewRouter()
		schemaHandler.Register(testRouter.PathPrefix("/schema").Subrouter())
		testServer := httptest.NewUnstartedServer(testRouter)
		testServer.Start()
		tableSchemaBytes, _ := json.Marshal(testTable)

		mockTableSchemaMutator.On("UpdateTable", "ns1", mock.Anything, mock.Anything).Return(nil)
		hostPort := testServer.Listener.Addr().String()
		req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("http://%s/schema/ns1/tables/%s", hostPort, "table1"), bytes.NewBuffer(tableSchemaBytes))
		resp, err := http.DefaultClient.Do(req)
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		testServer.Close()
	})

	t.Run("Should work for GetHash request", func(t *testing.T) {
		mockTableSchemaMutator := &mutatorMock.TableSchemaMutator{}
		p := SchemaHandlerParams{
			TableSchemaMutator: mockTableSchemaMutator,
			Logger:             sl,
		}
		schemaHandler := NewSchemaHandler(p)
		testRouter := mux.NewRouter()
		schemaHandler.Register(testRouter.PathPrefix("/schema").Subrouter())
		testServer := httptest.NewUnstartedServer(testRouter)
		testServer.Start()
		mockTableSchemaMutator.On("GetHash", "ns1").Return("someHash", nil)
		hostPort := testServer.Listener.Addr().String()
		resp, err := http.Get(fmt.Sprintf("http://%s/schema/ns1/hash", hostPort))
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		testServer.Close()
	})
}
