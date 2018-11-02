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

package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"

	"github.com/uber/aresdb/memstore"
	memMocks "github.com/uber/aresdb/memstore/mocks"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/metastore/mocks"

	"github.com/gorilla/mux"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
	"github.com/uber/aresdb/metastore"
	"github.com/uber/aresdb/utils"
)

var _ = ginkgo.Describe("SchemaHandler", func() {

	var testServer *httptest.Server
	var hostPort string
	var testTable = metaCom.Table{
		Name: "testTable",
		Columns: []metaCom.Column{
			{
				Name: "col1",
				Type: "Int32",
			},
		},
	}
	var testTableSchema = memstore.TableSchema{
		EnumDicts: map[string]memstore.EnumDict{
			"testColumn": {
				ReverseDict: []string{"a", "b", "c"},
				Dict: map[string]int{
					"a": 0,
					"b": 1,
					"c": 2,
				},
			},
		},
		Schema: testTable,
	}

	testMetaStore := &mocks.MetaStore{}
	var testMemStore *memMocks.MemStore

	ginkgo.BeforeEach(func() {
		testMemStore = CreateMemStore(&testTableSchema, 0, nil, nil)
		schemaHandler := NewSchemaHandler(testMetaStore)
		testRouter := mux.NewRouter()
		schemaHandler.Register(testRouter.PathPrefix("/schema").Subrouter())
		testServer = httptest.NewUnstartedServer(WithPanicHandling(testRouter))
		testServer.Start()
		hostPort = testServer.Listener.Addr().String()
	})

	ginkgo.AfterEach(func() {
		testServer.Close()
	})

	ginkgo.It("ListTables should work", func() {
		testMemStore.On("GetSchemas").Return(map[string]*memstore.TableSchema{"testTable": nil})
		testMetaStore.On("ListTables").Return([]string{"testTable"}, nil)
		hostPort := testServer.Listener.Addr().String()
		resp, err := http.Get(fmt.Sprintf("http://%s/schema/tables", hostPort))
		Ω(err).Should(BeNil())
		respBody, err := ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusOK))
		Ω(respBody).Should(Equal([]byte(`["testTable"]`)))
	})

	ginkgo.It("GetTable should work", func() {
		testMetaStore.On("GetTable", "testTable").Return(&testTable, nil)
		testMetaStore.On("GetTable", "unknown").Return(nil, metastore.ErrTableDoesNotExist)
		resp, err := http.Get(fmt.Sprintf("http://%s/schema/tables/%s", hostPort, "testTable"))
		Ω(resp.StatusCode).Should(Equal(http.StatusOK))
		Ω(err).Should(BeNil())
		respBody, err := ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		respTable := metaCom.Table{}
		json.Unmarshal(respBody, &respTable)
		Ω(respTable).Should(Equal(testTableSchema.Schema))

		Ω(resp.StatusCode).Should(Equal(http.StatusOK))

		resp, err = http.Get(fmt.Sprintf("http://%s/schema/tables/%s", hostPort, "unknown"))
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusNotFound))
	})

	ginkgo.It("AddTable should work", func() {

		tableSchemaBytes, _ := json.Marshal(testTableSchema.Schema)

		testMetaStore.On("CreateTable", mock.Anything).Return(nil).Once()
		resp, _ := http.Post(fmt.Sprintf("http://%s/schema/tables", hostPort), "application/json", bytes.NewBuffer(tableSchemaBytes))
		Ω(resp.StatusCode).Should(Equal(http.StatusOK))

		testMetaStore.On("CreateTable", mock.Anything).Return(errors.New("Failed to create table")).Once()
		resp, _ = http.Post(fmt.Sprintf("http://%s/schema/tables", hostPort), "application/json", bytes.NewBuffer(tableSchemaBytes))
		Ω(resp.StatusCode).Should(Equal(http.StatusInternalServerError))

		tableSchemaBytes = []byte(`{"name": ""`)
		resp, _ = http.Post(fmt.Sprintf("http://%s/schema/tables", hostPort), "application/json", bytes.NewBuffer(tableSchemaBytes))
		Ω(resp.StatusCode).Should(Equal(http.StatusBadRequest))
	})

	ginkgo.It("UpdateTableConfig should work", func() {
		tableSchemaBytes, _ := json.Marshal(testTableSchema.Schema)

		testMetaStore.On("CreateTable", mock.Anything, mock.Anything).Return(nil).Once()
		resp, _ := http.Post(fmt.Sprintf("http://%s/schema/tables", hostPort), "application/json", bytes.NewBuffer(tableSchemaBytes))
		Ω(resp.StatusCode).Should(Equal(http.StatusOK))

		testMetaStore.On("UpdateTableConfig", mock.Anything, mock.Anything).Return(nil).Once()
		req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("http://%s/schema/tables/%s", hostPort, testTableSchema.Schema.Name), bytes.NewBuffer(tableSchemaBytes))
		resp, _ = http.DefaultClient.Do(req)
		Ω(resp.StatusCode).Should(Equal(http.StatusOK))

		testMetaStore.On("UpdateTableConfig", mock.Anything, mock.Anything).Return(errors.New("Failed to create table")).Once()
		req, _ = http.NewRequest(http.MethodPut, fmt.Sprintf("http://%s/schema/tables/%s", hostPort, testTableSchema.Schema.Name), bytes.NewBuffer(tableSchemaBytes))
		resp, _ = http.DefaultClient.Do(req)
		Ω(resp.StatusCode).Should(Equal(http.StatusInternalServerError))

		tableSchemaBytes = []byte(`{"name": ""`)
		req, _ = http.NewRequest(http.MethodPut, fmt.Sprintf("http://%s/schema/tables/%s", hostPort, testTableSchema.Schema.Name), bytes.NewBuffer(tableSchemaBytes))
		resp, _ = http.DefaultClient.Do(req)
		Ω(resp.StatusCode).Should(Equal(http.StatusBadRequest))
	})

	ginkgo.It("DeleteTable should work", func() {
		testMetaStore.On("DeleteTable", mock.Anything).Return(nil).Once()
		req, _ := http.NewRequest(http.MethodDelete, fmt.Sprintf("http://%s/schema/tables/%s", hostPort, "testTable"), &bytes.Buffer{})
		resp, _ := http.DefaultClient.Do(req)
		Ω(resp.StatusCode).Should(Equal(http.StatusOK))

		testMetaStore.On("DeleteTable", mock.Anything).Return(errors.New("Failed to delete table")).Once()
		req, _ = http.NewRequest(http.MethodDelete, fmt.Sprintf("http://%s/schema/tables/%s", hostPort, "testTable"), &bytes.Buffer{})
		resp, _ = http.DefaultClient.Do(req)
		Ω(resp.StatusCode).Should(Equal(http.StatusInternalServerError))
		bs, err := ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		defer resp.Body.Close()

		var errResp utils.APIError
		err = json.Unmarshal(bs, &errResp)
		Ω(err).Should(BeNil())
		Ω(errResp.Message).Should(Equal("Failed to delete table"))
	})

	ginkgo.It("AddColumn should work", func() {
		columnBytes := []byte(`{"name": "testCol", "type":"Int32", "defaultValue": "1"}`)
		testMetaStore.On("AddColumn", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		resp, _ := http.Post(fmt.Sprintf("http://%s/schema/tables/%s/columns", hostPort, "testTable"), "application/json", bytes.NewBuffer(columnBytes))
		Ω(resp.StatusCode).Should(Equal(http.StatusOK))

		columnBytes = []byte(`{"name": "testCol", "type":"Int32"}`)
		testMetaStore.On("AddColumn", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		resp, _ = http.Post(fmt.Sprintf("http://%s/schema/tables/%s/columns", hostPort, "testTable"), "application/json", bytes.NewBuffer(columnBytes))
		Ω(resp.StatusCode).Should(Equal(http.StatusOK))

		errorColumnBytes := []byte(`{"name": "testCol"`)
		resp, _ = http.Post(fmt.Sprintf("http://%s/schema/tables/%s/columns", hostPort, "testTable"), "application/json", bytes.NewBuffer(errorColumnBytes))
		Ω(resp.StatusCode).Should(Equal(http.StatusBadRequest))

		testMetaStore.On("AddColumn", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("Failed to add columns")).Once()
		resp, _ = http.Post(fmt.Sprintf("http://%s/schema/tables/%s/columns", hostPort, "testTable"), "application/json", bytes.NewBuffer(columnBytes))
		Ω(resp.StatusCode).Should(Equal(http.StatusInternalServerError))

		errorColumnBytes = []byte(`{"name": "testCol", "type": ""}`)
		testMetaStore.On("AddColumn", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		resp, _ = http.Post(fmt.Sprintf("http://%s/schema/tables/%s/columns", hostPort, "testTable"), "application/json", bytes.NewBuffer(errorColumnBytes))
		Ω(resp.StatusCode).Should(Equal(http.StatusBadRequest))

		errorColumnBytes = []byte(`{"name": "testCol", "type": "Int32", "defaultValue": "hello"}`)
		testMetaStore.On("AddColumn", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		resp, _ = http.Post(fmt.Sprintf("http://%s/schema/tables/%s/columns", hostPort, "testTable"), "application/json", bytes.NewBuffer(errorColumnBytes))
		Ω(resp.StatusCode).Should(Equal(http.StatusBadRequest))
	})

	ginkgo.It("DeleteColumn should work", func() {
		testMetaStore.On("DeleteColumn", mock.Anything, mock.Anything).Return(nil).Once()
		req, _ := http.NewRequest(http.MethodDelete, fmt.Sprintf("http://%s/schema/tables/%s/columns/%s", hostPort, "testTable", "testColumn"), &bytes.Buffer{})
		resp, _ := http.DefaultClient.Do(req)
		Ω(resp.StatusCode).Should(Equal(http.StatusOK))

		testMetaStore.On("DeleteColumn", mock.Anything, mock.Anything).Return(errors.New("Failed to delete columns")).Once()
		req, _ = http.NewRequest(http.MethodDelete, fmt.Sprintf("http://%s/schema/tables/%s/columns/%s", hostPort, "testTable", "testColumn"), &bytes.Buffer{})
		resp, _ = http.DefaultClient.Do(req)
		Ω(resp.StatusCode).Should(Equal(http.StatusInternalServerError))
	})

	ginkgo.It("UpdateColumn should work", func() {
		testColumnConfig1 := metaCom.ColumnConfig{
			PreloadingDays: 2,
			Priority:       3,
		}

		b, err := json.Marshal(testColumnConfig1)
		Ω(err).Should(BeNil())

		testMetaStore.On("UpdateColumn", mock.Anything, mock.Anything, mock.Anything).
			Return(nil).Once()
		req, _ := http.NewRequest(
			http.MethodPut, fmt.Sprintf("http://%s/schema/tables/%s/columns/%s",
				hostPort, "testTable", "testColumn"), bytes.NewReader(b))
		resp, _ := http.DefaultClient.Do(req)
		Ω(resp.StatusCode).Should(Equal(http.StatusOK))

		testMetaStore.On("UpdateColumn", mock.Anything, mock.Anything).
			Return(errors.New("failed to update columns")).Once()
		req, _ = http.NewRequest(http.MethodPut, fmt.Sprintf("http://%s/schema/tables/%s/columns/%s",
			hostPort, "testTable", "testColumn"), bytes.NewReader(b))
		resp, _ = http.DefaultClient.Do(req)
		Ω(resp.StatusCode).Should(Equal(http.StatusInternalServerError))
	})

	ginkgo.It("validateDefaultValue should work", func() {
		Ω(validateDefaultValue("trues", metaCom.Bool)).ShouldNot(BeNil())
		Ω(validateDefaultValue("true", metaCom.Bool)).Should(BeNil())

		Ω(validateDefaultValue("1000", metaCom.Uint8)).ShouldNot(BeNil())
		Ω(validateDefaultValue("0", metaCom.Uint8)).Should(BeNil())

		Ω(validateDefaultValue("100000", metaCom.Uint16)).ShouldNot(BeNil())
		Ω(validateDefaultValue("0", metaCom.Uint16)).Should(BeNil())

		Ω(validateDefaultValue("100000000000000", metaCom.Uint32)).ShouldNot(BeNil())
		Ω(validateDefaultValue("0", metaCom.Uint32)).Should(BeNil())

		Ω(validateDefaultValue("1000", metaCom.Int8)).ShouldNot(BeNil())
		Ω(validateDefaultValue("0", metaCom.Int8)).Should(BeNil())

		Ω(validateDefaultValue("100000000", metaCom.Int16)).ShouldNot(BeNil())
		Ω(validateDefaultValue("0", metaCom.Int16)).Should(BeNil())

		Ω(validateDefaultValue("100000000000000", metaCom.Int32)).ShouldNot(BeNil())
		Ω(validateDefaultValue("0", metaCom.Int32)).Should(BeNil())

		Ω(validateDefaultValue("1.s", metaCom.Float32)).ShouldNot(BeNil())
		Ω(validateDefaultValue("0.0", metaCom.Float32)).Should(BeNil())

		Ω(validateDefaultValue("00000000000000000000000000000000000000", metaCom.UUID)).ShouldNot(BeNil())
		Ω(validateDefaultValue("2cdc434e-4752-11e8-842f-0ed5f89f718b", metaCom.UUID)).Should(BeNil())

		Ω(validateDefaultValue("-122.44231998 37.77901703", metaCom.GeoPoint)).Should(BeNil())
		Ω(validateDefaultValue("-122.44231998,37.77901703", metaCom.GeoPoint)).Should(BeNil())
		Ω(validateDefaultValue("Point(-122.44231998,37.77901703)", metaCom.GeoPoint)).Should(BeNil())
		Ω(validateDefaultValue("  Point( -122.44231998 37.77901703  ) ", metaCom.GeoPoint)).Should(BeNil())
	})
})
