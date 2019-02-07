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
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"

	"github.com/uber/aresdb/memstore"
	memCom "github.com/uber/aresdb/memstore/common"
	memMocks "github.com/uber/aresdb/memstore/mocks"
	metaCom "github.com/uber/aresdb/metastore/common"

	"github.com/gorilla/mux"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
)

var _ = ginkgo.Describe("DataHandler", func() {
	var testServer *httptest.Server
	var testSchema = memstore.NewTableSchema(&metaCom.Table{
		Name:        "abc",
		IsFactTable: false,
		Config: &metaCom.TableConfig{
			BatchSize: 10,
		},
	})

	var memStore *memMocks.MemStore
	ginkgo.BeforeEach(func() {
		memStore = CreateMemStore(testSchema, 0, nil, CreateMockDiskStore())
		memStore.On("HandleIngestion", "abc", 0, mock.Anything).Return(nil)
		dataHandler := NewDataHandler(memStore)
		testRouter := mux.NewRouter()
		dataHandler.Register(testRouter.PathPrefix("/data").Subrouter())

		testServer = httptest.NewUnstartedServer(WithPanicHandling(testRouter))
		testServer.Start()
	})

	ginkgo.AfterEach(func() {
		testServer.Close()
	})

	ginkgo.It("PostData fails on invalid request", func() {
		hostPort := testServer.Listener.Addr().String()
		resp, err := http.Post(fmt.Sprintf("http://%s/data/abc/0", hostPort), "application/upsert-data", bytes.NewBuffer([]byte{}))
		Ω(err).Should(BeNil())
		_, err = ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusBadRequest))
	})

	ginkgo.It("PostData should succeed on empty data post", func() {
		buffer, _ := memCom.NewUpsertBatchBuilder().ToByteArray()
		hostPort := testServer.Listener.Addr().String()
		resp, err := http.Post(fmt.Sprintf("http://%s/data/abc/0", hostPort), "application/upsert-data", bytes.NewBuffer(buffer))
		Ω(err).Should(BeNil())
		_, err = ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusOK))
	})
})
