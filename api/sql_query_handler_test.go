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
	"github.com/uber/aresdb/cluster/topology"
	"io/ioutil"
	"net/http"
	"net/http/httptest"

	memCom "github.com/uber/aresdb/memstore/common"
	memMocks "github.com/uber/aresdb/memstore/mocks"
	metaCom "github.com/uber/aresdb/metastore/common"

	"github.com/gorilla/mux"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber/aresdb/common"
)

var _ = ginkgo.Describe("QueryHandler SQL", func() {
	var testServer *httptest.Server
	var testSchema = memCom.NewTableSchema(&metaCom.Table{
		Name:        "trips",
		IsFactTable: false,
		Columns: []metaCom.Column{
			{
				Name: "request_at",
				Type: "Uint32",
			},
			{
				Name: "fare_total",
				Type: "Flaot",
			},
			{
				Name: "city_id",
				Type: "Uint16",
			},
			{
				Name: "status",
				Type: "SmallEnum",
			},
		},
		Config: metaCom.TableConfig{
			BatchSize: 10,
		},
	})

	var memStore *memMocks.MemStore
	ginkgo.BeforeEach(func() {
		memStore = CreateMemStore(testSchema, 0, nil, CreateMockDiskStore())
		queryHandler := NewQueryHandler(
			memStore,
			topology.NewStaticShardOwner([]int{0}),
			common.QueryConfig{
				DeviceMemoryUtilization: 1.0,
			})
		testRouter := mux.NewRouter()
		testRouter.HandleFunc("/sql", queryHandler.HandleSQL).Methods(http.MethodGet, http.MethodPost)
		testServer = httptest.NewUnstartedServer(WithPanicHandling(testRouter))
		testServer.Start()
	})

	ginkgo.AfterEach(func() {
		testServer.Close()
	})

	ginkgo.It("HandleSQL should succeed on valid requests", func() {
		hostPort := testServer.Listener.Addr().String()
		// Invalid timeBucketizer days.
		query := `
			{
			  "queries": [
				"SELECT count(*) AS value FROM trips WHERE status='completed' AND aql_time_filter(request_at, \"24 hours ago\", \"this quarter-hour\", America/New_York) GROUP BY aql_time_bucket_hour(request_at, \"\", America/New_York) "
			  ]
			}
		`
		resp, err := http.Post(fmt.Sprintf("http://%s/sql", hostPort), "application/json", bytes.NewBuffer([]byte(query)))
		Ω(err).Should(BeNil())
		bs, err := ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(string(bs)).Should(MatchJSON(`{
				"results": [
				  {}
				]
			  }`))
		Ω(resp.StatusCode).Should(Equal(http.StatusOK))
	})

	ginkgo.It("HandleSQL should fail on request that cannot be unmarshaled", func() {
		hostPort := testServer.Listener.Addr().String()
		resp, err := http.Post(fmt.Sprintf("http://%s/sql", hostPort), "application/json", bytes.NewBuffer([]byte{}))
		Ω(err).Should(BeNil())
		bs, err := ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(string(bs)).Should(MatchJSON(`
			{"message":"Bad request: failed to unmarshal request body","cause":{"Offset":0}}
		`))
		Ω(resp.StatusCode).Should(Equal(http.StatusBadRequest))
	})

	ginkgo.It("HandleSQL should succeed on empty query requests", func() {
		hostPort := testServer.Listener.Addr().String()
		query := `
			{
			  "queries": [
			  ]
			}
		`
		resp, err := http.Post(fmt.Sprintf("http://%s/sql", hostPort), "application/json", bytes.NewBuffer([]byte(query)))
		Ω(err).Should(BeNil())
		bs, err := ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusOK))
		Ω(string(bs)).Should(MatchJSON(`
			{
				"results": []
            }
		`))
	})

	ginkgo.It("HandleSQL should fail requests without queries", func() {
		hostPort := testServer.Listener.Addr().String()
		query := `
			{"q":{}}
		`
		resp, err := http.Post(fmt.Sprintf("http://%s/sql", hostPort), "application/json", bytes.NewBuffer([]byte(query)))
		Ω(err).Should(BeNil())
		bs, err := ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusBadRequest))
		Ω(string(bs)).Should(ContainSubstring("Bad request: missing/invalid parameter"))
	})
})
