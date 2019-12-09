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
	"github.com/uber/aresdb/query"
	"github.com/uber/aresdb/utils"
	"io/ioutil"
	"net/http"
	"net/http/httptest"

	memCom "github.com/uber/aresdb/memstore/common"
	memMocks "github.com/uber/aresdb/memstore/mocks"
	metaCom "github.com/uber/aresdb/metastore/common"

	"encoding/json"
	"github.com/gorilla/mux"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	"github.com/uber/aresdb/common"
	queryCom "github.com/uber/aresdb/query/common"
)

var _ = ginkgo.Describe("QueryHandler", func() {
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
			},
			10)
		testRouter := mux.NewRouter()
		testRouter.HandleFunc("/aql", utils.ApplyHTTPWrappers(queryHandler.HandleAQL)).Methods(http.MethodGet, http.MethodPost)
		testServer = httptest.NewUnstartedServer(WithPanicHandling(testRouter))
		testServer.Start()
	})

	ginkgo.AfterEach(func() {
		testServer.Close()
	})

	ginkgo.It("HandleAQL should succeed on valid requests", func() {
		hostPort := testServer.Listener.Addr().String()
		// Invalid timeBucketizer days.
		query := `
			{
			  "queries": [
				{
				  "measures": [
					{
					  "sqlExpression": "count(*)"
					}
				  ],
				  "rowFilters": [
					"trips.status!='ACTIVE'"
				  ],
				  "table": "trips",
				  "timeFilter": {
					"column": "trips.request_at",
					"from": "-6d"
				  },
				  "dimensions": [
					{
					  "sqlExpression": "trips.request_at",
					  "timeBucketizer": "day",
					  "timeUnit": "second"
					}
				  ]
				}
			  ]
			}
		`
		resp, err := http.Post(fmt.Sprintf("http://%s/aql", hostPort), "application/json", bytes.NewBuffer([]byte(query)))
		Ω(err).Should(BeNil())
		bs, err := ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(string(bs)).Should(MatchJSON(`{
				"results": [
				  {}
				]
			  }`))
		Ω(resp.StatusCode).Should(Equal(http.StatusOK))

		query = `
			{
			  "queries": [
				{
				  "measures": [
					{
					  "sqlExpression": "1"
					}
				  ],
				  "rowFilters": [
					"trips.status!='ACTIVE'"
				  ],
				  "table": "trips",
				  "timeFilter": {
					"column": "trips.request_at",
					"from": "-6d"
				  },
				  "dimensions": [
					{
					  "sqlExpression": "trips.request_at"
					}
				  ]
				}
			  ]
			}
		`
		resp, err = http.Post(fmt.Sprintf("http://%s/aql", hostPort), "application/json", bytes.NewBuffer([]byte(query)))
		Ω(err).Should(BeNil())
		bs, err = ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(string(bs)).Should(MatchJSON(`{
				"results": [
				  {
					"headers": [
						"trips.request_at"
					],
					"matrixData": []
				  }
				]
			  }`))
		Ω(resp.StatusCode).Should(Equal(http.StatusOK))
	})

	ginkgo.It("HandleAQL should fail on request that cannot be unmarshaled", func() {
		hostPort := testServer.Listener.Addr().String()
		resp, err := http.Post(fmt.Sprintf("http://%s/aql", hostPort), "application/json", bytes.NewBuffer([]byte{}))
		Ω(err).Should(BeNil())
		bs, err := ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(string(bs)).Should(MatchJSON(`
			{"message":"Bad request: failed to unmarshal request body","cause":{"Offset":0}}
		`))
		Ω(resp.StatusCode).Should(Equal(http.StatusBadRequest))
	})

	ginkgo.It("HandleAQL should succeed on empty query requests", func() {
		hostPort := testServer.Listener.Addr().String()
		query := `
			{
			  "queries": [
			  ]
			}
		`
		resp, err := http.Post(fmt.Sprintf("http://%s/aql", hostPort), "application/json", bytes.NewBuffer([]byte(query)))
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

	ginkgo.It("HandleAQL should fail requests without queries", func() {
		hostPort := testServer.Listener.Addr().String()
		query := `
			{"q":{}}
		`
		resp, err := http.Post(fmt.Sprintf("http://%s/aql", hostPort), "application/json", bytes.NewBuffer([]byte(query)))
		Ω(err).Should(BeNil())
		bs, err := ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusBadRequest))
		Ω(string(bs)).Should(ContainSubstring("Bad request: missing/invalid parameter"))
	})

	ginkgo.It("HandleAQL should work for queries overwritten by parameter q", func() {
		hostPort := testServer.Listener.Addr().String()
		query := "{}"
		resp, err := http.Post(fmt.Sprintf("http://%s/aql?q={\"queries\":[]}", hostPort), "application/json", bytes.NewBuffer([]byte(query)))
		Ω(err).Should(BeNil())
		_, err = ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusOK))
	})

	ginkgo.It("ReportError should work", func() {
		rw := NewHLLQueryResponseWriter()
		Ω(rw.GetStatusCode()).Should(Equal(http.StatusOK))
		rw.ReportError(0, "test", errors.New("test err"), http.StatusBadRequest)
		Ω(rw.GetStatusCode()).Should(Equal(http.StatusBadRequest))

		hllRW := rw.(*HLLQueryResponseWriter)
		Ω(hllRW.response.GetBytes()).Should(Equal([]byte{2, 1, 237, 172, 0, 0, 0, 0, 8, 0, 0, 0,
			1, 0, 0, 0, 116, 101, 115, 116, 32, 101, 114, 114, 0, 0, 0, 0, 0, 0, 0, 0}))
	})

	ginkgo.It("ReportQueryContext should work", func() {
		rw := NewHLLQueryResponseWriter()
		Ω(func() { rw.ReportQueryContext(nil) }).ShouldNot(Panic())
	})

	ginkgo.It("ReportResult should work", func() {
		rw := NewHLLQueryResponseWriter()
		rw.ReportResult(0, &query.AQLQueryContext{HLLQueryResult: []byte{0, 0, 0, 0, 0, 0, 0, 0}})

		hllRW := rw.(*HLLQueryResponseWriter)
		Ω(hllRW.response.GetBytes()).Should(Equal([]byte{2, 1, 237, 172, 0, 0, 0, 0, 8, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}))
	})

	ginkgo.It("Report hll result in JsonResponseWriter should work", func() {
		q := &queryCom.AQLQuery{
			Table: "trips",
		}

		oopk := query.OOPKContext{
			// C.AGGR_HLL
			AggregateType: 10,
		}

		data, err := ioutil.ReadFile("../testing/data/query/hll")
		Ω(err).Should(BeNil())
		rw := NewJSONQueryResponseWriter(2)
		rw.ReportResult(0, &query.AQLQueryContext{
			Query:          q,
			OOPK:           oopk,
			HLLQueryResult: []byte{0, 0, 0, 0, 0, 0, 0, 0},
		})
		rw.ReportResult(1, &query.AQLQueryContext{
			Query:          q,
			OOPK:           oopk,
			HLLQueryResult: data,
		})

		Ω(rw.(*JSONQueryResponseWriter).response.Results).Should(HaveLen(2))
		resultJson, err := json.Marshal(rw.(*JSONQueryResponseWriter).response.Results)
		Ω(resultJson).Should(MatchJSON(`
		[
			null,
			{
				"1": {
					"c": {
						"2": 2
					}
				},
				"4294967295": {
					"d": {
						"514": 4
					}
				},
				"NULL": {
					"NULL": {
						"NULL": 3
					}
				}
			}
		]
		`))
		Ω(rw.(*JSONQueryResponseWriter).response.Errors).Should(HaveLen(2))
		Ω(rw.(*JSONQueryResponseWriter).response.Errors[0]).ShouldNot(BeNil())
		Ω(rw.(*JSONQueryResponseWriter).response.Errors[1]).Should(BeNil())
	})

	ginkgo.It("Verbose should work", func() {
		hostPort := testServer.Listener.Addr().String()
		query := `
			{
			  "queries": [
				{
				  "measures": [
					{
					  "sqlExpression": "count(*)"
					}
				  ],
				  "rowFilters": [
					"trips.status!='ACTIVE'"
				  ],
				  "table": "trips",
				  "timeFilter": {
					"column": "trips.request_at",
					"from": "-6d"
				  },
				  "dimensions": [
					{
					  "sqlExpression": "trips.request_at",
					  "timeBucketizer": "day",
					  "timeUnit": "second"
					}
				  ]
				}
			  ]
			}
		`
		resp, err := http.Post(fmt.Sprintf("http://%s/aql?verbose=1", hostPort), "application/json", bytes.NewBuffer([]byte(query)))
		Ω(err).Should(BeNil())
		bs, err := ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(string(bs)).Should(ContainSubstring("FLOOR"))
		Ω(string(bs)).Should(ContainSubstring("Unsigned"))
		Ω(string(bs)).Should(ContainSubstring("allBatches"))

		query = `
			{
			  "queries": [
				{
				  "measures": [
					{
					  "sqlExpression": "1"
					}
				  ],
				  "rowFilters": [
					"trips.status!='ACTIVE'"
				  ],
				  "table": "trips",
				  "timeFilter": {
					"column": "trips.request_at",
					"from": "-6d"
				  },
				  "dimensions": [
					{
					  "sqlExpression": "trips.request_at"
					}
				  ]
				}
			  ]
			}
		`
		resp, err = http.Post(fmt.Sprintf("http://%s/aql?verbose=1", hostPort), "application/json", bytes.NewBuffer([]byte(query)))
		Ω(err).Should(BeNil())
		bs, err = ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(string(bs)).Should(ContainSubstring("mainTableCommonFilters"))
		Ω(string(bs)).Should(ContainSubstring("allBatches"))
	})
})
