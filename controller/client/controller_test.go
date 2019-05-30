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
package client

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/gorilla/mux"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber/aresdb/metastore/common"
)

var _ = ginkgo.Describe("Controller", func() {
	var testServer *httptest.Server
	var hostPort string

	headers := http.Header{
		"Foo": []string{"bar"},
	}

	table := common.Table{
		Version: 0,
		Name:    "test1",
		Columns: []common.Column{
			{
				Name: "col1",
				Type: "int32",
			},
		},
	}
	table1 := common.Table{
		Version: 0,
		Name:    "test2",
		Columns: []common.Column{
			{
				Name: "col1",
				Type: "bool",
			},
		},
	}
	tableBytes, _ := json.Marshal(table)
	tableBytes1, _ := json.Marshal(table1)
	tables := []common.Table{
		table,
		table1,
	}

	column2EnumCases := []string{"1"}
	enumCasesBytes, _ := json.Marshal(column2EnumCases)
	column2extendedEnumIDs := []int{2}
	enumIDBytes, _ := json.Marshal(column2extendedEnumIDs)

	ginkgo.BeforeEach(func() {
		testRouter := mux.NewRouter()
		testServer = httptest.NewUnstartedServer(testRouter)
		testRouter.HandleFunc("/schema/ns1/tables", func(w http.ResponseWriter, r *http.Request) {
			b, _ := json.Marshal(tables)
			w.Write(b)
		})
		testRouter.HandleFunc("/schema/ns_baddata/tables", func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(`"bad data`))
		})
		testRouter.HandleFunc("/schema/ns1/hash", func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte("123"))
		})
		testRouter.HandleFunc("/assignment/ns1/hash/0", func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte("123"))
		})
		testRouter.HandleFunc("/assignment/ns1/assignments/0", func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(`
{  
   "subscriber":"0",
   "jobs":[  
      {  
         "job":"client_info_test_1",
         "version":1,
         "aresTableConfig":{  
            "name":"client_info_test_1",
            "cluster":"",
            "schema":{  
               "name":"",
               "columns":null,
               "primaryKeyColumns":null,
               "isFactTable":false,
               "config":{  

               },
               "version":0
            }
         },
         "streamConfig":{  
            "topic":"hp-styx-rta-client_info",
            "kafkaClusterName":"kloak-sjc1-lossless",
            "kafkaClusterFile":"clusters.yaml",
            "topicType":"heatpipe",
            "lastestOffset":true,
            "errorThreshold":10,
            "statusCheckInterval":60,
            "autoRecoveryThreshold":8,
            "processorCount":1,
            "batchSize":32768,
            "maxBatchDelayMS":10000,
            "megaBytePerSec":600,
            "restartOnFailure":true,
            "restartInterval":300,
            "failureHandler":{  
               "type":"retry",
               "config":{  
                  "initRetryIntervalInSeconds":60,
                  "multiplier":1,
                  "maxRetryMinutes":525600
               }
            }
         }
      }
   ]
}`))
		})
		testRouter.HandleFunc("/schema/ns1/tables/test1", func(w http.ResponseWriter, r *http.Request) {
			w.Write(tableBytes)
		})
		testRouter.HandleFunc("/schema/ns1/tables/test2", func(w http.ResponseWriter, r *http.Request) {
			w.Write(tableBytes1)
		})
		testRouter.HandleFunc("/schema/ns1/tables/test1/columns/col2/enum-cases", func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet {
				w.Write(enumCasesBytes)
			} else if r.Method == http.MethodPost {
				w.Write(enumIDBytes)
			}
		})
		testRouter.HandleFunc("/schema/ns_baddata/tables/test1", func(w http.ResponseWriter, r *http.Request) {
			w.Write(enumCasesBytes)
		})
		testRouter.HandleFunc("/schema/ns_baddata/tables/test1/columns/col2/enum-cases", func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet {
				w.Write(enumIDBytes)
			} else if r.Method == http.MethodPost {
				w.Write(enumCasesBytes)
			}
		})
		testServer.Start()
		hostPort = testServer.Listener.Addr().String()
	})

	ginkgo.AfterEach(func() {
		testServer.Close()
	})

	ginkgo.It("NewControllerHTTPClient should work", func() {
		c := NewControllerHTTPClient(hostPort, 20*time.Second, headers)
		Ω(c.address).Should(Equal(hostPort))
		Ω(c.headers).Should(Equal(headers))

		hash, err := c.GetSchemaHash("ns1")
		Ω(err).Should(BeNil())
		Ω(hash).Should(Equal("123"))

		tablesGot, err := c.GetAllSchema("ns1")
		Ω(err).Should(BeNil())
		Ω(tablesGot).Should(Equal(tables))

		hash, err = c.GetAssignmentHash("ns1", "0")
		Ω(err).Should(BeNil())
		Ω(hash).Should(Equal("123"))

		_, err = c.GetAssignment("ns1", "0")
		Ω(err).Should(BeNil())

		c.SetNamespace("ns1")
		tableAddressesGot, err := c.FetchAllSchemas()
		Ω(err).Should(BeNil())
		Ω(*tableAddressesGot[0]).Should(Equal(table))
		Ω(*tableAddressesGot[1]).Should(Equal(table1))

		tableGot, err := c.FetchSchema("test1")
		Ω(err).Should(BeNil())
		Ω(*tableGot).Should(Equal(table))

		enumCasesGot, err := c.FetchAllEnums("test1", "col2")
		Ω(err).Should(BeNil())
		Ω(enumCasesGot).Should(Equal(column2EnumCases))

		column2extendedEnumIDsGot, err := c.ExtendEnumCases("test1", "col2", []string{"2"})
		Ω(err).Should(BeNil())
		Ω(column2extendedEnumIDsGot).Should(Equal(column2extendedEnumIDs))
	})

	ginkgo.It("should fail with errors", func() {
		c := NewControllerHTTPClient(hostPort, 2*time.Second, headers)
		_, err := c.GetSchemaHash("bad_ns")
		Ω(err).ShouldNot(BeNil())
		tablesGot, err := c.GetAllSchema("bad_ns")
		Ω(err).ShouldNot(BeNil())
		Ω(tablesGot).Should(BeNil())
		c.SetNamespace("bad_ns")
		_, err = c.FetchAllSchemas()
		Ω(err).ShouldNot(BeNil())
		_, err = c.FetchAllEnums("test1", "col1")
		Ω(err).ShouldNot(BeNil())
		_, err = c.FetchAllEnums("test1", "col2")
		Ω(err).ShouldNot(BeNil())
		_, err = c.ExtendEnumCases("test1", "col2", []string{"2"})
		Ω(err).ShouldNot(BeNil())

		_, err = c.GetAllSchema("ns_baddata")
		Ω(err).ShouldNot(BeNil())
		c.SetNamespace("ns_baddata")
		_, err = c.FetchAllSchemas()
		Ω(err).ShouldNot(BeNil())
		_, err = c.FetchAllEnums("test1", "col1")
		Ω(err).ShouldNot(BeNil())
		_, err = c.FetchAllEnums("test1", "col2")
		Ω(err).ShouldNot(BeNil())
		_, err = c.ExtendEnumCases("test1", "col2", []string{"2"})
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("buildRequest should work", func() {
		c := NewControllerHTTPClient(hostPort, 20*time.Second, headers)
		headerLen := len(c.headers)
		req, err := c.buildRequest(http.MethodGet, "somepath", nil)
		Ω(err).Should(BeNil())
		Ω(req.Header).Should(HaveLen(2))
		Ω(c.headers).Should(HaveLen(headerLen))
	})
})
