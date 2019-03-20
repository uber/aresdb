package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"time"

	mux "github.com/gorilla/mux"
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

	tables := []common.Table{
		{
			Version: 0,
			Name:    "test1",
			Columns: []common.Column{
				{
					Name: "col1",
					Type: "int32",
				},
			},
		},
	}

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
	})

	ginkgo.It("should fail with errors", func() {
		c := NewControllerHTTPClient(hostPort, 2*time.Second, headers)
		_, err := c.GetSchemaHash("bad_ns")
		Ω(err).ShouldNot(BeNil())
		tablesGot, err := c.GetAllSchema("bad_ns")
		Ω(err).ShouldNot(BeNil())
		Ω(tablesGot).Should(BeNil())

		_, err = c.GetAllSchema("ns_baddata")
		Ω(err).ShouldNot(BeNil())
	})
})
