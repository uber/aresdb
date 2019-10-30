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

package job

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"

	"github.com/golang/mock/gomock"
	"github.com/gorilla/mux"
	"github.com/m3db/m3/src/cluster/client/etcd"
	"github.com/m3db/m3/src/cluster/services"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/client"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/subscriber/common/consumer/kafka"
	"github.com/uber/aresdb/subscriber/common/message"
	"github.com/uber/aresdb/subscriber/common/rules"
	"github.com/uber/aresdb/subscriber/common/sink"
	"github.com/uber/aresdb/subscriber/config"
	"github.com/uber/aresdb/utils"
	"go.uber.org/zap"
)

var _ = Describe("controller", func() {
	serviceConfig := config.ServiceConfig{
		Environment: utils.EnvironmentContext{
			Deployment:         "test",
			RuntimeEnvironment: "test",
			Zone:               "local",
			InstanceID:         "0",
		},
		Logger:           zap.NewNop(),
		Scope:            tally.NoopScope,
		ControllerConfig: &config.ControllerConfig{},
	}
	serviceConfig.ActiveJobs = []string{"job1"}
	sinkConfig := config.SinkConfig{
		SinkModeStr:           "aresDB",
		AresDBConnectorConfig: client.ConnectorConfig{Address: "localhost:8888"},
	}
	serviceConfig.ActiveAresClusters = map[string]config.SinkConfig{
		"dev-ares01": sinkConfig,
	}
	config.ActiveAresNameSpace = "dev01"

	var testServer *httptest.Server
	var address string

	tables := []metaCom.Table{
		{
			Version: 0,
			Name:    "test1",
			Columns: []metaCom.Column{
				{
					Name: "col1",
					Type: "int32",
				},
			},
		},
	}

	testTableNames := []string{"a"}
	re := regexp.MustCompile("/schema/job1/tables/a/columns/(.+)/enum-cases")
	testTables := map[string]metaCom.Table{
		"a": {
			Name: "a",
			Columns: []metaCom.Column{
				{
					Name: "col0",
					Type: metaCom.Int32,
				},
				{
					Name: "col1",
					Type: metaCom.Int32,
				},
				{
					Name: "col1_hll",
					Type: metaCom.UUID,
					HLLConfig: metaCom.HLLConfig{
						IsHLLColumn: true,
					},
				},
				{
					Name: "col2",
					Type: metaCom.BigEnum,
				},
				{
					Name: "col3",
					Type: metaCom.Bool,
				},
				{
					Name:              "col4",
					Type:              metaCom.BigEnum,
					DisableAutoExpand: true,
					CaseInsensitive:   true,
				},
				{
					Name:              "col5",
					Type:              metaCom.BigEnum,
					DisableAutoExpand: true,
					CaseInsensitive:   true,
				},
			},
			PrimaryKeyColumns: []int{1},
			IsFactTable:       true,
		},
	}

	// this is the enum cases at first
	initialColumn2EnumCases := map[string][]string{
		"col2": {"1"},
		"col4": {"a"},
		"col5": {"A"},
	}

	// extendedEnumIDs
	column2extendedEnumIDs := []int{2}

	// set up aresDB server
	BeforeEach(func() {
		testServer = httptest.NewUnstartedServer(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "tables") && r.Method == http.MethodGet {
					tableListBytes, _ := json.Marshal(testTableNames)
					w.WriteHeader(http.StatusOK)
					w.Write(tableListBytes)
				} else if strings.HasSuffix(r.URL.Path, "tables/a") && r.Method == http.MethodGet {
					tableBytes, _ := json.Marshal(testTables["a"])
					w.WriteHeader(http.StatusOK)
					w.Write(tableBytes)
				} else if strings.HasSuffix(r.URL.Path, "enum-cases") {
					if r.Method == http.MethodGet {
						column := string(re.FindSubmatch([]byte(r.URL.Path))[1])
						var enumBytes []byte
						if enumCases, ok := initialColumn2EnumCases[column]; ok {
							enumBytes, _ = json.Marshal(enumCases)
						}

						w.WriteHeader(http.StatusOK)
						w.Write(enumBytes)
					} else if r.Method == http.MethodPost {
						enumIDBytes, _ := json.Marshal(column2extendedEnumIDs)
						w.WriteHeader(http.StatusOK)
						w.Write(enumIDBytes)
					}
				} else if strings.Contains(r.URL.Path, "data") && r.Method == http.MethodPost {
					var err error
					_, err = ioutil.ReadAll(r.Body)
					if err != nil {
						w.WriteHeader(http.StatusInternalServerError)
					} else {
						w.WriteHeader(http.StatusOK)
					}
				}
			}))
		testServer.Start()
		address = testServer.Listener.Addr().String()

		sinkConfig := config.SinkConfig{
			SinkModeStr:           "aresDB",
			AresDBConnectorConfig: client.ConnectorConfig{Address: address},
		}
		serviceConfig.ActiveAresClusters = map[string]config.SinkConfig{
			"dev-ares01": sinkConfig,
		}
	})

	AfterEach(func() {
		testServer.Close()
	})

	// set up controller server
	BeforeEach(func() {
		testRouter := mux.NewRouter()
		testServer = httptest.NewUnstartedServer(testRouter)
		testRouter.HandleFunc("/schema/dev01/tables", func(w http.ResponseWriter, r *http.Request) {
			b, _ := json.Marshal(tables)
			w.Write(b)
		})
		testRouter.HandleFunc("/schema/dev01/hash", func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte("12345"))
		})
		testRouter.HandleFunc("/assignment/dev01/hash/0", func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte("12345"))
		})
		testRouter.HandleFunc("/assignment/dev01/assignments/0", func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(`
{  
   "subscriber":"0",
   "instances":{
      "dev-ares02":{
         "sinkMode":"aresDB",
         "aresDB":{
           
         }
      }
   },
   "jobs":[  
      {  
         "job":"job1",
         "version":1,
         "numShards":8,
         "aresTableConfig":{  
            "name":"job1",
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
                  "maxRetryMinutes":15
               }
            }
         }
      }
   ]
}`))
		})
		testServer.Start()
		address = testServer.Listener.Addr().String()
		serviceConfig.ControllerConfig.Address = address
	})

	AfterEach(func() {
		testServer.Close()
	})

	It("NewController without zk and etcd", func() {
		paramsR := rules.Params{
			ServiceConfig: serviceConfig}

		rst, _ := rules.NewJobConfigs(paramsR)
		params := Params{
			ServiceConfig:    serviceConfig,
			JobConfigs:       rst.JobConfigs,
			SinkInitFunc:     sink.NewAresDatabase,
			ConsumerInitFunc: kafka.NewKafkaConsumer,
			DecoderInitFunc:  message.NewDefaultDecoder,
		}
		controller := NewController(params)
		Ω(controller).ShouldNot(BeNil())
		Ω(controller.Drivers["job1"]).ShouldNot(BeNil())
		Ω(controller.Drivers["job1"]["dev-ares01"]).ShouldNot(BeNil())
		controller.deleteDriver(controller.Drivers["job1"]["dev-ares01"],
			"dev-ares01", controller.Drivers["job1"])
		Ω(controller.Drivers["job1"]["dev-ares01"]).Should(BeNil())
		ok := controller.addDriver(params.JobConfigs["job1"]["dev-ares01"], "dev-ares01",
			controller.Drivers["job1"], true)
		controller.serviceConfig.ActiveAresClusters["dev-ares01"] = sinkConfig
		Ω(ok).Should(Equal(true))

		controller.jobNS = "dev01"
		update, newHash := controller.updateAssignmentHash()
		Ω(update).Should(Equal(true))
		Ω(newHash).Should(Equal("12345"))

		controller.SyncUpJobConfigs()
		Ω(controller.Drivers["job1"]).ShouldNot(BeNil())
		Ω(controller.Drivers["job1"]["dev-ares01"]).Should(BeNil())

	})

	It("connectEtcdServices", func() {
		paramsR := rules.Params{
			ServiceConfig: serviceConfig}

		rst, _ := rules.NewJobConfigs(paramsR)
		params := Params{
			ServiceConfig:    serviceConfig,
			JobConfigs:       rst.JobConfigs,
			SinkInitFunc:     sink.NewAresDatabase,
			ConsumerInitFunc: kafka.NewKafkaConsumer,
			DecoderInitFunc:  message.NewDefaultDecoder,
		}

		params.ServiceConfig.EtcdConfig.EtcdConfig = etcd.Configuration{
			Zone:    "local",
			Env:     "test",
			Service: "ares-subscriber",
			ETCDClusters: []etcd.ClusterConfig{
				{
					Zone: "local",
					Endpoints: []string{
						"i1",
					},
				},
			},
		}
		config.ActiveJobNameSpace = "test"

		etcdServices, err := connectEtcdServices(params)
		Ω(etcdServices).ShouldNot(BeNil())
		Ω(err).Should(BeNil())
	})

	It("registerHeartBeatService", func() {
		paramsR := rules.Params{
			ServiceConfig: serviceConfig}

		rst, _ := rules.NewJobConfigs(paramsR)
		params := Params{
			ServiceConfig:    serviceConfig,
			JobConfigs:       rst.JobConfigs,
			SinkInitFunc:     sink.NewAresDatabase,
			ConsumerInitFunc: kafka.NewKafkaConsumer,
			DecoderInitFunc:  message.NewDefaultDecoder,
		}

		params.ServiceConfig.EtcdConfig.EtcdConfig = etcd.Configuration{
			Zone:    "local",
			Env:     "test",
			Service: "ares-subscriber",
			ETCDClusters: []etcd.ClusterConfig{
				{
					Zone: "local",
					Endpoints: []string{
						"i1",
					},
				},
			},
		}
		params.ServiceConfig.EtcdConfig.Mutex = &sync.Mutex{}
		config.ActiveJobNameSpace = "test"

		params.ServiceConfig.HeartbeatConfig = &config.HeartBeatConfig{
			Enabled:       true,
			Timeout:       30,
			Interval:      10,
			CheckInterval: 2,
		}

		ctrl := gomock.NewController(GinkgoT())
		defer ctrl.Finish()

		mockServices := services.NewMockServices(ctrl)
		mockServices.EXPECT().SetMetadata(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		mockServices.EXPECT().Advertise(gomock.Any()).Return(nil).AnyTimes()
		mockServices.EXPECT().Unadvertise(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		controller := &Controller{
			serviceConfig: serviceConfig,
			etcdServices: mockServices,
			isTest: true,
		}
		config.EtcdCfgEvent <- 1
		err := registerHeartBeatService(controller, params)
		Ω(err).Should(BeNil())
		Ω(controller.etcdServiceId).ShouldNot(BeNil())
		Ω(controller.etcdPlacementInstance).ShouldNot(BeNil())
		Ω(controller.etcdServices).ShouldNot(BeNil())
		controller.RestartEtcdHBService(params)
	})
})
