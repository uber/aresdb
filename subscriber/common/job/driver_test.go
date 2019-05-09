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
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/client"
	"github.com/uber/aresdb/subscriber/common/consumer/kafka"
	"github.com/uber/aresdb/subscriber/common/sink"

	"encoding/json"
	"fmt"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/subscriber/common/message"
	"github.com/uber/aresdb/subscriber/common/rules"
	"github.com/uber/aresdb/subscriber/common/tools"
	"github.com/uber/aresdb/subscriber/config"
	"github.com/uber/aresdb/utils"
	"go.uber.org/zap"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"time"
)

var _ = Describe("driver", func() {
	serviceConfig := config.ServiceConfig{
		Environment: utils.EnvironmentContext{
			Deployment:         "test",
			RuntimeEnvironment: "test",
			Zone:               "local",
		},
		Logger: zap.NewNop(),
		Scope:  tally.NoopScope,
	}
	serviceConfig.ActiveJobs = []string{"job1"}
	sinkConfig := config.SinkConfig{
		SinkModeStr:           "aresDB",
		AresDBConnectorConfig: client.ConnectorConfig{Address: "localhost:8888"},
	}
	serviceConfig.ActiveAresClusters = map[string]config.SinkConfig{
		"dev01": sinkConfig,
	}

	rootPath := tools.GetModulePath("")
	os.Chdir(rootPath)
	jobConfigs := make(rules.JobConfigs)
	err := rules.AddLocalJobConfig(serviceConfig, jobConfigs)
	if err != nil {
		panic("Failed to AddLocalJobConfig")
	}
	if jobConfigs["job1"]["dev01"] == nil {
		panic("Failed to get (jobConfigs[\"job1\"][\"dev01\"]")
	} else {
		jobConfigs["job1"]["dev01"].AresTableConfig.Cluster = "dev01"
	}
	jobConfig := jobConfigs["job1"]["dev01"]

	var address string
	var testServer *httptest.Server
	testTableNames := []string{"a"}
	re := regexp.MustCompile("/schema/tables/a/columns/(.+)/enum-cases")
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
			"dev01": sinkConfig,
		}
	})

	AfterEach(func() {
		testServer.Close()
	})

	It("NewDriver", func() {
		driver, err := NewDriver(jobConfig, serviceConfig, nil, NewStreamingProcessor, sink.NewAresDatabase,
			kafka.NewKafkaConsumer, message.NewDefaultDecoder)
		Ω(driver).ShouldNot(BeNil())
		Ω(err).Should(BeNil())

		driver.addProcessors(2)
		Ω(len(driver.processors)).Should(Equal(2))

		_, err = driver.MarshalJSON()
		Ω(err).Should(BeNil())

		errors := driver.GetErrors()
		Ω(errors).ShouldNot(BeNil())

		driver.restartProcessor(1)

		ok := driver.RemoveProcessor(0)
		Ω(ok).Should(Equal(true))

		ok = driver.RemoveProcessor(1)
		Ω(ok).Should(Equal(true))

		ok = driver.RemoveProcessor(0)
		Ω(ok).Should(Equal(false))

		driver.addProcessors(1)

		t := time.NewTicker(time.Second)
		go driver.monitorStatus(t)
		go driver.monitorErrors()
		go driver.limitRate()

		t.Stop()
		driver.errors <- ProcessorError{
			1,
			int64(12345),
			fmt.Errorf("this is error"),
		}

		close(driver.processorMsgSizes)
		driver.Stop()
	})
})
