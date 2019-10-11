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
	"github.com/Shopify/sarama"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/client"
	"github.com/uber/aresdb/client/mocks"
	memCom "github.com/uber/aresdb/memstore/common"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/subscriber/common/consumer/kafka"
	"github.com/uber/aresdb/subscriber/common/message"
	"github.com/uber/aresdb/subscriber/common/rules"
	"github.com/uber/aresdb/subscriber/common/sink"
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

var _ = Describe("streaming_processor", func() {
	var broker *sarama.MockBroker
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

	mockConnector := mocks.Connector{}
	table := "test"
	columnNames := []string{"c1", "c2", "c3"}
	pk := map[string]int{"c1": 0}
	modes := []memCom.ColumnUpdateMode{
		memCom.UpdateOverwriteNotNull,
		memCom.UpdateOverwriteNotNull,
		memCom.UpdateOverwriteNotNull,
	}
	destination := sink.Destination{
		Table:           table,
		ColumnNames:     columnNames,
		PrimaryKeys:     pk,
		AresUpdateModes: modes,
	}
	rows := []client.Row{
		{"1570489452", "v12", "v13"},
		{"1570489552", "v22", "v23"},
		{"1570489652", "v32", "v33"},
	}

	aresDB := &sink.AresDatabase{
		ServiceConfig: serviceConfig,
		Scope:         tally.NoopScope,
		ClusterName:   "dev01",
		Connector:     &mockConnector,
		JobConfig:     jobConfig,
	}

	msg := &kafka.KafkaMessage{
		ConsumerMessage: &sarama.ConsumerMessage{
			Topic:     "topic",
			Partition: 0,
			Offset:    0,
			Value:     []byte(`{"project": "ares-subscriber"}`),
			Key:       []byte("key"),
		},
	}

	errMsg := &kafka.KafkaMessage{
		ConsumerMessage: &sarama.ConsumerMessage{
			Topic:     "topic",
			Partition: 0,
			Offset:    0,
			Value:     []byte(`{project: ares-subscriber}`),
			Key:       []byte("key"),
		},
	}

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
					Type: metaCom.Uint32,
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

		// kafka broker mock setup
		broker = sarama.NewMockBrokerAddr(serviceConfig.Logger.Sugar(), 1, jobConfigs["job1"]["dev01"].StreamingConfig.KafkaBroker)
		mockFetchResponse := sarama.NewMockFetchResponse(serviceConfig.Logger.Sugar(), 1)
		for i := 0; i < 10; i++ {
			mockFetchResponse.SetMessage("job1-topic", 0, int64(i+1234), sarama.StringEncoder("foo"))
		}

		broker.SetHandlerByMap(map[string]sarama.MockResponse{
			"MetadataRequest": sarama.NewMockMetadataResponse(serviceConfig.Logger.Sugar()).
				SetBroker(broker.Addr(), broker.BrokerID()).
				SetLeader("job1-topic", 0, broker.BrokerID()),
			"OffsetRequest": sarama.NewMockOffsetResponse(serviceConfig.Logger.Sugar()).
				SetOffset("job1-topic", 0, sarama.OffsetOldest, 0).
				SetOffset("job1-topic", 0, sarama.OffsetNewest, 2345),
			"FetchRequest": mockFetchResponse,
			"JoinGroupRequest": sarama.NewMockConsumerMetadataResponse(serviceConfig.Logger.Sugar()).
				SetCoordinator("ares-subscriber_test_job1_dev01_streaming", broker),
			"OffsetCommitRequest": sarama.NewMockOffsetCommitResponse(serviceConfig.Logger.Sugar()),
		})
	})

	AfterEach(func() {
		testServer.Close()
		broker.Close()
	})
	It("NewStreamingProcessor", func() {
		p, err := NewStreamingProcessor(1, jobConfig, nil, sink.NewAresDatabase, kafka.NewKafkaConsumer, message.NewDefaultDecoder,
			make(chan ProcessorError, 1), make(chan int64, 1), serviceConfig)
		Ω(p).ShouldNot(BeNil())
		Ω(err).Should(BeNil())

		sinkConfig := config.SinkConfig{
			SinkModeStr:           "aresDB",
			AresDBConnectorConfig: client.ConnectorConfig{Address: address},
		}
		serviceConfig.ActiveAresClusters = map[string]config.SinkConfig{
			"dev01": sinkConfig,
		}
		p, err = NewStreamingProcessor(1, jobConfig, nil, sink.NewAresDatabase, kafka.NewKafkaConsumer, message.NewDefaultDecoder,
			make(chan ProcessorError), make(chan int64), serviceConfig)
		Ω(p).ShouldNot(BeNil())
		Ω(p.(*StreamingProcessor).highLevelConsumer).ShouldNot(BeNil())
		Ω(p.(*StreamingProcessor).sink).ShouldNot(BeNil())
		Ω(p.(*StreamingProcessor).context).ShouldNot(BeNil())
		Ω(p.GetID()).Should(Equal(1))
		Ω(p.GetContext()).ShouldNot(BeNil())
		Ω(p.(*StreamingProcessor).batcher).ShouldNot(BeNil())
		Ω(p.(*StreamingProcessor).parser).ShouldNot(BeNil())
		Ω(err).Should(BeNil())

		_, err = p.(*StreamingProcessor).decodeMessage(msg)
		Ω(err).Should(BeNil())

		_, err = p.(*StreamingProcessor).decodeMessage(errMsg)
		Ω(err).ShouldNot(BeNil())

		p.(*StreamingProcessor).parser.Transformations = map[string]*rules.TransformationConfig{
			"c1": &rules.TransformationConfig{},
			"c2": &rules.TransformationConfig{},
			"c3": &rules.TransformationConfig{},
		}
		p.(*StreamingProcessor).sink = aresDB
		mockConnector.On("Insert",
			table, columnNames, rows).
			Return(6, nil)
		batch := []interface{}{
			&message.Message{
				MsgInSubTS:    time.Now(),
				MsgMetaDataTS: time.Now(),
				DecodedMessage: map[string]interface{}{
					"msg": map[string]interface{}{
						"c1": "1570489452",
						"c2": "v12",
						"c3": "v13",
					},
				},
			},
			&message.Message{
				MsgInSubTS:    time.Now(),
				MsgMetaDataTS: time.Now(),
				DecodedMessage: map[string]interface{}{
					"msg": map[string]interface{}{
						"c1": "1570489552",
						"c2": "v22",
						"c3": "v23",
					},
				},
			},
			&message.Message{
				MsgInSubTS:    time.Now(),
				MsgMetaDataTS: time.Now(),
				DecodedMessage: map[string]interface{}{
					"msg": map[string]interface{}{
						"c1": "1570489652",
						"c2": "v32",
						"c3": "v33",
					},
				},
			},
		}
		p.(*StreamingProcessor).saveToDestination(batch, destination)
		p.(*StreamingProcessor).reportMessageAge(&message.Message{
			MsgMetaDataTS: time.Now(),
			RawMessage:    msg,
		})

		go p.Run()
		p.Restart()
		p.(*StreamingProcessor).highLevelConsumer.(*kafka.KafkaConsumer).SetClosed(make(chan struct{}, 1))
		p.(*StreamingProcessor).highLevelConsumer.(*kafka.KafkaConsumer).Close()

		p.(*StreamingProcessor).reInitialize()
		go p.Run()
		p.(*StreamingProcessor).highLevelConsumer.(*kafka.KafkaConsumer).SetClosed(make(chan struct{}, 1))
		p.(*StreamingProcessor).highLevelConsumer.(*kafka.KafkaConsumer).Close()
		p.Stop()
	})
	It("HandleFailure", func() {
		mockConnector.On("Insert",
			table, columnNames, rows).
			Return(6, nil)
		failureHandler := initFailureHandler(serviceConfig, jobConfig, aresDB)
		failureHandler.(*RetryFailureHandler).interval = 1
		failureHandler.(*RetryFailureHandler).maxElapsedTime = 2 * time.Microsecond
		failureHandler.HandleFailure(destination, rows)
	})
})
