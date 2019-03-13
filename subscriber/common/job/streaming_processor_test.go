package job

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/client"
	"github.com/uber/aresdb/client/mocks"
	memCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/subscriber/common/consumer"
	"github.com/uber/aresdb/subscriber/common/database"
	"github.com/uber/aresdb/subscriber/common/message"
	"github.com/uber/aresdb/subscriber/common/rules"
	"github.com/uber/aresdb/subscriber/common/tools"
	"github.com/uber/aresdb/subscriber/config"
	"github.com/uber/aresdb/utils"
	"go.uber.org/zap"
	"os"
	"time"
	"net/http/httptest"
	metaCom "github.com/uber/aresdb/metastore/common"
	"regexp"
	"net/http"
	"strings"
	"encoding/json"
	"io/ioutil"
)

var _ = Describe("streaming_processor", func() {
	serviceConfig := config.ServiceConfig{
		Environment: utils.EnvironmentContext{
			Deployment:         "test",
			RuntimeEnvironment: "test",
			Zone:               "local",
		},
		Logger: zap.NewNop(),
		Scope:  tally.NoopScope,
	}
	serviceConfig.ActiveJobs = []string{"dispatch_driver_rejected"}
	serviceConfig.ActiveAresClusters = map[string]client.ConnectorConfig{
		"dev01": client.ConnectorConfig{Address: "localhost:8888"},
	}

	rootPath := tools.GetModulePath("")
	os.Chdir(rootPath)
	jobConfigs := make(rules.JobConfigs)
	err := rules.AddLocalJobConfig(serviceConfig, jobConfigs)
	if err != nil {
		panic("Failed to AddLocalJobConfig")
	}
	if jobConfigs["dispatch_driver_rejected"]["dev01"] == nil {
		panic("Failed to get (jobConfigs[\"dispatch_driver_rejected\"][\"dev01\"]")
	} else {
		jobConfigs["dispatch_driver_rejected"]["dev01"].AresTableConfig.Cluster = "dev01"
	}
	jobConfig := jobConfigs["dispatch_driver_rejected"]["dev01"]

	mockConnector := mocks.Connector{}
	table := "test"
	columnNames := []string{"c1", "c2", "c3"}
	pk := map[string]interface{}{"c1": nil}
	modes := []memCom.ColumnUpdateMode{
		memCom.UpdateOverwriteNotNull,
		memCom.UpdateOverwriteNotNull,
		memCom.UpdateOverwriteNotNull,
	}
	destination := database.Destination{
		Table:           table,
		ColumnNames:     columnNames,
		PrimaryKeys:     pk,
		AresUpdateModes: modes,
	}
	rows := []client.Row{
		{"v11", "v12", "v13"},
		{"v21", "v22", "v23"},
		{"v31", "v32", "v33"},
	}
	
	aresDB := &database.AresDatabase{
		ServiceConfig: serviceConfig,
		Scope:         tally.NoopScope,
		ClusterName:   "dev01",
		Connector:     &mockConnector,
		JobName:       "dispatch_driver_rejected",
	}

	//hlConsumer, _ := consumer.NewKafkaConsumer(jobConfigs["dispatch_driver_rejected"]["dev01"], serviceConfig)
	//decoder, _ := message.NewDefaultDecoder(jobConfig, serviceConfig)
	//failureHandler := initFailureHandler(serviceConfig, jobConfig, aresDB)

	topic := "topic"
	msg := &consumer.KafkaMessage{
		&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: int32(0),
				Offset:    0,
			},
			Value: []byte(`{"project": "ares-subscriber"}`),
			Key:   []byte("key"),
		},
		nil,
		"kloak-sjc1-agg1",
	}

	errMsg := &consumer.KafkaMessage{
		&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: int32(0),
				Offset:    0,
			},
			Value: []byte(`{project: ares-subscriber}`),
			Key:   []byte("key"),
		},
		nil,
		"kloak-sjc1-agg1",
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

	var insertBytes []byte
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
					insertBytes, err = ioutil.ReadAll(r.Body)
					if err != nil {
						w.WriteHeader(http.StatusInternalServerError)
					} else {
						w.WriteHeader(http.StatusOK)
					}
				}
			}))
		testServer.Start()
		address = testServer.Listener.Addr().String()
	})

	AfterEach(func() {
		testServer.Close()
	})
	It("NewStreamingProcessor", func() {
		p, err := NewStreamingProcessor(1, jobConfig, consumer.NewKafkaConsumer, message.NewDefaultDecoder,
			make(chan ProcessorError), make(chan int64), serviceConfig)
		Ω(p).Should(BeNil())
		Ω(err).ShouldNot(BeNil())

		serviceConfig.ActiveAresClusters = map[string]client.ConnectorConfig{
			"dev01": client.ConnectorConfig{Address: address},
		}
		p, err = NewStreamingProcessor(1, jobConfig, consumer.NewKafkaConsumer, message.NewDefaultDecoder,
			make(chan ProcessorError), make(chan int64), serviceConfig)
		Ω(p).ShouldNot(BeNil())
		Ω(p.(*StreamingProcessor).highLevelConsumer).ShouldNot(BeNil())
		Ω(p.(*StreamingProcessor).database).ShouldNot(BeNil())
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
		p.(*StreamingProcessor).database = aresDB
		mockConnector.On("Insert",
			table, columnNames, rows).
			Return(6, nil)
		batch := []interface{}{
			&message.Message{
				MsgInSubTS:    time.Now(),
				MsgMetaDataTS: time.Now(),
				DecodedMessage: map[string]interface{}{
					"msg": map[string]interface{}{
						"c1": "v11",
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
						"c1": "v21",
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
						"c1": "v31",
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
		p.(*StreamingProcessor).highLevelConsumer.(*consumer.KafkaConsumer).Close()

		p.(*StreamingProcessor).reInitialize()
		go p.Run()
		p.Stop()
	})
	It("HandleFailure", func() {
		failureHandler := initFailureHandler(serviceConfig, jobConfig, aresDB)
		failureHandler.(*RetryFailureHandler).interval = 1
		failureHandler.(*RetryFailureHandler).maxElapsedTime = 2 * time.Microsecond
		failureHandler.HandleFailure(destination, rows)
	})
})
