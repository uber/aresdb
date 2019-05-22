package sink

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/gorilla/mux"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/client"
	controllerCom "github.com/uber/aresdb/controller/common"
	memCom "github.com/uber/aresdb/memstore/common"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/subscriber/common/rules"
	"github.com/uber/aresdb/subscriber/config"
	"go.uber.org/zap"
	"net/http"
	"net/http/httptest"
	"os"
	"time"
)

var _ = Describe("Kafka producer", func() {
	var testServer *httptest.Server
	var seedBroker *sarama.MockBroker
	var leader *sarama.MockBroker
	var aresControllerClient controllerCom.ControllerClient

	rows := []client.Row{
		{"1", "v12", "true"},
		{"2", "v22", "true"},
		{"3", "v32", "false"},
	}
	cluster := "ns1"
	table := "test"
	columnNames := []string{"c1", "c2", "c3"}
	pk := map[string]int{"c1": 0}
	pkInSchema := map[string]int{"c1": 1}
	modes := []memCom.ColumnUpdateMode{
		memCom.UpdateOverwriteNotNull,
		memCom.UpdateOverwriteNotNull,
		memCom.UpdateOverwriteNotNull,
	}
	destination := Destination{
		Table:               table,
		ColumnNames:         columnNames,
		PrimaryKeys:         pk,
		PrimaryKeysInSchema: pkInSchema,
		AresUpdateModes:     modes,
	}
	serviceConfig := config.ServiceConfig{
		Logger:           zap.NewNop(),
		Scope:            tally.NoopScope,
		ControllerConfig: &config.ControllerConfig{},
	}

	jobConfig := rules.JobConfig{
		AresTableConfig: rules.AresTableConfig{
			Table: metaCom.Table{
				Name:        "test",
				IsFactTable: true,
				Columns: []metaCom.Column{
					{
						Name: "c2",
						Type: "string",
					},
					{
						Name: "c1",
						Type: "Int8",
					},
					{
						Name: "c3",
						Type: "Bool",
					},
				},
				Config: metaCom.TableConfig{
					BatchSize: 10,
				},
				PrimaryKeyColumns: []int{1},
			},
		},
	}

	tableBytes, _ := json.Marshal(jobConfig.AresTableConfig.Table)
	tables := []metaCom.Table{
		jobConfig.AresTableConfig.Table,
	}

	column2EnumCases := []string{"v12", "v22"}
	enumCasesBytes, _ := json.Marshal(column2EnumCases)
	column2extendedEnumIDs := []int{3}
	enumIDBytes, _ := json.Marshal(column2extendedEnumIDs)

	kafkaConfig := config.KafkaProducerConfig{
		RetryMax:     0,
		TimeoutInSec: 1,
	}
	sinkCfg := config.SinkConfig{
		SinkModeStr:         "kafka",
		KafkaProducerConfig: kafkaConfig,
	}
	topic := fmt.Sprintf("%s-%s", cluster, table)

	BeforeEach(func() {
		// ares controller http server setup
		testRouter := mux.NewRouter()
		testServer = httptest.NewUnstartedServer(testRouter)
		testRouter.HandleFunc("/schema/ns1/tables", func(w http.ResponseWriter, r *http.Request) {
			b, _ := json.Marshal(tables)
			w.Write(b)
		})

		testRouter.HandleFunc("/schema/ns1/tables/test1", func(w http.ResponseWriter, r *http.Request) {
			w.Write(tableBytes)
		})
		testRouter.HandleFunc("/enum/ns1/test1/columns/c2/enum-cases", func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet {
				w.Write(enumCasesBytes)
			} else if r.Method == http.MethodPost {
				w.Write(enumIDBytes)
			}
		})
		testServer.Start()
		serviceConfig.ControllerConfig.Address = testServer.Listener.Addr().String()
		aresControllerClient = controllerCom.NewControllerHTTPClient(serviceConfig.ControllerConfig.Address,
			time.Duration(serviceConfig.ControllerConfig.Timeout)*time.Second,
			http.Header{
				"RPC-Caller":  []string{os.Getenv("UDEPLOY_APP_ID")},
				"RPC-Service": []string{serviceConfig.ControllerConfig.ServiceName},
			})
		aresControllerClient.(*controllerCom.ControllerHTTPClient).SetNamespace(cluster)

		// kafka broker mock setup
		seedBroker = sarama.NewMockBroker(serviceConfig.Logger.Sugar(), 1)
		leader = sarama.NewMockBroker(serviceConfig.Logger.Sugar(), 2)
		sinkCfg.KafkaProducerConfig.Brokers = seedBroker.Addr()

		metadataResponse := new(sarama.MetadataResponse)
		metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
		metadataResponse.AddTopicPartition(topic, 0, leader.BrokerID(), nil, nil, nil, sarama.ErrNoError)
		metadataResponse.AddTopicPartition(topic, 1, leader.BrokerID(), nil, nil, nil, sarama.ErrNoError)
		metadataResponse.AddTopicPartition(topic, 2, leader.BrokerID(), nil, nil, nil, sarama.ErrNoError)
		seedBroker.Returns(metadataResponse)

		prodSuccess := new(sarama.ProduceResponse)
		prodSuccess.AddTopicPartition(topic, 0, sarama.ErrNoError)
		prodSuccess.AddTopicPartition(topic, 1, sarama.ErrNoError)
		prodSuccess.AddTopicPartition(topic, 2, sarama.ErrNoError)
		for i := 0; i < 4; i++ {
			leader.Returns(prodSuccess)
		}
	})

	AfterEach(func() {
		testServer.Close()
		leader.Close()
		seedBroker.Close()
	})

	It("NewKafkaPublisher", func() {
		jobConfig.SetPrimaryKeyBytes(1)
		publisher, err := NewKafkaPublisher(serviceConfig, &jobConfig, cluster, sinkCfg, aresControllerClient)
		Ω(err).Should(BeNil())
		Ω(publisher).ShouldNot(BeNil())

		clusterGot := publisher.Cluster()
		Ω(clusterGot).Should(Equal(cluster))

		destination.NumShards = 0
		err = publisher.Save(destination, rows)
		Ω(err).Should(BeNil())

		destination.NumShards = 3
		err = publisher.Save(destination, rows)
		Ω(err).Should(BeNil())

		publisher.Shutdown()
	})
})
