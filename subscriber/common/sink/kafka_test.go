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
	memCom "github.com/uber/aresdb/memstore/common"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/subscriber/common/rules"
	"github.com/uber/aresdb/subscriber/config"
	"go.uber.org/zap"
	"net/http"
	"net/http/httptest"
)

var _ = Describe("Kafka producer", func() {
	var testServer *httptest.Server
	var seedBroker *sarama.MockBroker
	var leader *sarama.MockBroker

	rows := []client.Row{
		{"1", "v12", "true"},
		{"2", "v22", "true"},
		{"3", "v32", "false"},
	}
	cluster := "test"
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
		Logger: zap.NewNop(),
		Scope:  tally.NoopScope,
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
						Type: "int",
					},
					{
						Name: "c3",
						Type: "bool",
					},
				},
				Config: metaCom.TableConfig{
					BatchSize: 10,
				},
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
		Brokers:      seedBroker.Addr(),
		RetryMax:     3,
		TimeoutInSec: 10,
	}
	topic := fmt.Sprint("%s-%s", table, cluster)

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

		// kafka broker mock setup
		seedBroker = sarama.NewMockBroker(serviceConfig.Logger.Sugar(), 1)
		leader = sarama.NewMockBroker(serviceConfig.Logger.Sugar(), 2)

		metadataResponse := new(sarama.MetadataResponse)
		metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
		metadataResponse.AddTopicPartition(topic, 0, leader.BrokerID(), nil, nil, nil, sarama.ErrNoError)
		seedBroker.Returns(metadataResponse)

		prodSuccess := new(sarama.ProduceResponse)
		prodSuccess.AddTopicPartition(topic, 0, sarama.ErrNoError)
		prodSuccess.AddTopicPartition(topic, 1, sarama.ErrNoError)
		prodSuccess.AddTopicPartition(topic, 2, sarama.ErrNoError)
	})

	AfterEach(func() {
		testServer.Close()
		leader.Close()
		seedBroker.Close()
	})

	It("NewKafkaPublisher", func() {
		publisher, err := NewKafkaPublisher(&jobConfig, serviceConfig, cluster, kafkaConfig)
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
	})
})
