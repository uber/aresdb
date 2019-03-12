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

	hlConsumer, _ := consumer.NewKafkaConsumer(jobConfigs["dispatch_driver_rejected"]["dev01"], serviceConfig)
	decoder, _ := message.NewDefaultDecoder(jobConfig, serviceConfig)
	failureHandler := initFailureHandler(serviceConfig, jobConfig, aresDB)

	processor := &StreamingProcessor{
		ID:            1,
		jobConfig:     jobConfig,
		cluster:       "dev01",
		serviceConfig: serviceConfig,
		scope: serviceConfig.Scope.Tagged(map[string]string{
			"job":         "dispatch_driver_rejected",
			"aresCluster": "dev01",
		}),
		database:          aresDB,
		failureHandler:    failureHandler,
		highLevelConsumer: hlConsumer,
		consumerInitFunc:  consumer.NewKafkaConsumer,
		msgSizes:          make(chan int64),
		parser:            message.NewParser(jobConfig, serviceConfig),
		decoder:           decoder,
		shutdown:          make(chan bool),
		close:             make(chan bool),
		errors:            make(chan ProcessorError),
		context: &ProcessorContext{
			StartTime: time.Now(),
			Errors: processorErrors{
				errors: make([]ProcessorError, jobConfig.StreamingConfig.ErrorThreshold*10),
			},
		},
	}

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

	It("NewStreamingProcessor", func() {
		p, err := NewStreamingProcessor(1, jobConfig, consumer.NewKafkaConsumer, message.NewDefaultDecoder,
			make(chan ProcessorError), make(chan int64), serviceConfig)
		Ω(p).Should(BeNil())
		Ω(err).ShouldNot(BeNil())

	})
	It("Run", func() {
		id := processor.GetID()
		Ω(id).Should(Equal(1))

		ctx := processor.GetContext()
		Ω(ctx).ShouldNot(BeNil())

		processor.initBatcher()

		_, err := processor.decodeMessage(msg)
		Ω(err).Should(BeNil())

		mockConnector.On("Insert",
			table, columnNames, rows).
			Return(6, nil)
		processor.writeRow(rows, destination)

		processor.reportMessageAge(&message.Message{
			MsgMetaDataTS: time.Now(),
			RawMessage:    msg,
		})

		go processor.Run()

		processor.Stop()
	})
	It("Restart", func() {
		processor.reInitialize()
		processor.Restart()
	})
})
