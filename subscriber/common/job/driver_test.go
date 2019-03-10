package job

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/client"
	"github.com/uber/aresdb/subscriber/common/rules"
	"github.com/uber/aresdb/subscriber/common/tools"
	"github.com/uber/aresdb/subscriber/config"
	"github.com/uber/aresdb/utils"
	"go.uber.org/zap"
	"os"
	"github.com/uber/aresdb/subscriber/common/consumer"
	"github.com/uber/aresdb/subscriber/common/message"
	"time"
	"github.com/uber/aresdb/subscriber/common/database"
	"github.com/uber/aresdb/client/mocks"
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

	driver, err :=  NewDriver(jobConfig, serviceConfig, NewStreamingProcessor,
		consumer.NewKafkaConsumer, message.NewDefaultDecoder)
	driver.processors = []Processor{processor}

	It("NewDriver", func() {
		Ω(driver).ShouldNot(BeNil())
		Ω(err).Should(BeNil())
	})

	It("MarshalJSON", func() {
		_, err := driver.MarshalJSON()
		Ω(err).Should(BeNil())
	})

	It("GetErrors", func() {
		errors := driver.GetErrors()
		Ω(errors).ShouldNot(BeNil())
	})

	It("", func() {
		ok := driver.RemoveProcessor(0)
		Ω(ok).Should(Equal(false))
	})

	It("Stop", func() {
		go driver.monitorStatus(time.NewTicker(time.Duration(driver.statusCheckInterval) * time.Second))
		go driver.monitorErrors()
		go driver.limitRate()

		driver.Stop()
	})
})


