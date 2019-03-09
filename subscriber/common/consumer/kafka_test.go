package consumer

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
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
)

var _ = Describe("KafkaConsumer", func() {
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
		jobConfigs["dispatch_driver_rejected"]["dev01"].AresTableConfig.Cluster = "dev-ares01"
	}

	It("KafkaConsumer functions", func() {
		kc, err := NewKafkaConsumer(jobConfigs["dispatch_driver_rejected"]["dev01"], serviceConfig)
		Ω(err).Should(BeNil())
		Ω(kc).ShouldNot(BeNil())

		groupId := kc.Name()
		Ω(groupId).Should(Equal("ares-subscriber_test_dispatch_driver_rejected_dev-ares01_streaming"))

		topics := kc.Topics()
		len := len(topics)
		Ω(len).Should(Equal(1))
		Ω(topics[0]).Should(Equal("hp_demand_job-offer-expired"))

		errCh := kc.Errors()
		Ω(errCh).ShouldNot(BeNil())

		msgCh := kc.Messages()
		Ω(msgCh).ShouldNot(BeNil())

		closeCh := kc.Closed()
		Ω(closeCh).ShouldNot(BeNil())

		err = kc.Close()
		Ω(err).Should(BeNil())
	})

	It("KafkaMessage functions", func() {
		message := &KafkaMessage{
			&kafka.Message{},
			nil,
			"kloak-sjc1-agg1",
		}

		cluster := message.Cluster()
		Ω(cluster).Should(Equal("kloak-sjc1-agg1"))
	})
})
