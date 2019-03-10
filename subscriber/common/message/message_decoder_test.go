package message

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
)

var _ = Describe("message decoder tests", func() {

	It("stringMessage tests", func() {
		msg := &stringMessage{
			"topic",
			"message",
		}

		Ω(string(msg.Key())).Should(Equal(""))
		Ω(string(msg.Value())).Should(Equal("message"))
		Ω(msg.Topic()).Should(Equal("topic"))
		Ω(msg.Partition()).Should(Equal(int32(0)))
		Ω(msg.Offset()).Should(Equal(int64(0)))
		msg.Ack()
		msg.Nack()
	})

	It("NewDefaultDecoder", func() {
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

		decoder, err := NewDefaultDecoder(jobConfigs["dispatch_driver_rejected"]["dev01"], serviceConfig)
		Ω(decoder).ShouldNot(BeNil())
		Ω(err).Should(BeNil())
	})

})
