package rules

import (
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/client"
	"github.com/uber/aresdb/subscriber/common/tools"
	"github.com/uber/aresdb/subscriber/config"
	"github.com/uber/aresdb/utils"
	"go.uber.org/zap"
)

var _ = Describe("job_config", func() {
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
	jobConfigs := make(JobConfigs)
	It("AddLocalJobConfig", func() {
		rootPath := tools.GetModulePath("")
		os.Chdir(rootPath)
		err := AddLocalJobConfig(serviceConfig, jobConfigs)
		Ω(err).Should(BeNil())
		Ω(jobConfigs["dispatch_driver_rejected"]).ShouldNot(BeNil())
		Ω(jobConfigs["dispatch_driver_rejected"]["dev01"]).ShouldNot(BeNil())
	})
})
