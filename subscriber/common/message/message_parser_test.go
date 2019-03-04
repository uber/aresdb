package message

import (
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/client"
	"github.com/uber/aresdb/subscriber/common/rules"
	"github.com/uber/aresdb/subscriber/common/tools"
	"github.com/uber/aresdb/subscriber/config"
	"github.com/uber/aresdb/utils"
	"go.uber.org/zap"
)

var _ = Describe("message_parser", func() {
	serviceConfig := config.ServiceConfig{
		Environment: utils.EnvironmentContext{
			Deployment: "test",
		},
		Logger: zap.NewNop(),
		Scope:  tally.NoopScope,
	}
	serviceConfig.ActiveJobs = []string{"dispatch_driver_rejected"}
	serviceConfig.ActiveAresClusters = map[string]client.ConnectorConfig{
		"dev01": client.ConnectorConfig{Address: "localhost:8888"},
	}
	jobConfigs := make(rules.JobConfigs)
	serviceConfig.Environment.RuntimeEnvironment = "test"
	serviceConfig.Environment.Zone = "local"
	rootPath := tools.GetModulePath("")
	os.Chdir(rootPath)
	rules.AddLocalJobConfig(serviceConfig, jobConfigs)

	mp := &Parser{
		ServiceConfig:   serviceConfig,
		JobName:         jobConfigs["dispatch_driver_rejected"]["dev01"].Name,
		Cluster:         jobConfigs["dispatch_driver_rejected"]["dev01"].AresTableConfig.Cluster,
		Transformations: jobConfigs["dispatch_driver_rejected"]["dev01"].GetTranformations(),
	}

	It("populateDestination", func() {
		mp.populateDestination(jobConfigs["dispatch_driver_rejected"]["dev01"])
		Ω(mp.Destination).ShouldNot(BeNil())
	})
})
