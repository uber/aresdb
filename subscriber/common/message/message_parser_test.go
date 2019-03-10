package message

import (
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/client"
	memCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/subscriber/common/database"
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

	It("NewParser", func() {
		parser := NewParser(jobConfigs["dispatch_driver_rejected"]["dev01"], serviceConfig)
		Ω(parser).ShouldNot(BeNil())
	})

	It("populateDestination", func() {
		mp.populateDestination(jobConfigs["dispatch_driver_rejected"]["dev01"])
		Ω(mp.Destination).ShouldNot(BeNil())
	})

	It("ParseMessage", func() {
		msg := map[string]interface{}{
			"project": "ares-subscriber",
		}

		dst := database.Destination{
			"table",
			[]string{"project"},
			map[string]interface{}{"1": "project"},
			[]memCom.ColumnUpdateMode{memCom.UpdateOverwriteNotNull},
		}
		mp.Transformations = map[string]*rules.TransformationConfig{
			"project": &rules.TransformationConfig{},
		}
		row, err := mp.ParseMessage(msg, dst)
		Ω(row).ShouldNot(BeNil())
		Ω(err).Should(BeNil())
	})
})
