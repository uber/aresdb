package job

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/client"
	"github.com/uber/aresdb/gateway/mocks"
	"github.com/uber/aresdb/subscriber/config"
	"github.com/uber/aresdb/utils"
	"go.uber.org/zap"
)

var _ = Describe("controller", func() {
	mockControllerClient := &mocks.ControllerClient{}
	serviceConfig := config.ServiceConfig{
		Environment: utils.EnvironmentContext{
			Deployment: "test",
		},
		Logger:           zap.NewNop(),
		Scope:            tally.NoopScope,
		ControllerConfig: &config.ControllerConfig{},
	}
	serviceConfig.ActiveJobs = []string{"dispatch_driver_rejected"}
	serviceConfig.ActiveAresClusters = map[string]client.ConnectorConfig{
		"dev01": {Address: "localhost:5436"},
	}
	drivers := make(Drivers)
	controller := &Controller{
		serviceConfig:        serviceConfig,
		aresControllerClient: mockControllerClient,
		Drivers:              drivers,
		jobNS:                "job_test",
		aresClusterNS:        "dev01",
	}
	It("SyncUpJobConfigs", func() {
		controller.SyncUpJobConfigs()
		Ω(controller.Drivers["dispatch_driver_rejected"]).ShouldNot(BeNil())
		Ω(controller.Drivers["dispatch_driver_rejected"]["dev01"]).ShouldNot(BeNil())
		controller.Drivers["dispatch_driver_rejected"]["dev01"].Stop()
	})
})
