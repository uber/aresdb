package config

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/utils"
	"go.uber.org/config"
	"go.uber.org/zap"
)

var _ = Describe("service_config", func() {
	ActiveAresNameSpace = "dev01-sjc1"
	ActiveJobNameSpace = "job-test"
	It("NewServiceConfig", func() {
		cfg, err := config.NewYAMLProviderFromFiles("test.yaml")
		Ω(err).Should(BeNil())

		p := Params{
			Environment: utils.EnvironmentContext{
				Deployment: "test",
			},
			Logger: zap.NewNop(),
			Scope:  tally.NoopScope,
			Config: cfg,
		}

		res, err := NewServiceConfig(p)
		Ω(err).Should(BeNil())
		Ω(res).ShouldNot(BeNil())
	})
})
