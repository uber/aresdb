//  Copyright (c) 2017-2018 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
	ActiveAresNameSpace = "dev01"
	ActiveJobNameSpace = "job-test"
	It("NewServiceConfig normal", func() {
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
	It("NewServiceConfig local", func() {
		cfg, err := config.NewYAMLProviderFromFiles("test-controller-disable.yaml")
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
	It("NewServiceConfig ares-ns-empty", func() {
		cfg, err := config.NewYAMLProviderFromFiles("test-ares-ns-empty.yaml")
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
		Ω(err).ShouldNot(BeNil())
		Ω(res).ShouldNot(BeNil())

		cfg, err = config.NewYAMLProviderFromFiles("test-ares-cluster-empty.yaml")
		Ω(err).Should(BeNil())

		p.Config = cfg

		res, err = NewServiceConfig(p)
		Ω(err).ShouldNot(BeNil())
		Ω(res).ShouldNot(BeNil())

		cfg, err = config.NewYAMLProviderFromFiles("test-job-empty.yaml")
		Ω(err).Should(BeNil())

		p.Config = cfg

		res, err = NewServiceConfig(p)
		Ω(err).ShouldNot(BeNil())
		Ω(res).ShouldNot(BeNil())

		ActiveAresNameSpace = "dev01-ares"
		cfg, err = config.NewYAMLProviderFromFiles("test.yaml")
		Ω(err).Should(BeNil())

		p.Config = cfg

		res, err = NewServiceConfig(p)
		Ω(err).ShouldNot(BeNil())
		Ω(res).ShouldNot(BeNil())
	})
})
