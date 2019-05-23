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

package rules

import (
	"encoding/json"
	"github.com/uber/aresdb/controller/models"
	"io/ioutil"
	"os"
	"path"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/client"
	memCom "github.com/uber/aresdb/memstore/common"
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
	serviceConfig.ActiveJobs = []string{"job1"}
	sinkConfig := config.SinkConfig{
		SinkModeStr:           "aresDB",
		AresDBConnectorConfig: client.ConnectorConfig{Address: "localhost:8888"},
	}
	serviceConfig.ActiveAresClusters = map[string]config.SinkConfig{
		"dev01": sinkConfig,
	}
	p := Params{
		ServiceConfig: serviceConfig,
	}

	It("NewJobConfigs", func() {
		serviceConfig.ActiveJobs = []string{"dispatch"}
		rootPath := tools.GetModulePath("")
		os.Chdir(rootPath)
		rst, err := NewJobConfigs(p)
		Ω(rst).ShouldNot(BeNil())
		Ω(err).Should(BeNil())
		Ω(rst.JobConfigs["job1"]).ShouldNot(BeNil())
		Ω(rst.JobConfigs["job1"]["dev01"]).ShouldNot(BeNil())

		dst := rst.JobConfigs["job1"]["dev01"].GetDestinations()
		transformation := rst.JobConfigs["job1"]["dev01"].GetTranformations()
		primaryKey := rst.JobConfigs["job1"]["dev01"].GetPrimaryKeys()
		Ω(dst).ShouldNot(BeNil())
		Ω(transformation).ShouldNot(BeNil())
		Ω(primaryKey).ShouldNot(BeNil())
	})

	It("parseUpdateMode", func() {
		mode := parseUpdateMode("overwrite_notnull")
		Ω(mode).Should(Equal(memCom.UpdateOverwriteNotNull))

		mode = parseUpdateMode("overwrite_force")
		Ω(mode).Should(Equal(memCom.UpdateForceOverwrite))

		mode = parseUpdateMode("min")
		Ω(mode).Should(Equal(memCom.UpdateWithMin))

		mode = parseUpdateMode("max")
		Ω(mode).Should(Equal(memCom.UpdateWithMax))

		mode = parseUpdateMode("")
		Ω(mode).Should(Equal(memCom.UpdateOverwriteNotNull))
	})

	It("NewAssignmentFromController", func() {
		rootPath := tools.GetModulePath("")
		bts, err := ioutil.ReadFile(path.Join(rootPath, "config", "test", "jobs", "job1-local.json"))
		Ω(err).Should(BeNil())
		jc := models.JobConfig{}
		err = json.Unmarshal(bts, &jc)
		Ω(err).Should(BeNil())

		assigned := models.IngestionAssignment{
			Subscriber: "0",
			Jobs:       []models.JobConfig{jc},
			Instances:  map[string]models.Instance{
				"instance0": {
					Address: "instance0:9374",
				},
			},
		}
		assignment, err := NewAssignmentFromController(&assigned)
		Ω(err).Should(BeNil())
		Ω(assignment.Jobs).Should(HaveLen(1))
	})
})
