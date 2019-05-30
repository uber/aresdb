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

var _ = Describe("message decoder tests", func() {

	It("StringMessage tests", func() {
		msg := &StringMessage{
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
		serviceConfig.ActiveJobs = []string{"job1"}
		sinkConfig := config.SinkConfig{
			SinkModeStr:           "aresDB",
			AresDBConnectorConfig: client.ConnectorConfig{Address: "localhost:8888"},
		}
		serviceConfig.ActiveAresClusters = map[string]config.SinkConfig{
			"dev01": sinkConfig,
		}

		rootPath := tools.GetModulePath("")
		os.Chdir(rootPath)
		jobConfigs := make(rules.JobConfigs)
		err := rules.AddLocalJobConfig(serviceConfig, jobConfigs)
		if err != nil {
			panic("Failed to AddLocalJobConfig")
		}
		if jobConfigs["job1"]["dev01"] == nil {
			panic("Failed to get (jobConfigs[\"job1\"][\"dev01\"]")
		} else {
			jobConfigs["job1"]["dev01"].AresTableConfig.Cluster = "dev01"
		}

		decoder, err := NewDefaultDecoder(jobConfigs["job1"]["dev01"], serviceConfig)
		Ω(decoder).ShouldNot(BeNil())
		Ω(err).Should(BeNil())
	})

})
