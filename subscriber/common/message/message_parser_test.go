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
	memCom "github.com/uber/aresdb/memstore/common"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/subscriber/common/rules"
	"github.com/uber/aresdb/subscriber/common/sink"
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
	serviceConfig.ActiveJobs = []string{"job1"}
	sinkConfig := config.SinkConfig{
		SinkModeStr:           "aresDB",
		AresDBConnectorConfig: client.ConnectorConfig{Address: "localhost:8888"},
	}
	serviceConfig.ActiveAresClusters = map[string]config.SinkConfig{
		"dev01": sinkConfig,
	}

	jobConfigs := make(rules.JobConfigs)
	serviceConfig.Environment.RuntimeEnvironment = "test"
	serviceConfig.Environment.Zone = "local"
	rootPath := tools.GetModulePath("")
	os.Chdir(rootPath)
	rules.AddLocalJobConfig(serviceConfig, jobConfigs)

	mp := &Parser{
		ServiceConfig:   serviceConfig,
		JobName:         jobConfigs["job1"]["dev01"].Name,
		Cluster:         jobConfigs["job1"]["dev01"].AresTableConfig.Cluster,
		Transformations: jobConfigs["job1"]["dev01"].GetTranformations(),
	}

	It("NewParser", func() {
		parser := NewParser(jobConfigs["job1"]["dev01"], serviceConfig)
		Ω(parser).ShouldNot(BeNil())
	})

	It("populateDestination", func() {
		mp.populateDestination(jobConfigs["job1"]["dev01"])
		Ω(mp.Destination).ShouldNot(BeNil())
	})

	It("ParseMessage", func() {
		msg32 := map[string]interface{}{
			"changed_at": "1570489452",
			"project":    "ares-subscriber",
		}

		msg64 := map[string]interface{}{
			"changed_at": "1570489452010",
			"project":    "ares-subscriber",
		}

		errMsg := map[string]interface{}{
			"changed_at": "abcd",
			"project":    "ares-subscriber",
		}

		dst := sink.Destination{
			Table:       "table",
			ColumnNames: []string{"changed_at", "project"},
			PrimaryKeys: map[string]int{
				"changed_at": 0,
				"project":    1},
			AresUpdateModes: []memCom.ColumnUpdateMode{memCom.UpdateOverwriteNotNull},
		}

		schema := metaCom.Table{
			IsFactTable: true,
			Config: metaCom.TableConfig{
				AllowMissingEventTime: true,
			},
		}
		columnDict := map[string]int{
			"changed_at": 0,
			"project":    1,
		}

		mp.Transformations = map[string]*rules.TransformationConfig{
			"changed_at": &rules.TransformationConfig{},
			"project": &rules.TransformationConfig{},
		}
		row, err := mp.ParseMessage(msg32, dst)
		Ω(row).ShouldNot(BeNil())
		Ω(err).Should(BeNil())
		err = mp.CheckTimeColumnExistence(&schema, columnDict, dst, row)
		Ω(err).Should(BeNil())

		row, err = mp.ParseMessage(msg64, dst)
		Ω(row).ShouldNot(BeNil())
		Ω(err).Should(BeNil())
		err = mp.CheckTimeColumnExistence(&schema, columnDict, dst, row)
		Ω(err).Should(BeNil())

		row, err = mp.ParseMessage(errMsg, dst)
		Ω(row).ShouldNot(BeNil())
		Ω(err).Should(BeNil())
		err = mp.CheckTimeColumnExistence(&schema, columnDict, dst, row)
		Ω(err).ShouldNot(BeNil())
	})
})
