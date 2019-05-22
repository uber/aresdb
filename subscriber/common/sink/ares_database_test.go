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

package sink

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/client"
	"github.com/uber/aresdb/client/mocks"
	"github.com/uber/aresdb/controller/models"
	memCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/subscriber/common/rules"
	"github.com/uber/aresdb/subscriber/config"
	"go.uber.org/zap"
)

var _ = Describe("AresDatabase client", func() {
	mockConnector := mocks.Connector{}
	cluster := "ares-dev"
	table := "test"
	columnNames := []string{"c1", "c2", "c3"}
	pk := map[string]int{"c1": 0}
	modes := []memCom.ColumnUpdateMode{
		memCom.UpdateOverwriteNotNull,
		memCom.UpdateOverwriteNotNull,
		memCom.UpdateOverwriteNotNull,
	}
	destination := Destination{
		Table:           table,
		ColumnNames:     columnNames,
		PrimaryKeys:     pk,
		AresUpdateModes: modes,
	}
	rows := []client.Row{
		{"v11", "v12", "v13"},
		{"v21", "v22", "v23"},
		{"v31", "v32", "v33"},
	}
	serviceConfig := config.ServiceConfig{
		Logger: zap.NewNop(),
		Scope:  tally.NoopScope,
	}
	jobConfig := rules.JobConfig{
		JobConfig: models.JobConfig{Name: "test"},
	}
	aresDB := &AresDatabase{
		ServiceConfig: serviceConfig,
		JobConfig:     &jobConfig,
		Scope:         tally.NoopScope,
		ClusterName:   cluster,
		Connector:     &mockConnector,
	}
	It("NewAresDatabase", func() {
		cfg := client.ConnectorConfig{
			Address: "localhost:8081",
		}
		sinkCfg := config.SinkConfig{
			SinkModeStr:           "aresDB",
			AresDBConnectorConfig: cfg,
		}
		_, err := NewAresDatabase(serviceConfig, &jobConfig, cluster, sinkCfg, nil)
		Ω(err).ShouldNot(BeNil())
	})
	It("Save", func() {
		mockConnector.On("Insert",
			table, columnNames, rows).
			Return(6, nil)
		err := aresDB.Save(destination, rows)
		Ω(err).Should(BeNil())

	})
	It("Cluster", func() {
		c := aresDB.Cluster()
		Ω(c).Should(Equal(cluster))
	})
	It("Shutdown", func() {
		aresDB.Shutdown()
	})
})
