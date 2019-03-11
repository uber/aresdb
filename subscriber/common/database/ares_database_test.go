package database

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/client"
	"github.com/uber/aresdb/client/mocks"
	memCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/subscriber/config"
	"go.uber.org/zap"
)

var _ = Describe("AresDatabase client", func() {
	mockConnector := mocks.Connector{}
	jobName := "test"
	cluster := "ares-dev"
	table := "test"
	columnNames := []string{"c1", "c2", "c3"}
	pk := map[string]interface{}{"c1": nil}
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
	rows := []Row{
		{"v11", "v12", "v13"},
		{"v21", "v22", "v23"},
		{"v31", "v32", "v33"},
	}
	serviceConfig := config.ServiceConfig{
		Logger: zap.NewNop(),
		Scope:  tally.NoopScope,
	}
	aresDB := &AresDatabase{
		ServiceConfig: serviceConfig,
		Scope:         tally.NoopScope,
		ClusterName:   cluster,
		Connector:     &mockConnector,
		JobName:       jobName,
	}
	It("NewAresDatabase", func() {
		 config := client.ConnectorConfig {
		 	Address: "localhost:8081",
		}
		_, err := NewAresDatabase(serviceConfig, jobName, cluster, config)
		Ω(err).ShouldNot(BeNil())
	})
	It("Save", func() {
		aresRows := make([]client.Row, 0, len(rows))
		for _, row := range rows {
			aresRows = append(aresRows, client.Row(row))
		}
		mockConnector.On("Insert",
			table, columnNames, aresRows).
			Return(3, nil)
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
