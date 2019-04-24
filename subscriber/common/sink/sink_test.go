package sink

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber/aresdb/client"
	memCom "github.com/uber/aresdb/memstore/common"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/subscriber/common/rules"
)

var _ = Describe("AresDatabase client", func() {
	It("Sharding", func() {
		rows := []client.Row{
			{"v11", "v12", "v13"},
			{"v21", "v22", "v23"},
			{"v31", "v32", "v33"},
		}
		table := "test"
		columnNames := []string{"c1", "c2", "c3"}
		pk := map[string]int{"c1": 0}
		pkInSchema := map[string]int{"c1": 1}
		modes := []memCom.ColumnUpdateMode{
			memCom.UpdateOverwriteNotNull,
			memCom.UpdateOverwriteNotNull,
			memCom.UpdateOverwriteNotNull,
		}
		destination := Destination{
			Table:               table,
			ColumnNames:         columnNames,
			PrimaryKeys:         pk,
			PrimaryKeysInSchema: pkInSchema,
			AresUpdateModes:     modes,
		}
		jobConfig := rules.JobConfig{
			AresTableConfig: rules.AresTableConfig{
				Table: metaCom.Table{
					Name:        "test",
					IsFactTable: true,
					Columns: []metaCom.Column{
						{
							Name: "c2",
							Type: "string",
						},
						{
							Name: "c1",
							Type: "string",
						},
						{
							Name: "c3",
							Type: "string",
						},
					},
					Config: metaCom.TableConfig{
						BatchSize: 10,
					},
				},
			},
		}
		destination.NumShards = 0
		batches := Sharding(rows, destination, &jobConfig)
		Ω(batches).Should(BeNil())

		destination.NumShards = 1
		batches = Sharding(rows, destination, &jobConfig)
		Ω(batches).Should(BeNil())

		destination.NumShards = 2
		batches = Sharding(rows, destination, &jobConfig)
		Ω(batches).ShouldNot(BeNil())
		Ω(len(batches)).Should(Equal(2))
	})
})
