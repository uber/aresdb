package sink

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber/aresdb/client"
	"github.com/uber/aresdb/controller/models"
	memCom "github.com/uber/aresdb/memstore/common"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/subscriber/common/rules"
	"fmt"
	"github.com/uber/aresdb/utils"
	"unsafe"
)

var _ = Describe("Sink", func() {
	It("Shard", func() {
		rows := []client.Row{
			{"1", "v12", "v13"},
			{"2", "v22", "v23"},
			{"3", "v32", "v33"},
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
			JobConfig: models.JobConfig{
				AresTableConfig: models.TableConfig{
					Table: &metaCom.Table{
						Name:        "test",
						IsFactTable: true,
						Columns: []metaCom.Column{
							{
								Name: "c2",
								Type: "string",
							},
							{
								Name: "c1",
								Type: "Int8",
							},
							{
								Name: "c3",
								Type: "string",
							},
						},
						Config: metaCom.TableConfig{
							BatchSize: 10,
						},
						PrimaryKeyColumns: []int{1},
					},
				},
			},
		}
		jobConfig.SetPrimaryKeyBytes(1)
		destination.NumShards = 0
		batches, _ := Shard(rows, destination, &jobConfig)
		Ω(batches).Should(BeNil())

		destination.NumShards = 1
		batches, _ = Shard(rows, destination, &jobConfig)
		Ω(batches).Should(BeNil())

		destination.NumShards = 2
		batches, _ = Shard(rows, destination, &jobConfig)
		Ω(batches).ShouldNot(BeNil())
		Ω(len(batches)).Should(Equal(2))
	})

	It("ShardFunc test", func() {
		jobConfig := &rules.JobConfig {
			JobConfig: models.JobConfig {
				AresTableConfig: models.TableConfig {
					Table: &metaCom.Table {
						Columns: []metaCom.Column {
							{
								Name: "ts",
								Type: metaCom.Uint32,
							},
							{
								Name: "id",
								Type: metaCom.UUID,
							},
						},
					},
				},
			},
		}
		primaryKeyValues := make([]memCom.DataValue, 1)
		keyVals := []string {
			"1e88a975-3d26-4277-ace9-bea91b072977",
			"98ba8df9-de8a-46b9-a101-c1a5313d8a97",
			"27fc581b-6df8-48e5-b1cc-3a8123efee5b",
			"9940594d-eb0b-47ef-b369-347c3f30e46a",
			"e5dde3c7-99f5-4a7b-9e6f-20558043a4fa",
			"1801737b-ef6f-4f64-8ea5-1a52d7d2d408",
			"5bb8e482-f3c1-4807-b9bf-a28161b9d3d4",
			"bbcbd1ec-b48a-4242-ab8e-d7a02ee8da41",
			"09473710-c721-47e3-92d8-a9927822c38c",
			"748a71d2-5829-491b-a6e0-f2c931cd777b",
			"ef1b8c98-bd55-44f1-947a-00d46977ca02",
			"871ddf63-6f25-4375-bb28-9815da9c6f73",
			"1125b3d5-fabc-4568-9523-b686cb80f9d1",
			"c87b12ef-44a3-4236-a486-e9ad5be517f7",
			"a98823a5-64ea-4683-a33d-ecd54a04d237",
		}
		var numShards uint32 = 8
		shardMap1 := make(map[uint32]struct{})
		shardMap2 := make(map[uint32]struct{})

		for _, keyVal := range keyVals {
			val, err := getDataValue(keyVal, 1, jobConfig)
			Ω(err).Should(BeNil())
			Ω(val.DataType).Should(Equal(memCom.UUID))
			Ω(val.ConvertToHumanReadable(memCom.UUID).(string)).Should(Equal(keyVal))
			primaryKeyValues[0] = val
			keyLen := memCom.DataTypeBits(val.DataType)/8
			pk, err := memCom.GetPrimaryKeyBytes(primaryKeyValues, keyLen);
			Ω(err).Should(BeNil())
			shardID1 := utils.Murmur3Sum32(unsafe.Pointer(&pk[0]), len(pk), hashSeed) % numShards
			shardID2 := utils.Murmur3Sum32(unsafe.Pointer(&pk[0]), len(pk), 0) % numShards

			fmt.Printf("new shard id: %d, old shard id: %d\n", shardID1, shardID2)
			shardMap1[shardID1] = struct{}{}
			shardMap2[shardID2] = struct{}{}
		}
		Ω(len(shardMap1)).Should(Equal(int(numShards)))
		Ω(len(shardMap2)).Should(Equal(int(2)))
	})
})
