package memstore

import (
	"github.com/onsi/ginkgo"
	diskMocks "github.com/uber/aresdb/diskstore/mocks"
	memCom "github.com/uber/aresdb/memstore/common"
	memMocks "github.com/uber/aresdb/memstore/common/mocks"
	metaCom "github.com/uber/aresdb/metastore/common"
	metaMocks "github.com/uber/aresdb/metastore/mocks"
)

var _ = ginkgo.Describe("table shard bootstrap", func() {

	table := "test"
	diskStore := &diskMocks.DiskStore{}
	metaStore := &metaMocks.MetaStore{}
	memStore := getFactory().NewMockMemStore()
	hostMemoryManager := &memMocks.HostMemoryManager{}

	shard := NewTableShard(&memCom.TableSchema{
		Schema: metaCom.Table{
			Name: table,
			Config: metaCom.TableConfig{
				ArchivingDelayMinutes:    500,
				ArchivingIntervalMinutes: 300,
				RedoLogRotationInterval:  10800,
				MaxRedoLogFileSize:       1 << 30,
				RecordRetentionInDays: 10,
			},
			IsFactTable:          true,
			ArchivingSortColumns: []int{1, 2},
			Columns: []metaCom.Column{
				{Deleted: false},
				{Deleted: false},
				{Deleted: false},
			},
		},
		ValueTypeByColumn: []memCom.DataType{memCom.Uint32, memCom.Bool, memCom.Float32},
		DefaultValues:     []*memCom.DataValue{&memCom.NullDataValue, &memCom.NullDataValue, &memCom.NullDataValue},
	}, metaStore, diskStore, hostMemoryManager, 0, memStore.redologManagerMaster)

	ginkgo.It("", func() {
		shard.Bootstrap()
	})

})