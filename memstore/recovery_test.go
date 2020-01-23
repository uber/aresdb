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

package memstore

import (
	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	"github.com/uber/aresdb/cluster/topology"
	"github.com/uber/aresdb/common"
	memCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/redolog"
	"github.com/uber/aresdb/testing"
	"github.com/uber/aresdb/utils"

	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
	diskMocks "github.com/uber/aresdb/diskstore/mocks"
	memComMocks "github.com/uber/aresdb/memstore/common/mocks"
	metaCom "github.com/uber/aresdb/metastore/common"
	metaMocks "github.com/uber/aresdb/metastore/mocks"

	"time"
)

var _ = ginkgo.Describe("recovery", func() {
	const (
		batchSize = 10
		tableName = "cities"
	)

	ginkgo.It("works for single upsert batch", func() {
		builder := memCom.NewUpsertBatchBuilder()
		builder.AddColumn(0, memCom.Uint32)
		builder.AddRow()
		builder.SetValue(0, 0, uint32(123))
		buffer, _ := builder.ToByteArray()

		file := &testing.TestReadWriteSyncCloser{}
		writer := utils.NewStreamDataWriter(file)
		writer.WriteUint32(redolog.UpsertHeader)
		writer.WriteUint32(uint32(len(buffer)))
		writer.Write(buffer)

		diskStore := CreateMockDiskStore()
		diskStore.On("ListLogFiles", "abc", 0).Return([]int64{1}, nil)
		diskStore.On("OpenLogFileForReplay", "abc", 0, int64(1)).Return(file, nil)

		events := make(chan metaCom.ShardOwnership)
		done := make(chan struct{})
		metaStore := &metaMocks.MetaStore{}
		metaStore.On("GetOwnedShards", "abc").Return([]int{0}, nil)
		metaStore.On("WatchShardOwnershipEvents").Return(
			(<-chan metaCom.ShardOwnership)(events),
			(chan<- struct{})(done), nil)
		metaStore.On("GetArchivingCutoff", "abc", 0).Return(uint32(0), nil)
		metaStore.On("GetBackfillProgressInfo", "abc", 0).Return(int64(0), uint32(0), nil)
		metaStore.On("GetBackfillProgressInfo", "abc", 1).Return(int64(0), uint32(0), nil)

		memstore := createMemStore("abc", 0, []memCom.DataType{memCom.Uint32}, []int{0}, 10, true, false, metaStore, diskStore)
		memstore.TableShards["abc"] = nil
		memstore.options.redoLogMaster.Stop()
		memstore.InitShards(false, topology.NewStaticShardOwner([]int{0}))
		shard := memstore.TableShards["abc"][0]
		Ω(len(shard.LiveStore.Batches)).Should(Equal(1))
		value, validity := ReadShardValue(shard, 0, []byte{123, 0, 0, 0})
		Ω(*(*uint32)(value)).Should(Equal(uint32(123)))
		Ω(validity).Should(BeTrue())
		//  Validate redo log max event time.
		redologManager := shard.LiveStore.RedoLogManager.(*redolog.FileRedoLogManager)
		Ω(len(redologManager.MaxEventTimePerFile)).Should(Equal(1))
		Ω(redologManager.MaxEventTimePerFile).Should(Equal(map[int64]uint32{1: 123}))

		// New shard abc-1 being assigned.
		file2 := &testing.TestReadWriteSyncCloser{}
		writer2 := utils.NewStreamDataWriter(file2)
		writer2.WriteUint32(redolog.UpsertHeader)
		writer2.WriteUint32(uint32(len(buffer)))
		writer2.Write(buffer)

		diskStore.On("ListLogFiles", "abc", 1).Return([]int64{1}, nil)
		diskStore.On("OpenLogFileForReplay", "abc", 1, int64(1)).Return(file2, nil)
		metaStore.On("GetArchivingCutoff", "abc", 1).Return(uint32(0), nil)

		events <- metaCom.ShardOwnership{TableName: "abc", Shard: 1, ShouldOwn: true}
		<-done

		shard = memstore.TableShards["abc"][1]
		Ω(len(shard.LiveStore.Batches)).Should(Equal(1))
		value, validity = ReadShardValue(shard, 0, []byte{123, 0, 0, 0})
		Ω(*(*uint32)(value)).Should(Equal(uint32(123)))
		Ω(validity).Should(BeTrue())
		//  Validate redo log max event time.
		Ω(len(redologManager.MaxEventTimePerFile)).Should(Equal(1))
		Ω(redologManager.MaxEventTimePerFile).Should(Equal(map[int64]uint32{1: 123}))

		Ω(shard.LiveStore.BackfillManager.CurrentRedoFile).Should(BeEquivalentTo(1))
		Ω(shard.LiveStore.BackfillManager.CurrentBatchOffset).Should(BeEquivalentTo(0))
	})

	ginkgo.It("loadsnapshots should not panic", func() {
		diskStore := &diskMocks.DiskStore{}
		metaStore := &metaMocks.MetaStore{}

		m := createMemStore(tableName, 0, []memCom.DataType{memCom.Uint16, memCom.SmallEnum, memCom.UUID, memCom.Uint32},
			[]int{0}, batchSize, false, false, metaStore, diskStore)
		m.loadSnapshots()
	})

	ginkgo.It("cleanOldSnapshotAndLogs should work", func() {
		diskStore := &diskMocks.DiskStore{}
		metaStore := &metaMocks.MetaStore{}
		diskStore.On("DeleteSnapshot", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		m := createMemStore(tableName, 0, []memCom.DataType{memCom.Uint16, memCom.SmallEnum, memCom.UUID, memCom.Uint32},
			[]int{0}, batchSize, false, false, metaStore, diskStore)
		shard, _ := m.GetTableShard(tableName, 0)
		shard.cleanOldSnapshotAndLogs(0, 0)

		diskStore.AssertCalled(utils.TestingT, "DeleteSnapshot", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})

	ginkgo.It("cleanOldSnapshotAndLogs should be blocked for DeleteSnapshot", func() {
		diskStore := &diskMocks.DiskStore{}
		metaStore := &metaMocks.MetaStore{}

		// no mock on DeleteSnapshot will be fine as it wont be called
		m := createMemStore(tableName, 0, []memCom.DataType{memCom.Uint16, memCom.SmallEnum, memCom.UUID, memCom.Uint32},
			[]int{0}, batchSize, false, false, metaStore, diskStore)

		shard, _ := m.GetTableShard(tableName, 0)
		shard.options.bootstrapToken = new(memComMocks.BootStrapToken)
		shard.options.bootstrapToken.(*memComMocks.BootStrapToken).On("AcquireToken", mock.Anything, mock.Anything).Return(false)
		shard.options.bootstrapToken.(*memComMocks.BootStrapToken).On("ReleaseToken", mock.Anything, mock.Anything).Return()
		shard.cleanOldSnapshotAndLogs(0, 0)
	})

	ginkgo.It("PlayRedoLog should work for file redolog", func() {
		diskStore := &diskMocks.DiskStore{}
		metaStore := &metaMocks.MetaStore{}

		builder := memCom.NewUpsertBatchBuilder()
		builder.AddColumn(0, memCom.Uint32)
		builder.AddRow()
		builder.SetValue(0, 0, uint32(123))
		buffer, _ := builder.ToByteArray()

		file := &testing.TestReadWriteSyncCloser{}
		writer := utils.NewStreamDataWriter(file)
		writer.WriteUint32(redolog.UpsertHeader)
		writer.WriteUint32(uint32(len(buffer)))
		writer.Write(buffer)

		diskStore.On("ListLogFiles", tableName, 0).Return([]int64{1}, nil)
		diskStore.On("OpenLogFileForReplay", tableName, 0, int64(1)).Return(file, nil)
		diskStore.On("OpenLogFileForAppend", mock.Anything, mock.Anything, mock.Anything).Return(&testing.TestReadWriteSyncCloser{}, nil)

		m := createMemStore(tableName, 0, []memCom.DataType{memCom.Uint32},
			[]int{0}, batchSize, true, false, metaStore, diskStore)
		shard, _ := m.GetTableShard(tableName, 0)
		shard.PlayRedoLog()
		Ω(shard.LiveStore.RedoLogManager.GetBatchRecovered()).Should(Equal(1))
		// add data after recovery
		batch, _ := memCom.NewUpsertBatch(buffer)
		err := shard.saveUpsertBatch(batch, 1, 0, false, false)
		Ω(err).Should(BeNil())
	})

	ginkgo.It("PlayRedoLog should work for kafka ingestion", func() {
		diskStore := &diskMocks.DiskStore{}
		metaStore := &metaMocks.MetaStore{}

		builder := memCom.NewUpsertBatchBuilder()
		builder.AddColumn(0, memCom.Uint32)
		builder.AddRow()
		builder.SetValue(0, 0, uint32(123))
		buffer, _ := builder.ToByteArray()

		file := &testing.TestReadWriteSyncCloser{}
		writer := utils.NewStreamDataWriter(file)
		writer.WriteUint32(redolog.UpsertHeader)
		writer.WriteUint32(uint32(len(buffer)))
		writer.Write(buffer)

		namespace := "ns1"
		c := &common.RedoLogConfig{
			DiskConfig: common.DiskRedoLogConfig{
				Disabled: false,
			},
			KafkaConfig: common.KafkaRedoLogConfig{
				Brokers: []string{
					"host1",
					"host2",
				},
				Enabled:     true,
				TopicSuffix: "staging",
			},
		}

		diskStore.On("ListLogFiles", tableName, 1).Return([]int64{1}, nil)
		diskStore.On("OpenLogFileForReplay", tableName, 1, int64(1)).Return(file, nil)
		diskStore.On("OpenLogFileForAppend", mock.Anything, mock.Anything, mock.Anything).Return(&testing.TestReadWriteSyncCloser{}, nil)
		metaStore.On("GetRedoLogCommitOffset", tableName, 1).Return(int64(1), nil)
		//metaStore.On("GetRedoLogCheckpointOffset", tableName, 1).Return(int64(5), nil)

		m := createMemStore(tableName, 0, []memCom.DataType{memCom.Uint32},
			[]int{0}, batchSize, true, false, metaStore, diskStore)
		// close the default one
		m.options.redoLogMaster.Stop()
		//reassign
		consumer, _ := testing.MockKafkaConsumerFunc(nil)
		m.options.redoLogMaster, _ = redolog.NewKafkaRedoLogManagerMaster(namespace, c, diskStore, metaStore, consumer)
		schema, _ := m.GetSchema(tableName)
		shard := NewTableShard(schema, metaStore, diskStore, m.HostMemManager, 1, 1, m.options)

		upsertBatch, _ := memCom.NewUpsertBatch(buffer)
		for i := 0; i < 10; i++ {
			consumer.(*mocks.Consumer).ExpectConsumePartition(utils.GetTopicFromTable("ns1", tableName, "staging"), int32(1), mocks.AnyOffset).
				YieldMessage(&sarama.ConsumerMessage{
					Value: upsertBatch.GetBuffer(),
				})
		}

		shard.PlayRedoLog()
		time.Sleep(time.Millisecond * 100)

		Ω(shard.LiveStore.RedoLogManager.GetBatchRecovered()).Should(Equal(1))
		Ω(shard.LiveStore.RedoLogManager.GetBatchReceived()).Should(Equal(10))
		shard.LiveStore.RedoLogManager.Close()
	})
})
