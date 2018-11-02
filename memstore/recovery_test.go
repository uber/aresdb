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
	"code.uber.internal/data/ares/memstore/common"
	memCom "code.uber.internal/data/ares/memstore/common"
	"code.uber.internal/data/ares/metastore"
	"code.uber.internal/data/ares/metastore/mocks"
	"code.uber.internal/data/ares/testing"
	"code.uber.internal/data/ares/utils"

	diskMocks "code.uber.internal/data/ares/diskstore/mocks"
	metaMocks "code.uber.internal/data/ares/metastore/mocks"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
)

var _ = ginkgo.Describe("recovery", func() {
	const (
		batchSize = 10
		tableName = "cities"
	)

	ginkgo.It("works for single upsert batch", func() {
		builder := common.NewUpsertBatchBuilder()
		builder.AddColumn(0, common.Uint32)
		builder.AddRow()
		builder.SetValue(0, 0, uint32(123))
		buffer, _ := builder.ToByteArray()

		file := &testing.TestReadWriteCloser{}
		writer := utils.NewStreamDataWriter(file)
		writer.WriteUint32(UpsertHeader)
		writer.WriteUint32(uint32(len(buffer)))
		writer.Write(buffer)

		diskStore := CreateMockDiskStore()
		diskStore.On("ListLogFiles", "abc", 0).Return([]int64{1}, nil)
		diskStore.On("OpenLogFileForReplay", "abc", 0, int64(1)).Return(file, nil)

		events := make(chan metastore.ShardOwnership)
		done := make(chan struct{})
		metaStore := &mocks.MetaStore{}
		metaStore.On("GetOwnedShards", "abc").Return([]int{0}, nil)
		metaStore.On("WatchShardOwnershipEvents").Return(
			(<-chan metastore.ShardOwnership)(events),
			(chan<- struct{})(done), nil)
		metaStore.On("GetArchivingCutoff", "abc", 0).Return(uint32(0), nil)
		metaStore.On("GetBackfillProgressInfo", "abc", 0).Return(int64(0), uint32(0), nil)
		metaStore.On("GetBackfillProgressInfo", "abc", 1).Return(int64(0), uint32(0), nil)

		memstore := createMemStore("abc", 0, []common.DataType{common.Uint32}, []int{0}, 10, true, metaStore, diskStore)
		memstore.TableShards["abc"] = nil
		memstore.InitShards(false)
		shard := memstore.TableShards["abc"][0]
		Ω(len(shard.LiveStore.Batches)).Should(Equal(1))
		value, validity := ReadShardValue(shard, 0, []byte{123, 0, 0, 0})
		Ω(*(*uint32)(value)).Should(Equal(uint32(123)))
		Ω(validity).Should(BeTrue())
		//  Validate redo log max event time.
		Ω(len(shard.LiveStore.RedoLogManager.MaxEventTimePerFile)).Should(Equal(1))
		Ω(shard.LiveStore.RedoLogManager.MaxEventTimePerFile).Should(Equal(map[int64]uint32{1: 123}))

		// New shard abc-1 being assigned.
		file2 := &testing.TestReadWriteCloser{}
		writer2 := utils.NewStreamDataWriter(file2)
		writer2.WriteUint32(UpsertHeader)
		writer2.WriteUint32(uint32(len(buffer)))
		writer2.Write(buffer)

		diskStore.On("ListLogFiles", "abc", 1).Return([]int64{1}, nil)
		diskStore.On("OpenLogFileForReplay", "abc", 1, int64(1)).Return(file2, nil)
		metaStore.On("GetArchivingCutoff", "abc", 1).Return(uint32(0), nil)

		events <- metastore.ShardOwnership{"abc", 1, true}
		<-done

		shard = memstore.TableShards["abc"][1]
		Ω(len(shard.LiveStore.Batches)).Should(Equal(1))
		value, validity = ReadShardValue(shard, 0, []byte{123, 0, 0, 0})
		Ω(*(*uint32)(value)).Should(Equal(uint32(123)))
		Ω(validity).Should(BeTrue())
		//  Validate redo log max event time.
		Ω(len(shard.LiveStore.RedoLogManager.MaxEventTimePerFile)).Should(Equal(1))
		Ω(shard.LiveStore.RedoLogManager.MaxEventTimePerFile).Should(Equal(map[int64]uint32{1: 123}))

		Ω(shard.LiveStore.BackfillManager.CurrentRedoFile).Should(BeEquivalentTo(1))
		Ω(shard.LiveStore.BackfillManager.CurrentBatchOffset).Should(BeEquivalentTo(0))
	})

	ginkgo.It("loadsnapshots should not panic", func() {
		diskStore := &diskMocks.DiskStore{}
		metaStore := &metaMocks.MetaStore{}

		m := createMemStore(tableName, 0, []memCom.DataType{memCom.Uint16, memCom.SmallEnum, memCom.UUID, memCom.Uint32},
			[]int{0}, batchSize, false, metaStore, diskStore)
		m.loadSnapshots()
	})

	ginkgo.It("cleanOldSnapshotAndLogs should work", func() {
		diskStore := &diskMocks.DiskStore{}
		metaStore := &metaMocks.MetaStore{}
		diskStore.On("DeleteSnapshot", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		m := createMemStore(tableName, 0, []memCom.DataType{memCom.Uint16, memCom.SmallEnum, memCom.UUID, memCom.Uint32},
			[]int{0}, batchSize, false, metaStore, diskStore)
		shard, _ := m.GetTableShard(tableName, 0)
		shard.cleanOldSnapshotAndLogs(0, 0)

		diskStore.AssertCalled(utils.TestingT, "DeleteSnapshot", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})
})
