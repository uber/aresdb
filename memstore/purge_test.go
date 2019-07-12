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
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
	"github.com/uber/aresdb/common"
	diskStoreMocks "github.com/uber/aresdb/diskstore/mocks"
	memCom "github.com/uber/aresdb/memstore/common"
	memComMocks "github.com/uber/aresdb/memstore/common/mocks"
	metaCom "github.com/uber/aresdb/metastore/common"

	metaStoreMocks "github.com/uber/aresdb/metastore/mocks"
	"github.com/uber/aresdb/redolog"
	"github.com/uber/aresdb/utils"
)

var _ = ginkgo.Describe("Purge", func() {
	var metaStore *metaStoreMocks.MetaStore
	var diskStore *diskStoreMocks.DiskStore
	var memStore *memStoreImpl
	var tableShard *TableShard
	var hostMemoryManager memCom.HostMemoryManager

	testTable := "test"
	testShardID := 0
	bootstrapToken := new(memComMocks.BootStrapToken)

	ginkgo.BeforeEach(func() {
		tableSchema := memCom.NewTableSchema(&metaCom.Table{
			Name: testTable,
			Columns: []metaCom.Column{
				{
					Name: "c0",
					Type: metaCom.Uint32,
				},
				{
					Name: "c1",
					Type: metaCom.Uint32,
				},
				{
					Name: "c2",
					Type: metaCom.Uint32,
				},
			},
			Config: metaCom.TableConfig{
				RecordRetentionInDays: 1,
			},
		})

		diskStore = &diskStoreMocks.DiskStore{}
		metaStore = &metaStoreMocks.MetaStore{}
		redologManagerMaster, _ := redolog.NewRedoLogManagerMaster("", &common.RedoLogConfig{}, diskStore, metaStore)

		options := NewOptions(bootstrapToken, redologManagerMaster)

		memStore = &memStoreImpl{
			TableShards: map[string]map[int]*TableShard{
				testTable: {},
			},
			TableSchemas: map[string]*memCom.TableSchema{
				testTable: tableSchema,
			},
			diskStore:      diskStore,
			metaStore:      metaStore,
			HostMemManager: hostMemoryManager,
			options:        options,
		}
		hostMemoryManager = NewHostMemoryManager(memStore, 1<<10)
		tableShard = NewTableShard(tableSchema, metaStore, diskStore, hostMemoryManager, testShardID, options)

		archiveBatch0, err := GetFactory().ReadArchiveBatch("archiving/archiveBatch0")
		Ω(err).Should(BeNil())
		archiveBatch1, err := GetFactory().ReadArchiveBatch("archiving/archiveBatch0")
		Ω(err).Should(BeNil())

		archivestore := NewArchiveStore(tableShard)
		tableShard.ArchiveStore = archivestore
		tableShard.ArchiveStore.CurrentVersion = NewArchiveStoreVersion(86400*2, tableShard)
		tableShard.ArchiveStore.CurrentVersion.Batches = map[int32]*ArchiveBatch{
			1: {
				Batch: *archiveBatch0,
			},
			2: {
				Batch: *archiveBatch1,
			},
		}

		memStore.TableShards[testTable][testShardID] = tableShard
		metaStore.On("PurgeArchiveBatches", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil)
	})

	ginkgo.AfterEach(func() {
		utils.ResetClockImplementation()
	})

	ginkgo.It("purge should work", func() {
		jobDetail := &PurgeJobDetail{}

		mockReporter := func(key string, mutator PurgeJobDetailMutator) {
			mutator(jobDetail)
		}
		Ω(tableShard.ArchiveStore.CurrentVersion.Batches).Should(HaveKey(int32(1)))
		Ω(tableShard.ArchiveStore.CurrentVersion.Batches).Should(HaveKey(int32(2)))

		diskStore.On("DeleteBatches", testTable, testShardID, 0, 2).
			Return(1, nil).Once()
		bootstrapToken.On("AcquireToken", mock.Anything, mock.Anything).Return(true).Once()
		bootstrapToken.On("ReleaseToken", mock.Anything, mock.Anything).Return().Once()

		err := memStore.Purge(testTable, testShardID, 0, 2, mockReporter)
		Ω(err).Should(BeNil())
		Ω(tableShard.ArchiveStore.CurrentVersion.Batches).ShouldNot(HaveKey(int32(1)))
		Ω(tableShard.ArchiveStore.CurrentVersion.Batches).Should(HaveKey(int32(2)))
		metaStore.AssertNumberOfCalls(utils.TestingT, "PurgeArchiveBatches", 1)
		diskStore.AssertNumberOfCalls(utils.TestingT, "DeleteBatches", 1)

		Ω(jobDetail.NumBatches).Should(Equal(1))
		Ω(jobDetail.Stage).Should(BeEquivalentTo("complete"))
	})

	ginkgo.It("purge should be blocked", func() {
		jobDetail := &PurgeJobDetail{}

		mockReporter := func(key string, mutator PurgeJobDetailMutator) {
			mutator(jobDetail)
		}
		Ω(tableShard.ArchiveStore.CurrentVersion.Batches).Should(HaveKey(int32(1)))
		Ω(tableShard.ArchiveStore.CurrentVersion.Batches).Should(HaveKey(int32(2)))

		bootstrapToken.On("AcquireToken", mock.Anything, mock.Anything).Return(false).Once()
		bootstrapToken.On("ReleaseToken", mock.Anything, mock.Anything).Return().Once()

		err := memStore.Purge(testTable, testShardID, 0, 2, mockReporter)
		// purge should always be no err, but here since no mock added, if not disabled, there will be mock error
		Ω(err).Should(BeNil())
	})
})
