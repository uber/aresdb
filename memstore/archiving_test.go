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
	"sort"

	diskMocks "github.com/uber/aresdb/diskstore/mocks"
	metaMocks "github.com/uber/aresdb/metastore/mocks"
	utilsMocks "github.com/uber/aresdb/utils/mocks"

	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
	"github.com/uber/aresdb/common"
	memCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/memstore/list"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/redolog"
	"github.com/uber/aresdb/utils"
)

var _ = ginkgo.Describe("archiving", func() {
	var batch99, batch101, batch110 *memCom.Batch
	var archiveBatch0 *ArchiveBatch
	var vs LiveStore
	//var dataTypes []memCom.DataType
	//var archivingJob *ArchivingJob
	var scheduler *schedulerImpl
	var jobManager *archiveJobManager
	var cutoff uint32 = 140
	var oldCutoff uint32 = 100
	var m *memStoreImpl
	table := "table1"
	shardID := 0
	key := getIdentifier(table, shardID, memCom.ArchivingJobType)
	day := 0

	m = GetFactory().NewMockMemStore()
	hostMemoryManager := NewHostMemoryManager(m, 1<<32)
	shard := NewTableShard(&memCom.TableSchema{
		Schema: metaCom.Table{
			Name: table,
			Config: metaCom.TableConfig{
				ArchivingDelayMinutes:    500,
				ArchivingIntervalMinutes: 300,
				RedoLogRotationInterval:  10800,
				MaxRedoLogFileSize:       1 << 30,
			},
			IsFactTable:          true,
			ArchivingSortColumns: []int{1, 2},
			Columns: []metaCom.Column{
				{Deleted: false},
				{Deleted: false},
				{Deleted: false},
				{Deleted: false},
			},
		},
		ValueTypeByColumn: []memCom.DataType{memCom.Uint32, memCom.Bool, memCom.Float32, memCom.ArrayInt16},
		DefaultValues:     []*memCom.DataValue{&memCom.NullDataValue, &memCom.NullDataValue, &memCom.NullDataValue, &memCom.NullDataValue},
	}, nil, nil, hostMemoryManager, shardID, m.options)

	shard.ArchiveStore = &ArchiveStore{CurrentVersion: &ArchiveStoreVersion{
		ArchivingCutoff: 0,
		Batches:         map[int32]*ArchiveBatch{},
	}}

	shardMap := map[int]*TableShard{
		shardID: shard,
	}

	ginkgo.BeforeEach(func() {
		//dataTypes = []memCom.DataType{memCom.Uint32, memCom.Bool, memCom.Float32}
		var err error
		batch110, err = GetFactory().ReadLiveBatch("archiving/batch-110")
		Ω(err).Should(BeNil())
		batch101, err = GetFactory().ReadLiveBatch("archiving/batch-101")
		Ω(err).Should(BeNil())
		batch99, err = GetFactory().ReadLiveBatch("archiving/batch-99")
		Ω(err).Should(BeNil())
		tmpBatch, err := GetFactory().ReadArchiveBatch("archiving/archiveBatch0")
		Ω(err).Should(BeNil())
		archiveBatch0 = &ArchiveBatch{
			Version: 0,
			Size:    5,
			Shard:   shardMap[0],
			Batch:   *tmpBatch,
		}
		vs = LiveStore{
			LastReadRecord: memCom.RecordID{BatchID: -101, Index: 3},
			Batches: map[int32]*LiveBatch{
				-110: {
					Batch:     *batch110,
					Capacity:  5,
					liveStore: nil,
				},
				-101: {
					Batch:     *batch101,
					Capacity:  5,
					liveStore: nil,
				},
				-99: {
					Batch:     *batch99,
					Capacity:  5,
					liveStore: nil,
				},
			},
			tableSchema:       shard.Schema,
			PrimaryKey:        NewPrimaryKey(16, true, 0, hostMemoryManager),
			HostMemoryManager: hostMemoryManager,
		}

		redoLogManagerMaster, _ := redolog.NewRedoLogManagerMaster("", &common.RedoLogConfig{}, m.diskStore, m.metaStore)

		shardMap[shardID].diskStore = m.diskStore
		shardMap[shardID].metaStore = m.metaStore
		shardMap[shardID].LiveStore = &vs
		shardMap[shardID].ArchiveStore.CurrentVersion.ArchivingCutoff = 100
		shardMap[shardID].ArchiveStore.CurrentVersion.shard = shardMap[shardID]
		shardMap[shardID].ArchiveStore.CurrentVersion.Batches[0] = archiveBatch0
		// Map from max event time to file creation time.
		shardMap[shardID].LiveStore.RedoLogManager, err = redoLogManagerMaster.NewRedologManager(table, shardID, false, &shard.Schema.Schema.Config)
		shardMap[shardID].LiveStore.RedoLogManager.UpdateMaxEventTime(1, 1)

		// make purge to pass
		shardMap[shardID].LiveStore.BackfillManager = NewBackfillManager(table, shardID, BackfillConfig{
			MaxBufferSize:            1 << 32,
			BackfillThresholdInBytes: 1 << 21,
		})
		shardMap[shardID].LiveStore.BackfillManager.LastRedoFile = 2
		shardMap[shardID].LiveStore.BackfillManager.LastBatchOffset = 1
		m.TableShards[table] = shardMap

		scheduler = newScheduler(m)
		jobManager = scheduler.jobManagers[memCom.ArchivingJobType].(*archiveJobManager)
	})

	ginkgo.AfterEach(func() {
		batch110.SafeDestruct()
		batch101.SafeDestruct()
		batch99.SafeDestruct()
	})

	ginkgo.It("snapshots live vector store", func() {
		ss := vs.snapshot()
		Ω(ss).Should(Equal(liveStoreSnapshot{
			numRecordsInLastBatch: 3,
			batches: [][]memCom.VectorParty{
				batch110.Columns,
				batch101.Columns,
			},
			batchIDs: []int32{-110, -101},
		}))
	})

	ginkgo.It("creates archiving patches", func() {
		ss := vs.snapshot()
		patchByDay := ss.createArchivingPatches(cutoff, oldCutoff, []int{1, 2},
			jobManager.reportArchiveJobDetail, key, table, shardID)
		Ω(patchByDay[0].sortColumns).Should(Equal(
			[]int{1, 2},
		))
		Ω(patchByDay[0].recordIDs).Should(Equal(
			[]memCom.RecordID{
				{BatchID: 0, Index: 1},
				{BatchID: 0, Index: 2},
				{BatchID: 0, Index: 3},
				{BatchID: 0, Index: 4},
				{BatchID: 1, Index: 0},
				{BatchID: 1, Index: 1},
				{BatchID: 1, Index: 2},
			},
		))
		scheduler.RLock()
		Ω(*(jobManager.getJobDetail(key))).Should(Equal(ArchiveJobDetail{
			JobDetail: JobDetail{
				Current:    2,
				Total:      2,
				NumRecords: 7,
			},
			Stage: "create patch",
		}))
		scheduler.RUnlock()
	})

	ginkgo.It("sorts", func() {
		ss := vs.snapshot()
		patchByDay := ss.createArchivingPatches(cutoff, oldCutoff, []int{1, 2}, jobManager.reportArchiveJobDetail, key, table, shardID)
		sort.Sort(patchByDay[0])
		Ω(patchByDay[0].recordIDs).Should(Equal(
			[]memCom.RecordID{
				{BatchID: 0, Index: 3}, // null, 1.2
				{BatchID: 1, Index: 0}, // false, null
				{BatchID: 0, Index: 1}, // false, 1.0
				{BatchID: 1, Index: 2}, // false, 1.2
				{BatchID: 0, Index: 4}, // false, 1.3
				{BatchID: 0, Index: 2}, // true, null
				{BatchID: 1, Index: 1}, // true, 1.1
			},
		))
		Ω(patchByDay[0].sortColumns).Should(Equal(
			[]int{1, 2},
		))
	})

	ginkgo.It("archive", func() {
		tableShard := shardMap[shardID]

		// Following calls are expected.
		oldVersion := tableShard.ArchiveStore.CurrentVersion
		(m.metaStore).(*metaMocks.MetaStore).On(
			"AddArchiveBatchVersion", table, shardID, day, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		(m.metaStore).(*metaMocks.MetaStore).On(
			"UpdateArchivingCutoff", table, shardID, mock.Anything).Return(nil)
		(m.diskStore).(*diskMocks.DiskStore).On(
			"DeleteBatchVersions", table, shardID, day, mock.Anything, mock.Anything).Return(nil)
		(m.diskStore).(*diskMocks.DiskStore).On(
			"DeleteLogFile", table, shardID, int64(1)).Return(nil)

		writer := new(utilsMocks.WriteSyncCloser)
		writer.On("Write", mock.Anything).Return(0, nil)
		writer.On("Close").Return(nil)
		writer.On("Sync").Return(nil)

		(m.diskStore).(*diskMocks.DiskStore).On(
			"OpenVectorPartyFileForWrite", table, mock.Anything, shardID, day, mock.Anything, mock.Anything).Return(writer, nil)

		redologManager := tableShard.LiveStore.RedoLogManager.(*redolog.FileRedoLogManager)
		redologManager.CurrentFileCreationTime = 2

		timeIncrementer := &utils.TimeIncrementer{IncBySecond: 1}
		utils.SetClockImplementation(timeIncrementer.Now)
		err := m.Archive(table, shardID, cutoff, jobManager.reportArchiveJobDetail)
		jobManager.RLock()
		jobManager.jobDetails[key].LastDuration = 0
		Ω(*(jobManager.jobDetails[key])).Should(Equal(ArchiveJobDetail{
			JobDetail: JobDetail{
				Current:         1,
				Total:           1,
				NumRecords:      7,
				NumAffectedDays: 1,
			},
			Stage:         "complete",
			CurrentCutoff: 140,
			RunningCutoff: 140,
		}))
		jobManager.RUnlock()
		Ω(err).Should(BeNil())

		// New version of archiving store should be as expected
		Ω(tableShard.ArchiveStore.CurrentVersion).ShouldNot(BeIdenticalTo(oldVersion))
		Ω(tableShard.ArchiveStore.CurrentVersion.ArchivingCutoff).Should(BeEquivalentTo(cutoff))
		Ω(tableShard.ArchiveStore.CurrentVersion.Batches).Should(HaveKey(int32(0)))
		mergedBatch := tableShard.ArchiveStore.CurrentVersion.Batches[0]
		Ω(mergedBatch.Size).Should(BeEquivalentTo(12))
		Ω(mergedBatch.Columns).Should(HaveLen(4))

		timeColumn := mergedBatch.Columns[0]
		Ω(timeColumn.GetLength()).Should(BeEquivalentTo(12))
		Ω(timeColumn.(memCom.CVectorParty).GetMode()).Should(BeEquivalentTo(memCom.AllValuesPresent))

		// Old version of archiving store should be purged.
		for i, column := range archiveBatch0.Columns {
			if i != 3 && i != 4 {
				Ω(column.(*archiveVectorParty).values).Should(BeNil())
				Ω(column.(*archiveVectorParty).nulls).Should(BeNil())
				Ω(column.(*archiveVectorParty).counts).Should(BeNil())
			} else {
				Ω(column.(*list.ArchiveVectorParty).GetBytes()).Should(Equal(int64(0)))
			}
		}

		// If a batch is partially read, it should not be purged
		for i, column := range batch101.Columns {
			if i != 3 && i != 4 {
				Ω(column.(*cLiveVectorParty).GetMode()).ShouldNot(BeEquivalentTo(memCom.AllValuesDefault))
				Ω(column.(*cLiveVectorParty).values).ShouldNot(BeNil())
			} else {
				Ω(column.(*list.LiveVectorParty).GetBytes()).ShouldNot(Equal(int64(0)))
			}
		}

		// array value validation
		arrayColumn := mergedBatch.Columns[3]
		Ω(arrayColumn.GetLength()).Should(BeEquivalentTo(12))
		val, valid := arrayColumn.AsList().GetListValue(11)
		Ω(valid).Should(BeTrue())
		reader := memCom.NewArrayValueReader(memCom.ArrayInt16, val)
		Ω(*(*uint16)(reader.Get(0))).Should(Equal(uint16(21)))

		// MaxEventTimePerFile should be purged.
		Ω(redologManager.MaxEventTimePerFile).ShouldNot(HaveKey(int64(1)))

		// Archive again, there should be no crashes or errors.
		Ω(m.Archive(table, shardID, cutoff+100, jobManager.reportArchiveJobDetail)).Should(BeNil())
		utils.ResetClockImplementation()
	})

	ginkgo.It("create patch for table with invalid event time", func() {
		table := "table2"
		shardID := 0
		key := getIdentifier(table, shardID, memCom.ArchivingJobType)
		liveStore := &LiveStore{}
		batch120, err := GetFactory().ReadLiveBatch("archiving/batch-120")
		Ω(err).Should(BeNil())
		liveStore.Batches = map[int32]*LiveBatch{
			-120: {
				Batch:     *batch120,
				Capacity:  6,
				liveStore: nil,
			},
			-110: {
				Batch:     *batch110,
				Capacity:  6,
				liveStore: nil,
			},
		}
		ss := liveStore.snapshot()
		mockReporter := func(key string, mutator ArchiveJobDetailMutator) {}
		patchByDay := ss.createArchivingPatches(cutoff, oldCutoff, []int{1, 2},
			mockReporter, key, table, shardID)
		Ω(patchByDay[0].sortColumns).Should(Equal(
			[]int{1, 2},
		))

		sort.Slice(patchByDay[0].recordIDs, func(i, j int) bool {
			if patchByDay[0].recordIDs[i].BatchID == patchByDay[0].recordIDs[j].BatchID {
				return patchByDay[0].recordIDs[i].Index < patchByDay[0].recordIDs[j].Index
			}
			return patchByDay[0].recordIDs[i].BatchID < patchByDay[0].recordIDs[j].BatchID
		})

		Ω(patchByDay[0].recordIDs).Should(Or(Equal(
			[]memCom.RecordID{
				{BatchID: 0, Index: 1},
				{BatchID: 0, Index: 2},
				{BatchID: 1, Index: 1},
				{BatchID: 1, Index: 2},
				{BatchID: 1, Index: 3},
				{BatchID: 1, Index: 4},
			}), Equal(
			[]memCom.RecordID{
				{BatchID: 0, Index: 1},
				{BatchID: 0, Index: 2},
				{BatchID: 0, Index: 3},
				{BatchID: 0, Index: 4},
				{BatchID: 1, Index: 1},
				{BatchID: 1, Index: 2},
			}),
		))
	})

	ginkgo.It("purge live batch with missing event time", func() {
		liveStore := &LiveStore{
			tableSchema: &memCom.TableSchema{
				Schema: metaCom.Table{
					Config: metaCom.TableConfig{
						AllowMissingEventTime: true,
					},
				},
			},
		}
		batch120, err := GetFactory().ReadLiveBatch("archiving/batch-120")
		Ω(err).Should(BeNil())

		// all event time are old
		var cutoff uint32 = 150
		liveStore.Batches = map[int32]*LiveBatch{
			-120: {
				Batch:          *batch120,
				Capacity:       6,
				MaxArrivalTime: 150,
				liveStore:      nil,
			},
			-110: {
				Batch:          *batch110,
				Capacity:       6,
				MaxArrivalTime: 150,
				liveStore:      nil,
			},
		}
		batchIDs := liveStore.getBatchIDsToPurge(cutoff)
		Ω(batchIDs).Should(Equal([]int32{-110}))
	})
})
