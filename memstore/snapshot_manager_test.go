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

	"github.com/pkg/errors"
	memCom "github.com/uber/aresdb/memstore/common"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/metastore/mocks"
	"github.com/uber/aresdb/utils"
	"time"
)

var _ = ginkgo.Describe("snapshot manager", func() {
	table := "table1"
	shardID := 0

	m = getFactory().NewMockMemStore()
	hostMemoryManager := NewHostMemoryManager(m, 1<<32)
	shard := NewTableShard(&TableSchema{
		Schema: metaCom.Table{
			Name: table,
			Config: metaCom.TableConfig{
				SnapshotThreshold:       100,
				SnapshotIntervalMinutes: 5,
			},
			IsFactTable: false,
			Columns: []metaCom.Column{
				{Deleted: false},
				{Deleted: false},
				{Deleted: false},
			},
		},
		ValueTypeByColumn: []memCom.DataType{memCom.Uint32, memCom.Bool, memCom.Float32},
		DefaultValues:     []*memCom.DataValue{&memCom.NullDataValue, &memCom.NullDataValue, &memCom.NullDataValue},
	}, nil, nil, hostMemoryManager, shardID)
	var metaStore *mocks.MetaStore

	var snapshotManager *SnapshotManager

	ginkgo.BeforeEach(func() {
		snapshotManager = NewSnapshotManager(shard)
		metaStore = new(mocks.MetaStore)
		shard.metaStore = metaStore
	})

	ginkgo.AfterEach(func() {
		shard.metaStore = nil
	})

	ginkgo.It("NewSnapshotManager should work", func() {
		Ω(snapshotManager).ShouldNot(BeNil())
		Ω(snapshotManager.SnapshotInterval).Should(Equal(
			time.Duration(shard.Schema.Schema.Config.SnapshotIntervalMinutes) * time.Minute))
		Ω(snapshotManager.SnapshotThreshold).Should(Equal(shard.Schema.Schema.Config.SnapshotThreshold))
	})

	ginkgo.It("StartSnapshot, ApplyUpsertBatch and then Done should work", func() {
		redoLogFile, offset, numMutations, record := snapshotManager.StartSnapshot()
		Ω(redoLogFile).Should(BeZero())
		Ω(offset).Should(BeZero())
		Ω(numMutations).Should(BeZero())
		Ω(record.BatchID).Should(BeZero())
		Ω(record.Index).Should(BeZero())

		record = RecordID{BatchID: 10, Index: 11}
		snapshotManager.ApplyUpsertBatch(100, 100, 100, record)

		redoLogFile, offset, numMutations, record = snapshotManager.StartSnapshot()
		Ω(redoLogFile).Should(BeEquivalentTo(100))
		Ω(offset).Should(BeEquivalentTo(100))
		Ω(numMutations).Should(BeEquivalentTo(100))
		Ω(record.BatchID).Should(BeEquivalentTo(10))
		Ω(record.Index).Should(BeEquivalentTo(11))

		record = RecordID{BatchID: 100, Index: 101}
		snapshotManager.ApplyUpsertBatch(100, 200, 100, record)

		Ω(snapshotManager.CurrentRedoFile).Should(BeEquivalentTo(100))
		Ω(snapshotManager.CurrentBatchOffset).Should(BeEquivalentTo(200))
		Ω(snapshotManager.NumMutations).Should(BeEquivalentTo(200))
		Ω(snapshotManager.CurrentRecord.BatchID).Should(BeEquivalentTo(record.BatchID))
		Ω(snapshotManager.CurrentRecord.Index).Should(BeEquivalentTo(record.Index))

		timeIncrementer := &utils.TimeIncrementer{IncBySecond: 1}
		utils.SetClockImplementation(timeIncrementer.Now)
		metaStore.On("UpdateSnapshotProgress", table, shardID, redoLogFile, offset, record.BatchID, record.Index).Return(nil)
		Ω(snapshotManager.Done(redoLogFile, offset, numMutations, record)).Should(BeNil())
		Ω(snapshotManager.LastSnapshotTime).Should(Equal(time.Unix(1, 0)))

		Ω(snapshotManager.LastRedoFile).Should(BeEquivalentTo(100))
		Ω(snapshotManager.LastBatchOffset).Should(BeEquivalentTo(100))
		Ω(numMutations).Should(BeEquivalentTo(100))

		offset += 100
		err := errors.New("error when UpdateSnapshotProgress")
		metaStore.On("UpdateSnapshotProgress", table, shardID, redoLogFile, offset, record.BatchID, record.Index).Return(err)
		Ω(snapshotManager.Done(redoLogFile, offset, numMutations, record)).Should(Equal(err))
	})

	ginkgo.It("QualifyForSnapshot should work", func() {
		record := RecordID{BatchID: 100, Index: 101}

		snapshotManager.LastSnapshotTime = time.Unix(0, 0)

		// time = 0, mutation = 0, not trigger
		utils.SetCurrentTime(time.Unix(0, 0))
		Ω(snapshotManager.QualifyForSnapshot()).Should(BeFalse())

		// time = 300, mutation = 0, not trigger
		utils.SetCurrentTime(time.Unix(300, 0))
		Ω(snapshotManager.QualifyForSnapshot()).Should(BeFalse())

		// time = 300, mutation = 1, trigger
		snapshotManager.ApplyUpsertBatch(100, 300, 1, record)
		Ω(snapshotManager.QualifyForSnapshot()).Should(BeTrue())
		metaStore.On("UpdateSnapshotProgress", table, shardID, int64(100), uint32(300), record.BatchID, record.Index).Return(nil)
		Ω(snapshotManager.Done(100, 300, 1, record)).Should(BeNil())

		// time = 300, mutation = 100, trigger
		snapshotManager.ApplyUpsertBatch(100, 300, 100, record)
		Ω(snapshotManager.QualifyForSnapshot()).Should(BeTrue())
		utils.ResetClockImplementation()
	})

})
