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
	"code.uber.internal/data/ares/utils"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"time"
)

var _ = ginkgo.Describe("ingestion", func() {
	ginkgo.It("works for empty upsert batch", func() {
		memstore := createMemStore("abc", 0, []common.DataType{}, []int{}, 10, false, nil, CreateMockDiskStore())
		buffer, _ := common.NewUpsertBatchBuilder().ToByteArray()
		upsertBatch, _ := NewUpsertBatch(buffer)
		err := memstore.HandleIngestion("abc", 0, upsertBatch)
		Ω(err).Should(BeNil())
		shard, _ := memstore.GetTableShard("abc", 0)
		Ω(shard.LiveStore.lastModifiedTimePerColumn).Should(BeNil())
	})

	ginkgo.It("returns error for unrecognized table", func() {
		memstore := createMemStore("abc", 0, []common.DataType{}, []int{}, 10, false, nil, CreateMockDiskStore())
		buffer, _ := common.NewUpsertBatchBuilder().ToByteArray()
		upsertBatch, _ := NewUpsertBatch(buffer)
		err := memstore.HandleIngestion("def", 0, upsertBatch)
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("returns error for missing primary key", func() {
		memstore := createMemStore("abc", 0, []common.DataType{common.Uint8}, []int{0}, 10, false, nil, CreateMockDiskStore())
		buffer, _ := common.NewUpsertBatchBuilder().ToByteArray()
		upsertBatch, _ := NewUpsertBatch(buffer)
		err := memstore.HandleIngestion("abc", 0, upsertBatch)
		Ω(err).ShouldNot(BeNil())
		shard, _ := memstore.GetTableShard("abc", 0)
		Ω(shard.LiveStore.lastModifiedTimePerColumn).Should(BeNil())
	})

	ginkgo.It("works for valid upsert batch without rows", func() {
		memstore := createMemStore("abc", 0, []common.DataType{common.Uint8}, []int{0}, 10, false, nil, CreateMockDiskStore())
		builder := common.NewUpsertBatchBuilder()
		builder.AddColumn(0, common.Uint8)
		buffer, _ := builder.ToByteArray()
		upsertBatch, _ := NewUpsertBatch(buffer)
		err := memstore.HandleIngestion("abc", 0, upsertBatch)
		Ω(err).Should(BeNil())
		shard, err := memstore.GetTableShard("abc", 0)
		Ω(shard.LiveStore.LastReadRecord.BatchID).Should(Equal(BaseBatchID))
		Ω(shard.LiveStore.LastReadRecord.Index).Should(Equal(uint32(0)))
		Ω(shard.LiveStore.lastModifiedTimePerColumn).Should(BeNil())
	})

	ginkgo.It("returns error for row with missing primary key", func() {
		memstore := createMemStore("abc", 0, []common.DataType{common.Uint8}, []int{0}, 10, false, nil, CreateMockDiskStore())
		builder := common.NewUpsertBatchBuilder()
		builder.AddColumn(0, common.Uint8)
		builder.AddRow()
		buffer, _ := builder.ToByteArray()
		upsertBatch, _ := NewUpsertBatch(buffer)
		err := memstore.HandleIngestion("abc", 0, upsertBatch)
		Ω(err).ShouldNot(BeNil())
		shard, _ := memstore.GetTableShard("abc", 0)
		Ω(shard.LiveStore.lastModifiedTimePerColumn).Should(BeNil())
	})

	ginkgo.It("works for one row, one column", func() {
		memstore := createMemStore("abc", 0, []common.DataType{common.Uint8}, []int{0}, 10, false, nil, CreateMockDiskStore())
		builder := common.NewUpsertBatchBuilder()
		builder.AddColumn(0, common.Uint8)
		builder.AddRow()
		builder.SetValue(0, 0, uint8(123))
		buffer, _ := builder.ToByteArray()
		upsertBatch, _ := NewUpsertBatch(buffer)
		err := memstore.HandleIngestion("abc", 0, upsertBatch)
		Ω(err).Should(BeNil())

		shard, err := memstore.GetTableShard("abc", 0)
		Ω(shard.LiveStore.LastReadRecord.BatchID).Should(Equal(BaseBatchID))
		Ω(shard.LiveStore.lastModifiedTimePerColumn).Should(BeNil())

		value, valid := ReadShardValue(shard, 0, []byte{123})
		Ω(valid).Should(BeTrue())
		Ω(*(*uint8)(value)).Should(Equal(uint8(123)))

		Ω(shard.LiveStore.LastReadRecord.Index).Should(Equal(uint32(1)))
	})

	ginkgo.It("skip old records", func() {
		memstore := createMemStore("abc", 0, []common.DataType{common.Uint8}, []int{0}, 10, true, nil, CreateMockDiskStore())
		shard, err := memstore.GetTableShard("abc", 0)
		Ω(err).Should(BeNil())
		shard.Schema.Schema.Config.RecordRetentionInDays = 10

		builder := common.NewUpsertBatchBuilder()
		builder.AddColumn(0, common.Uint8)
		builder.AddRow()
		builder.SetValue(0, 0, uint8(123))
		buffer, _ := builder.ToByteArray()
		upsertBatch, _ := NewUpsertBatch(buffer)
		err = memstore.HandleIngestion("abc", 0, upsertBatch)
		Ω(err).Should(BeNil())

		Ω(shard.LiveStore.LastReadRecord.BatchID).Should(Equal(BaseBatchID))
		Ω(shard.LiveStore.lastModifiedTimePerColumn).Should(BeNil())

		vp, index := getVectorParty(shard, 0, []byte{123})
		Ω(vp).Should(BeNil())
		Ω(index).Should(Equal(0))
		Ω(shard.LiveStore.LastReadRecord.Index).Should(BeEquivalentTo(0))
	})

	ginkgo.It("skip future records", func() {
		utils.SetCurrentTime(time.Unix(100, 0))
		defer utils.ResetClockImplementation()
		memstore := createMemStore("abc", 0, []common.DataType{common.Uint8}, []int{0}, 10, true, nil, CreateMockDiskStore())
		shard, err := memstore.GetTableShard("abc", 0)
		Ω(err).Should(BeNil())

		builder := common.NewUpsertBatchBuilder()
		builder.AddColumn(0, common.Uint8)
		builder.AddRow()
		builder.SetValue(0, 0, uint8(123))
		builder.AddRow()
		builder.SetValue(1, 0, uint8(99))
		buffer, _ := builder.ToByteArray()
		upsertBatch, _ := NewUpsertBatch(buffer)
		err = memstore.HandleIngestion("abc", 0, upsertBatch)
		Ω(err).Should(BeNil())

		Ω(shard.LiveStore.LastReadRecord.BatchID).Should(Equal(BaseBatchID))
		Ω(shard.LiveStore.lastModifiedTimePerColumn).Should(Equal([]uint32{99}))

		vp, index := getVectorParty(shard, 0, []byte{123})
		Ω(vp).Should(BeNil())
		Ω(index).Should(Equal(0))
		Ω(shard.LiveStore.LastReadRecord.Index).Should(BeEquivalentTo(1))

		vp, index = getVectorParty(shard, 0, []byte{99})
		Ω(vp).ShouldNot(BeNil())
		Ω(index).Should(Equal(0))
	})

	ginkgo.It("works for inserting duplicated rows", func() {
		// Make sure batch is going correctly.
		memstore := createMemStore("abc", 0, []common.DataType{common.Uint8, common.Bool}, []int{1}, 10, false, nil, CreateMockDiskStore())
		builder := common.NewUpsertBatchBuilder()
		builder.AddColumn(0, common.Uint8)
		builder.AddColumn(1, common.Bool)
		builder.AddRow()
		builder.SetValue(0, 1, true)
		builder.SetValue(0, 0, uint8(123))
		builder.AddRow()
		builder.SetValue(1, 1, false)
		builder.AddRow()
		builder.SetValue(2, 1, true)
		builder.SetValue(2, 0, uint8(125))

		buffer, _ := builder.ToByteArray()
		upsertBatch, _ := NewUpsertBatch(buffer)
		err := memstore.HandleIngestion("abc", 0, upsertBatch)
		Ω(err).Should(BeNil())

		shard, err := memstore.GetTableShard("abc", 0)
		Ω(shard.LiveStore.LastReadRecord.BatchID).Should(Equal(BaseBatchID))
		Ω(shard.LiveStore.LastReadRecord.Index).Should(Equal(uint32(2)))

		Ω(shard.LiveStore.lastModifiedTimePerColumn).Should(BeNil())

		value, valid := ReadShardValue(shard, 0, []byte{1})
		Ω(valid).Should(BeTrue())
		Ω(*(*uint8)(value)).Should(Equal(uint8(125)))

		boolValue, valid := ReadShardBool(shard, 1, []byte{1})
		Ω(valid).Should(BeTrue())
		Ω(boolValue).Should(BeTrue())

		value, valid = ReadShardValue(shard, 0, []byte{0})
		Ω(valid).Should(BeFalse())

		boolValue, valid = ReadShardBool(shard, 1, []byte{0})
		Ω(valid).Should(BeTrue())
		Ω(boolValue).Should(BeFalse())
	})

	ginkgo.It("works for composite primary key types", func() {
		// Make sure batch is going correctly.
		memstore := createMemStore("abc", 0, []common.DataType{common.Uint16, common.Bool, common.Float32}, []int{1, 0}, 10, false, nil, CreateMockDiskStore())
		builder := common.NewUpsertBatchBuilder()
		builder.AddColumn(0, common.Uint16)
		builder.AddColumn(1, common.Bool)
		builder.AddColumn(2, common.Float32)
		builder.AddRow()
		builder.SetValue(0, 1, true)
		builder.SetValue(0, 0, uint16(123))
		builder.AddRow()
		builder.SetValue(1, 1, false)
		builder.SetValue(1, 0, uint16(456))
		builder.SetValue(1, 2, float32(4.56))

		buffer, _ := builder.ToByteArray()
		upsertBatch, _ := NewUpsertBatch(buffer)
		err := memstore.HandleIngestion("abc", 0, upsertBatch)
		Ω(err).Should(BeNil())

		shard, err := memstore.GetTableShard("abc", 0)
		Ω(shard.LiveStore.LastReadRecord.BatchID).Should(Equal(BaseBatchID))
		Ω(shard.LiveStore.LastReadRecord.Index).Should(Equal(uint32(2)))
		Ω(shard.LiveStore.lastModifiedTimePerColumn).Should(BeNil())

		value, valid := ReadShardValue(shard, 0, []byte{1, 123, 0})
		Ω(valid).Should(BeTrue())
		Ω(*(*uint16)(value)).Should(Equal(uint16(123)))
		boolValue, valid := ReadShardBool(shard, 1, []byte{1, 123, 0})
		Ω(valid).Should(BeTrue())
		Ω(boolValue).Should(BeTrue())
		value, valid = ReadShardValue(shard, 2, []byte{1, 123, 0})
		Ω(valid).Should(BeFalse())

		value, valid = ReadShardValue(shard, 0, []byte{0, 200, 1})
		Ω(valid).Should(BeTrue())
		Ω(*(*uint16)(value)).Should(Equal(uint16(456)))
		boolValue, valid = ReadShardBool(shard, 1, []byte{0, 200, 1})
		Ω(valid).Should(BeTrue())
		Ω(boolValue).Should(BeFalse())
		value, valid = ReadShardValue(shard, 2, []byte{0, 200, 1})
		Ω(valid).Should(BeTrue())
		Ω(*(*float32)(value)).Should(Equal(float32(4.56)))
	})

	ginkgo.It("batch grows correctly", func() {
		// Make sure batch is going correctly.
		memstore := createMemStore("abc", 0, []common.DataType{common.Uint8}, []int{0}, 2, false, nil, CreateMockDiskStore())
		builder := common.NewUpsertBatchBuilder()
		builder.AddColumn(0, common.Uint8)
		builder.AddRow()
		builder.SetValue(0, 0, uint8(100))
		builder.AddRow()
		builder.SetValue(1, 0, uint8(101))
		builder.AddRow()
		builder.SetValue(2, 0, uint8(102))
		builder.AddRow()
		builder.SetValue(3, 0, uint8(103))
		builder.AddRow()
		builder.SetValue(4, 0, uint8(104))

		buffer, _ := builder.ToByteArray()
		upsertBatch, _ := NewUpsertBatch(buffer)
		err := memstore.HandleIngestion("abc", 0, upsertBatch)
		Ω(err).Should(BeNil())

		shard, err := memstore.GetTableShard("abc", 0)
		Ω(shard.LiveStore.LastReadRecord.BatchID).Should(Equal(BaseBatchID + 2))
		Ω(shard.LiveStore.LastReadRecord.Index).Should(Equal(uint32(1)))
		Ω(shard.LiveStore.lastModifiedTimePerColumn).Should(BeNil())

		value, valid := ReadShardValue(shard, 0, []byte{100})
		Ω(valid).Should(BeTrue())
		Ω(*(*uint8)(value)).Should(Equal(uint8(100)))
		value, valid = ReadShardValue(shard, 0, []byte{101})
		Ω(valid).Should(BeTrue())
		Ω(*(*uint8)(value)).Should(Equal(uint8(101)))
		value, valid = ReadShardValue(shard, 0, []byte{102})
		Ω(valid).Should(BeTrue())
		Ω(*(*uint8)(value)).Should(Equal(uint8(102)))
		value, valid = ReadShardValue(shard, 0, []byte{103})
		Ω(valid).Should(BeTrue())
		Ω(*(*uint8)(value)).Should(Equal(uint8(103)))
		value, valid = ReadShardValue(shard, 0, []byte{104})
		Ω(valid).Should(BeTrue())
		Ω(*(*uint8)(value)).Should(Equal(uint8(104)))
	})

	ginkgo.It("batch grows correctly with batch size changes", func() {
		// Make sure batch is going correctly.
		memstore := createMemStore("abc", 0, []common.DataType{common.Uint8}, []int{0}, 1, false, nil, CreateMockDiskStore())
		builder := common.NewUpsertBatchBuilder()
		builder.AddColumn(0, common.Uint8)
		builder.AddRow()
		builder.SetValue(0, 0, uint8(100))
		builder.AddRow()
		builder.SetValue(1, 0, uint8(101))

		buffer, _ := builder.ToByteArray()
		upsertBatch, _ := NewUpsertBatch(buffer)
		err := memstore.HandleIngestion("abc", 0, upsertBatch)
		Ω(err).Should(BeNil())

		shard, err := memstore.GetTableShard("abc", 0)
		Ω(shard.LiveStore.LastReadRecord.BatchID).Should(Equal(BaseBatchID + 2))
		Ω(shard.LiveStore.LastReadRecord.Index).Should(Equal(uint32(0)))

		Ω(shard.LiveStore.NextWriteRecord.BatchID).Should(Equal(BaseBatchID + 2))
		Ω(shard.LiveStore.NextWriteRecord.Index).Should(Equal(uint32(0)))

		Ω(shard.LiveStore.lastModifiedTimePerColumn).Should(BeNil())

		value, valid := ReadShardValue(shard, 0, []byte{100})
		Ω(valid).Should(BeTrue())
		Ω(*(*uint8)(value)).Should(Equal(uint8(100)))
		value, valid = ReadShardValue(shard, 0, []byte{101})
		Ω(valid).Should(BeTrue())
		Ω(*(*uint8)(value)).Should(Equal(uint8(101)))

		builder.ResetRows()
		builder.AddRow()
		builder.SetValue(0, 0, uint8(102))
		builder.AddRow()
		builder.SetValue(1, 0, uint8(103))
		builder.AddRow()
		builder.SetValue(2, 0, uint8(101))

		buffer, _ = builder.ToByteArray()
		upsertBatch, _ = NewUpsertBatch(buffer)

		// Update batch size to 2.
		shard.LiveStore.BatchSize = 2
		err = memstore.HandleIngestion("abc", 0, upsertBatch)
		Ω(err).Should(BeNil())
		Ω(shard.LiveStore.lastModifiedTimePerColumn).Should(BeNil())

		Ω(shard.LiveStore.LastReadRecord.BatchID).Should(Equal(BaseBatchID + 3))
		Ω(shard.LiveStore.LastReadRecord.Index).Should(Equal(uint32(0)))

		Ω(shard.LiveStore.NextWriteRecord.BatchID).Should(Equal(BaseBatchID + 3))
		Ω(shard.LiveStore.NextWriteRecord.Index).Should(Equal(uint32(0)))

		value, valid = ReadShardValue(shard, 0, []byte{100})
		Ω(valid).Should(BeTrue())
		Ω(*(*uint8)(value)).Should(Equal(uint8(100)))
		value, valid = ReadShardValue(shard, 0, []byte{101})
		Ω(valid).Should(BeTrue())
		Ω(*(*uint8)(value)).Should(Equal(uint8(101)))
		value, valid = ReadShardValue(shard, 0, []byte{102})
		Ω(valid).Should(BeTrue())
		Ω(*(*uint8)(value)).Should(Equal(uint8(102)))
		value, valid = ReadShardValue(shard, 0, []byte{103})
		Ω(valid).Should(BeTrue())
		Ω(*(*uint8)(value)).Should(Equal(uint8(103)))
	})

	ginkgo.It("inserts record if event time is greater than cut off time", func() {
		memstore := createMemStore("abc", 0, []common.DataType{common.Uint32}, []int{0}, 10, true, nil, CreateMockDiskStore())
		builder := common.NewUpsertBatchBuilder()
		builder.AddColumn(0, common.Uint32)
		builder.AddRow()
		builder.SetValue(0, 0, uint32(3))
		buffer, _ := builder.ToByteArray()
		upsertBatch, _ := NewUpsertBatch(buffer)
		shard, err := memstore.GetTableShard("abc", 0)
		shard.LiveStore.PrimaryKey.UpdateEventTimeCutoff(2)
		shard.LiveStore.ArchivingCutoffHighWatermark = 2
		err = memstore.HandleIngestion("abc", 0, upsertBatch)
		Ω(shard.LiveStore.lastModifiedTimePerColumn).Should(Equal([]uint32{3}))
		Ω(err).Should(BeNil())
		_, valid := ReadShardValue(shard, 0, []byte{3, 0, 0, 0})
		Ω(valid).Should(BeTrue())
	})

	ginkgo.It("backfill data if event time is less than cut off time", func() {
		memstore := createMemStore("abc", 0, []common.DataType{common.Uint32}, []int{0}, 10, true, nil, CreateMockDiskStore())
		builder := common.NewUpsertBatchBuilder()
		builder.AddColumn(0, common.Uint32)
		builder.AddRow()
		builder.SetValue(0, 0, uint32(3))
		builder.AddRow()
		builder.SetValue(1, 0, uint32(1))
		buffer, _ := builder.ToByteArray()
		upsertBatch, _ := NewUpsertBatch(buffer)
		shard, err := memstore.GetTableShard("abc", 0)
		shard.LiveStore.PrimaryKey.UpdateEventTimeCutoff(2)
		shard.LiveStore.ArchivingCutoffHighWatermark = 2
		err = memstore.HandleIngestion("abc", 0, upsertBatch)
		Ω(err).Should(BeNil())
		Ω(shard.LiveStore.lastModifiedTimePerColumn).Should(Equal([]uint32{3}))
		_, valid := ReadShardValue(shard, 0, []byte{3, 0, 0, 0})
		Ω(valid).Should(BeTrue())

		// Verify upsertBatches to be backfilled
		backfillUpsertBatchs := shard.LiveStore.BackfillManager.UpsertBatches
		Ω(len(backfillUpsertBatchs)).Should(Equal(1))
		// Verify row count
		Ω(backfillUpsertBatchs[0].NumRows).Should(Equal(1))
		// Verify column id
		Ω(backfillUpsertBatchs[0].NumColumns).Should(Equal(1))
		columnID, err := backfillUpsertBatchs[0].GetColumnID(0)
		Ω(err).Should(BeNil())
		Ω(columnID).Should(Equal(0))
		// Verify value
		value, valid, err := backfillUpsertBatchs[0].GetValue(0, 0)
		Ω(err).Should(BeNil())
		Ω(valid).Should(Equal(true))
		Ω(*(*uint32)(value)).Should(Equal(uint32(1)))

	})

	ginkgo.It("returns error fact table's first column is not uint32", func() {
		memstore := createMemStore("abc", 0, []common.DataType{common.Uint32}, []int{0}, 10, true, nil, CreateMockDiskStore())
		builder := common.NewUpsertBatchBuilder()
		builder.AddColumn(0, common.Uint8)
		buffer, _ := builder.ToByteArray()
		upsertBatch, _ := NewUpsertBatch(buffer)
		err := memstore.HandleIngestion("abc", 0, upsertBatch)
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("returns error if fact table's value is invalid", func() {
		memstore := createMemStore("abc", 0, []common.DataType{common.Uint32, common.Uint8}, []int{1}, 10, true, nil, CreateMockDiskStore())
		builder := common.NewUpsertBatchBuilder()
		builder.AddColumn(0, common.Uint32)
		builder.AddColumn(1, common.Uint8)
		builder.AddRow()
		builder.SetValue(0, 1, uint8(123))
		buffer, _ := builder.ToByteArray()
		upsertBatch, _ := NewUpsertBatch(buffer)
		err := memstore.HandleIngestion("abc", 0, upsertBatch)
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("works for fact table", func() {
		memstore := createMemStore("abc", 0, []common.DataType{common.Uint32, common.Uint8}, []int{1}, 10, true, nil, CreateMockDiskStore())
		builder := common.NewUpsertBatchBuilder()
		// Put event time to the 2nd column.
		builder.AddColumn(1, common.Uint8)
		builder.AddColumn(0, common.Uint32)
		builder.AddRow()
		builder.SetValue(0, 1, uint32(23456))
		builder.SetValue(0, 0, uint8(123))
		buffer, _ := builder.ToByteArray()
		upsertBatch, _ := NewUpsertBatch(buffer)
		shard, err := memstore.GetTableShard("abc", 0)

		err = memstore.HandleIngestion("abc", 0, upsertBatch)
		Ω(err).Should(BeNil())
		Ω(shard.LiveStore.lastModifiedTimePerColumn).Should(Equal([]uint32{23456, 23456}))

		Ω(shard.LiveStore.LastReadRecord.BatchID).Should(Equal(BaseBatchID))

		value, valid := ReadShardValue(shard, 0, []byte{123})
		Ω(valid).Should(BeTrue())
		Ω(*(*uint32)(value)).Should(Equal(uint32(23456)))

		value, valid = ReadShardValue(shard, 1, []byte{123})
		Ω(valid).Should(BeTrue())
		Ω(*(*uint8)(value)).Should(Equal(uint8(123)))

		Ω(shard.LiveStore.LastReadRecord.Index).Should(Equal(uint32(1)))
		redoFile := shard.LiveStore.RedoLogManager.CurrentFileCreationTime

		// advance batch offset by 1.
		err = memstore.HandleIngestion("abc", 0, upsertBatch)
		Ω(err).Should(BeNil())
		Ω(shard.LiveStore.RedoLogManager.MaxEventTimePerFile[redoFile]).Should(Equal(uint32(23456)))
		Ω(shard.LiveStore.BackfillManager.CurrentRedoFile).Should(BeEquivalentTo(redoFile))
		Ω(shard.LiveStore.BackfillManager.CurrentBatchOffset).Should(BeEquivalentTo(1))
	})

	ginkgo.It("works for dimension table", func() {
		memstore := createMemStore("abc", 0, []common.DataType{common.Uint32, common.Uint8}, []int{1}, 10, false, nil, CreateMockDiskStore())
		builder := common.NewUpsertBatchBuilder()
		// Put event time to the 2nd column.
		builder.AddColumn(1, common.Uint8)
		builder.AddColumn(0, common.Uint32)
		builder.AddRow()
		builder.SetValue(0, 1, uint32(23456))
		builder.SetValue(0, 0, uint8(123))
		buffer, _ := builder.ToByteArray()
		upsertBatch, _ := NewUpsertBatch(buffer)
		shard, err := memstore.GetTableShard("abc", 0)

		err = memstore.HandleIngestion("abc", 0, upsertBatch)
		Ω(err).Should(BeNil())
		Ω(shard.LiveStore.lastModifiedTimePerColumn).Should(BeNil())

		Ω(shard.LiveStore.LastReadRecord.BatchID).Should(Equal(BaseBatchID))

		value, valid := ReadShardValue(shard, 0, []byte{123})
		Ω(valid).Should(BeTrue())
		Ω(*(*uint32)(value)).Should(Equal(uint32(23456)))

		value, valid = ReadShardValue(shard, 1, []byte{123})
		Ω(valid).Should(BeTrue())
		Ω(*(*uint8)(value)).Should(Equal(uint8(123)))

		Ω(shard.LiveStore.LastReadRecord.Index).Should(Equal(uint32(1)))
		redoFile := shard.LiveStore.RedoLogManager.CurrentFileCreationTime

		// advance batch offset by 1.
		err = memstore.HandleIngestion("abc", 0, upsertBatch)
		Ω(err).Should(BeNil())
		Ω(shard.LiveStore.RedoLogManager.MaxEventTimePerFile[redoFile]).Should(Equal(uint32(0)))
		Ω(shard.LiveStore.SnapshotManager.CurrentRedoFile).Should(BeEquivalentTo(redoFile))
		Ω(shard.LiveStore.SnapshotManager.CurrentBatchOffset).Should(BeEquivalentTo(1))
	})

	ginkgo.It("returns error for unrecognized table", func() {
		memstore := createMemStore("abc", 0, []common.DataType{common.Uint8}, []int{0}, 10, false, nil, CreateMockDiskStore())
		builder := common.NewUpsertBatchBuilder()
		builder.AddColumn(0, common.Uint8)
		buffer, _ := builder.ToByteArray()
		upsertBatch, _ := NewUpsertBatch(buffer)

		err := memstore.HandleIngestion("def", 0, upsertBatch)
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("returns error for unrecognized columns", func() {
		memstore := createMemStore("abc", 0, []common.DataType{common.Uint8}, []int{0}, 10, false, nil, CreateMockDiskStore())
		builder := common.NewUpsertBatchBuilder()
		builder.AddColumn(0, common.Uint8)
		builder.AddColumn(1, common.Uint8)
		buffer, _ := builder.ToByteArray()
		upsertBatch, _ := NewUpsertBatch(buffer)

		err := memstore.HandleIngestion("abc", 0, upsertBatch)
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("returns error for mismatched column type", func() {
		memstore := createMemStore("abc", 0, []common.DataType{common.Uint8}, []int{0}, 10, false, nil, CreateMockDiskStore())
		builder := common.NewUpsertBatchBuilder()
		builder.AddColumn(0, common.Uint16)
		buffer, _ := builder.ToByteArray()
		upsertBatch, _ := NewUpsertBatch(buffer)

		err := memstore.HandleIngestion("abc", 0, upsertBatch)
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("ingestion works for overwrite", func() {
		memstore := createMemStore("abc", 0, []common.DataType{common.Uint8, common.Uint8}, []int{0}, 10, false, nil, CreateMockDiskStore())
		builder := common.NewUpsertBatchBuilder()
		builder.AddColumnWithUpdateMode(0, common.Uint8, common.UpdateOverwriteNotNull)
		builder.AddColumnWithUpdateMode(1, common.Uint8, common.UpdateOverwriteNotNull)
		builder.AddRow()
		builder.SetValue(0, 0, uint8(123))
		builder.SetValue(0, 1, uint8(1))
		buffer, _ := builder.ToByteArray()
		upsertBatch, _ := NewUpsertBatch(buffer)
		err := memstore.HandleIngestion("abc", 0, upsertBatch)
		Ω(err).Should(BeNil())

		shard, err := memstore.GetTableShard("abc", 0)
		Ω(shard.LiveStore.LastReadRecord.BatchID).Should(Equal(BaseBatchID))
		Ω(shard.LiveStore.lastModifiedTimePerColumn).Should(BeNil())
		value, valid := ReadShardValue(shard, 1, []byte{123})
		Ω(valid).Should(BeTrue())
		Ω(*(*uint8)(value)).Should(Equal(uint8(1)))

		Ω(shard.LiveStore.LastReadRecord.Index).Should(Equal(uint32(1)))

		builder = common.NewUpsertBatchBuilder()
		builder.AddColumnWithUpdateMode(0, common.Uint8, common.UpdateOverwriteNotNull)
		builder.AddColumnWithUpdateMode(1, common.Uint8, common.UpdateOverwriteNotNull)
		builder.AddRow()
		builder.SetValue(0, 0, uint8(123))
		builder.SetValue(0, 1, uint8(2))
		buffer, _ = builder.ToByteArray()
		upsertBatch, _ = NewUpsertBatch(buffer)
		err = memstore.HandleIngestion("abc", 0, upsertBatch)
		Ω(err).Should(BeNil())

		shard, err = memstore.GetTableShard("abc", 0)
		Ω(shard.LiveStore.LastReadRecord.BatchID).Should(Equal(BaseBatchID))
		Ω(shard.LiveStore.lastModifiedTimePerColumn).Should(BeNil())

		value, valid = ReadShardValue(shard, 1, []byte{123})
		Ω(valid).Should(BeTrue())
		Ω(*(*uint8)(value)).Should(Equal(uint8(2)))

		builder = common.NewUpsertBatchBuilder()
		builder.AddColumnWithUpdateMode(0, common.Uint8, common.UpdateOverwriteNotNull)
		builder.AddColumnWithUpdateMode(1, common.Uint8, common.UpdateOverwriteNotNull)
		builder.AddRow()
		builder.SetValue(0, 0, uint8(123))
		builder.SetValue(0, 1, nil)
		buffer, _ = builder.ToByteArray()
		upsertBatch, _ = NewUpsertBatch(buffer)
		err = memstore.HandleIngestion("abc", 0, upsertBatch)
		Ω(err).Should(BeNil())

		shard, err = memstore.GetTableShard("abc", 0)
		Ω(shard.LiveStore.LastReadRecord.BatchID).Should(Equal(BaseBatchID))
		Ω(shard.LiveStore.lastModifiedTimePerColumn).Should(BeNil())

		value, valid = ReadShardValue(shard, 1, []byte{123})
		Ω(valid).Should(BeTrue())
		Ω(*(*uint8)(value)).Should(Equal(uint8(2)))

		builder = common.NewUpsertBatchBuilder()
		builder.AddColumnWithUpdateMode(0, common.Uint8, common.UpdateOverwriteNotNull)
		builder.AddColumnWithUpdateMode(1, common.Uint8, common.UpdateForceOverwrite)
		builder.AddRow()
		builder.SetValue(0, 0, uint8(123))
		builder.SetValue(0, 1, nil)
		buffer, _ = builder.ToByteArray()
		upsertBatch, _ = NewUpsertBatch(buffer)
		err = memstore.HandleIngestion("abc", 0, upsertBatch)
		Ω(err).Should(BeNil())

		shard, err = memstore.GetTableShard("abc", 0)
		Ω(shard.LiveStore.LastReadRecord.BatchID).Should(Equal(BaseBatchID))
		Ω(shard.LiveStore.lastModifiedTimePerColumn).Should(BeNil())

		value, valid = ReadShardValue(shard, 1, []byte{123})
		Ω(valid).ShouldNot(BeTrue())
		Ω((*uint8)(value)).Should(BeNil())

		builder = common.NewUpsertBatchBuilder()
		builder.AddColumnWithUpdateMode(0, common.Uint8, common.UpdateOverwriteNotNull)
		builder.AddColumnWithUpdateMode(1, common.Uint8, common.UpdateForceOverwrite)
		builder.AddRow()
		builder.SetValue(0, 0, uint8(123))
		builder.SetValue(0, 1, 3)
		buffer, _ = builder.ToByteArray()
		upsertBatch, _ = NewUpsertBatch(buffer)
		err = memstore.HandleIngestion("abc", 0, upsertBatch)
		Ω(err).Should(BeNil())

		shard, err = memstore.GetTableShard("abc", 0)
		Ω(shard.LiveStore.LastReadRecord.BatchID).Should(Equal(BaseBatchID))
		Ω(shard.LiveStore.lastModifiedTimePerColumn).Should(BeNil())

		value, valid = ReadShardValue(shard, 1, []byte{123})
		Ω(valid).Should(BeTrue())
		Ω(*(*uint8)(value)).Should(Equal(uint8(3)))
	})

	ginkgo.It("ingestion works for addition func", func() {
		memstore := createMemStore("abc", 0, []common.DataType{common.Uint8, common.Uint8}, []int{0}, 10, false, nil, CreateMockDiskStore())
		builder := common.NewUpsertBatchBuilder()
		builder.AddColumn(0, common.Uint8)
		builder.AddColumnWithUpdateMode(1, common.Uint8, common.UpdateOverwriteNotNull)
		builder.AddRow()
		builder.SetValue(0, 0, uint8(123))
		builder.SetValue(0, 1, uint8(1))
		buffer, _ := builder.ToByteArray()
		upsertBatch, _ := NewUpsertBatch(buffer)
		err := memstore.HandleIngestion("abc", 0, upsertBatch)
		Ω(err).Should(BeNil())

		shard, err := memstore.GetTableShard("abc", 0)
		Ω(shard.LiveStore.LastReadRecord.BatchID).Should(Equal(BaseBatchID))
		Ω(shard.LiveStore.lastModifiedTimePerColumn).Should(BeNil())
		value, valid := ReadShardValue(shard, 1, []byte{123})
		Ω(valid).Should(BeTrue())
		Ω(*(*uint8)(value)).Should(Equal(uint8(1)))

		Ω(shard.LiveStore.LastReadRecord.Index).Should(Equal(uint32(1)))

		builder = common.NewUpsertBatchBuilder()
		builder.AddColumn(0, common.Uint8)
		builder.AddColumnWithUpdateMode(1, common.Uint8, common.UpdateWithAddition)
		builder.AddRow()
		builder.SetValue(0, 0, uint8(123))
		builder.SetValue(0, 1, uint8(2))
		buffer, _ = builder.ToByteArray()
		upsertBatch, _ = NewUpsertBatch(buffer)
		err = memstore.HandleIngestion("abc", 0, upsertBatch)
		Ω(err).Should(BeNil())

		shard, err = memstore.GetTableShard("abc", 0)
		Ω(shard.LiveStore.LastReadRecord.BatchID).Should(Equal(BaseBatchID))
		Ω(shard.LiveStore.lastModifiedTimePerColumn).Should(BeNil())
		value, valid = ReadShardValue(shard, 1, []byte{123})
		Ω(valid).Should(BeTrue())
		Ω(*(*uint8)(value)).Should(Equal(uint8(3)))
	})
})
