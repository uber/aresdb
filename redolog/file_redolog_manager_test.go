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

package redolog

import (
	"time"

	"sort"

	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
	"github.com/uber/aresdb/diskstore/mocks"
	"github.com/uber/aresdb/common"
	memCom"github.com/uber/aresdb/memstore/common"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/testing"
	"github.com/uber/aresdb/utils"
)

func CreateMockDiskStore() *mocks.DiskStore {
	diskStore := &mocks.DiskStore{}
	diskStore.On("OpenLogFileForAppend", mock.Anything, mock.Anything, mock.Anything).Return(&testing.TestReadWriteCloser{}, nil)
	return diskStore
}

var _ = ginkgo.Describe("redo_log_manager", func() {
	table := "abc"
	shard := 0
	tableConfig := &metaCom.TableConfig{
		RedoLogRotationInterval: 10,
		MaxRedoLogFileSize:      1 << 30,
	}

	redoLogCfg := &common.RedoLogConfig{
		DiskConfig: common.DiskRedoLogConfig{
			Disabled: false,
		},
	}

	ginkgo.It("create new redo log file if there's no redo file", func() {
		utils.SetClockImplementation(func() time.Time {
			return time.Unix(int64(5), 0)
		})

		f, _ := NewRedoLogManagerMaster(redoLogCfg, CreateMockDiskStore(), nil)
		m, _ := f.NewRedologManager(table, shard, tableConfig)
		redoManager := m.(*FileRedoLogManager)

		Ω(redoManager.currentLogFile).Should(BeNil())

		buffer, _ := memCom.NewUpsertBatchBuilder().ToByteArray()
		upsertBatch, _ := memCom.NewUpsertBatch(buffer)

		redoManager.AppendToRedoLog(upsertBatch)
		Ω(redoManager.currentLogFile).ShouldNot(BeNil())
		Ω(redoManager.CurrentFileCreationTime).Should(Equal(int64(5)))
		Ω(len(redoManager.MaxEventTimePerFile)).Should(Equal(1))

		utils.ResetClockImplementation()
	})

	ginkgo.It("reuse previous redo log file if the previous one is not too old", func() {
		utils.SetClockImplementation(func() time.Time {
			return time.Unix(int64(5), 0)
		})

		f, _ := NewRedoLogManagerMaster(redoLogCfg, CreateMockDiskStore(), nil)
		m, _ := f.NewRedologManager(table, shard, tableConfig)
		redoManager := m.(*FileRedoLogManager)

		buffer, _ := memCom.NewUpsertBatchBuilder().ToByteArray()
		upsertBatch, _ := memCom.NewUpsertBatch(buffer)

		redoManager.AppendToRedoLog(upsertBatch)
		Ω(redoManager.currentLogFile).ShouldNot(BeNil())
		Ω(redoManager.CurrentFileCreationTime).Should(Equal(int64(5)))

		utils.SetClockImplementation(func() time.Time {
			return time.Unix(int64(7), 0)
		})

		redoManager.AppendToRedoLog(upsertBatch)
		Ω(redoManager.currentLogFile).ShouldNot(BeNil())
		Ω(redoManager.CurrentFileCreationTime).Should(Equal(int64(5)))

		utils.ResetClockImplementation()
	})

	ginkgo.It("rotate new redo log file if the previous one is too old", func() {
		utils.SetClockImplementation(func() time.Time {
			return time.Unix(int64(5), 0)
		})
		f, _ := NewRedoLogManagerMaster(redoLogCfg, CreateMockDiskStore(), nil)
		m, _ := f.NewRedologManager(table, shard, tableConfig)
		redoManager := m.(*FileRedoLogManager)

		buffer, _ := memCom.NewUpsertBatchBuilder().ToByteArray()
		upsertBatch, _ := memCom.NewUpsertBatch(buffer)

		redoManager.AppendToRedoLog(upsertBatch)
		redoManager.MaxEventTimePerFile[redoManager.CurrentFileCreationTime] = uint32(234)
		Ω(redoManager.currentLogFile).ShouldNot(BeNil())
		Ω(redoManager.CurrentFileCreationTime).Should(Equal(int64(5)))

		utils.SetClockImplementation(func() time.Time {
			return time.Unix(int64(15), 0)
		})

		redoManager.AppendToRedoLog(upsertBatch)
		Ω(redoManager.currentLogFile).ShouldNot(BeNil())
		Ω(redoManager.CurrentFileCreationTime).Should(Equal(int64(15)))
		Ω(redoManager.MaxEventTimePerFile).ShouldNot(BeEmpty())
		Ω(redoManager.MaxEventTimePerFile[5]).Should(Equal(uint32(234)))
		Ω(redoManager.MaxEventTimePerFile[15]).Should(Equal(uint32(0)))

		utils.ResetClockImplementation()
	})

	ginkgo.It("works for Iterator iterator with 0 files", func() {
		diskStore := &mocks.DiskStore{}
		diskStore.On("ListLogFiles", mock.Anything, mock.Anything).Return([]int64{}, nil)
		f, _ := NewRedoLogManagerMaster(redoLogCfg, diskStore, nil)
		m, _ := f.NewRedologManager(table, shard, tableConfig)
		redoManager := m.(*FileRedoLogManager)

		nextUpsertBatch, err := redoManager.Iterator()
		Ω(err).Should(BeNil())
		Ω(nextUpsertBatch()).Should(BeNil())
		diskStore.AssertExpectations(utils.TestingT)
	})

	ginkgo.It("works for Iterator iterator with 3 batches in 2 file", func() {
		buffer, _ := memCom.NewUpsertBatchBuilder().ToByteArray()

		file1 := &testing.TestReadWriteCloser{}
		streamWriter1 := utils.NewStreamDataWriter(file1)
		streamWriter1.WriteUint32(UpsertHeader)
		streamWriter1.WriteUint32(uint32(len(buffer)))
		streamWriter1.Write(buffer)
		streamWriter1.WriteUint32(uint32(len(buffer)))
		streamWriter1.Write(buffer)

		file2 := &testing.TestReadWriteCloser{}
		streamWriter2 := utils.NewStreamDataWriter(file2)
		streamWriter2.WriteUint32(UpsertHeader)
		streamWriter2.WriteUint32(uint32(len(buffer)))
		streamWriter2.Write(buffer)

		diskStore := &mocks.DiskStore{}
		diskStore.On("ListLogFiles", mock.Anything, mock.Anything).Return([]int64{1, 2}, nil)
		diskStore.On("OpenLogFileForReplay", mock.Anything, mock.Anything, int64(1)).Return(file1, nil)
		diskStore.On("OpenLogFileForReplay", mock.Anything, mock.Anything, int64(2)).Return(file2, nil)

		f, _ := NewRedoLogManagerMaster(redoLogCfg, diskStore, nil)
		m, _ := f.NewRedologManager(table, shard, tableConfig)
		redoManager := m.(*FileRedoLogManager)

		nextUpsertBatch, _ := redoManager.Iterator()

		batchInfo := nextUpsertBatch()
		Ω(batchInfo.Batch).ShouldNot(BeNil())
		Ω(batchInfo.RedoLogFile).Should(Equal(int64(1)))

		batchInfo  = nextUpsertBatch()
		Ω(batchInfo.Batch).ShouldNot(BeNil())
		Ω(batchInfo.RedoLogFile).Should(Equal(int64(1)))

		batchInfo = nextUpsertBatch()
		Ω(batchInfo.Batch).ShouldNot(BeNil())
		Ω(batchInfo.RedoLogFile).Should(Equal(int64(2)))

		batchInfo = nextUpsertBatch()
		Ω(batchInfo).Should(BeNil())

		diskStore.AssertExpectations(utils.TestingT)
	})

	ginkgo.It("truncate redo log file works for invalid size", func() {
		buffer, _ := memCom.NewUpsertBatchBuilder().ToByteArray()
		correctBufferSize := len(buffer)

		file1 := &testing.TestReadWriteCloser{}
		streamWriter1 := utils.NewStreamDataWriter(file1)
		streamWriter1.WriteUint32(UpsertHeader)
		streamWriter1.WriteUint32(uint32(len(buffer)))
		streamWriter1.Write(buffer)
		streamWriter1.WriteUint32(uint32(len(buffer)))
		streamWriter1.Write(buffer)

		file2 := &testing.TestReadWriteCloser{}
		streamWriter2 := utils.NewStreamDataWriter(file2)
		streamWriter2.WriteUint32(UpsertHeader)
		streamWriter2.WriteUint32(uint32(len(buffer)))
		streamWriter2.Write(buffer)
		streamWriter2.WriteInt32(0)

		diskStore := &mocks.DiskStore{}
		diskStore.On("ListLogFiles", mock.Anything, mock.Anything).Return([]int64{1, 2}, nil)
		diskStore.On("OpenLogFileForReplay", mock.Anything, mock.Anything, int64(1)).Return(file1, nil)
		diskStore.On("OpenLogFileForReplay", mock.Anything, mock.Anything, int64(2)).Return(file2, nil)
		// magic header (uint32) + size (uint32) + correctBufferSize
		diskStore.On("TruncateLogFile", "abc", 0, int64(2), int64(4+4+correctBufferSize)).Return(nil)

		f, _ := NewRedoLogManagerMaster(redoLogCfg, diskStore, nil)
		m, _ := f.NewRedologManager(table, shard, tableConfig)
		redoManager := m.(*FileRedoLogManager)

		nextUpsertBatch, _ := redoManager.Iterator()

		batchInfo := nextUpsertBatch()
		Ω(batchInfo.Batch).ShouldNot(BeNil())
		Ω(batchInfo.RedoLogFile).Should(Equal(int64(1)))

		batchInfo = nextUpsertBatch()
		Ω(batchInfo.Batch).ShouldNot(BeNil())
		Ω(batchInfo.RedoLogFile).Should(Equal(int64(1)))

		batchInfo = nextUpsertBatch()
		Ω(batchInfo.Batch).ShouldNot(BeNil())
		Ω(batchInfo.RedoLogFile).Should(Equal(int64(2)))

		// Last batch is truncated.
		batchInfo = nextUpsertBatch()
		Ω(batchInfo).Should(BeNil())

		diskStore.AssertExpectations(utils.TestingT)
	})

	ginkgo.It("truncate redo log file works for invalid upsert batch", func() {
		buffer, _ := memCom.NewUpsertBatchBuilder().ToByteArray()
		correctBufferSize := len(buffer)

		file1 := &testing.TestReadWriteCloser{}
		streamWriter1 := utils.NewStreamDataWriter(file1)
		streamWriter1.WriteUint32(UpsertHeader)
		streamWriter1.WriteUint32(uint32(len(buffer)))
		streamWriter1.Write(buffer)
		streamWriter1.WriteUint32(uint32(len(buffer)))
		streamWriter1.Write(buffer)

		file2 := &testing.TestReadWriteCloser{}
		streamWriter2 := utils.NewStreamDataWriter(file2)
		streamWriter2.WriteUint32(UpsertHeader)
		streamWriter2.WriteUint32(uint32(len(buffer)))
		streamWriter2.Write(buffer)
		// Only have two bytes.
		streamWriter2.WriteInt32(20)
		b := [20]byte{2, 3, 3, 3, 3, 3, 1, 2, 2}
		streamWriter2.Write(b[:])

		diskStore := &mocks.DiskStore{}
		diskStore.On("ListLogFiles", mock.Anything, mock.Anything).Return([]int64{1, 2}, nil)
		diskStore.On("OpenLogFileForReplay", mock.Anything, mock.Anything, int64(1)).Return(file1, nil)
		diskStore.On("OpenLogFileForReplay", mock.Anything, mock.Anything, int64(2)).Return(file2, nil)
		// magic header (uint32) + size (uint32) + correctBufferSize
		diskStore.On("TruncateLogFile", "abc", 0, int64(2), int64(4+4+correctBufferSize)).Return(nil)

		f, _ := NewRedoLogManagerMaster(redoLogCfg, diskStore, nil)
		m, _ := f.NewRedologManager(table, shard, tableConfig)
		redoManager := m.(*FileRedoLogManager)

		nextUpsertBatch, _ := redoManager.Iterator()

		batchInfo := nextUpsertBatch()
		Ω(batchInfo.Batch).ShouldNot(BeNil())
		Ω(batchInfo.RedoLogFile).Should(Equal(int64(1)))

		batchInfo = nextUpsertBatch()
		Ω(batchInfo.Batch).ShouldNot(BeNil())
		Ω(batchInfo.RedoLogFile).Should(Equal(int64(1)))

		batchInfo = nextUpsertBatch()
		Ω(batchInfo.Batch).ShouldNot(BeNil())
		Ω(batchInfo.RedoLogFile).Should(Equal(int64(2)))

		// Last batch is truncated.
		batchInfo = nextUpsertBatch()
		Ω(batchInfo).Should(BeNil())

		diskStore.AssertExpectations(utils.TestingT)
	})

	ginkgo.It("truncate redo log file works for insufficient buffer capacity", func() {
		buffer, _ := memCom.NewUpsertBatchBuilder().ToByteArray()
		correctBufferSize := len(buffer)

		file1 := &testing.TestReadWriteCloser{}
		streamWriter1 := utils.NewStreamDataWriter(file1)
		streamWriter1.WriteUint32(UpsertHeader)
		streamWriter1.WriteUint32(uint32(len(buffer)))
		streamWriter1.Write(buffer)
		streamWriter1.WriteUint32(uint32(len(buffer)))
		streamWriter1.Write(buffer)

		file2 := &testing.TestReadWriteCloser{}
		streamWriter2 := utils.NewStreamDataWriter(file2)
		streamWriter2.WriteUint32(UpsertHeader)
		streamWriter2.WriteUint32(uint32(len(buffer)))
		streamWriter2.Write(buffer)
		// Only have two bytes.
		streamWriter2.WriteInt32(28)
		b := [20]byte{2, 3, 3, 3, 3, 3, 1, 2, 2}
		streamWriter2.Write(b[:])

		diskStore := &mocks.DiskStore{}
		diskStore.On("ListLogFiles", mock.Anything, mock.Anything).Return([]int64{1, 2}, nil)
		diskStore.On("OpenLogFileForReplay", mock.Anything, mock.Anything, int64(1)).Return(file1, nil)
		diskStore.On("OpenLogFileForReplay", mock.Anything, mock.Anything, int64(2)).Return(file2, nil)
		// magic header (uint32) + size (uint32) + correctBufferSize
		diskStore.On("TruncateLogFile", "abc", 0, int64(2), int64(4+4+correctBufferSize)).Return(nil)

		f, _ := NewRedoLogManagerMaster(redoLogCfg, diskStore, nil)
		m, _ := f.NewRedologManager(table, shard, tableConfig)
		redoManager := m.(*FileRedoLogManager)

		nextUpsertBatch, _ := redoManager.Iterator()

		batchInfo := nextUpsertBatch()
		Ω(batchInfo.Batch).ShouldNot(BeNil())
		Ω(batchInfo.RedoLogFile).Should(Equal(int64(1)))

		batchInfo = nextUpsertBatch()
		Ω(batchInfo.Batch).ShouldNot(BeNil())
		Ω(batchInfo.RedoLogFile).Should(Equal(int64(1)))

		batchInfo = nextUpsertBatch()
		Ω(batchInfo.Batch).ShouldNot(BeNil())
		Ω(batchInfo.RedoLogFile).Should(Equal(int64(2)))

		// Last batch is truncated.
		batchInfo = nextUpsertBatch()
		Ω(batchInfo).Should(BeNil())

		diskStore.AssertExpectations(utils.TestingT)
	})

	ginkgo.It("truncate redo log file should continue to read next file", func() {
		buffer, _ := memCom.NewUpsertBatchBuilder().ToByteArray()
		correctBufferSize := len(buffer)

		file1 := &testing.TestReadWriteCloser{}
		streamWriter1 := utils.NewStreamDataWriter(file1)
		streamWriter1.WriteUint32(UpsertHeader)
		streamWriter1.WriteUint32(uint32(len(buffer)))
		streamWriter1.Write(buffer)
		streamWriter1.WriteUint32(uint32(len(buffer)))
		streamWriter1.Write(buffer)

		file2 := &testing.TestReadWriteCloser{}
		streamWriter2 := utils.NewStreamDataWriter(file2)
		streamWriter2.WriteUint32(UpsertHeader)
		streamWriter2.WriteUint32(uint32(len(buffer)))
		streamWriter2.Write(buffer)
		// Only have two bytes.
		streamWriter2.WriteInt32(28)
		b := [20]byte{2, 3, 3, 3, 3, 3, 1, 2, 2}
		streamWriter2.Write(b[:])

		file3 := &testing.TestReadWriteCloser{}
		streamWriter3 := utils.NewStreamDataWriter(file3)
		streamWriter3.WriteUint32(UpsertHeader)
		streamWriter3.WriteUint32(uint32(len(buffer)))
		streamWriter3.Write(buffer)

		diskStore := &mocks.DiskStore{}
		diskStore.On("ListLogFiles", mock.Anything, mock.Anything).Return([]int64{1, 2, 3}, nil)
		diskStore.On("OpenLogFileForReplay", mock.Anything, mock.Anything, int64(1)).Return(file1, nil)
		diskStore.On("OpenLogFileForReplay", mock.Anything, mock.Anything, int64(2)).Return(file2, nil)
		diskStore.On("OpenLogFileForReplay", mock.Anything, mock.Anything, int64(3)).Return(file3, nil)
		// magic header (uint32) + size (uint32) + correctBufferSize
		diskStore.On("TruncateLogFile", "abc", 0, int64(2), int64(4+4+correctBufferSize)).Return(nil)

		f, _ := NewRedoLogManagerMaster(redoLogCfg, diskStore, nil)
		m, _ := f.NewRedologManager(table, shard, tableConfig)
		redoManager := m.(*FileRedoLogManager)

		nextUpsertBatch, _ := redoManager.Iterator()

		batchInfo := nextUpsertBatch()
		Ω(batchInfo.Batch).ShouldNot(BeNil())
		Ω(batchInfo.RedoLogFile).Should(Equal(int64(1)))

		batchInfo = nextUpsertBatch()
		Ω(batchInfo.Batch).ShouldNot(BeNil())
		Ω(batchInfo.RedoLogFile).Should(Equal(int64(1)))

		batchInfo = nextUpsertBatch()
		Ω(batchInfo.Batch).ShouldNot(BeNil())
		Ω(batchInfo.RedoLogFile).Should(Equal(int64(2)))

		// Should be able to read batch in next file
		batchInfo = nextUpsertBatch()
		Ω(batchInfo.Batch).ShouldNot(BeNil())
		Ω(batchInfo.RedoLogFile).Should(Equal(int64(3)))

		batchInfo = nextUpsertBatch()
		Ω(batchInfo).Should(BeNil())

		diskStore.AssertExpectations(utils.TestingT)
	})

	ginkgo.It("works for Iterator iterator with empty file", func() {
		buffer, _ := memCom.NewUpsertBatchBuilder().ToByteArray()

		file1 := &testing.TestReadWriteCloser{}
		streamWriter1 := utils.NewStreamDataWriter(file1)
		streamWriter1.WriteUint32(UpsertHeader)

		file2 := &testing.TestReadWriteCloser{}
		streamWriter2 := utils.NewStreamDataWriter(file2)
		streamWriter2.WriteUint32(UpsertHeader)
		streamWriter2.WriteUint32(uint32(len(buffer)))
		streamWriter2.Write(buffer)

		file3 := &testing.TestReadWriteCloser{}
		streamWriter3 := utils.NewStreamDataWriter(file3)
		streamWriter3.WriteUint32(UpsertHeader)

		diskStore := &mocks.DiskStore{}
		diskStore.On("ListLogFiles", mock.Anything, mock.Anything).Return([]int64{1, 2, 3}, nil)
		diskStore.On("OpenLogFileForReplay", mock.Anything, mock.Anything, int64(1)).Return(file1, nil)
		diskStore.On("OpenLogFileForReplay", mock.Anything, mock.Anything, int64(2)).Return(file2, nil)
		diskStore.On("OpenLogFileForReplay", mock.Anything, mock.Anything, int64(3)).Return(file3, nil)
		f, _ := NewRedoLogManagerMaster(redoLogCfg, diskStore, nil)
		m, _ := f.NewRedologManager(table, shard, tableConfig)
		redoManager := m.(*FileRedoLogManager)

		nextUpsertBatch, _ := redoManager.Iterator()

		batchInfo := nextUpsertBatch()
		Ω(batchInfo.Batch).ShouldNot(BeNil())
		Ω(batchInfo.RedoLogFile).Should(Equal(int64(2)))

		batchInfo = nextUpsertBatch()
		Ω(batchInfo).Should(BeNil())
		diskStore.AssertExpectations(utils.TestingT)
	})

	ginkgo.It("getRedoLogFilesToPurge should work", func() {
		redoManager := newFileRedoLogManager(10, 1<<30, CreateMockDiskStore(), "abc", 0)
		redoManager.MaxEventTimePerFile[1] = 100
		redoManager.MaxEventTimePerFile[2] = 200
		redoManager.MaxEventTimePerFile[3] = 300
		redoManager.CurrentFileCreationTime = 3
		// batch counts
		redoManager.BatchCountPerFile[1] = 10
		redoManager.BatchCountPerFile[2] = 20
		redoManager.BatchCountPerFile[3] = 30

		Ω(redoManager.getRedoLogFilesToPurge(0, 0, 0)).Should(BeEmpty())
		Ω(redoManager.getRedoLogFilesToPurge(1, 0, 0)).Should(BeEmpty())
		Ω(redoManager.getRedoLogFilesToPurge(4, 0, 0)).Should(BeEmpty())

		// file '2' not fully backfill yet
		creationTimes := redoManager.getRedoLogFilesToPurge(400, 2, 15)
		sort.Sort(utils.Int64Array(creationTimes))
		Ω(creationTimes).Should(Equal([]int64{1}))

		// current file '3' shouldn't be included
		creationTimes = redoManager.getRedoLogFilesToPurge(400, 3, 29)
		sort.Sort(utils.Int64Array(creationTimes))
		Ω(creationTimes).Should(Equal([]int64{1, 2}))
	})

	ginkgo.It("CheckpointRedolog should work", func() {
		diskStore := CreateMockDiskStore()
		diskStore.On("DeleteLogFile", "abc", 0, mock.Anything).Return(nil)
		f, _ := NewRedoLogManagerMaster(redoLogCfg, diskStore, nil)
		m, _ := f.NewRedologManager(table, shard, tableConfig)
		redoManager := m.(*FileRedoLogManager)

		redoManager.MaxEventTimePerFile[1] = 100
		redoManager.MaxEventTimePerFile[2] = 200
		redoManager.MaxEventTimePerFile[3] = 300
		redoManager.CurrentFileCreationTime = 3
		redoManager.BatchCountPerFile[1] = 10
		redoManager.BatchCountPerFile[2] = 20
		redoManager.BatchCountPerFile[3] = 30

		err := m.CheckpointRedolog(400, 3, 29)
		Ω(err).Should(BeNil())
		Ω(redoManager.MaxEventTimePerFile).ShouldNot(HaveKey(1))
		Ω(redoManager.MaxEventTimePerFile).ShouldNot(HaveKey(2))
		Ω(redoManager.BatchCountPerFile).ShouldNot(HaveKey(1))
		Ω(redoManager.BatchCountPerFile).ShouldNot(HaveKey(2))
	})
})
