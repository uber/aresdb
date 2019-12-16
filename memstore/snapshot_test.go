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
	diskMocks "github.com/uber/aresdb/diskstore/mocks"
	metaMocks "github.com/uber/aresdb/metastore/mocks"
	utilsMocks "github.com/uber/aresdb/utils/mocks"

	"fmt"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
	memCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/testing"
	"github.com/uber/aresdb/utils"
	"io"
	"math"
	"os"
	"path/filepath"
)

// formt /tmp/data/myTable_0/snapshots/1499970253_200/-2147483648/1.data
func getSnapshotFilePath(table string, shard int,
	redoLogFile int64, offset uint32, batchID int, columnID int) string {
	return fmt.Sprintf("/tmp/data/%s_%d/snapshots/%d_%d/%d/%d.data", table, shard, redoLogFile, offset, batchID, columnID)
}

func openSnapshotVectorPartyFileForWrite(table string, shard int,
	redoLogFile int64, offset uint32, batchID int, columnID int) (io.WriteCloser, error) {
	snapshotFilePath := getSnapshotFilePath(table, shard, redoLogFile, offset, batchID, columnID)
	dir := filepath.Dir(snapshotFilePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(snapshotFilePath, os.O_CREATE|os.O_WRONLY, 0644)
	os.Truncate(snapshotFilePath, 0)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func openSnapshotVectorPartyFileForRead(table string, shard int,
	redoLogFile int64, offset uint32, batchID int, columnID int) (io.ReadCloser, error) {
	snapshotFilePath := getSnapshotFilePath(table, shard, redoLogFile, offset, batchID, columnID)
	f, err := os.OpenFile(snapshotFilePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	return f, nil
}

var _ = ginkgo.Describe("snapshot", func() {

	const (
		batchSize          = 10
		tableName          = "cities"
		redoLogFile int64  = 1518128587
		offset      uint32 = 100
		rows               = 15
	)

	diskStore := &diskMocks.DiskStore{}
	metaStore := &metaMocks.MetaStore{}

	diskStore.On("OpenLogFileForAppend", mock.Anything, mock.Anything, mock.Anything).Return(&testing.TestReadWriteSyncCloser{}, nil)

	lastBatchID := int32(math.MinInt32 + 1)
	lastIndex := uint32(5)

	currentRecord := memCom.RecordID{
		BatchID: lastBatchID,
		Index:   lastIndex,
	}

	var memStore *memStoreImpl
	var shard *TableShard

	ingestValues := func() error {
		builder := memCom.NewUpsertBatchBuilder()
		builder.AddColumn(0, memCom.Uint16)
		builder.AddColumn(1, memCom.SmallEnum)
		builder.AddColumn(2, memCom.UUID)
		for i := 0; i < rows; i++ {
			builder.AddRow()
			builder.SetValue(i, 0, uint16(i+1))
			builder.SetValue(i, 1, uint8(i%5))
			builder.SetValue(i, 2, "A424DBC38B0543F9A3A42CF0FC310A39")
		}
		buffer, _ := builder.ToByteArray()
		upsertBatch, _ := memCom.NewUpsertBatch(buffer)
		return memStore.HandleIngestion(tableName, 0, upsertBatch)
	}

	createSnapshot := func() {
		snapshotJobM := &snapshotJobManager{
			jobDetails: make(map[string]*SnapshotJobDetail),
			memStore:   memStore,
		}
		shard.LiveStore.SnapshotManager.LastRedoFile = 0
		shard.LiveStore.SnapshotManager.ApplyUpsertBatch(redoLogFile, offset, 10, currentRecord)

		diskStore.On(
			"OpenSnapshotVectorPartyFileForWrite", tableName, 0, redoLogFile, offset, int(lastBatchID-1), 0).
			Return(openSnapshotVectorPartyFileForWrite(tableName, 0, redoLogFile, offset, int(lastBatchID-1), 0))
		diskStore.On(
			"OpenSnapshotVectorPartyFileForWrite", tableName, 0, redoLogFile, offset, int(lastBatchID-1), 1).
			Return(openSnapshotVectorPartyFileForWrite(tableName, 0, redoLogFile, offset, int(lastBatchID-1), 1))
		diskStore.On(
			"OpenSnapshotVectorPartyFileForWrite", tableName, 0, redoLogFile, offset, int(lastBatchID-1), 2).
			Return(openSnapshotVectorPartyFileForWrite(tableName, 0, redoLogFile, offset, int(lastBatchID-1), 2))
		diskStore.On(
			"OpenSnapshotVectorPartyFileForWrite", tableName, 0, redoLogFile, offset, int(lastBatchID), 0).
			Return(openSnapshotVectorPartyFileForWrite(tableName, 0, redoLogFile, offset, int(lastBatchID), 0))
		diskStore.On(
			"OpenSnapshotVectorPartyFileForWrite", tableName, 0, redoLogFile, offset, int(lastBatchID), 1).
			Return(openSnapshotVectorPartyFileForWrite(tableName, 0, redoLogFile, offset, int(lastBatchID), 1))
		diskStore.On(
			"OpenSnapshotVectorPartyFileForWrite", tableName, 0, redoLogFile, offset, int(lastBatchID), 2).
			Return(openSnapshotVectorPartyFileForWrite(tableName, 0, redoLogFile, offset, int(lastBatchID), 2))

		metaStore.On("UpdateSnapshotProgress", tableName, 0, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		err := memStore.Snapshot(tableName, 0, snapshotJobM.reportSnapshotJobDetail)
		Ω(err).Should(BeNil())
	}

	ginkgo.AfterEach(func() {
		os.RemoveAll("/tmp/data")
	})

	ginkgo.BeforeEach(func() {
		memStore = createMemStore(tableName, 0, []memCom.DataType{memCom.Uint16, memCom.SmallEnum, memCom.UUID, memCom.Uint32},
			[]int{0}, batchSize, false, false, metaStore, diskStore)
		shard, _ = memStore.GetTableShard(tableName, 0)
	})

	ginkgo.It("snapshot should have no error", func() {
		err := ingestValues()
		Ω(err).Should(BeNil())

		snapshotJobM := &snapshotJobManager{
			jobDetails: make(map[string]*SnapshotJobDetail),
			memStore:   memStore,
		}
		shard.LiveStore.SnapshotManager.LastRedoFile = 0
		shard.LiveStore.SnapshotManager.ApplyUpsertBatch(redoLogFile+10, offset, 10, currentRecord)

		writer := new(utilsMocks.WriteSyncCloser)
		writer.On("Write", mock.Anything).Return(0, nil)
		writer.On("Close").Return(nil)
		writer.On("Sync").Return(nil)

		diskStore.On(
			"OpenSnapshotVectorPartyFileForWrite", tableName, 0, redoLogFile+10, offset, mock.Anything, mock.Anything).
			Return(writer, nil)
		diskStore.On("DeleteSnapshot", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		metaStore.On("UpdateSnapshotProgress", tableName, 0, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		err = memStore.Snapshot(tableName, 0, snapshotJobM.reportSnapshotJobDetail)
		Ω(err).Should(BeNil())

		lastRedoLogFile, lastOffset, _, lastRecordID := shard.LiveStore.SnapshotManager.GetLastSnapshotInfo()
		Ω(lastRedoLogFile).Should(Equal(redoLogFile + 10))
		Ω(lastOffset).Should(Equal(offset))
		Ω(lastRecordID).Should(Equal(currentRecord))
		diskStore.AssertCalled(utils.TestingT, "DeleteSnapshot", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})

	ginkgo.It("dimension table snapshot and recovery", func() {
		err := ingestValues()
		Ω(err).Should(BeNil())

		// generate snapshot files
		createSnapshot()
		lastRedoLogFile, lastOffset, _, lastRecordID := shard.LiveStore.SnapshotManager.GetLastSnapshotInfo()
		Ω(lastRedoLogFile).Should(Equal(redoLogFile))
		Ω(lastOffset).Should(Equal(offset))
		Ω(lastRecordID).Should(Equal(currentRecord))
		fInfo, err := os.Stat("/tmp/data/cities_0/snapshots/1518128587_100/-2147483648/1.data")
		Ω(err).Should(BeNil())
		Ω(fInfo.Size() > 0).Should(BeTrue())

		// using new instance to avoid data conflict
		memStore = createMemStore(tableName, 0, []memCom.DataType{memCom.Uint16, memCom.SmallEnum, memCom.UUID, memCom.Uint32},
			[]int{0}, batchSize, false, false, metaStore, diskStore)
		shard, _ = memStore.GetTableShard(tableName, 0)

		shard.LiveStore.SnapshotManager.ApplyUpsertBatch(0, 0, 0, memCom.RecordID{})
		shard.LiveStore.SnapshotManager.SetLastSnapshotInfo(redoLogFile, offset, currentRecord)

		batchIDs := []int{int(lastBatchID - 1), int(lastBatchID)}
		colIDs := []int{0, 1, 2}
		diskStore.On("ListSnapshotBatches", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(batchIDs, nil)
		diskStore.On("ListSnapshotVectorPartyFiles", tableName, 0, redoLogFile, offset, mock.Anything).Return(colIDs, nil)

		diskStore.On(
			"OpenSnapshotVectorPartyFileForRead", tableName, 0, redoLogFile, offset, int(lastBatchID-1), 0).
			Return(openSnapshotVectorPartyFileForRead(tableName, 0, redoLogFile, offset, int(lastBatchID-1), 0))
		diskStore.On(
			"OpenSnapshotVectorPartyFileForRead", tableName, 0, redoLogFile, offset, int(lastBatchID-1), 1).
			Return(openSnapshotVectorPartyFileForRead(tableName, 0, redoLogFile, offset, int(lastBatchID-1), 1))
		diskStore.On(
			"OpenSnapshotVectorPartyFileForRead", tableName, 0, redoLogFile, offset, int(lastBatchID-1), 2).
			Return(openSnapshotVectorPartyFileForRead(tableName, 0, redoLogFile, offset, int(lastBatchID-1), 2))
		diskStore.On(
			"OpenSnapshotVectorPartyFileForRead", tableName, 0, redoLogFile, offset, int(lastBatchID), 0).
			Return(openSnapshotVectorPartyFileForRead(tableName, 0, redoLogFile, offset, int(lastBatchID), 0))
		diskStore.On(
			"OpenSnapshotVectorPartyFileForRead", tableName, 0, redoLogFile, offset, int(lastBatchID), 1).
			Return(openSnapshotVectorPartyFileForRead(tableName, 0, redoLogFile, offset, int(lastBatchID), 1))
		diskStore.On(
			"OpenSnapshotVectorPartyFileForRead", tableName, 0, redoLogFile, offset, int(lastBatchID), 2).
			Return(openSnapshotVectorPartyFileForRead(tableName, 0, redoLogFile, offset, int(lastBatchID), 2))

		err = shard.LoadSnapshot()
		Ω(err).Should(BeNil())
		// check if the pointer advanced
		Ω(shard.LiveStore.NextWriteRecord.BatchID).Should(Equal(lastBatchID))
		Ω(shard.LiveStore.NextWriteRecord.Index).Should(Equal(lastIndex))
		Ω(shard.LiveStore.LastReadRecord.BatchID).Should(Equal(lastBatchID))
		Ω(shard.LiveStore.LastReadRecord.Index).Should(Equal(lastIndex))

		// check if the primray key rebuilt and can data be found
		primaryKeyBytes := shard.Schema.PrimaryKeyBytes
		key := make([]byte, primaryKeyBytes)
		primaryKeyValues := make([]memCom.DataValue, 1)

		for row := 0; row <= rows; row++ {
			primaryKeyValues[0], _ = memCom.ValueFromString(fmt.Sprintf("%d", row+1), memCom.Uint16)
			// truncate key
			key = key[:0]
			// write primary key bytes
			key, err = memCom.AppendPrimaryKeyBytes(key, memCom.NewSliceDataValueIterator(primaryKeyValues))

			Ω(err).Should(BeNil())
			record, found := shard.LiveStore.PrimaryKey.Find(key)
			if row != rows {
				Ω(found).Should(BeTrue())
				Ω(record.BatchID).Should(Equal(lastBatchID - 1 + int32(row/10)))
				Ω(record.Index).Should(Equal(uint32(row % 10)))
			} else {
				Ω(found).Should(BeFalse())
			}
		}
	})

	ginkgo.It("dimension table snapshot on deleted table should not return error", func() {
		snapshotJobM := &snapshotJobManager{
			jobDetails: make(map[string]*SnapshotJobDetail),
			memStore:   memStore,
		}
		err := memStore.Snapshot("NonExistTable", 0, snapshotJobM.reportSnapshotJobDetail)
		Ω(err).Should(BeNil())
	})
})
