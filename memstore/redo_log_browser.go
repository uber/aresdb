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
	"io"

	"code.uber.internal/data/ares/diskstore"
	"code.uber.internal/data/ares/utils"
)

// RedoLogBrowser is the interface to list redo log files, upsert batches and read upsert batch data.
type RedoLogBrowser interface {
	ListLogFiles() ([]int64, error)
	ListUpsertBatch(creationTime int64) ([]int64, error)
	ReadData(creationTime int64, upsertBatchOffset int64, start int, length int) (
		[][]interface{}, []string, int, error)
}

// redoLogBrowser is the implementation of RedoLogBrowser.
type redoLogBrowser struct {
	tableName string
	shardID   int
	diskStore diskstore.DiskStore
	schema    *TableSchema
}

// ListLogFiles lists all log files of a given table Shard.
func (rb *redoLogBrowser) ListLogFiles() ([]int64, error) {
	return rb.diskStore.ListLogFiles(rb.tableName, rb.shardID)
}

// ListUpsertBatches opens corresponding redo log file given creation time and returns starting offsets of upsert batches
// in this file.
func (rb *redoLogBrowser) ListUpsertBatch(creationTime int64) ([]int64, error) {
	var f utils.ReaderSeekerCloser
	var err error
	if f, err = rb.diskStore.OpenLogFileForReplay(rb.tableName, rb.shardID, creationTime); err != nil {
		return nil, err
	}

	defer f.Close()

	streamReader := utils.NewStreamDataReader(f)

	var size uint32
	// Read magic header.
	header, err := streamReader.ReadUint32()
	if err != nil {
		return nil, err
	}

	if header != UpsertHeader {
		return nil, utils.StackError(nil,
			"Invalid header %#x", header)
	}

	// Offset starts from magical header.
	var currentOffset int64 = 4

	var startOffsets []int64

	for size, err = streamReader.ReadUint32(); err != io.EOF; size, err = streamReader.ReadUint32() {
		if err != nil {
			return nil, err
		}

		var newOffset int64
		if newOffset, err = f.Seek(int64(size), io.SeekCurrent); err != nil {
			if err != nil {
				return nil, err
			}
		}

		desiredOffset := currentOffset + 4 + int64(size)
		if newOffset != desiredOffset {
			return nil, utils.StackError(nil,
				"Cannot seek to desired offset %d of redolog file ,current offset %d",
				desiredOffset, newOffset)
		}

		startOffsets = append(startOffsets, currentOffset)
		currentOffset = desiredOffset
	}
	return startOffsets, nil
}

// ReadData first locates the upsert batch using creationTime and upsertBatchOffset. It then returns data
// starting from given start and has length rows along with number of total rows and column names in this
// upsert batch.
func (rb *redoLogBrowser) ReadData(creationTime int64, upsertBatchOffset int64, start int, length int) (
	data [][]interface{}, columnNames []string, numRows int, err error) {
	var f utils.ReaderSeekerCloser
	if f, err = rb.diskStore.OpenLogFileForReplay(rb.tableName, rb.shardID, creationTime); err != nil {
		return
	}
	defer f.Close()

	var actualOffset int64
	if actualOffset, err = f.Seek(upsertBatchOffset, io.SeekStart); err != nil {
		return
	}

	if actualOffset != upsertBatchOffset {
		err = utils.StackError(nil,
			"Cannot seek to desired offset %d of redolog file ,current offset %d",
			upsertBatchOffset, actualOffset)
		return
	}

	var upsertBatch *UpsertBatch
	if upsertBatch, err = rb.readUpsertBatch(f); err != nil {
		return
	}

	columnNames, err = upsertBatch.GetColumnNames(rb.schema)
	if err != nil {
		return
	}

	data, err = upsertBatch.ReadData(start, length)
	if err != nil {
		return
	}
	numRows = upsertBatch.NumRows
	return
}

// readUpsertBatch reads an upsert batch from current offset of a stream.
func (rb *redoLogBrowser) readUpsertBatch(f utils.ReaderSeekerCloser) (*UpsertBatch, error) {
	streamReader := utils.NewStreamDataReader(f)
	size, err := streamReader.ReadUint32()
	if err != nil {
		return nil, err
	}
	buffer := make([]byte, size)
	if err = streamReader.Read(buffer); err != nil {
		return nil, err
	}

	return NewUpsertBatch(buffer)
}

// NewRedoLogBrowser creates a RedoLogBrowser using field from Shard.
func (shard *TableShard) NewRedoLogBrowser() RedoLogBrowser {
	return &redoLogBrowser{
		tableName: shard.Schema.Schema.Name,
		shardID:   shard.ShardID,
		schema:    shard.Schema,
		diskStore: shard.diskStore}
}
