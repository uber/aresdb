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
	"encoding/json"
	"io"
	"sync"

	"github.com/uber/aresdb/diskstore"
	"github.com/uber/aresdb/utils"
)



// UpsertHeader is the magic header written into the beginning of each redo log file.
const UpsertHeader uint32 = 0xADDAFEED

// fileRedologManager manages the redo log file append, rotation, purge. It is used by ingestion,
// recovery and archiving. Accessor must hold the TableShard.WriterLock to access it.
type fileRedologManager struct {
	// The lock is to protect MaxEventTimePerFile.
	sync.RWMutex `json:"-"`

	// The time interval of redo file rotations.
	RotationInterval int64 `json:"rotationInterval"`

	// The limit of redo file size to trigger rotations.
	MaxRedoLogSize int64 `json:"maxRedoLogSize"`

	// Current redo log size
	CurrentRedoLogSize uint32 `json:"currentRedoLogSize"`

	// size of all redologs
	TotalRedoLogSize uint `json:"totalRedologSize"`

	// The map with redo log creation time as the key and max event time as the value. Readers
	// need to hold the reader lock in accessing the field.
	MaxEventTimePerFile map[int64]uint32 `json:"maxEventTimePerFile"`

	// redo log creation time -> batch count mapping.
	// Readers need to hold the reader lock in accessing the field.
	BatchCountPerFile map[int64]uint32 `json:"batchCountPerFile"`

	// SizePerFile
	SizePerFile map[int64]uint32 `json:"sizePerFile"`

	// Current log file points to the current redo log file used for appending new upsert batches.
	currentLogFile io.WriteCloser

	// Current file creation time in milliseconds.
	CurrentFileCreationTime int64 `json:"currentFileCreationTime"`

	// Pointer to the disk store for redo log access.
	diskStore diskstore.DiskStore

	// Name of the table.
	tableName string

	// The shard id of the table.
	shard int
}

// NewFileRedoLogManager creates a new fileRedologManager instance.
func NewFileRedoLogManager(rotationInterval int64, maxRedoLogSize int64, diskStore diskstore.DiskStore, tableName string, shard int) RedologManager {
	return &fileRedologManager{
		RotationInterval:    rotationInterval,
		MaxEventTimePerFile: make(map[int64]uint32),
		BatchCountPerFile:   make(map[int64]uint32),
		SizePerFile:         make(map[int64]uint32),
		diskStore:           diskStore,
		tableName:           tableName,
		shard:               shard,
		MaxRedoLogSize:      maxRedoLogSize,
		CurrentRedoLogSize:  0,
	}
}

// Close closes the current log file.
func (r *fileRedologManager) Close() {
	if r.currentLogFile != nil {
		r.currentLogFile.Close()
	}
}

// openFileForWrite handles redo log file opening and rotation (if needed). It guarantees the
// validity of the currentLogFile upon return.
func (r *fileRedologManager) openFileForWrite(upsertBatchSize uint32) {
	dataTime := utils.Now().Unix()

	// If current file is still valid we just return the writer back.
	if r.currentLogFile != nil && dataTime < r.CurrentFileCreationTime+r.RotationInterval &&
		int64(r.CurrentRedoLogSize+upsertBatchSize+4) < r.MaxRedoLogSize {
		return
	}

	var err error
	if r.currentLogFile != nil {
		if err = r.currentLogFile.Close(); err != nil {
			utils.GetLogger().Panic("Failed to close current redo log file")
		}
	}

	if r.currentLogFile, err = r.diskStore.OpenLogFileForAppend(r.tableName, r.shard, dataTime); err != nil {
		utils.GetLogger().With(
			"table", r.tableName,
			"shard", r.shard,
			"error", err.Error()).Panic("Failed to open new redo log file")
	}

	writer := utils.NewStreamDataWriter(r.currentLogFile)
	if err = writer.WriteUint32(UpsertHeader); err != nil {
		utils.GetLogger().Panic("Failed to write magic header to the new redo log")
	}

	r.CurrentFileCreationTime = dataTime
	utils.GetReporter(r.tableName, r.shard).GetGauge(utils.CurrentRedologCreationTime).Update(float64(r.CurrentFileCreationTime))
	r.MaxEventTimePerFile[r.CurrentFileCreationTime] = 0
	r.SizePerFile[r.CurrentFileCreationTime] = 0
	utils.GetReporter(r.tableName, r.shard).GetGauge(utils.NumberOfRedologs).Update(float64(len(r.SizePerFile)))

	r.CurrentRedoLogSize = 4
}

// WriteUpsertBatch saves an upsert batch into disk before applying it. Any errors from diskStore
// will trigger system panic.
func (r *fileRedologManager) WriteUpsertBatch(upsertBatch *UpsertBatch) (int64, uint32) {
	r.openFileForWrite(uint32(len(upsertBatch.buffer)))

	buffer := upsertBatch.GetBuffer()

	writer := utils.NewStreamDataWriter(r.currentLogFile)
	// Write buffer size.
	if err := writer.WriteUint32(uint32(len(buffer))); err != nil {
		utils.GetLogger().With("error", err).Panic("Failed to write buffer size into the redo log")
	}

	if _, err := r.currentLogFile.Write(buffer); err != nil {
		utils.GetLogger().With("error", err).Panic("Failed to write upsert buffer into the redo log")
	}

	// update current redo log size
	r.CurrentRedoLogSize += uint32(len(upsertBatch.buffer)) + 4
	r.SizePerFile[r.CurrentFileCreationTime] += uint32(len(upsertBatch.buffer)) + 4
	r.TotalRedoLogSize += uint(len(upsertBatch.buffer)) + 4

	utils.GetReporter(r.tableName, r.shard).GetGauge(utils.CurrentRedologSize).Update(float64(r.CurrentRedoLogSize))
	utils.GetReporter(r.tableName, r.shard).GetGauge(utils.SizeOfRedologs).Update(float64(r.TotalRedoLogSize))

	// Update offset of the last batch for the current redolog
	return r.CurrentFileCreationTime, r.updateBatchCount(r.CurrentFileCreationTime) - 1
}

// UpdateMaxEventTime updates the max event time of the current redo log file.
// redoFile is the key to the corresponding redo file that needs to have the maxEventTime updated.
// redoFile == 0 is used in serving ingestion requests where the current file's max event time is
// updated. redoFile != 0 is used in recovery where the redo log file loaded from disk needs to
// get its max event time calculated.
func (r *fileRedologManager) UpdateMaxEventTime(eventTime uint32, redoFile int64) {
	r.Lock()
	defer r.Unlock()
	if _, ok := r.MaxEventTimePerFile[redoFile]; ok && eventTime <= r.MaxEventTimePerFile[redoFile] {
		return
	}

	r.MaxEventTimePerFile[redoFile] = eventTime
}

func (r *fileRedologManager) closeRedoLogFile(creationTime int64, offset uint32, currentFile *io.ReadCloser,
	currentIndex *int, needToTruncate bool) {
	// End of file encountered. Move to next file.
	if err := (*currentFile).Close(); err != nil {
		utils.GetLogger().With(
			"table", r.tableName,
			"shard", r.shard,
			"err", err,
			"file", creationTime).Panic("Failed to close redo log file")
	}
	*currentFile = nil
	*currentIndex++

	if needToTruncate {
		utils.GetLogger().Error("Corrupted file found, truncating it to resume processing")
		// truncate current file and move to next file.
		if err := r.diskStore.TruncateLogFile(r.tableName, r.shard, creationTime, int64(offset)); err != nil {
			utils.GetLogger().With(
				"table", r.tableName,
				"shard", r.shard,
				"err", err,
				"file", creationTime).Panic("Failed to truncate redo log file")
		}
		utils.GetReporter(r.tableName, r.shard).GetCounter(utils.RedoLogFileCorrupt).Inc(1)
	}
}

// NextUpsertBatch returns a functor that can be used to iterate over redo logs on disk and returns
// one UpsertBatch at each call. It returns nil to indicate the end of the upsert batch stream.
//
// Any failure in file reading and upsert batch creation will trigger system panic.
func (r *fileRedologManager) NextUpsertBatch() func() (*UpsertBatch, int64, uint32) {
	files, err := r.diskStore.ListLogFiles(r.tableName, r.shard)
	if err != nil {
		utils.GetLogger().Panic("Failed to list redo log files", err)
	}

	currentIndex := 0
	var currentReader utils.StreamDataReader
	var currentFile io.ReadCloser
	var offset uint32

	return func() (*UpsertBatch, int64, uint32) {
		for {
			// Open the next redo file.
			if currentFile == nil {
				// End of file list, done.
				if currentIndex >= len(files) {
					return nil, 0, 0
				}

				key := files[currentIndex]

				utils.GetLogger().With(
					"table", r.tableName,
					"shard", r.shard,
				).Infof("Start replaying redo log file %d [%d/%d]", key, currentIndex+1, len(files))
				currentFile, err = r.diskStore.OpenLogFileForReplay(r.tableName, r.shard, key)
				if err != nil {
					utils.GetLogger().Panicf("Failed to open redo log file %v for replay", key)
				}

				currentReader = utils.NewStreamDataReader(currentFile)

				// Read magic header. If magic number mismatches, this means the whole redolog file is corrupted.
				// We should immediately crash the server and let engineer to handle this.
				var header uint32
				if header, err = currentReader.ReadUint32(); err != nil {
					utils.GetLogger().Panicf("Failed to read magic header for redo log file %v", key)
				}

				if header != UpsertHeader {
					utils.GetLogger().Panicf("Invalid header %#x for redo log file %v", header, key)
				}
				offset = 4
			}

			// All later errors are recoverable and should be solved by truncate the redo log file.

			// Try to read the next batch in the file.
			size, err := currentReader.ReadUint32()
			if err == io.EOF {
				r.closeRedoLogFile(files[currentIndex], offset, &currentFile, &currentIndex, false)
			} else if err != nil {
				utils.GetLogger().Errorf("Failed to read size info of the next upsert batch %v",
					err)
				r.closeRedoLogFile(files[currentIndex], offset, &currentFile, &currentIndex, true)
			} else {
				offset += 4
				// Found an upsert batch to read.
				buffer := make([]byte, size)
				if err := currentReader.Read(buffer); err != nil {
					utils.GetLogger().Errorf(
						"Failed to read upsert batch of size %v from file %v at offset %v for table %v shard %v",
						size, files[currentIndex], offset, r.tableName, r.shard)
					r.closeRedoLogFile(files[currentIndex], offset-4, &currentFile, &currentIndex, true)
				} else {
					offset += size
					upsertBatch, err := NewUpsertBatch(buffer)
					if err != nil {
						utils.GetLogger().Errorf(
							"Failed to create upsert batch from buffer of size %v from file %v at offset %v for table %v shard %v",
							size, files[currentIndex], offset, r.tableName, r.shard)
						r.closeRedoLogFile(files[currentIndex], offset-4-size, &currentFile, &currentIndex, true)
					} else {
						// update total redolog size
						r.TotalRedoLogSize += uint(size + 4)
						// increment size per file
						r.SizePerFile[files[currentIndex]] += size + 4

						// update lastBatchOffset for the current redo log file
						return upsertBatch, files[currentIndex], r.updateBatchCount(files[currentIndex]) - 1
					}
				}
			}
		}
	}
}

// updateBatchCount saves/updates batch counts for the given redolog
func (r *fileRedologManager) updateBatchCount(redoFile int64) uint32 {
	r.RLock()
	defer r.RUnlock()
	if _, ok := r.BatchCountPerFile[redoFile]; !ok {
		r.BatchCountPerFile[redoFile] = 1
	} else {
		r.BatchCountPerFile[redoFile]++
	}
	return r.BatchCountPerFile[redoFile]
}

// getRedoLogFilesToPurge returns all redo log files whose max event time is less than cutoff and thus
// is eligible for purging. Readers need to hold the reader lock to access this function.
// At the same, make sure all records should've backfilled successfully
func (r *fileRedologManager) getRedoLogFilesToPurge(cutoff uint32, redoFileCheckpointed int64, batchOffset uint32) []int64 {
	r.RLock()
	var creationTimes []int64
	for creationTime, maxEventTime := range r.MaxEventTimePerFile {
		// exclude current redo file since it's used by ingestion
		if (creationTime < r.CurrentFileCreationTime || r.CurrentFileCreationTime == 0) && maxEventTime < cutoff {
			if creationTime < redoFileCheckpointed ||
				(creationTime == redoFileCheckpointed && r.BatchCountPerFile[redoFileCheckpointed] == batchOffset+1) {
				creationTimes = append(creationTimes, creationTime)
			}
		}
	}
	r.RUnlock()
	return creationTimes
}

// evictRedoLogData evict data belongs to redologs already purged from disk
func (r *fileRedologManager) evictRedoLogData(creationTime int64) {
	r.Lock()
	delete(r.MaxEventTimePerFile, creationTime)
	delete(r.BatchCountPerFile, creationTime)
	r.TotalRedoLogSize -= uint(r.SizePerFile[creationTime])
	delete(r.SizePerFile, creationTime)
	utils.GetReporter(r.tableName, r.shard).GetGauge(utils.NumberOfRedologs).Update(float64(len(r.SizePerFile)))
	utils.GetReporter(r.tableName, r.shard).GetGauge(utils.SizeOfRedologs).Update(float64(r.TotalRedoLogSize))
	r.Unlock()
}

// PurgeRedologFileAndData purges disk files and in memory data of redologs that are eligible to be purged.
func (r *fileRedologManager) PurgeRedologFileAndData(cutoff uint32, redoFileCheckpointed int64, batchOffset uint32) error {
	creationTimes := r.getRedoLogFilesToPurge(cutoff, redoFileCheckpointed, batchOffset)
	for _, creationTime := range creationTimes {
		if err := r.diskStore.DeleteLogFile(
			r.tableName, r.shard, creationTime); err != nil {
			return err
		}
		r.evictRedoLogData(creationTime)
	}
	return nil
}

// MarshalJSON marshals a fileRedologManager into json.
func (r *fileRedologManager) MarshalJSON() ([]byte, error) {
	// Avoid json.Marshal loop calls.
	type alias fileRedologManager
	r.RLock()
	defer r.RUnlock()
	return json.Marshal((*alias)(r))
}
