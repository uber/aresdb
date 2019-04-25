package memstore

import (
	"github.com/uber/aresdb/utils"
	"sync"
)

type PurgeType int

const (
	// PurgeLatest purges latest redolog file that match purge condition
	PurgeLatest PurgeType = iota
	// PurgeAll purges all redolog files that match purge condition
	PurgeAll
)

// Purger takes care of purging given fileID
type Purger func(fileID int64) error

type RotationCondition func(upsertBatch *UpsertBatch, condition *RedologFileRotator) bool

// RedologFileRotator
type RedologFileRotator struct {
	sync.RWMutex
	// BatchCountPerFile
	BatchCountPerFile map[int64]int `json:"batchCountPerFile"`
	// MaxEventTimePerFile
	MaxEventTimePerFile map[int64]uint32 `json:"maxEventTimePerFile"`
	// SizePerFile
	SizePerFile map[int64]uint32 `json:"sizePerFile"`
	TotalRedoLogSize uint
	CurrentFileID int64
	CurrentRedologSize int

	tableName string
	shard int

	conditions []RotationCondition
}

func SizeBasedRotation(maxRedologSize int) RotationCondition {
	return func(upsertBatch *UpsertBatch, r *RedologFileRotator) bool {
		return r.CurrentRedologSize+4+len(upsertBatch.buffer) < maxRedologSize
	}
}

func 

func (r *RedologFileRotator) AddUpsertBatch(batch *UpsertBatch, newFileID int64) (fileID, offset int) {
	// If current file is still valid we just return the writer back.
	if r.CurrentFileID < 0 {
		// new file
	}

	if r.currentLogFile != nil && dataTime < r.CurrentFileCreationTime+r.RotationInterval &&
		int64(r.CurrentRedoLogSize+upsertBatchSize+4) < r.MaxRedoLogSize {
		return
	}

	for _, c := range r.conditions {
		if c.Check(r) {

		}
	}

	if _, ok := r.BatchCountPerFile[redoFile]; !ok {
		r.BatchCountPerFile[redoFile] = 1
	} else {
		r.BatchCountPerFile[redoFile]++
	}

	return newFileID,
}

// update max eventime to redolog files
func (r *RedologFileRotator) UpdateMaxEventTime(fileID int64, eventTime uint32) {
	r.Lock()
	defer r.Unlock()
	if _, ok := r.MaxEventTimePerFile[fileID]; ok && eventTime <= r.MaxEventTimePerFile[fileID] {
		return
	}

	r.MaxEventTimePerFile[fileID] = eventTime
}

func (r *RedologFileRotator) PurgeRedologFiles(eventCutoff uint32, checkPointFileID int64, batchOffset int, purgeType PurgeType, purger Purger) error {
	fileIDs := r.getRedoLogFilesToPurge(eventCutoff, checkPointFileID, batchOffset)
	for _, fileID := range fileIDs {
		if err := purger(fileID); err != nil {
			return err
		}
		r.evictRedoLogData(fileID)
	}
	return nil
}

func (r *RedologFileRotator) getRedoLogFilesToPurge(cutoff uint32, redoFileCheckpointed int64, batchOffset int) []int64 {
	r.RLock()
	var fileIDs []int64
	for fileID, maxEventTime := range r.MaxEventTimePerFile {
		// exclude current redo file since it's used by ingestion
		if (fileID < r.CurrentFileID || r.CurrentFileID == 0) && maxEventTime < cutoff {
			if fileID < redoFileCheckpointed ||
				(fileID == redoFileCheckpointed && r.BatchCountPerFile[redoFileCheckpointed] == batchOffset+1) {
				fileIDs = append(fileIDs, fileID)
			}
		}
	}
	r.RUnlock()
	return fileIDs
}

func (r *RedologFileRotator) evictRedoLogData(fileID int64) {
	r.Lock()
	delete(r.MaxEventTimePerFile, fileID)
	delete(r.BatchCountPerFile, fileID)
	r.TotalRedoLogSize -= uint(r.SizePerFile[fileID])
	delete(r.SizePerFile, fileID)
	utils.GetReporter(r.tableName, r.shard).GetGauge(utils.NumberOfRedologs).Update(float64(len(r.SizePerFile)))
	utils.GetReporter(r.tableName, r.shard).GetGauge(utils.SizeOfRedologs).Update(float64(r.TotalRedoLogSize))
	r.Unlock()
}
