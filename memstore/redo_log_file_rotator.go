package memstore

import (
	"github.com/uber/aresdb/utils"
	"math"
	"sync"
	"time"
)

type PurgeType int

const (
	// PurgeByOrder purges files match purge condition according to creation order
	PurgeByOrder PurgeType = iota
	// PurgeAll purges all files that match purge condition
	PurgeAll
)
// Purger takes care of purging given fileID
type Purger func(fileID int64) error

type RedologFileRotator interface {
	GetOrCreateRedologFile(batch *UpsertBatch, now time.Time, newFileID int64) (fileID int64, newFile bool)
	CommitUpsertBatch(fileID int64, upsertBatch *UpsertBatch) (offset int)
	UpdateMaxEventTime(fileID int64, eventTime uint32)
	PurgeRedologFiles(eventCutoff uint32, checkPointFileID int64, batchOffset int, purgeType PurgeType, purger Purger) error
}

// redologFileRotator is the implementation of RedologFileRotator
type redologFileRotator struct {
	sync.RWMutex
	// BatchCountPerFile
	BatchCountPerFile map[int64]int `json:"batchCountPerFile"`
	// MaxEventTimePerFile
	MaxEventTimePerFile map[int64]uint32 `json:"maxEventTimePerFile"`
	// SizePerFile
	SizePerFile      map[int64]uint32 `json:"sizePerFile"`
	TotalRedoLogSize uint

	CurrentFileID           *int64
	CurrentFileCreationTime int64
	CurrentRedoLogSize         int

	RotationInterval int64
	MaxRedoLogSize int64

	tableName string
	shard int
}

// GetOrCreateRedologFile add a new upsert batch to the redolog manager and returns the fileID and offset
func (r *redologFileRotator) GetOrCreateRedologFile(batch *UpsertBatch, now time.Time, newFileID int64) (fileID int64, newFile bool) {
	upsertBatchSize := len(batch.buffer)
	if r.CurrentFileID == nil ||
		now.Unix() >= r.CurrentFileCreationTime+r.RotationInterval ||
		int64(r.CurrentRedoLogSize+upsertBatchSize+4) >= r.MaxRedoLogSize {
		newID := newFileID
		r.CurrentFileID = &newID
		r.CurrentFileCreationTime = now.Unix()
		r.MaxEventTimePerFile[*r.CurrentFileID] = 0
		r.SizePerFile[*r.CurrentFileID] = 0
		utils.GetReporter(r.tableName, r.shard).GetGauge(utils.CurrentRedologCreationTime).Update(float64(now.Unix()))
		utils.GetReporter(r.tableName, r.shard).GetGauge(utils.NumberOfRedologs).Update(float64(len(r.SizePerFile)))
		newFile = true
	}
	return *r.CurrentFileID, newFile
}

func (r *redologFileRotator) CommitUpsertBatch(fileID int64, upsertBatch *UpsertBatch) (offset int) {
	if _, ok := r.BatchCountPerFile[*r.CurrentFileID]; !ok {
		r.BatchCountPerFile[*r.CurrentFileID] = 1
	} else {
		r.BatchCountPerFile[*r.CurrentFileID]++
	}
	r.CurrentRedoLogSize += len(upsertBatch.buffer) + 4
	r.SizePerFile[*r.CurrentFileID] += uint32(len(upsertBatch.buffer)) + 4
	r.TotalRedoLogSize += uint(len(upsertBatch.buffer)) + 4
	return r.BatchCountPerFile[*r.CurrentFileID] - 1
}

// update max eventime to redolog files
func (r *redologFileRotator) UpdateMaxEventTime(fileID int64, eventTime uint32) {
	r.Lock()
	defer r.Unlock()
	if _, ok := r.MaxEventTimePerFile[fileID]; ok && eventTime <= r.MaxEventTimePerFile[fileID] {
		return
	}

	r.MaxEventTimePerFile[fileID] = eventTime
}

func (r *redologFileRotator) PurgeRedologFiles(eventCutoff uint32, checkPointFileID int64, batchOffset int, purgeType PurgeType, purger Purger) error {
	var fileIDs []int64
	switch purgeType {
	case PurgeByOrder:
		fileIDs = r.getPurgeableFilesByOrder(eventCutoff, checkPointFileID, batchOffset)
	case PurgeAll:
		fallthrough
	default:
		fileIDs = r.getAllPurgeableFiles(eventCutoff, checkPointFileID, batchOffset)
	}

	for _, fileID := range fileIDs {
		if err := purger(fileID); err != nil {
			return err
		}
		r.evictRedoLogData(fileID)
	}
	return nil
}

// check whether fileID matches purge condition
// assuming lock already held
func (r *redologFileRotator) checkPurgeConditionWithLock(fileID int64, maxEventTime, eventCutoff uint32, redoFileCheckpointed int64, batchOffset int) bool {
	// exclude current redo file since it's used by ingestion
	return (r.CurrentFileID == nil || fileID != *r.CurrentFileID) &&
			maxEventTime < eventCutoff &&
			(fileID < redoFileCheckpointed || (fileID == redoFileCheckpointed && r.BatchCountPerFile[redoFileCheckpointed] == batchOffset+1))
}

func (r *redologFileRotator) getAllPurgeableFiles(cutoff uint32, redoFileCheckpointed int64, batchOffset int) []int64 {
	r.RLock()
	var purgeableFileIDs []int64
	for fileID, maxEventTime := range r.MaxEventTimePerFile {
		if r.checkPurgeConditionWithLock(fileID, maxEventTime, cutoff, redoFileCheckpointed, batchOffset) {
			purgeableFileIDs = append(purgeableFileIDs, fileID)
		}
	}
	r.RUnlock()
	return purgeableFileIDs
}

func (r *redologFileRotator) getPurgeableFilesByOrder(cutoff uint32, redoFileCheckpointed int64, batchOffset int) []int64 {
	r.RLock()
	var purgeableFileIDs []int64
	var firstUnpurgable int64 = math.MaxInt64
	for fileID, maxEventTime := range r.MaxEventTimePerFile {
		if r.checkPurgeConditionWithLock(fileID, maxEventTime, cutoff, redoFileCheckpointed, batchOffset) && fileID < firstUnpurgable {
			purgeableFileIDs = append(purgeableFileIDs, fileID)
		} else {
			firstUnpurgable = fileID
		}
	}
	r.RUnlock()
	// last check for purgeable fileIDs to exclude fileIDs > firstUnpurgable
	last := 0
	for i := 0; i < len(purgeableFileIDs); i++ {
		if purgeableFileIDs[i] < firstUnpurgable {
			purgeableFileIDs[last], purgeableFileIDs[i] = purgeableFileIDs[i], purgeableFileIDs[last]
			last++
		}
	}
	return purgeableFileIDs[0:last]
}

func (r *redologFileRotator) evictRedoLogData(fileID int64) {
	r.Lock()
	delete(r.MaxEventTimePerFile, fileID)
	delete(r.BatchCountPerFile, fileID)
	r.TotalRedoLogSize -= uint(r.SizePerFile[fileID])
	delete(r.SizePerFile, fileID)
	utils.GetReporter(r.tableName, r.shard).GetGauge(utils.NumberOfRedologs).Update(float64(len(r.SizePerFile)))
	utils.GetReporter(r.tableName, r.shard).GetGauge(utils.SizeOfRedologs).Update(float64(r.TotalRedoLogSize))
	r.Unlock()
}
