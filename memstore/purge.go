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
	memCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/utils"
)

// Purge purges out of retention data for table shard
func (m *memStoreImpl) Purge(tableName string, shardID, batchIDStart, batchIDEnd int, reporter PurgeJobDetailReporter) error {
	start := utils.Now()
	jobKey := getIdentifier(tableName, shardID, memCom.PurgeJobType)
	purgeTimer := utils.GetReporter(tableName, shardID).GetTimer(utils.PurgeTimingTotal)
	defer func() {
		duration := utils.Now().Sub(start)
		purgeTimer.Record(duration)
		reporter(jobKey, func(status *PurgeJobDetail) {
			status.LastDuration = duration
		})
	}()

	shard, err := m.GetTableShard(tableName, shardID)
	if err != nil {
		return err
	}
	defer shard.Users.Done()

	currentVersion := shard.ArchiveStore.GetCurrentVersion()
	defer currentVersion.Users.Done()

	reporter(jobKey, func(status *PurgeJobDetail) {
		status.Stage = PurgeMetaData
		status.BatchIDStart = batchIDStart
		status.BatchIDEnd = batchIDEnd
	})

	// detach batch from memory
	var batchesToPurge []*ArchiveBatch
	currentVersion.Lock()
	for batchID, batch := range currentVersion.Batches {
		if batchID >= int32(batchIDStart) && batchID < int32(batchIDEnd) {
			delete(currentVersion.Batches, batchID)
			if batch != nil {
				batchesToPurge = append(batchesToPurge, batch)
			}
		}
	}
	currentVersion.Unlock()

	// delete metadata of batches within range
	err = shard.metaStore.PurgeArchiveBatches(tableName, shardID, batchIDStart, batchIDEnd)
	if err != nil {
		return err
	}

	reporter(jobKey, func(status *PurgeJobDetail) {
		status.Stage = PurgeDataFile
	})

	// delete data file on disk of batches within range
	numBatches, err := shard.diskStore.DeleteBatches(tableName, shardID, batchIDStart, batchIDEnd)
	if err != nil {
		return err
	}
	reporter(jobKey, func(status *PurgeJobDetail) {
		status.NumBatches = numBatches
	})
	utils.GetReporter(tableName, shardID).GetCounter(utils.PurgedBatches).Inc(int64(numBatches))

	reporter(jobKey, func(status *PurgeJobDetail) {
		status.Stage = PurgeMemory
		status.Current = 0
		status.Total = len(batchesToPurge)
	})

	for id, batch := range batchesToPurge {
		batch.Lock()
		reporter(jobKey, func(status *PurgeJobDetail) {
			status.Current = id
		})
		for columnID, vp := range batch.Columns {
			if vp != nil {
				// wait for users to finish
				vp.(memCom.ArchiveVectorParty).WaitForUsers(true)
				vp.SafeDestruct()
				shard.HostMemoryManager.ReportManagedObject(tableName, shardID, int(batch.BatchID), columnID, 0)
			}
		}
		batch.Unlock()
	}

	reporter(jobKey, func(status *PurgeJobDetail) {
		status.Stage = PurgeComplete
	})

	shard.ArchiveStore.PurgeManager.Lock()
	shard.ArchiveStore.PurgeManager.LastPurgeTime = utils.Now()
	shard.ArchiveStore.PurgeManager.Unlock()

	return nil
}
