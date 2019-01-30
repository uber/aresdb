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

//Snapshot is the process to write the current content of dimension table live store in memory to disk in order to
//	1.Facilitate recovery during server bootstrap.
//	2.Purge stale redo logs.
func (m *memStoreImpl) Snapshot(table string, shardID int, reporter SnapshotJobDetailReporter) error {
	snapshotTimer := utils.GetReporter(table, shardID).GetTimer(utils.SnapshotTimingTotal)
	start := utils.Now()
	jobKey := getIdentifier(table, shardID, memCom.SnapshotJobType)

	defer func() {
		duration := utils.Now().Sub(start)
		snapshotTimer.Record(duration)
		reporter(jobKey, func(status *SnapshotJobDetail) {
			status.LastDuration = duration
		})
	}()

	shard, err := m.GetTableShard(table, shardID)
	if err != nil {
		return err
	}

	defer shard.Users.Done()
	utils.GetLogger().With(
		"job", "snapshot",
		"table", table).Infof("Creating snapshot")

	snapshotMgr := shard.LiveStore.SnapshotManager
	// keep the current redofile and offset
	redoFile, batchOffset, numMutations, lastReadRecord := snapshotMgr.StartSnapshot()

	reporter(jobKey, func(status *SnapshotJobDetail) {
		status.RedologFile = redoFile
		status.BatchOffset = batchOffset
		status.Stage = SnapshotSnapshot
	})

	if numMutations > 0 {
		if err = m.createSnapshot(shard, redoFile, batchOffset); err != nil {
			return err
		}
	}

	// checkpoint snapshot progress
	snapshotMgr.Done(redoFile, batchOffset, numMutations, lastReadRecord)

	reporter(jobKey, func(status *SnapshotJobDetail) {
		status.Stage = SnapshotCleanup
	})

	shard.cleanOldSnapshotAndLogs(redoFile, batchOffset)

	reporter(jobKey, func(status *SnapshotJobDetail) {
		status.Stage = SnapshotComplete
	})

	utils.GetLogger().With(
		"job", "snapshot",
		"table", table).Infof("Snapshot done")

	return nil
}

func (m *memStoreImpl) createSnapshot(shard *TableShard, redoFile int64, batchOffset uint32) error {
	// Block column deletion
	shard.columnDeletion.Lock()
	defer shard.columnDeletion.Unlock()

	batchIDs, _ := shard.LiveStore.GetBatchIDs()

	for _, batchID := range batchIDs {
		batch := shard.LiveStore.GetBatchForRead(batchID)
		for colID, vp := range batch.Columns {
			if vp == nil {
				// column deleted likely
				continue
			}
			utils.GetLogger().With(
				"job", "snapshot",
				"table", shard.Schema.Schema.Name).Infof("batch: %d, columeID: %d", batchID, colID)

			serializer := NewVectorPartySnapshotSerializer(shard, colID, int(batchID), 0, 0, redoFile, batchOffset)
			if err := serializer.WriteVectorParty(vp); err != nil {
				batch.RUnlock()
				return err
			}
		}
		batch.RUnlock()
	}
	return nil
}
