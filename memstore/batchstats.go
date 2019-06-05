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
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/utils"
	"time"
)

// BatchStatsReporter is used to report batch level stats like row count
type BatchStatsReporter struct {
	intervalInSeconds int
	memStore          MemStore
	metaStore         metaCom.MetaStore
	stopChan          chan struct{}
}

// NewBatchStatsReporter create a new BatchStatsReporter instance
func NewBatchStatsReporter(intervalInSeconds int, memStore MemStore, metaStore metaCom.MetaStore) *BatchStatsReporter {
	return &BatchStatsReporter{
		intervalInSeconds: intervalInSeconds,
		memStore:          memStore,
		metaStore:         metaStore,
		stopChan:          make(chan struct{}),
	}
}

// Run is a ticker function to run report periodically
func (batchStats *BatchStatsReporter) Run() {
	tickChan := time.NewTicker(time.Second * time.Duration(batchStats.intervalInSeconds)).C

	for {
		select {
		case <-tickChan:
			batchStats.reportBatchStats()
		case <-batchStats.stopChan:
			return
		}
	}
}

// Stop to stop the stats reporter
func (batchStats *BatchStatsReporter) Stop() {
	close(batchStats.stopChan)
}

func (batchStats *BatchStatsReporter) reportBatchStats() {
	now := utils.Now().Unix()
	yesterdayBatch := int(now/86400) - 1
	batchIDs := map[int]string{
		-1:                  "now",
		yesterdayBatch:      "1dago",
		yesterdayBatch - 5:  "5dago",
		yesterdayBatch - 50: "50dago",
	}
	batchStats.reportBatchStat(batchIDs)
}

func (batchStats *BatchStatsReporter) reportBatchStat(batchIDs map[int]string) {
	timer := utils.GetRootReporter().GetTimer(utils.BatchSizeReportTime).Start()
	defer timer.Stop()

	tables := batchStats.memStore.GetSchemas()

	for table, schema := range tables {
		shards, err := batchStats.metaStore.GetOwnedShards(table)
		if err != nil {
			continue
		}
		for _, shardID := range shards {
			shard, err := batchStats.memStore.GetTableShard(table, shardID)
			if err != nil || shard == nil {
				continue
			}

			for batchID, name := range batchIDs {
				if batchID < 0 {
					totalSize := 0
					liveBatchIDs, numRecordsInLastBatch := shard.LiveStore.GetBatchIDs()
					for i, liveBatchID := range liveBatchIDs {
						batch := shard.LiveStore.GetBatchForRead(liveBatchID)
						if batch == nil {
							continue
						}
						size := batch.Capacity
						batch.RUnlock()
						if i == len(batchIDs)-1 {
							size = numRecordsInLastBatch
						}
						totalSize += size
					}
					utils.GetReporter(table, shardID).GetChildGauge(map[string]string{"time": name}, utils.BatchSize).Update(float64(totalSize))
				} else {
					if !schema.Schema.IsFactTable {
						continue
					}
					version := shard.ArchiveStore.GetCurrentVersion()
					batch := version.RequestBatch(int32(batchID))
					size := batch.Size
					version.Users.Done()
					utils.GetReporter(table, shardID).GetChildGauge(map[string]string{"time": name}, utils.BatchSize).Update(float64(size))
				}
			}
			shard.Users.Done()
		}
	}
}
