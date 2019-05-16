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
	"sync"

	"github.com/uber/aresdb/diskstore"
	"github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/metastore"
	"github.com/uber/aresdb/utils"
	"math"
)

// TableShard stores the data for one table shard in memory.
type TableShard struct {
	// Wait group used to prevent the stores from being prematurely deleted.
	Users sync.WaitGroup `json:"-"`

	ShardID int `json:"-"`

	// For convenience, reference to the table schema struct.
	Schema *TableSchema `json:"schema"`

	// For convenience.
	metaStore metastore.MetaStore
	diskStore diskstore.DiskStore
	// Ingestor
	ingestor PartitionIngestor

	// Live store. Its locks also cover the primary key.
	LiveStore *LiveStore `json:"liveStore"`

	// Archive store.
	ArchiveStore *ArchiveStore `json:"archiveStore"`

	// The special column deletion lock,
	// see https://docs.google.com/spreadsheets/d/1QI3s1_4wgP3Cy-IGoKFCx9BcN23FzIfZGRSNC8I-1Sk/edit#gid=0
	columnDeletion sync.Mutex

	// For convenience.
	HostMemoryManager common.HostMemoryManager `json:"-"`
}

// NewTableShard creates and initiates a table shard based on the schema.
func NewTableShard(schema *TableSchema, metaStore metastore.MetaStore, diskStore diskstore.DiskStore,
	hostMemoryManager common.HostMemoryManager, shard int, ingestorManager *IngestorManager,
	redoLogManagerFactory *RedoLogManagerFactory) *TableShard {

	tableShard := &TableShard{
		ShardID:           shard,
		Schema:            schema,
		diskStore:         diskStore,
		metaStore:         metaStore,
		HostMemoryManager: hostMemoryManager,
	}
	if ingestorManager != nil {
		ingestorManager.IngestPartition(schema.Schema.Name, shard)
	}

	archiveStore := NewArchiveStore(tableShard)
	tableShard.ArchiveStore = archiveStore
	tableShard.LiveStore = NewLiveStore(schema.Schema.Config.BatchSize, tableShard, metaStore, redoLogManagerFactory)
	return tableShard
}

// Destruct destructs the table shard.
// Caller must detach the shard from memstore first.
func (shard *TableShard) Destruct() {
	// TODO: if this blocks on archiving for too long, figure out a way to cancel it.
	if shard.ingestor != nil {
		shard.ingestor.Close()
		shard.ingestor = nil
	}

	shard.Users.Wait()

	shard.LiveStore.Destruct()

	if shard.Schema.Schema.IsFactTable {
		shard.ArchiveStore.Destruct()
	}
}

// StartIngestion start ingestion for this table/shard
func (s *TableShard) StartIngestion() error {
	if s.ingestor == nil {
		return nil
	}
	table := s.Schema.Schema.Name

	utils.GetLogger().With("action", IngestionAction, "table", table, "shard", s.ShardID).Info("Start ingesting")

	offset, err := s.metaStore.GetIngestionCheckpointOffset(table, s.ShardID)
	if err != nil {
		return utils.StackError(err, "Failed to get ingestion checkpoint offset, table: %s, shard: %d", table, s.ShardID)
	}

	err = s.ingestor.Ingest(offset, math.MaxInt64, false)
	if err != nil {
		return utils.StackError(err, "Failed to start ingestion, table: %s, shard: %d", table, s.ShardID)
	}

	go func() {
		for {
			batch := s.ingestor.Next()
			if batch == nil {
				utils.GetLogger().With("action", IngestionAction, "table", table, "shard", s.ShardID).Info("Ingestion stopped")
				return
			}
			s.WriteUpsertBatch(batch.Batch, batch.Offset)
		}
	}()
	return nil
}

func (s *TableShard) WriteUpsertBatch(upsertBatch *UpsertBatch, offset int64) error {
	utils.GetReporter(s.Schema.Schema.Name, s.ShardID).GetCounter(utils.IngestedUpsertBatches).Inc(1)
	utils.GetReporter(s.Schema.Schema.Name, s.ShardID).GetGauge(utils.UpsertBatchSize).Update(float64(len(upsertBatch.buffer)))

	s.Users.Add(1)
	defer s.Users.Done()

	// Put the memStore in writer lock mode so other writers cannot enter.
	s.LiveStore.WriterLock.Lock()

	// Persist upsertbatch or offset info to disk first.
	redoFile, batchOffset := s.LiveStore.RedoLogManager.RecordUpsertBatch(upsertBatch, offset)

	// Apply it to the memstore shard.
	needToWaitForBackfillBuffer, err := s.ApplyUpsertBatch(upsertBatch, redoFile, batchOffset, false)

	s.LiveStore.WriterLock.Unlock()

	// return immediately if it does not need to wait for backfill buffer availability
	if !needToWaitForBackfillBuffer {
		return err
	}

	// otherwise: block until backfill buffer becomes available again
	s.LiveStore.BackfillManager.WaitForBackfillBufferAvailability()

	return nil
}

// DeleteColumn deletes the data for the specified column.
func (shard *TableShard) DeleteColumn(columnID int) error {
	shard.columnDeletion.Lock()
	defer shard.columnDeletion.Unlock()

	// Delete from live store
	shard.LiveStore.WriterLock.Lock()
	batchIDs, _ := shard.LiveStore.GetBatchIDs()
	for _, batchID := range batchIDs {
		batch := shard.LiveStore.GetBatchForWrite(batchID)
		if batch == nil {
			continue
		}
		if columnID < len(batch.Columns) {
			vp := batch.Columns[columnID]
			if vp != nil {
				bytes := vp.GetBytes()
				batch.Columns[columnID] = nil
				vp.SafeDestruct()
				shard.HostMemoryManager.ReportUnmanagedSpaceUsageChange(int64(-bytes))
			}
		}
		batch.Unlock()
	}
	shard.LiveStore.WriterLock.Unlock()

	if !shard.Schema.Schema.IsFactTable {
		return nil
	}

	// Delete from disk store
	// Schema cannot be changed while this function is called.
	// Only delete unsorted columns from disk.
	if utils.IndexOfInt(shard.Schema.Schema.ArchivingSortColumns, columnID) < 0 {
		err := shard.diskStore.DeleteColumn(shard.Schema.Schema.Name, columnID, shard.ShardID)
		if err != nil {
			return err
		}
	}

	// Delete from archive store
	currentVersion := shard.ArchiveStore.GetCurrentVersion()
	defer currentVersion.Users.Done()

	var batches []*ArchiveBatch
	currentVersion.RLock()
	for _, batch := range currentVersion.Batches {
		batches = append(batches, batch)
	}
	currentVersion.RUnlock()

	for _, batch := range batches {
		batch.BlockingDelete(columnID)
	}
	return nil
}

// PreloadColumn loads the column into memory and wait for completion of loading
// within (startDay, endDay]. Note endDay is inclusive but startDay is exclusive.
func (shard *TableShard) PreloadColumn(columnID int, startDay int, endDay int) {
	archiveStoreVersion := shard.ArchiveStore.GetCurrentVersion()
	for batchID := endDay; batchID > startDay; batchID-- {
		batch := archiveStoreVersion.RequestBatch(int32(batchID))
		// Only do loading if this batch does not have any data yet.
		if batch.Size > 0 {
			vp := batch.RequestVectorParty(columnID)
			vp.WaitForDiskLoad()
			vp.Release()
		}
	}
	archiveStoreVersion.Users.Done()
}
