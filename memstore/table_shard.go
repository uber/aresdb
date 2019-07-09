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
	"github.com/uber/aresdb/datanode/bootstrap"
	"github.com/uber/aresdb/diskstore"
	"github.com/uber/aresdb/memstore/common"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/utils"
	"sync"
)

// TableShard stores the data for one table shard in memory.
type TableShard struct {
	// Wait group used to prevent the stores from being prematurely deleted.
	Users sync.WaitGroup `json:"-"`

	ShardID int `json:"-"`

	// For convenience, reference to the table schema struct.
	Schema *common.TableSchema `json:"schema"`

	// For convenience.
	metaStore metaCom.MetaStore
	diskStore diskstore.DiskStore
	options   Options

	// Live store. Its locks also cover the primary key.
	LiveStore *LiveStore `json:"liveStore"`

	// Archive store.
	ArchiveStore *ArchiveStore `json:"archiveStore"`

	// The special column deletion lock,
	// see https://docs.google.com/spreadsheets/d/1QI3s1_4wgP3Cy-IGoKFCx9BcN23FzIfZGRSNC8I-1Sk/edit#gid=0
	columnDeletion sync.Mutex

	// For convenience.
	HostMemoryManager common.HostMemoryManager `json:"-"`

	// bootstrapLock protects bootstrapState
	bootstrapLock  sync.RWMutex
	bootstrapState bootstrap.BootstrapState

	// BootstrapDetails shows the details of bootstrap
	BootstrapDetails bootstrap.BootstrapDetails `json:"bootstrapDetails,omitempty"`
	// needPeerCopy mark whether the table shard need to copy data from peer
	// before own disk data is available for serve
	// default to 0 (no need for peer copy)
	needPeerCopy uint32
}

// NewTableShard creates and initiates a table shard based on the schema.
func NewTableShard(schema *common.TableSchema, metaStore metaCom.MetaStore,
	diskStore diskstore.DiskStore, hostMemoryManager common.HostMemoryManager, shard int, options Options) *TableShard {
	tableShard := &TableShard{
		ShardID:           shard,
		Schema:            schema,
		diskStore:         diskStore,
		metaStore:         metaStore,
		HostMemoryManager: hostMemoryManager,
		options:           options,
		BootstrapDetails:  bootstrap.NewBootstrapDetails(),
	}

	archiveStore := NewArchiveStore(tableShard)
	tableShard.ArchiveStore = archiveStore
	tableShard.LiveStore = NewLiveStore(schema.Schema.Config.BatchSize, tableShard)
	return tableShard
}

// Destruct destructs the table shard.
// Caller must detach the shard from memstore first.
func (shard *TableShard) Destruct() {
	// TODO: if this blocks on archiving for too long, figure out a way to cancel it.
	shard.Users.Wait()

	shard.options.redoLogMaster.Close(shard.Schema.Schema.Name, shard.ShardID)

	shard.LiveStore.Destruct()

	if shard.Schema.Schema.IsFactTable {
		shard.ArchiveStore.Destruct()
	}
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
