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
	"container/heap"
	"sync"
	"sync/atomic"

	"github.com/uber/aresdb/metastore"
	"github.com/uber/aresdb/utils"

	rbt "github.com/emirpasic/gods/trees/redblacktree"
	"github.com/uber/aresdb/memstore/common"
)

// preloadJob defines the job struct to preload column when preloading days is changed.
type preloadJob struct {
	tableName         string
	columnID          int
	oldPreloadingDays int
	newPreloadingDays int
}

type hostMemoryManager struct {
	sync.RWMutex
	memStore  *memStoreImpl
	metaStore metastore.MetaStore
	// totalMemorySize is configurable uplimit.
	totalMemorySize int64
	// unManagedMemorySize and managedMemorySize reflect current memory usage status.
	unManagedMemorySize int64
	managedMemorySize   int64
	// Init maps for table -> (columnID -> columnBatchInfos) mapping
	batchInfosByColumn map[string]map[int]*columnBatchInfos
	// channel to send preloadJob.
	preloadJobChan chan preloadJob
	// channel to stop preload go routines.
	preloadStopChan chan struct{}
	// channel to send eviction job.
	// TODO: if later we find to many go routines waiting for the channel,
	// we will use sync.Cond to rewrite it.
	evictionJobChan chan struct{}
	// channel to stop eviction go routines.
	evictionStopChan chan struct{}
}

// shardBatchID is the internal data holder struct to store
// shardID and batchID which used as key in the columnBatchInfos.
type shardBatchID struct {
	shardID int
	batchID int
}

func newShardBatchID(shardID int, batchID int) shardBatchID {
	return shardBatchID{
		shardID: shardID,
		batchID: batchID,
	}
}

// shardBatchIDComparator provides a basic comparison on shardBatchID
func shardBatchIDComparator(a, b interface{}) int {
	aAsserted := a.(shardBatchID)
	bAsserted := b.(shardBatchID)
	if aAsserted.batchID == bAsserted.batchID {
		return bAsserted.shardID - aAsserted.shardID
	}
	return aAsserted.batchID - bAsserted.batchID
}

// columnBatchInfos is using RB-Tree data structure to hold shardBatchID to
// size mapping
type columnBatchInfos struct {
	table         string
	batchInfoByID *rbt.Tree
	sync.RWMutex
}

func newColumnBatchInfos(table string) *columnBatchInfos {
	return &columnBatchInfos{
		table:         table,
		batchInfoByID: rbt.NewWith(shardBatchIDComparator),
	}
}

// SetManagedObject is used to add a new batch/update an existing batch.
// Returns the bytes changes during this operation. For new batch, it's
// same as bytes value. For update batch, it's the value of
// bytesChanges = (currentBytes - oldBytes).
func (a *columnBatchInfos) SetManagedObject(shard, batchID int, bytes int64) int64 {
	a.Lock()
	defer a.Unlock()
	key := newShardBatchID(shard, batchID)
	oldSizeInterface, found := a.batchInfoByID.Get(key)
	bytesChanges := bytes
	if found {
		oldSize := oldSizeInterface.(int64)
		bytesChanges = bytes - oldSize
	}
	a.batchInfoByID.Put(key, bytes)
	return bytesChanges
}

// deleteManagedObject is used to delete a batch.
// Returns the bytes got changed.
func (a *columnBatchInfos) DeleteManagedObject(shard, batchID int) int64 {
	a.Lock()
	defer a.Unlock()
	bytesChange := int64(0)
	key := newShardBatchID(shard, batchID)
	sizeInterface, found := a.batchInfoByID.Get(key)

	if found {
		size := sizeInterface.(int64)
		bytesChange = 0 - size
		a.batchInfoByID.Remove(key)
	}
	return bytesChange
}

// GetArchiveMemoryUsageByShard returns memory usage [preload, non-preload] by shard
func (a *columnBatchInfos) GetArchiveMemoryUsageByShard(preloadDays int) map[int]*common.ColumnMemoryUsage {
	a.RLock()
	defer a.RUnlock()

	memoryUsageByShard := map[int]*common.ColumnMemoryUsage{}

	iterator := a.batchInfoByID.Iterator()
	for iterator.Next() {
		shardBatchID, _ := iterator.Key().(shardBatchID)
		batchID := shardBatchID.batchID
		shard := shardBatchID.shardID
		bytes := iterator.Value().(int64)

		_, shardExist := memoryUsageByShard[shard]
		if !shardExist {
			memoryUsageByShard[shard] = &common.ColumnMemoryUsage{}
		}
		if isPreloadingBatch(batchID, preloadDays) {
			memoryUsageByShard[shard].Preloaded += uint(bytes)
		} else {
			memoryUsageByShard[shard].NonPreloaded += uint(bytes)
		}

	}
	return memoryUsageByShard
}

// NewHostMemoryManager is used to init a HostMemoryManager.
func NewHostMemoryManager(memStore *memStoreImpl, totalMemorySize int64) common.HostMemoryManager {
	hostMemoryManager := &hostMemoryManager{
		memStore:            memStore,
		metaStore:           memStore.metaStore,
		totalMemorySize:     totalMemorySize,
		unManagedMemorySize: 0,
		managedMemorySize:   0,
		batchInfosByColumn:  make(map[string]map[int]*columnBatchInfos),
		preloadJobChan:      make(chan preloadJob),
		preloadStopChan:     make(chan struct{}),
		evictionJobChan:     make(chan struct{}),
		evictionStopChan:    make(chan struct{}),
	}
	utils.GetRootReporter().GetGauge(utils.TotalMemorySize).Update(float64(totalMemorySize))
	return hostMemoryManager
}

// All the following three functions trigger preloading and eviction
// asynchrounously. In addition, as time goes on, reloading and eviction can
// also be triggered automatically.

// ReportUnmanagedSpaceUsageChange : Increase/Decrease bytes to the unmanaged space
// usage (for live batches and PKeys).
// Positive bytes number means to increase UnmanagedSpaceUsage, negative number
// means to decrease UnmanagedSpaceUsage.
func (h *hostMemoryManager) ReportUnmanagedSpaceUsageChange(bytes int64) {
	atomic.AddInt64(&h.unManagedMemorySize, int64(bytes))
	utils.GetRootReporter().GetGauge(utils.UnmanagedMemorySize).Update(float64(h.getUnmanagedSpaceUsage()))
	if bytes < 0 {
		h.TriggerEviction()
	}
}

// ReportManagedObject : Report space usage for a managed object (archive batch vector party).
func (h *hostMemoryManager) ReportManagedObject(table string, shard, batchID, columnID int, bytes int64) {
	if bytes <= 0 {
		h.deleteManagedObject(table, shard, batchID, columnID)
	} else {
		h.addOrUpdateManagedObject(table, shard, batchID, columnID, bytes)
		h.TriggerEviction()
	}
	utils.GetRootReporter().GetGauge(utils.ManagedMemorySize).Update(float64(h.getManagedSpaceUsage()))
}

// Start will do a blocking preloading first and then start the go routines to do
// data preloading and eviction.
func (h *hostMemoryManager) Start() {
	h.preloadAll()
	utils.GetLogger().Info("HostMemoryManager: initial preloading done")
	// Preloader execution loop.
	go func() {
		for {
			select {
			case j := <-h.preloadJobChan:
				h.handleColumnPreloadingDaysChange(j)
			case <-h.preloadStopChan:
				return
			}
		}
	}()

	// Evictor execution loop.
	go func() {
		for {
			select {
			case <-h.evictionJobChan:
				h.tryEviction()
			case <-h.evictionStopChan:
				return
			}
		}
	}()
}

// Stop stops the gom rountines to do data preloading and eviction. It's a
// blocking call.
func (h *hostMemoryManager) Stop() {
	h.preloadStopChan <- struct{}{}
	h.evictionStopChan <- struct{}{}
}

// TriggerPreload will handle the column preloading days config change and
// trigger the column preloading if necessary. It's a asynchronous call.
func (h *hostMemoryManager) TriggerPreload(tableName string, columnID int,
	oldPreloadingDays int, newPreloadingDays int) {
	go func() {
		h.preloadJobChan <- preloadJob{
			tableName:         tableName,
			columnID:          columnID,
			oldPreloadingDays: oldPreloadingDays,
			newPreloadingDays: newPreloadingDays,
		}
	}()
}

// TriggerEviction triggers the eviction. It's a asynchronous call.
func (h *hostMemoryManager) TriggerEviction() {
	go func() { h.evictionJobChan <- struct{}{} }()
}

func (h *hostMemoryManager) getUnmanagedSpaceUsage() int64 {
	return atomic.LoadInt64(&h.unManagedMemorySize)
}

func (h *hostMemoryManager) getManagedSpaceUsage() int64 {
	return atomic.LoadInt64(&h.managedMemorySize)
}

// GetArchiveMemoryUsageByTableShard get the managed memory details by table shard and column
func (h *hostMemoryManager) GetArchiveMemoryUsageByTableShard() (map[string]map[string]*common.ColumnMemoryUsage, error) {
	h.RLock()
	defer h.RUnlock()
	// tableName_shardID -> columnName -> columnMemoryUsage
	managedMemoryUsage := map[string]map[string]*common.ColumnMemoryUsage{}
	for tableName, batchInfoByColumn := range h.batchInfosByColumn {
		tableSchema, err := h.memStore.GetSchema(tableName)
		if err != nil {
			return managedMemoryUsage, err
		}
		for columnID, batchInfo := range batchInfoByColumn {
			tableSchema.RLock()
			columnConfig := tableSchema.Schema.Columns[columnID]
			tableSchema.RUnlock()
			memoryUsageByShard := batchInfo.GetArchiveMemoryUsageByShard(columnConfig.Config.PreloadingDays)
			for shardID, columnMemoryUsage := range memoryUsageByShard {
				tableShard := getTableShardKey(tableName, shardID)
				if _, ok := managedMemoryUsage[tableShard]; ok {
					managedMemoryUsage[tableShard][columnConfig.Name] = columnMemoryUsage
				} else {
					managedMemoryUsage[tableShard] = map[string]*common.ColumnMemoryUsage{
						columnConfig.Name: columnMemoryUsage,
					}
				}
			}
		}
	}
	return managedMemoryUsage, nil
}

// managedObjectExists : Return whether the corresponding managed object exists in managed memory.
func (h *hostMemoryManager) managedObjectExists(table string, shard, batchID, columnID int) bool {
	h.RLock()
	defer h.RUnlock()
	tableInMemoryBatches, found := h.batchInfosByColumn[table]
	if !found {
		return false
	}

	columnBatchInfos, found := tableInMemoryBatches[columnID]
	if !found {
		return false
	}
	key := newShardBatchID(shard, batchID)
	_, found = columnBatchInfos.batchInfoByID.Get(key)
	return found
}

// AddOrUpdateManagedObject : Report space usage increase or update for a managed object (archive batch vector party).
func (h *hostMemoryManager) addOrUpdateManagedObject(table string, shard, batchID, columnID int, bytes int64) {
	h.Lock()
	tableInMemoryBatches, found := h.batchInfosByColumn[table]
	if !found {
		tableInMemoryBatches = make(map[int]*columnBatchInfos)
		h.batchInfosByColumn[table] = tableInMemoryBatches
	}
	columnBatchInfos, found := tableInMemoryBatches[columnID]
	if !found {
		columnBatchInfos = newColumnBatchInfos(table)
		tableInMemoryBatches[columnID] = columnBatchInfos
	}
	h.Unlock()

	bytesChange := columnBatchInfos.SetManagedObject(shard, batchID, bytes)
	atomic.AddInt64(&h.managedMemorySize, bytesChange)
	utils.GetLogger().Debugf("addOrUpdateManagedObject(%s,%d,%d,%d,%d), bytesChange = %d, "+
		"managedMemorySize=%d\n ", table, shard, batchID, columnID, bytes, bytesChange, h.getManagedSpaceUsage())
}

// deleteManagedObject : Report space usage reduce for a managed object (archive batch vector party).
func (h *hostMemoryManager) deleteManagedObject(table string, shard, batchID, columnID int) {
	h.Lock()
	utils.GetLogger().Debugf("Trying to deleteManagedObject for table: %s, Shard: %d, batchID: %d, in %+v", table, shard, batchID, h.batchInfosByColumn)
	tableInMemoryBatches, found := h.batchInfosByColumn[table]
	if !found {
		utils.GetLogger().Debugf("Not found tableInMemoryBatches for table: %s", table)
		h.Unlock()
		return
	}
	columnBatchInfos, found := tableInMemoryBatches[columnID]
	h.Unlock()

	if !found {
		utils.GetLogger().Debugf("Not found columnBatchInfos for columnID: %s in table: %s.", columnID, table)
		return
	}
	bytesChange := columnBatchInfos.DeleteManagedObject(shard, batchID)
	utils.GetLogger().Debugf("Before deleteManagedObject managedMemorySize : %d, bytesChange : %d", h.getManagedSpaceUsage(), bytesChange)
	atomic.AddInt64(&h.managedMemorySize, bytesChange)
	utils.GetLogger().Debugf("After deleteManagedObject managedMemorySize : %d", h.getManagedSpaceUsage())
	h.Lock()
	if columnBatchInfos.batchInfoByID.Size() == 0 {
		delete(tableInMemoryBatches, columnID)
	}
	h.Unlock()
	utils.GetLogger().Debugf("deleteManagedObject(%s,%d,%d,%d), bytesChange = %d, managedMemorySize=%d\n ", table, shard, batchID, columnID, bytesChange, h.getManagedSpaceUsage())
}

// preloadAll preloads recent days data for all columns of all table shards into memory.
// The number of preloading days is defined at each column level. This call will happen at
// shard initialization stage.
func (h *hostMemoryManager) preloadAll() {
	tableShardSnapshot := make(map[string][]int)

	// snapshot (tableName, shardID)s.
	h.memStore.RLock()
	for tableName, shardMap := range h.memStore.TableShards {
		tableShardSnapshot[tableName] = make([]int, 0, len(shardMap))
		for shardID := range shardMap {
			tableShardSnapshot[tableName] = append(tableShardSnapshot[tableName], shardID)
		}
	}
	h.memStore.RUnlock()

	currentDay := int(utils.Now().Unix() / 86400)
	for tableName, shardIDs := range tableShardSnapshot {
		for _, shardID := range shardIDs {
			tableShard, err := h.memStore.GetTableShard(tableName, shardID)
			// Table shard may have already been removed from this node.
			if err != nil {
				continue
			}
			tableShard.Schema.RLock()
			columns := tableShard.Schema.Schema.Columns
			tableShard.Schema.RUnlock()
			if tableShard.Schema.Schema.IsFactTable {
				archiveStoreVersion := tableShard.ArchiveStore.GetCurrentVersion()
				for columnID, column := range columns {
					if !column.Deleted {
						preloadingDays := column.Config.PreloadingDays
						tableShard.PreloadColumn(columnID, currentDay-preloadingDays, currentDay)
					}
				}
				archiveStoreVersion.Users.Done()
			}
			tableShard.Users.Done()
		}
	}
}

// handleColumnPreloadingDaysChange handles the preloading config change for a column.
func (h *hostMemoryManager) handleColumnPreloadingDaysChange(j preloadJob) {
	if j.newPreloadingDays <= j.oldPreloadingDays {
		return
	}
	shardIDs := make([]int, 0)

	// snapshot shardIDs.
	h.memStore.RLock()
	shardMap := h.memStore.TableShards[j.tableName]
	for shardID := range shardMap {
		shardIDs = append(shardIDs, shardID)
	}
	h.memStore.RUnlock()
	currentDay := int(utils.Now().Unix() / 86400)
	for _, shardID := range shardIDs {
		tableShard, err := h.memStore.GetTableShard(j.tableName, shardID)
		// Table shard may have already been removed from this node.
		if err != nil {
			continue
		}

		if tableShard.Schema.Schema.IsFactTable {
			tableShard.PreloadColumn(j.columnID, currentDay-j.newPreloadingDays, currentDay-j.oldPreloadingDays)
		}
		tableShard.Users.Done()
	}
}

// tryEviction : try to trigger eviction once
// unManagedMem + managedMem > totalAssignedMem. This method will pop batches
// from the per column holder data structure, calculate global priority
// based on column metadata, then push the batch into a priority queue.
// Eviction will happen through all the populated batches until memory usage
// decreases to a certain level. All failed eviction batches will be
// reinserted.
func (h *hostMemoryManager) tryEviction() {
	// Check if eviction should be triggered
	if (h.totalMemorySize - h.getManagedSpaceUsage() - h.getUnmanagedSpaceUsage()) < 0 {
		utils.GetLogger().Debugf("UnmanagedMem: %d + ManagedMem: %d is larger than totalMem: %d! Eviction is triggered.",
			h.getUnmanagedSpaceUsage(), h.getManagedSpaceUsage(), h.totalMemorySize)
		// Init all columnar priority batches.
		gpq := h.initialGlobalPriorityQueue()
		// Pop from globalPriorityQueueWithLock and do eviction
		for (h.totalMemorySize-h.getManagedSpaceUsage()-h.getUnmanagedSpaceUsage()) < 0 && !gpq.isEmpty() {
			globalPriorityItem := gpq.pop()

			batchPriority := globalPriorityItem.priority
			columnBatchInfos := globalPriorityItem.value
			columnIt := globalPriorityItem.it

			tableSchema, err := h.memStore.GetSchema(columnBatchInfos.table)

			tableSchema.RLock()
			preloadingDays := tableSchema.Schema.Columns[batchPriority.columnID].Config.PreloadingDays
			tableSchema.RUnlock()

			isPreloadingDays := isPreloadingBatch(batchPriority.batchID, preloadingDays)

			if isPreloadingDays {
				utils.GetReporter(columnBatchInfos.table, batchPriority.shardID).
					GetCounter(utils.PreloadingZoneEvicted).Inc(1)
				utils.GetLogger().With(
					"table", columnBatchInfos.table,
					"shard", batchPriority.shardID,
					"batch", batchPriority.batchID,
					"column", batchPriority.columnID,
				).Warn("Column in preloading zone is evicted")
			}

			ok, err := h.memStore.TryEvictBatchColumn(columnBatchInfos.table, batchPriority.shardID, int32(batchPriority.batchID), batchPriority.columnID)
			if ok {
				utils.GetLogger().Debugf("Successfully evict batch from memstore: table %s, shardID %d, batchID %d, columnID %d, size %d",
					columnBatchInfos.table, batchPriority.shardID, batchPriority.batchID, batchPriority.columnID, batchPriority.size)
			} else {
				utils.GetLogger().Debugf("Failed to evict batch from memstore: table %s, shardID %d, batchID %d, columnID %d, size %d, errors: %s",
					columnBatchInfos.table, batchPriority.shardID, batchPriority.batchID, batchPriority.columnID, batchPriority.size, err)
			}

			// Adding the corresponding next batch into priority queue.
			if columnIt.Next() {
				gpq.pushBatchIntoGlobalPriorityQueue(h, columnBatchInfos, batchPriority.columnID, columnIt)
			}
		}

		// Still cannot meet the memory constraints even after evictions.
		if h.totalMemorySize-h.getManagedSpaceUsage()-h.getUnmanagedSpaceUsage() < 0 {
			utils.GetRootReporter().GetCounter(utils.MemoryOverflow).Inc(1)
			utils.GetLogger().Warn("Still cannot meet the memory constraints even after evictions")
		}
	}
}

// pushBatchIntoGlobalPriorityQueue will generate a globalPriority object then
// push it into globalPriorityQueueWithLock.
func (gpq *globalPriorityQueue) pushBatchIntoGlobalPriorityQueue(h *hostMemoryManager,
	columnBatchInfos *columnBatchInfos, columnID int, columnIt rbt.Iterator) {

	// batchInfo := batchInfoInterface.(*archiveBatchInfo)
	sbID := columnIt.Key().(shardBatchID)
	size := columnIt.Value().(int64)
	tableSchema, err := h.memStore.GetSchema(columnBatchInfos.table)
	if err == nil {
		tableSchema.RLock()
		columnConfig := tableSchema.Schema.Columns[columnID]
		tableSchema.RUnlock()
		if !columnConfig.Deleted {
			preloadingDays := columnConfig.Config.PreloadingDays
			isPreloading := isPreloadingBatch(sbID.batchID, preloadingDays)
			batchPriority := createBatchPriority(sbID.shardID, columnID, isPreloading,
				columnConfig.Config.Priority, sbID.batchID, size)
			globalPriorityItem := &globalPriorityItem{
				value:    columnBatchInfos,
				it:       columnIt,
				priority: batchPriority,
			}
			gpq.push(globalPriorityItem)
			utils.GetLogger().Debugf("Pushed batchPrioirty %s to global queue", batchPriority)
		}
	}
}

// initialGlobalPriorityQueue will initialize a globalPriorityQueueWithLock and fetch
// one batch for each table column from batchInfosByColumn.
func (h *hostMemoryManager) initialGlobalPriorityQueue() *globalPriorityQueue {
	gpq := newGlobalPriorityQueue()
	utils.GetLogger().Debugf("Trying to init priority queue to hold batch objects")
	h.RLock()
	for tableName, columnsBatchesList := range h.batchInfosByColumn {
		utils.GetLogger().Debugf("Looking at table:%s, columnsBatchesList.size() = %d", tableName, len(columnsBatchesList))
		for columnID, columnBatchInfos := range columnsBatchesList {
			columnBatchIt := columnBatchInfos.batchInfoByID.Iterator()
			if columnBatchIt.Next() {
				gpq.pushBatchIntoGlobalPriorityQueue(h, columnBatchInfos, columnID, columnBatchIt)
			}
		}
	}
	h.RUnlock()
	return gpq
}

// globalPriority is the holding struct for all the neccesarry fields to
// compare batch priority in a global view.
type globalPriority struct {
	shardID  int
	columnID int

	// globalPriority comparison is based on the below 4 fields.
	isPreloading   bool
	columnPriority int64
	batchID        int
	size           int64
}

// globalPriorityComparator provides a basic comparison on globalPriority
func globalPriorityComparator(a, b interface{}) int {
	aAsserted := a.(*globalPriority)
	bAsserted := b.(*globalPriority)
	if aAsserted.isPreloading == bAsserted.isPreloading {
		if aAsserted.columnPriority == bAsserted.columnPriority {
			if aAsserted.batchID == bAsserted.batchID {
				return int(bAsserted.size - aAsserted.size)
			}
			return aAsserted.batchID - bAsserted.batchID
		}
		return int(aAsserted.columnPriority - bAsserted.columnPriority)
	} else if aAsserted.isPreloading {
		return 1
	} else {
		return -1
	}
}

func createBatchPriority(shardID, columnID int, isPreloading bool, columnPriority int64, batchID int, size int64) *globalPriority {
	return &globalPriority{
		shardID:        shardID,
		columnID:       columnID,
		isPreloading:   isPreloading,
		columnPriority: columnPriority,
		batchID:        batchID,
		size:           size,
	}
}

// globalPriorityQueue definition START.
// A globalPriorityQueue implements heap.Interface and holds many Item pointers.
// Sorting method is based on globalPriorityComparator for struct globalPriority.

type globalPriorityItem struct {
	value    *columnBatchInfos
	it       rbt.Iterator
	priority *globalPriority
}

type globalPriorityQueue []*globalPriorityItem

func (gpq globalPriorityQueue) Len() int { return len(gpq) }

func (gpq globalPriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return globalPriorityComparator(gpq[i].priority, gpq[j].priority) < 0
}

func (gpq globalPriorityQueue) Swap(i, j int) {
	gpq[i], gpq[j] = gpq[j], gpq[i]
}

func (gpq *globalPriorityQueue) Push(x interface{}) {
	globalPriorityItem := x.(*globalPriorityItem)
	*gpq = append(*gpq, globalPriorityItem)
}

func (gpq *globalPriorityQueue) push(item *globalPriorityItem) {
	heap.Push(gpq, item)
}

func (gpq *globalPriorityQueue) Pop() interface{} {
	old := *gpq
	n := len(old)
	globalPriorityItem := old[n-1]
	*gpq = old[0 : n-1]
	return globalPriorityItem
}

func (gpq *globalPriorityQueue) pop() *globalPriorityItem {
	globalPriorityItem := heap.Pop(gpq).(*globalPriorityItem)
	return globalPriorityItem
}

func newGlobalPriorityQueue() *globalPriorityQueue {
	gpq := make(globalPriorityQueue, 0)
	heap.Init(&gpq)
	return &gpq
}

func (gpq *globalPriorityQueue) isEmpty() bool {
	return gpq.Len() == 0
}
func (gpq *globalPriorityQueue) size() int {
	return gpq.Len()
}

// globalPriorityQueue definition END.

// isPreloadingBatch will check if a given batchID falling into the preloading
// zone.
// batchID is daysSinceEpoch value.
func isPreloadingBatch(batchID, preloadingDays int) bool {
	if int(utils.Now().Unix()/86400)-batchID < preloadingDays {
		return true
	}
	return false
}
