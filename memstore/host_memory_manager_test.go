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
	"github.com/uber/aresdb/memstore/vectors"
	"os"

	"github.com/uber/aresdb/common"
	diskMocks "github.com/uber/aresdb/diskstore/mocks"
	memCom "github.com/uber/aresdb/memstore/common"
	memComMocks "github.com/uber/aresdb/memstore/common/mocks"
	"github.com/uber/aresdb/metastore"
	metaCom "github.com/uber/aresdb/metastore/common"
	metaMocks "github.com/uber/aresdb/metastore/mocks"
	"github.com/uber/aresdb/utils"

	"bytes"
	"encoding/json"
	"io"
	"sync"
	"time"

	rbt "github.com/emirpasic/gods/trees/redblacktree"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
	"github.com/uber/aresdb/redolog"
	"go.uber.org/zap"
)

var _ = ginkgo.Describe("HostMemoryManager", func() {
	var today int
	testBasePath := "/tmp/testHostMemoryManager"

	var c1 memCom.VectorParty
	var err error
	var writer io.WriteCloser
	var buf *bytes.Buffer
	var testDiskStore *diskMocks.DiskStore
	var testMetaStore *metaMocks.MetaStore
	var testMemStore *memStoreImpl
	var testHostMemoryManager *hostMemoryManager
	var options Options
	namespace := ""

	ginkgo.BeforeEach(func() {
		utils.SetClockImplementation(func() time.Time {
			return time.Unix(10000, 0)
		})
		today = int(utils.Now().Unix() / 86400)
		os.Remove(testBasePath)
		os.MkdirAll(testBasePath, 0777)

		c1, err = GetFactory().ReadArchiveVectorParty("host-memory-manager/c1", &sync.RWMutex{})
		Ω(err).Should(BeNil())

		buf = &bytes.Buffer{}
		writer = &utils.ClosableBuffer{
			Buffer: buf,
		}

		testDiskStore = CreateMockDiskStore()
		testMetaStore = CreateMockMetaStore()
		redologManagerMaster, _ := redolog.NewRedoLogManagerMaster(namespace, &common.RedoLogConfig{}, testDiskStore, testMetaStore)
		bootstrapToken := new(memComMocks.BootStrapToken)
		options = NewOptions(bootstrapToken, redologManagerMaster)
		testMemStore = NewMemStore(testMetaStore, testDiskStore, options).(*memStoreImpl)
		testHostMemoryManager = NewHostMemoryManager(testMemStore, int64(1000)).(*hostMemoryManager)
		testMemStore.HostMemManager = testHostMemoryManager

		testDiskStore.On("OpenVectorPartyFileForWrite",
			mock.Anything, mock.Anything, mock.Anything,
			mock.Anything, mock.Anything, mock.Anything).Return(writer, nil)

		testDiskStore.On("ListLogFiles", mock.Anything, mock.Anything).
			Return(nil, nil).Once()

		serializer := memCom.NewVectorPartyArchiveSerializer(testHostMemoryManager, testDiskStore,
			"test", 0, 0, 0, 0, 0)

		Ω(serializer.WriteVectorParty(c1)).Should(BeNil())
	})

	ginkgo.AfterEach(func() {
		utils.ResetClockImplementation()
		options.redoLogMaster.Stop()
	})

	ginkgo.AfterSuite(func() {
		os.RemoveAll(testBasePath)
	})

	// Initialize logging.
	logger := zap.NewExample().Sugar()

	ginkgo.It("Test shardBatchID", func() {
		logger.Infof("Test shardBatchID Started")
		for i := 0; i < 100; i++ {
			shardBatchID := newShardBatchID(i, i)
			Ω(shardBatchID.shardID).Should(Equal(i))
			Ω(shardBatchID.batchID).Should(Equal(i))
		}
		logger.Infof("Test shardBatchID Finished")
	})

	ginkgo.It("Test Preloading", func() {
		logger.Infof("Test Preloading Started")
		for preloadingDays := 0; preloadingDays < 100; preloadingDays++ {
			for delay := 0; delay < preloadingDays-1; delay++ {
				batchID := today - delay
				Ω(isPreloadingBatch(batchID, preloadingDays)).Should(Equal(true))
			}
			for delay := preloadingDays + 1; delay < 100; delay++ {
				batchID := today - delay
				Ω(isPreloadingBatch(batchID, preloadingDays)).Should(Equal(false))
			}
		}
		logger.Infof("Test Preloading Finished")
	})

	ginkgo.It("Test columnBatchInfos", func() {
		logger.Infof("Test columnBatchInfos Started")
		columnBatchInfos := newColumnBatchInfos("mytable")
		size := int64(1000)
		for batchID := 1; batchID < 100; batchID++ {
			for shard := 0; shard < 5; shard++ {
				bytesChanges := columnBatchInfos.SetManagedObject(shard, batchID, size)
				Ω(bytesChanges).Should(Equal(size))
			}
		}
		for batchID := 99; batchID > 0; batchID-- {
			for shard := 9; shard > 4; shard-- {
				bytesChanges := columnBatchInfos.SetManagedObject(shard, batchID, size)
				Ω(bytesChanges).Should(Equal(size))
			}
		}
		iter := columnBatchInfos.batchInfoByID.Iterator()
		for batchID := 1; batchID < 100; batchID++ {
			for shard := 9; shard >= 0; shard-- {
				iter.Next()
				key := iter.Key().(shardBatchID)
				Ω(key.shardID).Should(Equal(shard))
				Ω(key.batchID).Should(Equal(batchID))
				Ω(iter.Value().(int64)).Should(Equal(size))
			}
		}
		logger.Infof("Test columnBatchInfos Finished")
	})

	ginkgo.It("Test BatchPriority", func() {
		logger.Infof("Test BatchPriority Started")
		shardID := 0
		columnID := 1
		isPreloading := true
		columnPriority := 0
		batchID := 17320
		size := int64(100)
		bp1 := createBatchPriority(shardID, columnID, isPreloading, int64(columnPriority), batchID, size)

		isPreloading = false
		columnPriority = 50
		batchID = 17320
		bp2 := createBatchPriority(shardID, columnID, isPreloading, int64(columnPriority), batchID, size)

		isPreloading = true
		columnPriority = 50
		batchID = 17320
		bp3 := createBatchPriority(shardID, columnID, isPreloading, int64(columnPriority), batchID, size)

		isPreloading = false
		columnPriority = 50
		batchID = 17312
		bp4 := createBatchPriority(shardID, columnID, isPreloading, int64(columnPriority), batchID, size)

		isPreloading = false
		columnPriority = 50
		batchID = 17312
		size = int64(500)
		bp5 := createBatchPriority(shardID, columnID, isPreloading, int64(columnPriority), batchID, size)

		// Global order: bp3>bp1>bp2>bp4>bp5
		Ω(globalPriorityComparator(bp1, bp2) > 0).Should(Equal(true))
		Ω(globalPriorityComparator(bp1, bp3) < 0).Should(Equal(true))
		Ω(globalPriorityComparator(bp1, bp4) > 0).Should(Equal(true))
		Ω(globalPriorityComparator(bp1, bp5) > 0).Should(Equal(true))
		Ω(globalPriorityComparator(bp2, bp1) < 0).Should(Equal(true))
		Ω(globalPriorityComparator(bp2, bp3) < 0).Should(Equal(true))
		Ω(globalPriorityComparator(bp2, bp4) > 0).Should(Equal(true))
		Ω(globalPriorityComparator(bp2, bp5) > 0).Should(Equal(true))
		Ω(globalPriorityComparator(bp3, bp1) > 0).Should(Equal(true))
		Ω(globalPriorityComparator(bp3, bp2) > 0).Should(Equal(true))
		Ω(globalPriorityComparator(bp3, bp4) > 0).Should(Equal(true))
		Ω(globalPriorityComparator(bp3, bp5) > 0).Should(Equal(true))
		Ω(globalPriorityComparator(bp4, bp1) < 0).Should(Equal(true))
		Ω(globalPriorityComparator(bp4, bp2) < 0).Should(Equal(true))
		Ω(globalPriorityComparator(bp4, bp3) < 0).Should(Equal(true))
		Ω(globalPriorityComparator(bp4, bp5) > 0).Should(Equal(true))
		Ω(globalPriorityComparator(bp5, bp1) < 0).Should(Equal(true))
		Ω(globalPriorityComparator(bp5, bp2) < 0).Should(Equal(true))
		Ω(globalPriorityComparator(bp5, bp3) < 0).Should(Equal(true))
		Ω(globalPriorityComparator(bp5, bp4) < 0).Should(Equal(true))

		// Test equal
		Ω(globalPriorityComparator(bp1, bp1) == 0).Should(Equal(true))
		Ω(globalPriorityComparator(bp2, bp2) == 0).Should(Equal(true))
		Ω(globalPriorityComparator(bp3, bp3) == 0).Should(Equal(true))
		Ω(globalPriorityComparator(bp4, bp4) == 0).Should(Equal(true))
		Ω(globalPriorityComparator(bp5, bp5) == 0).Should(Equal(true))
		logger.Infof("Test BatchPriority Finished")
	})

	ginkgo.It("Test globalPriorityQueue", func() {
		logger.Infof("Test globalPriorityQueue Started")
		shardID := 0
		columnID := 1
		isPreloading := true
		columnPriority := 0
		batchID := 17320
		size := int64(100)
		bp1 := createBatchPriority(shardID, columnID, isPreloading, int64(columnPriority), batchID, size)

		isPreloading = false
		columnPriority = 50
		batchID = 17320
		bp2 := createBatchPriority(shardID, columnID, isPreloading, int64(columnPriority), batchID, size)

		isPreloading = true
		columnPriority = 50
		batchID = 17320
		bp3 := createBatchPriority(shardID, columnID, isPreloading, int64(columnPriority), batchID, size)

		isPreloading = false
		columnPriority = 50
		batchID = 17312
		bp4 := createBatchPriority(shardID, columnID, isPreloading, int64(columnPriority), batchID, size)

		isPreloading = false
		columnPriority = 50
		batchID = 17312
		size = int64(500)
		bp5 := createBatchPriority(shardID, columnID, isPreloading, int64(columnPriority), batchID, size)

		gpq := newGlobalPriorityQueue()
		bsl1 := newColumnBatchInfos("myTable1")
		bsl2 := newColumnBatchInfos("myTable2")
		Ω(gpq.isEmpty()).Should(Equal(true))
		gpq.push(&globalPriorityItem{
			value:    bsl1,
			it:       rbt.Iterator{},
			priority: bp1,
		})

		gpq.push(&globalPriorityItem{
			value:    bsl1,
			it:       rbt.Iterator{},
			priority: bp2,
		})
		gpq.push(&globalPriorityItem{
			value:    bsl2,
			it:       rbt.Iterator{},
			priority: bp3,
		})
		gpq.push(&globalPriorityItem{
			value:    bsl2,
			it:       rbt.Iterator{},
			priority: bp4,
		})
		gpq.push(&globalPriorityItem{
			value:    bsl2,
			it:       rbt.Iterator{},
			priority: bp5,
		})

		Ω(gpq.size()).Should(Equal(5))
		globalPriorityItem1 := gpq.pop()
		Ω(gpq.size()).Should(Equal(4))
		globalPriorityItem2 := gpq.pop()
		Ω(gpq.size()).Should(Equal(3))
		globalPriorityItem3 := gpq.pop()
		Ω(gpq.size()).Should(Equal(2))
		globalPriorityItem4 := gpq.pop()
		Ω(gpq.size()).Should(Equal(1))
		globalPriorityItem5 := gpq.pop()
		Ω(gpq.isEmpty()).Should(Equal(true))

		// Global order: bp3>bp1>bp2>bp4>bp5
		Ω(globalPriorityItem1.priority.isPreloading).Should(Equal(false))
		Ω(globalPriorityItem2.priority.isPreloading).Should(Equal(false))
		Ω(globalPriorityItem3.priority.isPreloading).Should(Equal(false))
		Ω(globalPriorityItem4.priority.isPreloading).Should(Equal(true))
		Ω(globalPriorityItem5.priority.isPreloading).Should(Equal(true))

		Ω(globalPriorityItem1.priority.batchID).Should(Equal(17312))
		Ω(globalPriorityItem2.priority.batchID).Should(Equal(17312))
		Ω(globalPriorityItem3.priority.batchID).Should(Equal(17320))
		Ω(globalPriorityItem4.priority.batchID).Should(Equal(17320))
		Ω(globalPriorityItem5.priority.batchID).Should(Equal(17320))

		Ω(globalPriorityItem1.priority.columnPriority).Should(Equal(int64(50)))
		Ω(globalPriorityItem2.priority.columnPriority).Should(Equal(int64(50)))
		Ω(globalPriorityItem3.priority.columnPriority).Should(Equal(int64(50)))
		Ω(globalPriorityItem4.priority.columnPriority).Should(Equal(int64(0)))
		Ω(globalPriorityItem5.priority.columnPriority).Should(Equal(int64(50)))

		Ω(globalPriorityItem1.priority.size).Should(Equal(int64(500)))
		Ω(globalPriorityItem2.priority.size).Should(Equal(int64(100)))
		Ω(globalPriorityItem3.priority.size).Should(Equal(int64(100)))
		Ω(globalPriorityItem4.priority.size).Should(Equal(int64(100)))
		Ω(globalPriorityItem5.priority.size).Should(Equal(int64(100)))

		Ω(globalPriorityItem1.value.table).Should(Equal("myTable2"))
		Ω(globalPriorityItem2.value.table).Should(Equal("myTable2"))
		Ω(globalPriorityItem3.value.table).Should(Equal("myTable1"))
		Ω(globalPriorityItem4.value.table).Should(Equal("myTable1"))
		Ω(globalPriorityItem5.value.table).Should(Equal("myTable2"))
		logger.Infof("Test globalPriorityQueue Finished")

	})

	ginkgo.It("Test HostMemoryManager init and basic ops", func() {
		logger.Infof("Test HostMemoryManager init and basic ops Started")
		// Check totalMemorySize
		Ω(testHostMemoryManager.totalMemorySize).Should(Equal(int64(1000)))

		// Check unManagedMemorySize
		Ω(testHostMemoryManager.unManagedMemorySize).Should(Equal(int64(0)))
		testHostMemoryManager.ReportUnmanagedSpaceUsageChange(300)
		Ω(testHostMemoryManager.unManagedMemorySize).Should(Equal(int64(300)))
		testHostMemoryManager.ReportUnmanagedSpaceUsageChange(100)
		Ω(testHostMemoryManager.unManagedMemorySize).Should(Equal(int64(400)))
		testHostMemoryManager.ReportUnmanagedSpaceUsageChange(-200)
		Ω(testHostMemoryManager.unManagedMemorySize).Should(Equal(int64(200)))

		// Check managedMemorySize
		// Adding batch 15739 for columnID 0 with size 140
		testHostMemoryManager.ReportManagedObject("mytable", 0, 15739, 0, 140)
		Ω(testHostMemoryManager.managedMemorySize).Should(Equal(int64(140)))
		Ω(len(testHostMemoryManager.batchInfosByColumn["mytable"])).Should(Equal(1))

		// Adding batch 15739 for columnID 1 with size 140
		testHostMemoryManager.ReportManagedObject("mytable", 0, 15739, 1, 140)
		Ω(testHostMemoryManager.managedMemorySize).Should(Equal(int64(280)))
		Ω(len(testHostMemoryManager.batchInfosByColumn["mytable"])).Should(Equal(2))

		// Update batch 15739 for columnID 1 with size 440
		testHostMemoryManager.ReportManagedObject("mytable", 0, 15739, 1, 40)
		Ω(testHostMemoryManager.managedMemorySize).Should(Equal(int64(180)))

		// Delete batch 15739 for columnID 1
		testHostMemoryManager.ReportManagedObject("mytable", 0, 15739, 1, 0)
		Ω(testHostMemoryManager.managedMemorySize).Should(Equal(int64(140)))
		Ω(len(testHostMemoryManager.batchInfosByColumn["mytable"])).Should(Equal(1))

		// Adding batch 15739 for columnID 2 with size 140
		testHostMemoryManager.ReportManagedObject("mytable", 0, 15739, 2, 140)
		Ω(testHostMemoryManager.managedMemorySize).Should(Equal(int64(280)))
		Ω(len(testHostMemoryManager.batchInfosByColumn["mytable"])).Should(Equal(2))

		// Delete batch 15739 for columnID 2
		testHostMemoryManager.ReportManagedObject("mytable", 0, 15739, 2, -140)
		Ω(testHostMemoryManager.managedMemorySize).Should(Equal(int64(140)))
		Ω(len(testHostMemoryManager.batchInfosByColumn["mytable"])).Should(Equal(1))

		// Delete batch 15739 for columnID 2 again
		testHostMemoryManager.ReportManagedObject("mytable", 0, 15739, 2, 0)
		Ω(testHostMemoryManager.managedMemorySize).Should(Equal(int64(140)))
		Ω(len(testHostMemoryManager.batchInfosByColumn["mytable"])).Should(Equal(1))
		Ω(testHostMemoryManager.batchInfosByColumn["mytable"][0].batchInfoByID.Size()).Should(Equal(1))

		// Report batch 15740 for columnID 0 with size 150
		testHostMemoryManager.ReportManagedObject("mytable", 0, 15740, 0, 150)
		Ω(testHostMemoryManager.managedMemorySize).Should(Equal(int64(290)))
		Ω(len(testHostMemoryManager.batchInfosByColumn["mytable"])).Should(Equal(1))
		Ω(testHostMemoryManager.batchInfosByColumn["mytable"][0].batchInfoByID.Size()).Should(Equal(2))
		logger.Infof("Test HostMemoryManager init and basic ops Finished")

	})

	ginkgo.It("Test HostMemoryManager tryPreload and triggerPreload", func() {
		logger.Infof("Test HostMemoryManager tryPreload and triggerPreload Started")
		testTableName := "myTable"
		testTable := &metaCom.Table{
			Name:        testTableName,
			IsFactTable: true,
			Columns: []metaCom.Column{
				{
					Name: "c0",
					Type: metaCom.Uint32,
					Config: metaCom.ColumnConfig{
						PreloadingDays: 0,
						Priority:       0,
					},
				},
				{
					Name: "c1",
					Type: metaCom.Uint32,
					Config: metaCom.ColumnConfig{
						PreloadingDays: 5,
						Priority:       10,
					},
				},
				{
					Name: "c2",
					Type: metaCom.Uint32,
					Config: metaCom.ColumnConfig{
						PreloadingDays: 10,
						Priority:       10,
					},
				},
			},
			PrimaryKeyColumns: []int{1},
		}
		testTable.Config = metastore.DefaultTableConfig
		testTable.Config.BatchSize = 10
		testMetaStore, err := metastore.NewDiskMetaStore(testBasePath)
		Ω(err).Should(BeNil())
		testMemStore = NewMemStore(testMetaStore, testDiskStore, options).(*memStoreImpl)
		// Init HostMemoryManager
		testHostMemoryManager = NewHostMemoryManager(testMemStore, int64(20000)).(*hostMemoryManager)
		testMemStore.HostMemManager = testHostMemoryManager
		testHostMemoryManager.unManagedMemorySize = 0
		// Check totalMemorySize
		Ω(testHostMemoryManager.totalMemorySize).Should(Equal(int64(20000)))
		testSchema := memCom.NewTableSchema(testTable)
		for columnID := range testSchema.Schema.Columns {
			testSchema.SetDefaultValue(columnID)
		}
		testMemStore.TableShards[testTableName] = make(map[int]*TableShard)
		testMemStore.TableShards[testTableName][0] = NewTableShard(testSchema, testMetaStore,
			testDiskStore, testHostMemoryManager, 0, options)
		testMemStore.TableSchemas[testTableName] = testSchema

		testMemStore.TableShards[testTableName][0].ArchiveStore = &ArchiveStore{
			CurrentVersion: &ArchiveStoreVersion{
				Batches:         map[int32]*ArchiveBatch{},
				ArchivingCutoff: 100,
				shard:           testMemStore.TableShards[testTableName][0],
			},
		}

		// Set preload batches.
		// Note we allocate one more day after today to test date shift case.
		for day := today + 1; day > today-10; day-- {
			testMemStore.TableShards[testTableName][0].ArchiveStore.CurrentVersion.Batches[int32(day)] =
				&ArchiveBatch{
					Batch:   memCom.Batch{RWMutex: &sync.RWMutex{}},
					Size:    4,
					Shard:   testMemStore.TableShards[testTableName][0],
					BatchID: int32(day),
				}
		}

		// Note we allocate one more day after today to test date shift case.
		for columnID, column := range testTable.Columns {
			for day := today + 1; day > today-column.Config.PreloadingDays; day-- {
				reader := &utils.ClosableReader{
					Reader: bytes.NewReader(buf.Bytes()),
				}
				testDiskStore.On("OpenVectorPartyFileForRead",
					testTableName, columnID, 0, day, uint32(0), uint32(0)).Return(reader, nil)
			}
		}
		// avoid eviction.
		testHostMemoryManager.unManagedMemorySize = 0
		testHostMemoryManager.memStore.preloadAllFactTables()
		testHostMemoryManager.Start()
		defer testHostMemoryManager.Stop()
		Ω(testHostMemoryManager.getManagedSpaceUsage()).Should(Equal(int64(128 * (10 + 5))))

		for columnID, column := range testTable.Columns {
			for day := today; day > today-column.Config.PreloadingDays; day-- {
				Ω(testHostMemoryManager.managedObjectExists(testTableName, 0, day, columnID)).Should(BeTrue())
			}
		}

		// Test Preloading days not changed.
		newTableConfig := metaCom.ColumnConfig{
			PreloadingDays: 0,
			Priority:       0,
		}

		// Handle config changes.
		Ω(testMemStore.FetchSchema()).Should(BeNil())
		ownershipEvents, done2, err := testMetaStore.WatchShardOwnershipEvents()
		Ω(err).Should(BeNil())
		go func() {
			<-ownershipEvents
			done2 <- struct{}{}
		}()
		err = testMetaStore.CreateTable(testTable)
		Ω(err).Should(BeNil())
		// Should not trigger any preload.
		Ω(testMetaStore.UpdateColumn(testTableName, "c0", newTableConfig)).Should(BeNil())

		// Trigger twice to wait completion for first preloading.
		testHostMemoryManager.preloadJobChan <- preloadJob{
			tableName: testTableName, columnID: 0, oldPreloadingDays: 0, newPreloadingDays: 0}
		testHostMemoryManager.preloadJobChan <- preloadJob{}

		Ω(testHostMemoryManager.managedObjectExists(testTableName, 0, today, 0)).Should(BeFalse())

		// Test Preloading days increased.
		newTableConfig = metaCom.ColumnConfig{
			PreloadingDays: 1,
			Priority:       0,
		}
		// Set reader for column
		reader := &utils.ClosableReader{
			Reader: bytes.NewReader(buf.Bytes()),
		}

		testDiskStore.On("OpenVectorPartyFileForRead",
			testTableName, 0, 0, today, uint32(0), uint32(0)).Return(reader, nil)

		Ω(testMetaStore.UpdateColumn(testTableName, "c0", newTableConfig)).Should(BeNil())

		// Should trigger preload one day data for c0.
		testHostMemoryManager.preloadJobChan <- preloadJob{
			tableName:         testTableName,
			columnID:          0,
			oldPreloadingDays: 0,
			newPreloadingDays: 1,
		}
		testHostMemoryManager.preloadJobChan <- preloadJob{}

		Ω(testHostMemoryManager.managedObjectExists(testTableName, 0, today, 0)).Should(BeTrue())

		logger.Infof("Test HostMemoryManager tryPreload and triggerPreload Finished")
	})

	ginkgo.It("Test HostMemoryManager tryEviction", func() {
		logger.Infof("Test HostMemoryManager tryEviction Started")
		testTableName := "myTable"
		testTable := &metaCom.Table{
			Name:        testTableName,
			IsFactTable: true,
			Columns: []metaCom.Column{
				{
					Name: "c0",
					Config: metaCom.ColumnConfig{
						PreloadingDays: 0,
						Priority:       0,
					},
				},
				{
					Name: "c1",
					Config: metaCom.ColumnConfig{
						PreloadingDays: 5,
						Priority:       10,
					},
				},
				{
					Name: "c2",
					Config: metaCom.ColumnConfig{
						PreloadingDays: 10,
						Priority:       10,
					},
				},
				{
					Name: "c3",
					Config: metaCom.ColumnConfig{
						PreloadingDays: 10,
						Priority:       20,
					},
				},
			},
			Config: metaCom.TableConfig{
				BatchSize: 10,
			},
		}

		testMetaStore, err := metastore.NewDiskMetaStore(testBasePath)
		// watch schema change
		_, done, err := testMetaStore.WatchTableSchemaEvents()
		Ω(err).Should(BeNil())
		go func() {
			done <- struct{}{}
		}()

		ownershipEvents, done2, err := testMetaStore.WatchShardOwnershipEvents()
		Ω(err).Should(BeNil())
		go func() {
			<-ownershipEvents
			done2 <- struct{}{}
		}()

		testMetaStore.CreateTable(testTable)
		testSchema := memCom.NewTableSchema(testTable)
		testMemStore.TableShards[testTableName] = make(map[int]*TableShard)
		testMemStore.TableShards[testTableName][0] = NewTableShard(testSchema, testMetaStore,
			testDiskStore, testHostMemoryManager, 0, options)
		testMemStore.TableShards[testTableName][1] = NewTableShard(testSchema, testMetaStore,
			testDiskStore, testHostMemoryManager, 1, options)
		testMemStore.TableSchemas[testTableName] = testSchema

		testMemStore.TableShards[testTableName][0].ArchiveStore = &ArchiveStore{
			CurrentVersion: &ArchiveStoreVersion{
				Batches:         map[int32]*ArchiveBatch{},
				ArchivingCutoff: 100,
			},
		}

		testMemStore.TableShards[testTableName][1].ArchiveStore = &ArchiveStore{
			CurrentVersion: &ArchiveStoreVersion{
				Batches:         map[int32]*ArchiveBatch{},
				ArchivingCutoff: 100,
			},
		}

		// Init HostMemoryManager
		testMemStore.HostMemManager = testHostMemoryManager
		testHostMemoryManager.unManagedMemorySize = 0
		// Check totalMemorySize
		Ω(testHostMemoryManager.totalMemorySize).Should(Equal(int64(1000)))

		// Check unManagedMemorySize
		// Size should be equal to live store and primary key size.
		Ω(testHostMemoryManager.unManagedMemorySize).Should(Equal(int64(0)))
		testHostMemoryManager.ReportUnmanagedSpaceUsageChange(300)
		Ω(testHostMemoryManager.unManagedMemorySize).Should(Equal(int64(300)))

		// Check managedMemorySize
		// Test case 0:
		// Adding batch 15739 for columnID 0 with size 800
		testShard := testMemStore.TableShards[testTableName][0]
		testMemStore.TableShards[testTableName][0].ArchiveStore.CurrentVersion.Batches[15739] =
			CreateTestArchiveBatch(testShard, 15739)
		testHostMemoryManager.ReportManagedObject(testTableName, 0, 15739, 0, 800)
		Ω(testHostMemoryManager.managedMemorySize).Should(Equal(int64(800)))
		Ω(len(testHostMemoryManager.batchInfosByColumn[testTableName])).Should(Equal(1))
		// Adding batch 15740 for columnID 1 with size 800
		testMemStore.TableShards[testTableName][0].ArchiveStore.CurrentVersion.Batches[15740] =
			CreateTestArchiveBatch(testShard, 15740)
		testHostMemoryManager.ReportManagedObject(testTableName, 0, 15740, 1, 800)
		Ω(testHostMemoryManager.managedMemorySize).Should(Equal(int64(1600)))
		Ω(len(testHostMemoryManager.batchInfosByColumn[testTableName])).Should(Equal(2))
		// Call tryEviction explictly, should evict all.
		testHostMemoryManager.tryEviction()
		Ω(testHostMemoryManager.managedMemorySize).Should(Equal(int64(0)))
		Ω(len(testHostMemoryManager.batchInfosByColumn[testTableName])).Should(Equal(0))

		// Test case 1:
		// Adding batch 15739 for columnID 0 with size 800
		testMemStore.TableShards[testTableName][0].ArchiveStore.CurrentVersion.Batches[15739] =
			CreateTestArchiveBatch(testShard, 15739)
		testHostMemoryManager.ReportManagedObject(testTableName, 0, 15739, 0, 800)
		Ω(testHostMemoryManager.managedMemorySize).Should(Equal(int64(800)))
		Ω(len(testHostMemoryManager.batchInfosByColumn[testTableName])).Should(Equal(1))
		// Adding batch 15740 for columnID 1 with size 400
		testMemStore.TableShards[testTableName][0].ArchiveStore.CurrentVersion.Batches[15740] =
			CreateTestArchiveBatch(testShard, 15740)
		testHostMemoryManager.ReportManagedObject(testTableName, 0, 15740, 1, 400)
		Ω(testHostMemoryManager.managedMemorySize).Should(Equal(int64(1200)))
		Ω(len(testHostMemoryManager.batchInfosByColumn[testTableName])).Should(Equal(2))
		// Call tryEviction explictly. Should only evict one batch.
		println("Test case 1")
		testHostMemoryManager.tryEviction()
		Ω(testHostMemoryManager.managedMemorySize).Should(Equal(int64(400)))
		Ω(len(testHostMemoryManager.batchInfosByColumn[testTableName])).Should(Equal(1))

		// Test case 2:
		// Update batch 15740 for columnID 1 with size 1000
		testMemStore.TableShards[testTableName][0].ArchiveStore.CurrentVersion.Batches[15740] =
			CreateTestArchiveBatch(testShard, 15740)
		testHostMemoryManager.ReportManagedObject(testTableName, 0, 15740, 1, 1000)
		Ω(testHostMemoryManager.managedMemorySize).Should(Equal(int64(1000)))
		Ω(len(testHostMemoryManager.batchInfosByColumn[testTableName])).Should(Equal(1))
		// Call tryEviction explictly, nothing should be left.
		testHostMemoryManager.tryEviction()
		Ω(testHostMemoryManager.managedMemorySize).Should(Equal(int64(0)))
		Ω(len(testHostMemoryManager.batchInfosByColumn[testTableName])).Should(Equal(0))

		// Test case 3:
		// Adding batch 15739 for columnID 0 with size 800
		testMemStore.TableShards[testTableName][0].ArchiveStore.CurrentVersion.Batches[15739] =
			CreateTestArchiveBatch(testShard, 15739)
		testHostMemoryManager.ReportManagedObject(testTableName, 0, 15739, 0, 800)
		Ω(testHostMemoryManager.managedMemorySize).Should(Equal(int64(800)))
		Ω(len(testHostMemoryManager.batchInfosByColumn[testTableName])).Should(Equal(1))
		// Adding batch 15740 for columnID 0 with size 800
		testMemStore.TableShards[testTableName][0].ArchiveStore.CurrentVersion.Batches[15739] =
			CreateTestArchiveBatch(testShard, 15739)
		testHostMemoryManager.ReportManagedObject(testTableName, 0, 15740, 0, 800)
		Ω(testHostMemoryManager.managedMemorySize).Should(Equal(int64(1600)))
		Ω(len(testHostMemoryManager.batchInfosByColumn[testTableName])).Should(Equal(1))
		// Call tryEviction explictly, should evict all.
		testHostMemoryManager.tryEviction()
		Ω(testHostMemoryManager.managedMemorySize).Should(Equal(int64(0)))
		Ω(len(testHostMemoryManager.batchInfosByColumn[testTableName])).Should(Equal(0))

		// Try eviction in evictor execution loop.
		logger.Infof("Test HostMemoryManager tryEviction Finished")
	})

	ginkgo.It("Test HostMemoryManager triggerEviction", func() {
		logger.Infof("Test HostMemoryManager triggerEviction Started")
		testTableName := "myTable"
		testTable := &metaCom.Table{
			Name:        testTableName,
			IsFactTable: true,
			Columns: []metaCom.Column{
				{
					Name: "c0",
				},
				{
					Name: "c1",
				},
				{
					Name: "c2",
				},
				{
					Name: "c3",
				},
			},
			Config: metaCom.TableConfig{
				BatchSize: 10,
			},
		}

		testMetaStore, err := metastore.NewDiskMetaStore(testBasePath)
		// watch schema change
		_, done, err := testMetaStore.WatchTableSchemaEvents()
		Ω(err).Should(BeNil())
		go func() {
			done <- struct{}{}
		}()

		ownershipEvents, done2, err := testMetaStore.WatchShardOwnershipEvents()
		Ω(err).Should(BeNil())
		go func() {
			<-ownershipEvents
			done2 <- struct{}{}
		}()

		testMetaStore.CreateTable(testTable)
		testSchema := memCom.NewTableSchema(testTable)
		testMemStore.TableShards[testTableName] = make(map[int]*TableShard)
		testMemStore.TableShards[testTableName][0] = NewTableShard(testSchema, testMetaStore,
			testDiskStore, testHostMemoryManager, 0, options)
		testMemStore.TableSchemas[testTableName] = testSchema

		testMemStore.TableShards[testTableName][0].ArchiveStore = &ArchiveStore{
			CurrentVersion: &ArchiveStoreVersion{
				Batches:         map[int32]*ArchiveBatch{},
				ArchivingCutoff: 100,
			},
		}

		// Init HostMemoryManager
		testMemStore.HostMemManager = testHostMemoryManager
		testHostMemoryManager.unManagedMemorySize = 0
		// Check totalMemorySize
		Ω(testHostMemoryManager.totalMemorySize).Should(Equal(int64(1000)))

		// Start eviction executor loop.
		testHostMemoryManager.memStore.preloadAllFactTables()
		testHostMemoryManager.Start()
		defer testHostMemoryManager.Stop()
		// Check managedMemorySize
		// Test case 0:
		// Adding batch 15739 for columnID 0 with size 800
		testShard := testMemStore.TableShards[testTableName][0]
		testMemStore.TableShards[testTableName][0].ArchiveStore.CurrentVersion.Batches[15739] =
			CreateTestArchiveBatch(testShard, 15739)
		testHostMemoryManager.ReportManagedObject(testTableName, 0, 15739, 0, 800)
		Ω(testHostMemoryManager.managedMemorySize).Should(Equal(int64(800)))
		Ω(testHostMemoryManager.managedObjectExists(testTableName, 0, 15739, 0)).
			Should(BeTrue())

		testMemStore.TableShards[testTableName][0].ArchiveStore.CurrentVersion.Batches[15740] =
			CreateTestArchiveBatch(testShard, 15740)

		testHostMemoryManager.ReportManagedObject(testTableName, 0, 15740, 0, 300)
		// Wait for first eviction to finish.
		testHostMemoryManager.evictionJobChan <- struct{}{}
		testHostMemoryManager.evictionJobChan <- struct{}{}
		// Column for batch 15739 is evicted.
		Ω(testHostMemoryManager.managedMemorySize).Should(Equal(int64(300)))
		Ω(testHostMemoryManager.managedObjectExists(testTableName, 0, 15739, 0)).
			Should(BeFalse())
		Ω(testHostMemoryManager.managedObjectExists(testTableName, 0, 15740, 0)).
			Should(BeTrue())
		logger.Infof("Test HostMemoryManager triggerEviction Finished")
	})

	ginkgo.It("Test HostMemoryManager eviction with rlock", func() {
		logger.Infof("Test HostMemoryManager eviction with rlock Started")
		testMetaStore, err := metastore.NewDiskMetaStore(testBasePath)
		Ω(err).Should(BeNil())
		testTableName := "myTable"
		testTable := &metaCom.Table{
			Name:        testTableName,
			IsFactTable: true,
			Columns: []metaCom.Column{
				{
					Name: "c0",
					Config: metaCom.ColumnConfig{
						PreloadingDays: 0,
						Priority:       0,
					},
				},
				{
					Name: "c1",
					Config: metaCom.ColumnConfig{
						PreloadingDays: 5,
						Priority:       10,
					},
				},
				{
					Name: "c2",
					Config: metaCom.ColumnConfig{
						PreloadingDays: 10,
						Priority:       10,
					},
				},
				{
					Name: "c3",
					Config: metaCom.ColumnConfig{
						PreloadingDays: 10,
						Priority:       20,
					},
				},
			},
			Config: metaCom.TableConfig{
				BatchSize: 10,
			},
		}
		// watch schema change
		_, done, err := testMetaStore.WatchTableSchemaEvents()
		Ω(err).Should(BeNil())
		go func() {
			done <- struct{}{}
		}()

		ownershipEvents, done2, err := testMetaStore.WatchShardOwnershipEvents()
		Ω(err).Should(BeNil())
		go func() {
			<-ownershipEvents
			done2 <- struct{}{}
		}()

		testMetaStore.CreateTable(testTable)
		testSchema := memCom.NewTableSchema(testTable)
		testMemStore.TableShards[testTableName] = make(map[int]*TableShard)
		testMemStore.TableShards[testTableName][0] = NewTableShard(testSchema, testMetaStore,
			testDiskStore, testHostMemoryManager, 0, options)
		testMemStore.TableShards[testTableName][1] = NewTableShard(testSchema, testMetaStore,
			testDiskStore, testHostMemoryManager, 1, options)
		testMemStore.TableSchemas[testTableName] = testSchema

		testBatchID1 := int32(15739)
		testMemStore.TableShards[testTableName][0].ArchiveStore = &ArchiveStore{
			CurrentVersion: &ArchiveStoreVersion{
				Batches:         map[int32]*ArchiveBatch{},
				ArchivingCutoff: 100,
			},
		}
		testMemStore.TableShards[testTableName][0].ArchiveStore.CurrentVersion.Batches[15739] =
			CreateTestArchiveBatch(testMemStore.TableShards[testTableName][0], 15739)
		// Init HostMemoryManager
		testMemStore.HostMemManager = testHostMemoryManager
		testHostMemoryManager.unManagedMemorySize = 0
		// Check totalMemorySize
		Ω(testHostMemoryManager.totalMemorySize).Should(Equal(int64(1000)))

		// Check unManagedMemorySize
		Ω(testHostMemoryManager.unManagedMemorySize).Should(Equal(int64(0)))
		testHostMemoryManager.ReportUnmanagedSpaceUsageChange(300)
		Ω(testHostMemoryManager.unManagedMemorySize).Should(Equal(int64(300)))

		// Check managedMemorySize
		// Adding batch ${testBatchID1} for columnID 0 with size 140
		testHostMemoryManager.ReportManagedObject(testTableName, 0, int(testBatchID1), 0, 800)
		Ω(testHostMemoryManager.managedMemorySize).Should(Equal(int64(800)))
		Ω(len(testHostMemoryManager.batchInfosByColumn[testTableName])).Should(Equal(1))

		// Call tryEviction explictly
		// Pin vp so eviction will fail fast
		testMemStore.TableShards[testTableName][0].ArchiveStore.GetCurrentVersion().
			Batches[testBatchID1].Columns[0].(*archiveVectorParty).Pins = 1
		testHostMemoryManager.tryEviction()
		Ω(testHostMemoryManager.managedMemorySize).Should(Equal(int64(800)))
		Ω(len(testHostMemoryManager.batchInfosByColumn[testTableName])).Should(Equal(1))

		// Release vp so eviction will happen
		testMemStore.TableShards[testTableName][0].ArchiveStore.GetCurrentVersion().
			Batches[testBatchID1].Columns[0].(*archiveVectorParty).Pins = 0
		testHostMemoryManager.tryEviction()
		Ω(testHostMemoryManager.managedMemorySize).Should(Equal(int64(0)))
		Ω(len(testHostMemoryManager.batchInfosByColumn[testTableName])).Should(Equal(0))
		logger.Infof("Test HostMemoryManager eviction with rlock Finished")
	})

	ginkgo.It("GetMemoryUsageDetails", func() {
		logger.Infof("Test GetMemoryUsageDetails Started")
		testTableName := "myTable"
		testTable := &metaCom.Table{
			Name:        testTableName,
			IsFactTable: true,
			Columns: []metaCom.Column{
				{
					Name: "c0",
					Type: "UUID",
					Config: metaCom.ColumnConfig{
						PreloadingDays: 1,
						Priority:       0,
					},
				},
			},
			PrimaryKeyColumns: []int{0},
			Config: metaCom.TableConfig{
				BatchSize:                   10,
				InitialPrimaryKeyNumBuckets: 10,
			},
		}

		testSchema := memCom.NewTableSchema(testTable)
		testMemStore.TableShards[testTableName] = make(map[int]*TableShard)
		testMemStore.TableShards[testTableName][0] = NewTableShard(testSchema, testMetaStore,
			testDiskStore, testHostMemoryManager, 0, options)

		for i := 0; i < 10; i++ {
			liveBatch := &LiveBatch{
				Batch: memCom.Batch{
					RWMutex: &sync.RWMutex{},
					Columns: []memCom.VectorParty{
						// create dummy to make vp not nil
						&cLiveVectorParty{
							cVectorParty: cVectorParty{
								values: &vectors.Vector{Bytes: 128},
								nulls:  &vectors.Vector{Bytes: 128},
							},
						},
					},
				},
				Capacity:  10,
				liveStore: testMemStore.TableShards[testTableName][0].LiveStore,
			}
			testMemStore.TableShards[testTableName][0].LiveStore.Batches[int32(-2147483648+i)] = liveBatch
		}
		testMemStore.TableSchemas[testTableName] = testSchema
		testMemStore.TableShards[testTableName][0].LiveStore.LastReadRecord = memCom.RecordID{BatchID: -2147483648 + 9, Index: 10}

		utils.SetClockImplementation(func() time.Time {
			return time.Date(2018, 02, 15, 0, 0, 0, 0, time.UTC)
		})

		testHostMemoryManager.addOrUpdateManagedObject("myTable", 0, 17577, 0, 10)
		testHostMemoryManager.addOrUpdateManagedObject("myTable", 0, 17576, 0, 10)

		memDetails, err := testMemStore.GetMemoryUsageDetails()
		Ω(err).Should(BeNil())

		jsonBytes, _ := json.MarshalIndent(memDetails, "", "\t")
		Ω(string(jsonBytes)).Should(MatchJSON(`{
			"myTable_0": {
				"cols": {
					"c0": {
						"preloaded": 10,
						"nonPreloaded": 10,
						"live": 2560
					}
				},
				"pk": 2320
			}
		}`))
		logger.Infof("Test GetMemoryUsageDetails Finished")
	})
})

// CreateMemStore creates a mocked MetaStore for testing.
func CreateMockMetaStore() *metaMocks.MetaStore {
	metaStore := &metaMocks.MetaStore{}
	return metaStore
}

func CreateTestArchiveBatchColumns() []memCom.VectorParty {
	return []memCom.VectorParty{
		&archiveVectorParty{},
		&archiveVectorParty{},
	}
}

func CreateTestArchiveBatch(shard *TableShard, batchID int) *ArchiveBatch {
	return &ArchiveBatch{
		Batch: memCom.Batch{
			RWMutex: &sync.RWMutex{},
			Columns: CreateTestArchiveBatchColumns(),
		},
		Size:    5,
		Version: 0,
		Shard:   shard,
		BatchID: int32(batchID),
	}
}
