package query

import (
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
	"github.com/uber/aresdb/cluster/topology"
	"github.com/uber/aresdb/common"
	"github.com/uber/aresdb/diskstore"
	diskMocks "github.com/uber/aresdb/diskstore/mocks"
	"github.com/uber/aresdb/memstore"
	memCom "github.com/uber/aresdb/memstore/common"
	memComMocks "github.com/uber/aresdb/memstore/common/mocks"
	"github.com/uber/aresdb/memstore/list"
	memMocks "github.com/uber/aresdb/memstore/mocks"
	metaCom "github.com/uber/aresdb/metastore/common"
	metaMocks "github.com/uber/aresdb/metastore/mocks"
	queryCom "github.com/uber/aresdb/query/common"
	"github.com/uber/aresdb/redolog"

	"encoding/json"
	"sync"
	"unsafe"
)

var _ = ginkgo.Describe("aql_processor for array", func() {

	var batch99, batch101, batch110 *memCom.Batch
	var archiveBatch0 *memstore.ArchiveBatch
	var memStore memstore.MemStore
	var metaStore metaCom.MetaStore
	var diskStore diskstore.DiskStore
	var hostMemoryManager memCom.HostMemoryManager
	var shard *memstore.TableShard
	var options memstore.Options
	var redologManagerMaster *redolog.RedoLogManagerMaster
	table := "table1"
	shardID := 0
	testFactory := memstore.GetFactory()
	testFactory.RootPath = "../testing/data"

	ginkgo.BeforeEach(func() {
		hostMemoryManager = new(memComMocks.HostMemoryManager)
		hostMemoryManager.(*memComMocks.HostMemoryManager).On("ReportUnmanagedSpaceUsageChange", mock.Anything).Return()
		memStore = new(memMocks.MemStore)
		diskStore = new(diskMocks.DiskStore)

		var err error
		batch110, err = testFactory.ReadLiveBatch("archiving/batch-110")
		Ω(err).Should(BeNil())
		batch101, err = testFactory.ReadLiveBatch("archiving/batch-101")
		Ω(err).Should(BeNil())
		batch99, err = testFactory.ReadLiveBatch("archiving/batch-99")
		Ω(err).Should(BeNil())
		tmpBatch, err := testFactory.ReadArchiveBatch("archiving/archiveBatch0")

		Ω(err).Should(BeNil())

		metaStore = new(metaMocks.MetaStore)
		metaStore.(*metaMocks.MetaStore).On("GetArchiveBatchVersion", table, 0, mock.Anything, mock.Anything).Return(uint32(0), uint32(0), 0, nil)

		diskStore = new(diskMocks.DiskStore)
		diskStore.(*diskMocks.DiskStore).On(
			"OpenVectorPartyFileForRead", table, mock.Anything, shardID, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		redologManagerMaster, _ = redolog.NewRedoLogManagerMaster("", &common.RedoLogConfig{}, diskStore, metaStore)
		bootstrapToken := new(memComMocks.BootStrapToken)
		options = memstore.NewOptions(bootstrapToken, redologManagerMaster)
		shard = memstore.NewTableShard(&memCom.TableSchema{
			Schema: metaCom.Table{
				Name: table,
				Config: metaCom.TableConfig{
					ArchivingDelayMinutes:    500,
					ArchivingIntervalMinutes: 300,
				},
				IsFactTable:          true,
				ArchivingSortColumns: []int{1, 2},
				Columns: []metaCom.Column{
					{Deleted: false, Name: "c0", Type: metaCom.Uint32},
					{Deleted: false, Name: "c1", Type: metaCom.Bool},
					{Deleted: false, Name: "c2", Type: metaCom.Float32},
					{Deleted: false, Name: "c3", Type: metaCom.ArrayInt16},
					{Deleted: false, Name: "c4", Type: metaCom.ArrayUUID},
				},
			},
			ColumnIDs: map[string]int{
				"c0": 0,
				"c1": 1,
				"c2": 2,
				"c3": 3,
				"c4": 4,
			},
			ValueTypeByColumn: []memCom.DataType{memCom.Uint32, memCom.Bool, memCom.Float32, memCom.ArrayInt16, memCom.ArrayUUID},
			DefaultValues:     []*memCom.DataValue{&memCom.NullDataValue, &memCom.NullDataValue, &memCom.NullDataValue, &memCom.NullDataValue, &memCom.NullDataValue},
		}, metaStore, diskStore, hostMemoryManager, shardID, 1, options)

		archiveBatch0 = &memstore.ArchiveBatch{
			Version: 0,
			Size:    5,
			Shard:   shard,
			Batch: memCom.Batch{
				RWMutex: &sync.RWMutex{},
				Columns: tmpBatch.Columns,
			},
		}

		shard.ArchiveStore = &memstore.ArchiveStore{CurrentVersion: memstore.NewArchiveStoreVersion(100, shard)}
		shard.ArchiveStore.CurrentVersion.Batches[0] = archiveBatch0

		memStore.(*memMocks.MemStore).On("GetTableShard", "table1", 0).Run(func(args mock.Arguments) {
			shard.Users.Add(1)
		}).Return(shard, nil).Once()

		memStore.(*memMocks.MemStore).On("RLock").Return()
		memStore.(*memMocks.MemStore).On("RUnlock").Return()
		memStore.(*memMocks.MemStore).On("GetSchemas").Return(map[string]*memCom.TableSchema{
			table: shard.Schema,
		})

		shard.LiveStore = &memstore.LiveStore{
			LastReadRecord: memCom.RecordID{BatchID: -101, Index: 3},
			Batches: map[int32]*memstore.LiveBatch{
				-110: {
					Batch: memCom.Batch{
						RWMutex: &sync.RWMutex{},
						Columns: batch110.Columns,
					},
					Capacity: 6,
				},
				-101: {
					Batch: memCom.Batch{
						RWMutex: &sync.RWMutex{},
						Columns: batch101.Columns,
					},
					Capacity: 5,
				},
				-99: {
					Batch: memCom.Batch{
						RWMutex: &sync.RWMutex{},
						Columns: batch99.Columns,
					},
					Capacity: 5,
				},
			},
			PrimaryKey:        memstore.NewPrimaryKey(16, true, 0, hostMemoryManager),
			HostMemoryManager: hostMemoryManager,
		}
	})

	ginkgo.AfterEach(func() {
		batch110.SafeDestruct()
		batch101.SafeDestruct()
		batch99.SafeDestruct()
		da := getDeviceAllocator()
		Ω(da.(*memoryTrackingDeviceAllocatorImpl).memoryUsage[0]).Should(BeEquivalentTo(0))
		redologManagerMaster.Stop()
	})

	ginkgo.It("Copy array to device should work", func() {
		ctx := oopkBatchContext{}
		defer ctx.cleanupBeforeAggregation()
		var stream unsafe.Pointer
		testFactory := list.GetFactory()
		vp1, err := readDeviceVPSlice(testFactory, "list/live_vp_uint32", stream, ctx.device)
		Ω(err).Should(BeNil())
		Ω(vp1.length).Should(Equal(4))
		vp2, err := readDeviceVPSlice(testFactory, "list/live_vp_uuid", stream, ctx.device)
		Ω(err).Should(BeNil())
		Ω(vp2.length).Should(Equal(5))
		// use AfterEach to clean device memory
		ctx.columns = []deviceVectorPartySlice{
			vp1,
			vp2,
		}
	})

	var _ = ginkgo.It("array element_at should work 1", func() {
		qc := &AQLQueryContext{}
		q := &queryCom.AQLQuery{
			Table: table,
			Dimensions: []queryCom.Dimension{
				{Expr: "c0", TimeBucketizer: "m", TimeUnit: "second"},
			},
			Measures: []queryCom.Measure{
				{Expr: "count(c1)"},
			},
			TimeFilter: queryCom.TimeFilter{
				Column: "c0",
				From:   "1970-01-01",
				To:     "1970-01-02",
			},
			Filters: []string{"element_at(c3, -1)=143"},
		}
		qc.InitQCHelper()
		qc.Query = q

		qc.Compile(memStore, topology.NewStaticShardOwner([]int{0}))
		Ω(qc.Error).Should(BeNil())
		qc.FindDeviceForQuery(memStore, -1, NewDeviceManager(common.QueryConfig{
			DeviceMemoryUtilization: 1.0,
			DeviceChoosingTimeout:   -1,
		}), 100)
		Ω(qc.Device).Should(Equal(0))
		memStore.(*memMocks.MemStore).On("GetTableShard", "table1", 0).Run(func(args mock.Arguments) {
			shard.Users.Add(1)
		}).Return(shard, nil).Once()

		qc.ProcessQuery(memStore)
		Ω(qc.Error).Should(BeNil())
		qc.Postprocess()
		qc.ReleaseHostResultsBuffers()
		bs, err := json.Marshal(qc.Results)
		Ω(err).Should(BeNil())
		Ω(bs).Should(MatchJSON(` {
			"120": 2
		  }`))
	})

	var _ = ginkgo.It("array element_at should work for uuid", func() {
		qc := &AQLQueryContext{}
		qc.InitQCHelper()
		q := &queryCom.AQLQuery{
			Table: table,
			Dimensions: []queryCom.Dimension{
				{Expr: "c0", TimeBucketizer: "m", TimeUnit: "second"},
			},
			Measures: []queryCom.Measure{
				{Expr: "count(c1)"},
			},
			TimeFilter: queryCom.TimeFilter{
				Column: "c0",
				From:   "1970-01-01",
				To:     "1970-01-02",
			},
			Filters: []string{"element_at(c4, -1)='14000000-0000-0000-0300-000000000000'"},
		}
		qc.Query = q

		qc.Compile(memStore, topology.NewStaticShardOwner([]int{0}))
		Ω(qc.Error).Should(BeNil())
		qc.FindDeviceForQuery(memStore, -1, NewDeviceManager(common.QueryConfig{
			DeviceMemoryUtilization: 1.0,
			DeviceChoosingTimeout:   -1,
		}), 100)
		Ω(qc.Device).Should(Equal(0))
		memStore.(*memMocks.MemStore).On("GetTableShard", "table1", 0).Run(func(args mock.Arguments) {
			shard.Users.Add(1)
		}).Return(shard, nil).Once()

		qc.ProcessQuery(memStore)
		Ω(qc.Error).Should(BeNil())
		qc.Postprocess()
		qc.ReleaseHostResultsBuffers()
		bs, err := json.Marshal(qc.Results)
		Ω(err).Should(BeNil())
		Ω(bs).Should(MatchJSON(` {
			"120": 2
		  }`))
	})

	var _ = ginkgo.It("array contains should work", func() {
		qc := &AQLQueryContext{}
		qc.InitQCHelper()
		q := &queryCom.AQLQuery{
			Table: table,
			Dimensions: []queryCom.Dimension{
				{Expr: "c0", TimeBucketizer: "m", TimeUnit: "second"},
			},
			Measures: []queryCom.Measure{
				{Expr: "count(c1)"},
			},
			TimeFilter: queryCom.TimeFilter{
				Column: "c0",
				From:   "1970-01-01",
				To:     "1970-01-02",
			},
			Filters: []string{"contains(c3, 143)"},
		}
		qc.Query = q

		qc.Compile(memStore, topology.NewStaticShardOwner([]int{0}))
		Ω(qc.Error).Should(BeNil())
		qc.FindDeviceForQuery(memStore, -1, NewDeviceManager(common.QueryConfig{
			DeviceMemoryUtilization: 1.0,
			DeviceChoosingTimeout:   -1,
		}), 100)
		Ω(qc.Device).Should(Equal(0))
		memStore.(*memMocks.MemStore).On("GetTableShard", "table1", 0).Run(func(args mock.Arguments) {
			shard.Users.Add(1)
		}).Return(shard, nil).Once()
		qc.ProcessQuery(memStore)
		Ω(qc.Error).Should(BeNil())
		qc.Postprocess()
		qc.ReleaseHostResultsBuffers()
		bs, err := json.Marshal(qc.Results)
		Ω(err).Should(BeNil())
		Ω(bs).Should(MatchJSON(` {
			"120": 2
		  }`))
	})

	var _ = ginkgo.It("array length should work", func() {
		qc := &AQLQueryContext{}
		qc.InitQCHelper()
		q := &queryCom.AQLQuery{
			Table: table,
			Dimensions: []queryCom.Dimension{
				{Expr: "c0", TimeBucketizer: "m", TimeUnit: "second"},
			},
			Measures: []queryCom.Measure{
				{Expr: "count(c1)"},
			},
			TimeFilter: queryCom.TimeFilter{
				Column: "c0",
				From:   "1970-01-01",
				To:     "1970-01-02",
			},
			Filters: []string{"length(c3) = 2"},
		}
		qc.Query = q

		qc.Compile(memStore, topology.NewStaticShardOwner([]int{0}))
		Ω(qc.Error).Should(BeNil())
		qc.FindDeviceForQuery(memStore, -1, NewDeviceManager(common.QueryConfig{
			DeviceMemoryUtilization: 1.0,
			DeviceChoosingTimeout:   -1,
		}), 100)
		Ω(qc.Device).Should(Equal(0))
		memStore.(*memMocks.MemStore).On("GetTableShard", "table1", 0).Run(func(args mock.Arguments) {
			shard.Users.Add(1)
		}).Return(shard, nil).Once()
		qc.ProcessQuery(memStore)
		Ω(qc.Error).Should(BeNil())
		qc.Postprocess()
		qc.ReleaseHostResultsBuffers()
		bs, err := json.Marshal(qc.Results)
		Ω(err).Should(BeNil())
		Ω(bs).Should(MatchJSON(` {
			"0": 3,
			"60": 2
		  }`))
	})

	var _ = ginkgo.It("array length should work for UUID", func() {
		qc := &AQLQueryContext{}
		qc.InitQCHelper()
		q := &queryCom.AQLQuery{
			Table: table,
			Dimensions: []queryCom.Dimension{
				{Expr: "c0", TimeBucketizer: "m", TimeUnit: "second"},
			},
			Measures: []queryCom.Measure{
				{Expr: "count(c1)"},
			},
			TimeFilter: queryCom.TimeFilter{
				Column: "c0",
				From:   "1970-01-01",
				To:     "1970-01-02",
			},
			Filters: []string{"length(c4) = 2"},
		}
		qc.Query = q

		qc.Compile(memStore, topology.NewStaticShardOwner([]int{0}))
		Ω(qc.Error).Should(BeNil())
		qc.FindDeviceForQuery(memStore, -1, NewDeviceManager(common.QueryConfig{
			DeviceMemoryUtilization: 1.0,
			DeviceChoosingTimeout:   -1,
		}), 100)
		Ω(qc.Device).Should(Equal(0))
		memStore.(*memMocks.MemStore).On("GetTableShard", "table1", 0).Run(func(args mock.Arguments) {
			shard.Users.Add(1)
		}).Return(shard, nil).Once()
		qc.ProcessQuery(memStore)
		Ω(qc.Error).Should(BeNil())
		qc.Postprocess()
		qc.ReleaseHostResultsBuffers()
		bs, err := json.Marshal(qc.Results)
		Ω(err).Should(BeNil())
		Ω(bs).Should(MatchJSON(` {
			"0": 3,
			"60": 2
		  }`))
	})

	var _ = ginkgo.It("array query for non-aggregation query should work", func() {
		qc := &AQLQueryContext{}
		qc.InitQCHelper()
		q := &queryCom.AQLQuery{
			Table: table,
			Dimensions: []queryCom.Dimension{
				{Expr: "c0"},
				{Expr: "element_at(c3, 1)"},
				{Expr: "length(c3)"},
				{Expr: "element_at(c4, 1)"},
				{Expr: "length(c4)"},
			},
			Measures: []queryCom.Measure{
				{Expr: "1"},
			},
			TimeFilter: queryCom.TimeFilter{
				Column: "c0",
				From:   "1970-01-01",
				To:     "1970-01-02",
			},
		}
		qc.Query = q

		qc.Compile(memStore, topology.NewStaticShardOwner([]int{0}))
		Ω(qc.Error).Should(BeNil())
		qc.FindDeviceForQuery(memStore, -1, NewDeviceManager(common.QueryConfig{
			DeviceMemoryUtilization: 1.0,
			DeviceChoosingTimeout:   -1,
		}), 100)
		Ω(qc.Device).Should(Equal(0))
		memStore.(*memMocks.MemStore).On("GetTableShard", "table1", 0).Run(func(args mock.Arguments) {
			shard.Users.Add(1)
		}).Return(shard, nil).Once()
		qc.ProcessQuery(memStore)
		Ω(qc.Error).Should(BeNil())
		qc.Postprocess()
		qc.ReleaseHostResultsBuffers()
		bs, err := json.Marshal(qc.Results)
		Ω(err).Should(BeNil())
		Ω(bs).Should(MatchJSON(` {
        "headers": [
          "c0",
          "element_at(c3, 1)",
          "length(c3)",
          "element_at(c4, 1)",
          "length(c4)"
        ],
        "matrixData": [
          [
            "100",
            "NULL",
            "2",
            "NULL",
            "2"
          ],
          [
            "110",
            "121",
            "2",
            "12000000-0000-0000-0100-000000000000",
            "2"
          ],
          [
            "120",
            "NULL",
            "NULL",
            "NULL",
            "NULL"
          ],
          [
            "130",
            "132",
            "3",
            "13000000-0000-0000-0200-000000000000",
            "3"
          ],
          [
            "140",
            "142",
            "3",
            "14000000-0000-0000-0200-000000000000",
            "3"
          ],
          [
            "100",
            "12",
            "3",
            "01000000-0000-0000-0200-000000000000",
            "3"
          ],
          [
            "110",
            "NULL",
            "2",
            "NULL",
            "2"
          ],
          [
            "120",
            "NULL",
            "NULL",
            "NULL",
            "NULL"
          ],
          [
            "0",
            "312",
            "2",
            "03000000-0000-0000-0200-000000000000",
            "2"
          ],
          [
            "10",
            "NULL",
            "2",
            "NULL",
            "2"
          ],
          [
            "20",
            "NULL",
            "1",
            "NULL",
            "1"
          ],
          [
            "30",
            "541",
            "2",
            "06000000-0000-0000-0100-000000000000",
            "2"
          ],
          [
            "40",
            "NULL",
            "NULL",
            "NULL",
            "NULL"
          ]
        ]
      }`))
	})
})
