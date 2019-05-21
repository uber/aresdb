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

package query

import (
	"unsafe"

	"encoding/binary"
	"encoding/json"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/common"
	"github.com/uber/aresdb/diskstore"
	diskMocks "github.com/uber/aresdb/diskstore/mocks"
	"github.com/uber/aresdb/memstore"
	memCom "github.com/uber/aresdb/memstore/common"
	memComMocks "github.com/uber/aresdb/memstore/common/mocks"
	memMocks "github.com/uber/aresdb/memstore/mocks"
	"github.com/uber/aresdb/memutils"
	"github.com/uber/aresdb/metastore"
	metaCom "github.com/uber/aresdb/metastore/common"
	metaMocks "github.com/uber/aresdb/metastore/mocks"
	queryCom "github.com/uber/aresdb/query/common"
	"github.com/uber/aresdb/query/expr"
	"github.com/uber/aresdb/utils"
	"net/http/httptest"
)

// readDeviceVPSlice reads a vector party from file and also translate it to device vp format:
// counts, nulls and values will be stored in a continuous memory space.
func readDeviceVPSlice(factory memstore.TestFactoryT, name string, stream unsafe.Pointer,
	device int) (deviceVectorPartySlice, error) {
	sourceVP, err := factory.ReadArchiveVectorParty(name, &sync.RWMutex{})
	if err != nil {
		return deviceVectorPartySlice{}, err
	}

	hostVPSlice := sourceVP.GetHostVectorPartySlice(0, sourceVP.GetLength())
	deviceVPSlice := hostToDeviceColumn(hostVPSlice, device)
	copyHostToDevice(hostVPSlice, deviceVPSlice, stream, device)
	return deviceVPSlice, nil
}

var _ = ginkgo.Describe("aql_processor", func() {
	var batch99, batch101, batch110, batch120, batch130 *memstore.Batch
	var vs memstore.LiveStore
	var archiveBatch0 *memstore.ArchiveBatch
	var archiveBatch1 *memstore.ArchiveBatch
	var memStore memstore.MemStore
	var metaStore metastore.MetaStore
	var diskStore diskstore.DiskStore
	var hostMemoryManager memCom.HostMemoryManager
	var shard *memstore.TableShard
	table := "table1"
	shardID := 0

	testFactory := memstore.TestFactoryT{
		RootPath:   "../testing/data",
		FileSystem: utils.OSFileSystem{},
	}

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
		batch120, err = testFactory.ReadLiveBatch("live/batch-120")
		Ω(err).Should(BeNil())
		batch130, err = testFactory.ReadLiveBatch("live/batch-130")
		Ω(err).Should(BeNil())
		tmpBatch, err := testFactory.ReadArchiveBatch("archiving/archiveBatch0")
		tmpBatch1, err := testFactory.ReadArchiveBatch("archiving/archiveBatch1")

		Ω(err).Should(BeNil())

		metaStore = new(metaMocks.MetaStore)
		metaStore.(*metaMocks.MetaStore).On("GetArchiveBatchVersion", table, 0, mock.Anything, mock.Anything).Return(uint32(0), uint32(0), 0, nil)

		diskStore = new(diskMocks.DiskStore)
		diskStore.(*diskMocks.DiskStore).On(
			"OpenVectorPartyFileForRead", table, mock.Anything, shardID, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		shard = memstore.NewTableShard(&memstore.TableSchema{
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
				},
			},
			ColumnIDs: map[string]int{
				"c0": 0,
				"c1": 1,
				"c2": 2,
			},
			ValueTypeByColumn: []memCom.DataType{memCom.Uint32, memCom.Bool, memCom.Float32},
			DefaultValues:     []*memCom.DataValue{&memCom.NullDataValue, &memCom.NullDataValue, &memCom.NullDataValue},
		}, metaStore, diskStore, hostMemoryManager, shardID)

		shardMap := map[int]*memstore.TableShard{
			shardID: shard,
		}

		archiveBatch0 = &memstore.ArchiveBatch{
			Version: 0,
			Size:    5,
			Shard:   shard,
			Batch: memstore.Batch{
				RWMutex: &sync.RWMutex{},
				Columns: tmpBatch.Columns,
			},
		}
		archiveBatch1 = &memstore.ArchiveBatch{
			Version: 0,
			Size:    5,
			Shard:   shard,
			Batch: memstore.Batch{
				RWMutex: &sync.RWMutex{},
				Columns: tmpBatch1.Columns,
			},
		}

		shard.ArchiveStore = &memstore.ArchiveStore{CurrentVersion: memstore.NewArchiveStoreVersion(100, shard)}
		shard.ArchiveStore.CurrentVersion.Batches[0] = archiveBatch0

		memStore.(*memMocks.MemStore).On("GetTableShard", "table1", 0).Run(func(args mock.Arguments) {
			shard.Users.Add(1)
		}).Return(shard, nil).Once()

		memStore.(*memMocks.MemStore).On("RLock").Return()
		memStore.(*memMocks.MemStore).On("RUnlock").Return()
		memStore.(*memMocks.MemStore).On("GetSchemas").Return(map[string]*memstore.TableSchema{
			table: shard.Schema,
		})

		vs = memstore.LiveStore{
			LastReadRecord: memstore.RecordID{BatchID: -101, Index: 3},
			Batches: map[int32]*memstore.LiveBatch{
				-110: {
					Batch: memstore.Batch{
						RWMutex: &sync.RWMutex{},
						Columns: batch110.Columns,
					},
					Capacity: 5,
				},
				-101: {
					Batch: memstore.Batch{
						RWMutex: &sync.RWMutex{},
						Columns: batch101.Columns,
					},
					Capacity: 5,
				},
				-99: {
					Batch: memstore.Batch{
						RWMutex: &sync.RWMutex{},
						Columns: batch99.Columns,
					},
					Capacity: 5,
				},
			},
			PrimaryKey:        memstore.NewPrimaryKey(16, true, 0, hostMemoryManager),
			HostMemoryManager: hostMemoryManager,
		}

		shardMap[shardID].LiveStore = &vs
		shardMap[shardID].ArchiveStore.CurrentVersion.ArchivingCutoff = 100
		shardMap[shardID].ArchiveStore.CurrentVersion.Batches[0] = archiveBatch0
	})

	ginkgo.AfterEach(func() {
		batch110.SafeDestruct()
		batch101.SafeDestruct()
		batch99.SafeDestruct()
		da := getDeviceAllocator()
		Ω(da.(*memoryTrackingDeviceAllocatorImpl).memoryUsage[0]).Should(BeEquivalentTo(0))
	})

	ginkgo.It("prefilterSlice", func() {
		vp1, err := testFactory.ReadArchiveVectorParty("sortedVP7", nil)
		vp2, err := testFactory.ReadArchiveVectorParty("sortedVP6", nil)
		vp3, err := testFactory.ReadArchiveVectorParty("sortedVP8", nil)
		Ω(err).Should(BeNil())

		qc1 := AQLQueryContext{
			TableScanners: []*TableScanner{
				{
					EqualityPrefilterValues:  []uint32{1, 2},
					RangePrefilterBoundaries: [2]boundaryType{inclusiveBoundary, exclusiveBoundary},
					RangePrefilterValues:     [2]uint32{1, 3},
				},
			},
		}

		lowerBound, upperBound, vpSlice1 := qc1.prefilterSlice(vp1, 0, 0, 20)
		Ω(vpSlice1.ValueStartIndex).Should(Equal(0))
		Ω(vpSlice1.ValueBytes).Should(Equal(64))
		Ω(vpSlice1.NullStartIndex).Should(Equal(0))
		Ω(vpSlice1.NullBytes).Should(Equal(64))
		Ω(vpSlice1.CountStartIndex).Should(Equal(0))
		Ω(vpSlice1.CountBytes).Should(Equal(64))

		lowerBound, upperBound, vpSlice2 := qc1.prefilterSlice(vp2, 1, lowerBound, upperBound)
		Ω(vpSlice2.ValueStartIndex).Should(Equal(1))
		Ω(vpSlice2.ValueBytes).Should(Equal(64))
		Ω(vpSlice2.NullStartIndex).Should(Equal(1))
		Ω(vpSlice2.NullBytes).Should(Equal(64))
		Ω(vpSlice2.CountStartIndex).Should(Equal(1))
		Ω(vpSlice2.CountBytes).Should(Equal(64))

		lowerBound, upperBound, vpSlice3 := qc1.prefilterSlice(vp3, 2, lowerBound, upperBound)

		Ω(vpSlice3.ValueStartIndex).Should(Equal(1))
		Ω(vpSlice3.ValueBytes).Should(Equal(64))
		Ω(vpSlice3.NullStartIndex).Should(Equal(1))
		Ω(vpSlice3.NullBytes).Should(Equal(64))
		Ω(vpSlice3.CountStartIndex).Should(Equal(1))
		Ω(vpSlice3.CountBytes).Should(Equal(64))

		Ω(lowerBound).Should(Equal(2))
		Ω(upperBound).Should(Equal(4))

		qc2 := AQLQueryContext{
			TableScanners: []*TableScanner{
				{
					EqualityPrefilterValues:  []uint32{1},
					RangePrefilterBoundaries: [2]boundaryType{noBoundary, inclusiveBoundary},
					RangePrefilterValues:     [2]uint32{0, 1},
				},
			},
		}

		lowerBound, upperBound, vpSlice1 = qc2.prefilterSlice(vp1, 0, 0, 20)
		Ω(vpSlice1.ValueStartIndex).Should(Equal(0))
		Ω(vpSlice1.ValueBytes).Should(Equal(64))
		Ω(vpSlice1.NullStartIndex).Should(Equal(0))
		Ω(vpSlice1.NullBytes).Should(Equal(64))
		Ω(vpSlice1.CountStartIndex).Should(Equal(0))
		Ω(vpSlice1.CountBytes).Should(Equal(64))

		lowerBound, upperBound, vpSlice2 = qc2.prefilterSlice(vp2, 1, lowerBound, upperBound)
		Ω(vpSlice2.ValueStartIndex).Should(Equal(0))
		Ω(vpSlice2.ValueBytes).Should(Equal(64))
		Ω(vpSlice2.NullStartIndex).Should(Equal(0))
		Ω(vpSlice2.NullBytes).Should(Equal(64))
		Ω(vpSlice2.CountStartIndex).Should(Equal(0))
		Ω(vpSlice2.CountBytes).Should(Equal(64))

		lowerBound, upperBound, vpSlice3 = qc2.prefilterSlice(vp3, 2, lowerBound, upperBound)
		Ω(vpSlice3.ValueStartIndex).Should(Equal(0))
		Ω(vpSlice3.ValueBytes).Should(Equal(64))
		Ω(vpSlice3.NullStartIndex).Should(Equal(0))
		Ω(vpSlice3.NullBytes).Should(Equal(64))
		Ω(vpSlice3.CountStartIndex).Should(Equal(0))
		Ω(vpSlice3.CountBytes).Should(Equal(64))

		Ω(lowerBound).Should(Equal(0))
		Ω(upperBound).Should(Equal(2))

		qc3 := AQLQueryContext{
			TableScanners: []*TableScanner{
				{
					EqualityPrefilterValues:  []uint32{1},
					RangePrefilterBoundaries: [2]boundaryType{exclusiveBoundary, noBoundary},
					RangePrefilterValues:     [2]uint32{1, 0},
				},
			},
		}

		lowerBound, upperBound, vpSlice1 = qc3.prefilterSlice(vp1, 0, 0, 20)
		Ω(vpSlice1.ValueStartIndex).Should(Equal(0))
		Ω(vpSlice1.ValueBytes).Should(Equal(64))
		Ω(vpSlice1.NullStartIndex).Should(Equal(0))
		Ω(vpSlice1.NullBytes).Should(Equal(64))
		Ω(vpSlice1.CountStartIndex).Should(Equal(0))
		Ω(vpSlice1.CountBytes).Should(Equal(64))

		lowerBound, upperBound, vpSlice2 = qc3.prefilterSlice(vp2, 1, lowerBound, upperBound)
		Ω(vpSlice2.ValueStartIndex).Should(Equal(1))
		Ω(vpSlice2.ValueBytes).Should(Equal(64))
		Ω(vpSlice2.NullStartIndex).Should(Equal(1))
		Ω(vpSlice2.NullBytes).Should(Equal(64))
		Ω(vpSlice2.CountStartIndex).Should(Equal(1))
		Ω(vpSlice2.CountBytes).Should(Equal(64))

		lowerBound, upperBound, vpSlice3 = qc3.prefilterSlice(vp3, 2, lowerBound, upperBound)
		Ω(vpSlice3.ValueStartIndex).Should(Equal(1))
		Ω(vpSlice3.ValueBytes).Should(Equal(64))
		Ω(vpSlice3.NullStartIndex).Should(Equal(1))
		Ω(vpSlice3.NullBytes).Should(Equal(64))
		Ω(vpSlice3.CountStartIndex).Should(Equal(1))
		Ω(vpSlice3.CountBytes).Should(Equal(64))

		Ω(lowerBound).Should(Equal(2))
		Ω(upperBound).Should(Equal(5))
	})

	ginkgo.It("makeForeignColumnVectorInput", func() {
		values := [64]byte{}
		nulls := [64]byte{}

		table := foreignTable{
			batches: [][]deviceVectorPartySlice{
				{
					{
						values:    devicePointer{pointer: unsafe.Pointer(&values[0])},
						nulls:     devicePointer{pointer: unsafe.Pointer(&nulls[0])},
						length:    10,
						valueType: memCom.Uint16,
					},
					{
						values:    devicePointer{pointer: unsafe.Pointer(&values[0])},
						nulls:     devicePointer{pointer: unsafe.Pointer(&nulls[0])},
						length:    10,
						valueType: memCom.Uint8,
					},
				},
				{
					{
						values:    devicePointer{pointer: unsafe.Pointer(&values[0])},
						nulls:     devicePointer{pointer: unsafe.Pointer(&nulls[0])},
						length:    10,
						valueType: memCom.Uint16,
					},
					{
						values:    devicePointer{pointer: unsafe.Pointer(&values[0])},
						nulls:     devicePointer{pointer: unsafe.Pointer(&nulls[0])},
						length:    10,
						valueType: memCom.Uint8,
					},
				},
			},
		}

		inputVector := makeForeignColumnInput(0, unsafe.Pointer(uintptr(0)), table, nil, 0)
		Ω(inputVector.Type).Should(Equal(uint32(3)))
	})

	ginkgo.It("makeVectorPartySliceInput", func() {
		values := [64]byte{}
		nulls := [64]byte{}
		counts := [64]byte{}

		column := deviceVectorPartySlice{
			values:          devicePointer{pointer: unsafe.Pointer(&values[0])},
			nulls:           devicePointer{pointer: unsafe.Pointer(&nulls[0])},
			counts:          devicePointer{pointer: unsafe.Pointer(&counts[0])},
			length:          10,
			valueType:       memCom.Uint16,
			valueStartIndex: 1,
			nullStartIndex:  0,
			countStartIndex: 1,
		}
		inputVector := makeVectorPartySliceInput(column)
		// uint32(0) corresponds to VectorPartySlice in C.enum_InputVectorType
		Ω(inputVector.Type).Should(Equal(uint32(0)))
	})

	ginkgo.It("makeConstantInput", func() {
		inputVector := makeConstantInput(float64(1.0), false)
		// uint32(2) corresponds to ConstantInput in C.enum_InputVectorType
		Ω(inputVector.Type).Should(Equal(uint32(2)))

		inputVector = makeConstantInput(int(1), false)
		Ω(inputVector.Type).Should(Equal(uint32(2)))

		v := &expr.GeopointLiteral{Val: [2]float32{1.1, 2.2}}
		inputVector = makeConstantInput(v, false)
		Ω(inputVector.Type).Should(Equal(uint32(2)))
	})

	ginkgo.It("makeScratchSpaceInput", func() {
		values := [64]byte{}
		nulls := [64]byte{}
		inputVector := makeScratchSpaceInput(unsafe.Pointer(&values[0]), unsafe.Pointer(&nulls[0]), uint32(1))
		// uint32(1) corresponds to ScratchSpaceInput in C.enum_InputVectorType
		Ω(inputVector.Type).Should(Equal(uint32(1)))
	})

	ginkgo.It("makeScratchSpaceOutput", func() {
		values := [64]byte{}
		nulls := [64]byte{}
		outputVector := makeScratchSpaceOutput(unsafe.Pointer(&values[0]), unsafe.Pointer(&nulls[0]), uint32(0))
		// uint32(0) corresponds to ScratchSpaceOutput in C.enum_OutputVectorType
		Ω(outputVector.Type).Should(Equal(uint32(0)))
	})

	ginkgo.It("makeMeasureVectorOutput", func() {
		measureValues := [64]byte{}
		outputVector := makeMeasureVectorOutput(unsafe.Pointer(&measureValues[0]), uint32(6), uint32(0))
		// uint32(0) corresponds to ScratchSpaceOutput in C.enum_OutputVectorType
		Ω(outputVector.Type).Should(Equal(uint32(1)))
	})

	ginkgo.It("makeDimensionVectorOutput", func() {
		dimValues := [64]byte{}
		outputVector := makeDimensionVectorOutput(unsafe.Pointer(&dimValues[0]), 0, 4, uint32(0))
		// uint32(0) corresponds to ScratchSpaceOutput in C.enum_OutputVectorType
		Ω(outputVector.Type).Should(Equal(uint32(2)))
	})

	ginkgo.It("evaluateFilterExpression", func() {
		ctx := oopkBatchContext{}
		defer ctx.cleanupDeviceResultBuffers()
		defer ctx.cleanupBeforeAggregation()
		defer ctx.swapResultBufferForNextBatch()
		var stream unsafe.Pointer
		vp0, err := readDeviceVPSlice(testFactory, "sortedVP8", stream, ctx.device)
		Ω(err).Should(BeNil())
		vp1, err := readDeviceVPSlice(testFactory, "sortedVP7", stream, ctx.device)
		Ω(err).Should(BeNil())
		vp2, err := readDeviceVPSlice(testFactory, "sortedVP6", stream, ctx.device)
		Ω(err).Should(BeNil())
		columns := []deviceVectorPartySlice{
			vp0,
			vp1,
			vp2,
		}

		tableScanners := []*TableScanner{
			{
				ColumnsByIDs: map[int]int{
					1: 1,
					2: 2,
				},
			},
		}
		foreignTables := make([]*foreignTable, 0)

		ctx.prepareForFiltering(columns, 0, 0, stream)
		Ω(ctx.size).Should(Equal(6))
		initIndexVector(ctx.indexVectorD.getPointer(), 0, ctx.size, stream, 0)

		// expr: -vp1 == -2 || vp2 >= 2

		// id| vp0 | vp1 | vp2 | res |
		// ---------------------------
		// 0 | 1 0 | 1 0 | 1 0 |     |
		// 1 | 1 2 |	 | 2 2 |  y  |
		// 2 | 2 3 |	 |     |  y  |
		// 3 | 3 4 |     |     |  y  |
		// 4 | 2 5 | 2 5 | 1 5 |  y  |
		// 5 | 2 10|	 | 2 10|  y  |

		exp := &expr.BinaryExpr{
			Op: expr.OR,
			LHS: &expr.BinaryExpr{
				Op: expr.EQ,
				LHS: &expr.UnaryExpr{
					Op: expr.UNARY_MINUS,
					Expr: &expr.VarRef{
						Val:      "vp1",
						ColumnID: 1,
					},
				},
				RHS: &expr.NumberLiteral{
					Val:      0,
					Int:      -2,
					ExprType: expr.Signed,
				},
			},
			RHS: &expr.BinaryExpr{
				Op: expr.GTE,
				LHS: &expr.VarRef{
					Val:      "vp2",
					ColumnID: 2,
				},
				RHS: &expr.NumberLiteral{
					Val:      0,
					Int:      2,
					ExprType: expr.Unsigned,
				},
			},
		}

		ctx.processExpression(exp, nil, tableScanners, foreignTables, stream, 0, ctx.filterAction)
		Ω(ctx.size).Should(Equal(5))
		ctx.cleanupBeforeAggregation()
		ctx.swapResultBufferForNextBatch()

		vp0, err = readDeviceVPSlice(testFactory, "sortedVP8", stream, ctx.device)
		Ω(err).Should(BeNil())
		vp1, err = readDeviceVPSlice(testFactory, "sortedVP7", stream, ctx.device)
		Ω(err).Should(BeNil())
		vp2, err = readDeviceVPSlice(testFactory, "sortedVP6", stream, ctx.device)
		Ω(err).Should(BeNil())
		columns = []deviceVectorPartySlice{
			vp0,
			vp1,
			vp2,
		}

		// expr: vp2 / 2 == 0
		exp = &expr.BinaryExpr{
			Op: expr.EQ,
			LHS: &expr.BinaryExpr{
				Op: expr.DIV,
				LHS: &expr.VarRef{
					Val:      "vp2",
					ColumnID: 2,
				},
				RHS: &expr.NumberLiteral{
					Val:      2,
					Int:      2,
					Expr:     "2",
					ExprType: expr.Signed,
				},
			},
			RHS: &expr.NumberLiteral{
				Val:      0,
				Int:      0,
				Expr:     "0",
				ExprType: expr.Signed,
			},
		}
		ctx.prepareForFiltering(columns, 0, 0, stream)
		initIndexVector(ctx.indexVectorD.getPointer(), 0, ctx.size, stream, 0)
		ctx.processExpression(exp, nil, tableScanners, foreignTables, stream, 0, ctx.filterAction)
		Ω(ctx.size).Should(Equal(2))
		Ω(*(*uint32)(utils.MemAccess(ctx.indexVectorD.getPointer(), 0))).Should(Equal(uint32(0)))
		Ω(*(*uint32)(utils.MemAccess(ctx.indexVectorD.getPointer(), 4))).Should(Equal(uint32(4)))
		ctx.cleanupBeforeAggregation()
		ctx.swapResultBufferForNextBatch()
	})

	ginkgo.It("evaluateVarRefDimensionExpression", func() {
		var stream unsafe.Pointer
		ctx := oopkBatchContext{}
		vpSlice0, err := readDeviceVPSlice(testFactory, "sortedVP8", stream, ctx.device)
		Ω(err).Should(BeNil())
		vpSlice1, err := readDeviceVPSlice(testFactory, "sortedVP7", stream, ctx.device)
		Ω(err).Should(BeNil())
		vpSlice2, err := readDeviceVPSlice(testFactory, "sortedVP6", stream, ctx.device)
		Ω(err).Should(BeNil())
		columns := []deviceVectorPartySlice{
			vpSlice0,
			vpSlice1,
			vpSlice2,
		}
		tableScanners := []*TableScanner{
			{
				ColumnsByIDs: map[int]int{
					1: 1,
					2: 2,
				},
			},
		}

		foreignTables := make([]*foreignTable, 0)
		oopkContext := OOPKContext{
			// only one 2 byte dimension
			NumDimsPerDimWidth: queryCom.DimCountsPerDimWidth{0, 0, 0, 1, 0},
			// 2 bytes + 1 validity byte
			DimRowBytes: 3,
		}

		// test boolean dimension value
		ctx.prepareForFiltering(columns, 0, 0, stream)
		initIndexVector(ctx.indexVectorD.getPointer(), 0, ctx.size, stream, 0)
		ctx.prepareForDimAndMeasureEval(oopkContext.DimRowBytes, 4, oopkContext.NumDimsPerDimWidth, false, stream)
		valueOffset, nullOffset := queryCom.GetDimensionStartOffsets(oopkContext.NumDimsPerDimWidth, 0, ctx.resultCapacity)
		dimensionExprRootAction := ctx.makeWriteToDimensionVectorAction(valueOffset, nullOffset, 0)
		// vp2
		exp := &expr.VarRef{
			Val:      "vp2",
			ExprType: expr.Unsigned,
			TableID:  0,
			ColumnID: 2,
			DataType: memCom.Uint16,
		}
		ctx.processExpression(exp, nil, tableScanners, foreignTables, stream, 0, dimensionExprRootAction)
		Ω(*(*[18]uint8)(ctx.dimensionVectorD[0].getPointer())).Should(Equal([18]uint8{1, 0, 2, 0, 2, 0, 2, 0, 1, 0, 2, 0, 1, 1, 1, 1, 1, 1}))
		ctx.cleanupBeforeAggregation()
		ctx.swapResultBufferForNextBatch()
		ctx.cleanupDeviceResultBuffers()
	})

	ginkgo.It("evaluateBooleanDimensionExpression", func() {
		var stream unsafe.Pointer
		ctx := oopkBatchContext{}
		vpSlice0, err := readDeviceVPSlice(testFactory, "sortedVP8", stream, ctx.device)
		Ω(err).Should(BeNil())
		vpSlice1, err := readDeviceVPSlice(testFactory, "sortedVP7", stream, ctx.device)
		Ω(err).Should(BeNil())
		vpSlice2, err := readDeviceVPSlice(testFactory, "sortedVP6", stream, ctx.device)
		Ω(err).Should(BeNil())

		columns := []deviceVectorPartySlice{
			vpSlice0,
			vpSlice1,
			vpSlice2,
		}
		tableScanners := []*TableScanner{
			{
				ColumnsByIDs: map[int]int{
					1: 1,
					2: 2,
				},
			},
		}
		foreignTables := make([]*foreignTable, 0)
		oopkContext := OOPKContext{
			NumDimsPerDimWidth: queryCom.DimCountsPerDimWidth{0, 0, 0, 0, 1},
			// test boolean dimension value
			// 1 byte + 1 validity byte
			DimRowBytes: 2,
		}

		ctx.prepareForFiltering(columns, 0, 0, stream)
		initIndexVector(ctx.indexVectorD.getPointer(), 0, ctx.size, stream, 0)
		ctx.prepareForDimAndMeasureEval(oopkContext.DimRowBytes, 4, oopkContext.NumDimsPerDimWidth, false, stream)
		valueOffset, nullOffset := queryCom.GetDimensionStartOffsets(oopkContext.NumDimsPerDimWidth, 0, ctx.resultCapacity)
		dimensionExprRootAction := ctx.makeWriteToDimensionVectorAction(valueOffset, nullOffset, 0)
		// vp2 == 2
		exp := &expr.BinaryExpr{
			Op: expr.EQ,
			LHS: &expr.VarRef{
				Val:      "vp2",
				ColumnID: 2,
			},
			RHS: &expr.NumberLiteral{
				Val:      2,
				Int:      2,
				Expr:     "2",
				ExprType: expr.Signed,
			},
			ExprType: expr.Boolean,
		}
		ctx.processExpression(exp, nil, tableScanners, foreignTables, stream, 0, dimensionExprRootAction)
		Ω(*(*[12]uint8)(ctx.dimensionVectorD[0].getPointer())).Should(Equal([12]uint8{0, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1}))
		ctx.cleanupBeforeAggregation()
		ctx.swapResultBufferForNextBatch()
		ctx.cleanupDeviceResultBuffers()
	})

	ginkgo.It("evaluateDimensionExpression", func() {
		ctx := oopkBatchContext{}
		var stream unsafe.Pointer
		vpSlice0, err := readDeviceVPSlice(testFactory, "sortedVP8", stream, ctx.device)
		Ω(err).Should(BeNil())
		vpSlice1, err := readDeviceVPSlice(testFactory, "sortedVP7", stream, ctx.device)
		Ω(err).Should(BeNil())
		vpSlice2, err := readDeviceVPSlice(testFactory, "sortedVP6", stream, ctx.device)
		Ω(err).Should(BeNil())

		columns := []deviceVectorPartySlice{
			vpSlice0,
			vpSlice1,
			vpSlice2,
		}
		tableScanners := []*TableScanner{
			{
				ColumnsByIDs: map[int]int{
					1: 1,
					2: 2,
				},
			},
		}
		foreignTables := make([]*foreignTable, 0)
		oopkContext := OOPKContext{
			NumDimsPerDimWidth: queryCom.DimCountsPerDimWidth{0, 0, 1, 0, 0},
			DimRowBytes:        5,
		}

		// id|vp0|vp1|vp2|
		// ---------------------
		// 0 | 1 | 1 | 1 |
		// 1 | 1 | 1 | 2 |
		// 2 | 2 | 1 | 2 |
		// 3 | 3 | 1 | 2 |
		// 4 | 2 | 2 | 1 |
		// 5 | 2 | 2 | 2 |

		// vp2 / 2
		exp := &expr.BinaryExpr{
			Op: expr.DIV,
			LHS: &expr.VarRef{
				Val:      "vp2",
				ColumnID: 2,
			},
			RHS: &expr.NumberLiteral{
				Val:      2,
				Int:      2,
				Expr:     "2",
				ExprType: expr.Signed,
			},
			ExprType: expr.Signed,
		}

		ctx.prepareForFiltering(columns, 0, 0, stream)
		initIndexVector(ctx.indexVectorD.getPointer(), 0, ctx.size, stream, 0)
		ctx.prepareForDimAndMeasureEval(oopkContext.DimRowBytes, 4, oopkContext.NumDimsPerDimWidth, false, stream)
		valueOffset, nullOffset := queryCom.GetDimensionStartOffsets(oopkContext.NumDimsPerDimWidth, 0, ctx.resultCapacity)
		dimensionExprRootAction := ctx.makeWriteToDimensionVectorAction(valueOffset, nullOffset, 0)
		ctx.processExpression(exp, nil, tableScanners, foreignTables, stream, 0, dimensionExprRootAction)
		Ω(*(*[30]uint8)(ctx.dimensionVectorD[0].getPointer())).Should(Equal([30]uint8{0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 1, 1, 1, 1}))
		ctx.cleanupBeforeAggregation()
		ctx.swapResultBufferForNextBatch()
		ctx.cleanupDeviceResultBuffers()

	})

	ginkgo.It("evaluateMeasureExpression", func() {
		ctx := oopkBatchContext{}
		var stream unsafe.Pointer
		vpSlice0, err := readDeviceVPSlice(testFactory, "sortedVP8", stream, ctx.device)
		Ω(err).Should(BeNil())
		vpSlice1, err := readDeviceVPSlice(testFactory, "sortedVP7", stream, ctx.device)
		Ω(err).Should(BeNil())
		vpSlice2, err := readDeviceVPSlice(testFactory, "sortedVP6", stream, ctx.device)
		Ω(err).Should(BeNil())

		columns := []deviceVectorPartySlice{
			vpSlice0,
			vpSlice1,
			vpSlice2,
		}

		tableScanners := []*TableScanner{
			{
				ColumnsByIDs: map[int]int{
					1: 1,
					2: 2,
				},
			},
		}
		foreignTables := make([]*foreignTable, 0)

		// id|cnt|vp0|vp1|vp2|
		// ---------------------
		// 0 | 2 | 1 | 1 | 1 |
		// 1 | 1 | 1 | 1 | 2 |
		// 2 | 1 | 2 | 1 | 2 |
		// 3 | 1 | 3 | 1 | 2 |
		// 4 | 5 | 2 | 2 | 1 |
		// 5 |10 | 2 | 2 | 2 |

		// vp2 / 2
		exp := &expr.BinaryExpr{
			Op: expr.DIV,
			LHS: &expr.VarRef{
				Val:      "vp2",
				ColumnID: 2,
			},
			RHS: &expr.NumberLiteral{
				Val:      2,
				Int:      2,
				Expr:     "2",
				ExprType: expr.Signed,
			},
		}

		dimRowBytes := 8
		ctx.prepareForFiltering(columns, 0, 0, stream)
		initIndexVector(ctx.indexVectorD.getPointer(), 0, ctx.size, stream, 0)

		ctx.prepareForDimAndMeasureEval(dimRowBytes, 4, queryCom.DimCountsPerDimWidth{}, false, stream)
		measureExprRootAction := ctx.makeWriteToMeasureVectorAction(uint32(1), 4)
		ctx.processExpression(exp, nil, tableScanners, foreignTables, stream, 0, measureExprRootAction)

		Ω(*(*uint32)(ctx.measureVectorD[0].getPointer())).Should(Equal(uint32(0)))
		Ω(*(*uint32)(utils.MemAccess(ctx.measureVectorD[0].getPointer(), 4))).Should(Equal(uint32(1)))
		Ω(*(*uint32)(utils.MemAccess(ctx.measureVectorD[0].getPointer(), 8))).Should(Equal(uint32(1)))
		Ω(*(*uint32)(utils.MemAccess(ctx.measureVectorD[0].getPointer(), 12))).Should(Equal(uint32(1)))
		Ω(*(*uint32)(utils.MemAccess(ctx.measureVectorD[0].getPointer(), 16))).Should(Equal(uint32(0)))
		Ω(*(*uint32)(utils.MemAccess(ctx.measureVectorD[0].getPointer(), 20))).Should(Equal(uint32(10)))
		ctx.cleanupBeforeAggregation()
		ctx.swapResultBufferForNextBatch()
		ctx.cleanupDeviceResultBuffers()
	})

	ginkgo.It("sort", func() {
		var stream unsafe.Pointer
		// 3 total elements
		dimensionVectorH := [9]uint8{2, 0, 1, 0, 2, 0, 1, 1, 1}
		dimensionVectorD := deviceAllocate(3*3, 0)
		memutils.AsyncCopyHostToDevice(dimensionVectorD.getPointer(), unsafe.Pointer(&dimensionVectorH), 3*3, stream, 0)

		hashVectorD := deviceAllocate(8*3, 0)
		dimIndexVectorH := [3]uint32{}
		dimIndexVectorD := deviceAllocate(4*3, 0)
		initIndexVector(dimIndexVectorD.getPointer(), 0, 3, stream, 0)

		measureVectorH := [3]uint32{22, 11, 22}
		measureVectorD := deviceAllocate(4*3, 0)
		memutils.AsyncCopyHostToDevice(measureVectorD.getPointer(), unsafe.Pointer(&measureVectorH), 4*3, stream, 0)

		numDims := queryCom.DimCountsPerDimWidth{0, 0, 0, 1, 0}
		batchCtx := oopkBatchContext{
			dimensionVectorD: [2]devicePointer{dimensionVectorD, nullDevicePointer},
			hashVectorD:      [2]devicePointer{hashVectorD, nullDevicePointer},
			dimIndexVectorD:  [2]devicePointer{dimIndexVectorD, nullDevicePointer},
			measureVectorD:   [2]devicePointer{measureVectorD, nullDevicePointer},
			size:             3,
			resultSize:       0,
			resultCapacity:   3,
		}

		batchCtx.sortByKey(numDims, stream, 0)
		memutils.AsyncCopyDeviceToHost(unsafe.Pointer(&measureVectorH), measureVectorD.getPointer(), 12, stream, 0)
		memutils.AsyncCopyDeviceToHost(unsafe.Pointer(&dimIndexVectorH), dimIndexVectorD.getPointer(), 12, stream, 0)

		Ω(dimIndexVectorH).Should(Or(Equal([3]uint32{0, 2, 1}), Equal([3]uint32{1, 0, 2}), Equal([3]uint32{1, 2, 0}), Equal([3]uint32{2, 0, 1})))

		deviceFreeAndSetNil(&dimensionVectorD)
		deviceFreeAndSetNil(&dimIndexVectorD)
		deviceFreeAndSetNil(&hashVectorD)
		deviceFreeAndSetNil(&measureVectorD)
	})

	ginkgo.It("reduce", func() {
		// one 4 byte dim
		numDims := queryCom.DimCountsPerDimWidth{0, 0, 1, 0, 0}
		var stream unsafe.Pointer

		dimensionInputVector := [5]uint32{1, 1, 1, 2, 0x01010101}
		hashInputVector := [4]uint64{1, 1, 1, 2}
		dimIndexInputVector := [4]uint32{0, 1, 2, 3}
		measureInputVector := [4]uint32{1, 2, 3, 4}

		var dimensionOutputVector [5]uint32
		var hashOutputVector [4]uint64
		var dimIndexOutputVector [4]uint32
		var measureOutputVector [4]uint32

		batchCtx := oopkBatchContext{
			dimensionVectorD: [2]devicePointer{{pointer: unsafe.Pointer(&dimensionInputVector)}, {pointer: unsafe.Pointer(&dimensionOutputVector)}},
			hashVectorD:      [2]devicePointer{{pointer: unsafe.Pointer(&hashInputVector)}, {pointer: unsafe.Pointer(&hashOutputVector)}},
			dimIndexVectorD:  [2]devicePointer{{pointer: unsafe.Pointer(&dimIndexInputVector)}, {pointer: unsafe.Pointer(&dimIndexOutputVector)}},
			measureVectorD:   [2]devicePointer{{pointer: unsafe.Pointer(&measureInputVector)}, {pointer: unsafe.Pointer(&measureOutputVector)}},
			size:             4,
			resultSize:       0,
			resultCapacity:   4,
		}

		// 1 is AGGR_SUM_UNSIGNED
		batchCtx.reduceByKey(numDims, 4, uint32(1), stream, 0)
		Ω(dimensionOutputVector).Should(Equal([5]uint32{1, 2, 0, 0, 0x0101}))
		Ω(measureOutputVector).Should(Equal([4]uint32{6, 4, 0, 0}))
	})

	ginkgo.It("estimateScratchSpaceMemUsage should work", func() {
		expression, _ := expr.ParseExpr(`a`)
		currentMemUsage, maxMemUsage := estimateScratchSpaceMemUsage(expression, 100, true)
		Ω(currentMemUsage).Should(Equal(0))
		Ω(maxMemUsage).Should(Equal(0))

		currentMemUsage, maxMemUsage = estimateScratchSpaceMemUsage(expression, 100, false)
		Ω(currentMemUsage).Should(Equal(0))
		Ω(maxMemUsage).Should(Equal(0))

		expression, _ = expr.ParseExpr(`a + b`)
		currentMemUsage, maxMemUsage = estimateScratchSpaceMemUsage(expression, 100, true)
		Ω(currentMemUsage).Should(Equal(0))
		Ω(maxMemUsage).Should(Equal(0))

		currentMemUsage, maxMemUsage = estimateScratchSpaceMemUsage(expression, 100, false)
		Ω(currentMemUsage).Should(Equal(500))
		Ω(maxMemUsage).Should(Equal(500))

		expression, _ = expr.ParseExpr(`(a+b) + (c+d)`)
		currentMemUsage, maxMemUsage = estimateScratchSpaceMemUsage(expression, 100, true)
		Ω(currentMemUsage).Should(Equal(0))
		Ω(maxMemUsage).Should(Equal(1000))

		currentMemUsage, maxMemUsage = estimateScratchSpaceMemUsage(expression, 100, false)
		Ω(currentMemUsage).Should(Equal(500))
		Ω(maxMemUsage).Should(Equal(1500))

		expression, _ = expr.ParseExpr(`((a1+b1) + (c1+d1)) * ((a2+b2) + (c2+d2))`)
		currentMemUsage, maxMemUsage = estimateScratchSpaceMemUsage(expression, 100, true)
		Ω(currentMemUsage).Should(Equal(0))
		Ω(maxMemUsage).Should(Equal(1500))

		currentMemUsage, maxMemUsage = estimateScratchSpaceMemUsage(expression, 100, false)
		Ω(currentMemUsage).Should(Equal(500))
		Ω(maxMemUsage).Should(Equal(1500))
	})

	ginkgo.It("copy dimension vector should work", func() {
		dvDevice := [40]uint8{2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 1, 0, 0, 0, 0, 0, 2, 1, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0}
		dvHostExpected := [20]uint8{2, 0, 0, 0, 1, 0, 0, 0, 2, 0, 1, 0, 2, 1, 1, 1, 1, 1, 1, 1}
		ctx := oopkBatchContext{
			dimensionVectorD: [2]devicePointer{{pointer: unsafe.Pointer(&dvDevice[0])}, nullDevicePointer},
			resultSize:       2,
			resultCapacity:   4,
		}
		dvHost := [20]uint8{}
		dimensionVectorHost := unsafe.Pointer(&dvHost[0])
		numDims := queryCom.DimCountsPerDimWidth{0, 0, 1, 1, 1}
		var stream unsafe.Pointer
		asyncCopyDimensionVector(dimensionVectorHost, ctx.dimensionVectorD[0].getPointer(), ctx.resultSize, 0,
			numDims, ctx.resultSize, ctx.resultCapacity, memutils.AsyncCopyDeviceToHost, stream, 0)
		Ω(dvHost).Should(Equal(dvHostExpected))
	})

	ginkgo.It("copy dimension vector with offset should work", func() {
		dvDevice := [40]uint8{2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 1, 0, 0, 0, 0, 0, 2, 1, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0}
		dvHostExpected := [40]uint8{0, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 1, 0, 0, 0, 0, 2, 1, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 0}
		ctx := oopkBatchContext{
			dimensionVectorD: [2]devicePointer{{pointer: unsafe.Pointer(&dvDevice[0])}, nullDevicePointer},
			resultSize:       2,
			resultCapacity:   4,
		}
		dvHost := [40]uint8{}
		dimensionVectorHost := unsafe.Pointer(&dvHost[0])
		numDims := queryCom.DimCountsPerDimWidth{0, 0, 1, 1, 1}
		var stream unsafe.Pointer
		asyncCopyDimensionVector(dimensionVectorHost, ctx.dimensionVectorD[0].getPointer(), ctx.resultSize, 1,
			numDims, ctx.resultCapacity, ctx.resultCapacity, memutils.AsyncCopyDeviceToHost, stream, 0)
		Ω(dvHost).Should(Equal(dvHostExpected))
	})

	ginkgo.It("prepareTimezoneTable", func() {
		memStore := new(memMocks.MemStore)
		memStore.On("GetSchema", mock.Anything).Return(&memstore.TableSchema{
			EnumDicts: map[string]memstore.EnumDict{
				"timezone": {
					ReverseDict: []string{"America/Los_Angeles"},
				},
			},
		}, nil)

		utils.Init(common.AresServerConfig{Query: common.QueryConfig{TimezoneTable: common.TimezoneConfig{
			TableName: "tableName",
		}}}, common.NewLoggerFactory().GetDefaultLogger(), common.NewLoggerFactory().GetDefaultLogger(), tally.NewTestScope("test", nil))
		qc := &AQLQueryContext{
			timezoneTable: timezoneTableContext{tableColumn: "timezone"},
		}
		qc.prepareTimezoneTable(memStore)
		Ω(qc.Error).Should(BeNil())
		Ω(da.(*memoryTrackingDeviceAllocatorImpl).memoryUsage[0]).Should(BeEquivalentTo(2))
		deviceFreeAndSetNil(&qc.OOPK.currentBatch.timezoneLookupD)
		utils.ResetDefaults()
	})

	ginkgo.It("ProcessQuery should work", func() {
		qc := &AQLQueryContext{}
		q := &AQLQuery{
			Table: table,
			Dimensions: []Dimension{
				{Expr: "c0", TimeBucketizer: "m", TimeUnit: "millisecond"},
			},
			Measures: []Measure{
				{Expr: "count(c1)"},
			},
			TimeFilter: TimeFilter{
				Column: "c0",
				From:   "1970-01-01",
				To:     "1970-01-02",
			},
		}
		qc.Query = q

		qc = q.Compile(memStore, false, nil)
		Ω(qc.Error).Should(BeNil())
		qc.ProcessQuery(memStore)
		Ω(qc.Error).Should(BeNil())
		qc.Postprocess()
		qc.ReleaseHostResultsBuffers()
		bs, err := json.Marshal(qc.Results)
		Ω(err).Should(BeNil())
		Ω(bs).Should(MatchJSON(` {
			"0": 5,
			"60000": 4,
			"120000": 3
		  }`))

		bc := qc.OOPK.currentBatch
		// Check whether pointers are properly cleaned up.
		Ω(len(qc.OOPK.foreignTables)).Should(BeZero())
		Ω(qc.cudaStreams[0]).Should(BeZero())
		Ω(qc.cudaStreams[1]).Should(BeZero())

		Ω(bc.dimensionVectorD[0]).Should(BeZero())
		Ω(bc.dimensionVectorD[1]).Should(BeZero())

		Ω(bc.dimIndexVectorD[0]).Should(BeZero())
		Ω(bc.dimIndexVectorD[1]).Should(BeZero())

		Ω(bc.hashVectorD[0]).Should(BeZero())
		Ω(bc.hashVectorD[1]).Should(BeZero())

		Ω(bc.measureVectorD[0]).Should(BeZero())
		Ω(bc.measureVectorD[1]).Should(BeZero())

		Ω(bc.resultSize).Should(BeZero())
		Ω(bc.resultCapacity).Should(BeZero())

		Ω(len(bc.columns)).Should(BeZero())

		Ω(bc.indexVectorD).Should(BeZero())
		Ω(bc.predicateVectorD).Should(BeZero())

		Ω(bc.size).Should(BeZero())

		Ω(len(bc.foreignTableRecordIDsD)).Should(BeZero())
		Ω(len(bc.exprStackD)).Should(BeZero())

		Ω(qc.OOPK.measureVectorH).Should(BeZero())
		Ω(qc.OOPK.dimensionVectorH).Should(BeZero())

		Ω(qc.OOPK.hllVectorD).Should(BeZero())
		Ω(qc.OOPK.hllDimRegIDCountD).Should(BeZero())
	})

	ginkgo.It("ProcessQuery should work for timezone column queries", func() {
		timezoneTable := "table2"
		memStore := new(memMocks.MemStore)

		mainTableSchema := metaCom.Table{
			Name: table,
			Config: metaCom.TableConfig{
				ArchivingDelayMinutes:    500,
				ArchivingIntervalMinutes: 300,
			},
			IsFactTable: true,
			Columns: []metaCom.Column{
				{Deleted: false, Name: "c0", Type: metaCom.Uint32},
				{Deleted: false, Name: "city_id", Type: metaCom.Uint32},
			},
		}
		timezoneTableSchema := metaCom.Table{
			Name: timezoneTable,
			Config: metaCom.TableConfig{
				ArchivingDelayMinutes:    500,
				ArchivingIntervalMinutes: 300,
			},
			IsFactTable: false,
			Columns: []metaCom.Column{
				{Deleted: false, Name: "id", Type: metaCom.Uint32},
				{Deleted: false, Name: "timezone", Type: metaCom.SmallEnum},
			},
			PrimaryKeyColumns: []int{0},
		}
		memStore.On("GetSchemas", mock.Anything).Return(map[string]*memstore.TableSchema{
			table: {
				Schema:            mainTableSchema,
				ColumnIDs:         map[string]int{"c0": 0, "city_id": 1},
				ValueTypeByColumn: []memCom.DataType{memCom.Uint32, memCom.Uint32},
				DefaultValues:     []*memCom.DataValue{&memCom.NullDataValue, &memCom.NullDataValue},
			},
			timezoneTable: {
				Schema:            timezoneTableSchema,
				ColumnIDs:         map[string]int{"id": 0, "timezone": 1},
				ValueTypeByColumn: []memCom.DataType{memCom.Uint32, memCom.SmallEnum},
				DefaultValues:     []*memCom.DataValue{&memCom.NullDataValue, &memCom.NullDataValue},
				EnumDicts: map[string]memstore.EnumDict{
					"id": {},
					"timezone": {
						ReverseDict: []string{"Africa/Algiers"},
					},
				},
			},
		})
		memStore.On("GetSchema", table).Return(&memstore.TableSchema{
			Schema:            mainTableSchema,
			ColumnIDs:         map[string]int{"c0": 0, "city_id": 1},
			ValueTypeByColumn: []memCom.DataType{memCom.Uint32, memCom.Uint32},
			DefaultValues:     []*memCom.DataValue{&memCom.NullDataValue, &memCom.NullDataValue},
		}, nil)
		memStore.On("GetSchema", timezoneTable).Return(&memstore.TableSchema{
			Schema:            timezoneTableSchema,
			ColumnIDs:         map[string]int{"id": 0, "timezone": 1},
			ValueTypeByColumn: []memCom.DataType{memCom.Uint32, memCom.SmallEnum},
			DefaultValues:     []*memCom.DataValue{&memCom.NullDataValue, &memCom.NullDataValue},
			EnumDicts: map[string]memstore.EnumDict{
				"id": {},
				"timezone": {
					ReverseDict: []string{"Africa/Algiers", "", ""},
				},
			},
		}, nil)
		memStore.On("RLock").Return(nil)
		memStore.On("RUnlock").Return(nil)

		timezoneTableShard := memstore.NewTableShard(&memstore.TableSchema{
			Schema:            timezoneTableSchema,
			ValueTypeByColumn: []memCom.DataType{memCom.Uint32, memCom.SmallEnum},
			DefaultValues:     []*memCom.DataValue{&memCom.NullDataValue, &memCom.NullDataValue},
		}, metaStore, diskStore, hostMemoryManager, shardID)
		timezoneTableBatch := memstore.LiveBatch{
			Batch: memstore.Batch{
				RWMutex: &sync.RWMutex{},
				Columns: batch120.Columns,
			},
			Capacity: 5,
		}
		timezoneTableShard.LiveStore = &memstore.LiveStore{
			LastReadRecord: memstore.RecordID{BatchID: -90, Index: 0},
			Batches: map[int32]*memstore.LiveBatch{
				-2147483648: &timezoneTableBatch,
			},
			PrimaryKey:        memstore.NewPrimaryKey(4, false, 5, hostMemoryManager),
			HostMemoryManager: hostMemoryManager,
		}
		// build foreign table primary key index
		keyBytes := make([]byte, 4)
		recordID := memstore.RecordID{BatchID: -2147483648, Index: 0}
		for i := 0; i < 5; i++ {
			binary.LittleEndian.PutUint32(keyBytes, 100+uint32(i)*10)
			recordID.Index = uint32(i)
			timezoneTableShard.LiveStore.PrimaryKey.FindOrInsert(keyBytes, recordID, 0)
		}

		mainTableShard := memstore.NewTableShard(&memstore.TableSchema{
			Schema:            mainTableSchema,
			ValueTypeByColumn: []memCom.DataType{memCom.Uint32, memCom.Uint32},
			DefaultValues:     []*memCom.DataValue{&memCom.NullDataValue, &memCom.NullDataValue},
		}, metaStore, diskStore, hostMemoryManager, shardID)
		mainTableShard.LiveStore = &memstore.LiveStore{
			LastReadRecord: memstore.RecordID{BatchID: -90, Index: 0},
			Batches: map[int32]*memstore.LiveBatch{
				-2147483648: {
					Batch: memstore.Batch{
						RWMutex: &sync.RWMutex{},
						Columns: batch130.Columns,
					},
					Capacity: 5,
				},
			},
			PrimaryKey:        memstore.NewPrimaryKey(16, true, 0, hostMemoryManager),
			HostMemoryManager: hostMemoryManager,
		}
		memStore.On("GetTableShard", timezoneTable, 0).Run(func(args mock.Arguments) {
			timezoneTableShard.Users.Add(1)
		}).Return(timezoneTableShard, nil).Once()
		memStore.On("GetTableShard", table, 0).Run(func(args mock.Arguments) {
			mainTableShard.Users.Add(1)
		}).Return(mainTableShard, nil).Once()

		utils.Init(common.AresServerConfig{Query: common.QueryConfig{TimezoneTable: common.TimezoneConfig{
			TableName: timezoneTable,
		}}}, common.NewLoggerFactory().GetDefaultLogger(), common.NewLoggerFactory().GetDefaultLogger(), tally.NewTestScope("test", nil))

		qc := &AQLQueryContext{}
		q := &AQLQuery{
			Table: table,
			Dimensions: []Dimension{
				{Expr: "c0", TimeBucketizer: "3m", TimeUnit: "second"},
			},
			Measures: []Measure{
				{Expr: "count(*)"},
			},
			TimeFilter: TimeFilter{
				Column: "c0",
				From:   "1970-01-01",
				To:     "1970-01-02",
			},
			Timezone: "timezone(city_id)",
		}
		qc.Query = q

		qc = q.Compile(memStore, false, nil)
		Ω(qc.Error).Should(BeNil())
		Ω(qc.TableScanners).Should(HaveLen(2))
		qc.ProcessQuery(memStore)
		Ω(qc.Error).Should(BeNil())
		Ω(qc.OOPK.currentBatch.timezoneLookupDSize).Should(Equal(3))
		qc.Postprocess()
		qc.ReleaseHostResultsBuffers()
		bs, err := json.Marshal(qc.Results)
		Ω(err).Should(BeNil())
		Ω(bs).Should(MatchJSON(` {
			"0": 4,
			"3600": 1
		}`))

		bc := qc.OOPK.currentBatch
		Ω(bc.timezoneLookupD).Should(BeZero())
		utils.ResetDefaults()
	})

	ginkgo.It("dimValResVectorSize should work", func() {
		Ω(dimValResVectorSize(3, queryCom.DimCountsPerDimWidth{0, 0, 1, 1, 1})).Should(Equal(30))
		Ω(dimValResVectorSize(3, queryCom.DimCountsPerDimWidth{0, 0, 2, 1, 1})).Should(Equal(45))
		Ω(dimValResVectorSize(3, queryCom.DimCountsPerDimWidth{0, 0, 1, 0, 0})).Should(Equal(15))
		Ω(dimValResVectorSize(3, queryCom.DimCountsPerDimWidth{0, 0, 1, 1, 0})).Should(Equal(24))
		Ω(dimValResVectorSize(3, queryCom.DimCountsPerDimWidth{0, 0, 1, 0, 1})).Should(Equal(21))
		Ω(dimValResVectorSize(3, queryCom.DimCountsPerDimWidth{0, 0, 0, 1, 1})).Should(Equal(15))
		Ω(dimValResVectorSize(3, queryCom.DimCountsPerDimWidth{0, 0, 0, 1, 0})).Should(Equal(9))
		Ω(dimValResVectorSize(3, queryCom.DimCountsPerDimWidth{0, 0, 0, 0, 1})).Should(Equal(6))
		Ω(dimValResVectorSize(0, queryCom.DimCountsPerDimWidth{0, 0, 1, 1, 1})).Should(Equal(0))
	})

	ginkgo.It("getGeoShapeLatLongSlice", func() {
		var lats, longs []float32

		shape := memCom.GeoShapeGo{
			Polygons: [][]memCom.GeoPointGo{
				{
					{
						90.0,
						-180.0,
					},
					{
						90.0,
						-180.0,
					},
				},
				{
					{
						90.0,
						-180.0,
					},
					{
						90.0,
						-180.0,
					},
				},
			},
		}

		lats, longs, numPoints := getGeoShapeLatLongSlice(lats, longs, shape)
		Ω(numPoints).Should(Equal(5))
		expectedLats := []float32{
			90.0,
			90.0,
			math.MaxFloat32,
			90.0,
			90.0,
		}
		expectedLongs := []float32{
			-180.0,
			-180.0,
			math.MaxFloat32,
			-180.0,
			-180.0,
		}
		Ω(lats).Should(Equal(expectedLats))
		Ω(longs).Should(Equal(expectedLongs))
	})

	ginkgo.It("evaluateGeoIntersect should work", func() {
		mockMemoryManager := new(memComMocks.HostMemoryManager)
		mockMemoryManager.On("ReportUnmanagedSpaceUsageChange", mock.Anything).Return()

		// prepare trip table
		tripsSchema := &memstore.TableSchema{
			Schema: metaCom.Table{
				Name: "trips",
			},
			ColumnIDs:             map[string]int{"request_at": 0, "request_point": 1},
			ValueTypeByColumn:     []memCom.DataType{memCom.Uint32, memCom.GeoPoint},
			PrimaryKeyColumnTypes: []memCom.DataType{memCom.Uint32},
			PrimaryKeyBytes:       4,
		}
		tripTimeLiveVP := memstore.NewLiveVectorParty(5, memCom.Uint32, memCom.NullDataValue, mockMemoryManager)
		tripTimeLiveVP.Allocate(false)
		pointLiveVP := memstore.NewLiveVectorParty(5, memCom.GeoPoint, memCom.NullDataValue, mockMemoryManager)
		pointLiveVP.Allocate(false)

		// 5 points (0,0),(3,2.5),(1.5, 3.5),(1.5,4.5),null
		//            in   in 2    in 3       out      out
		points := [4][2]float32{{0, 0}, {3, 2.5}, {1.5, 3.5}, {1.5, 4.5}}
		isPointValid := [5]bool{true, true, true, true, false}
		for i := 0; i < 5; i++ {
			requestTime := uint32(0)
			tripTimeLiveVP.SetDataValue(i, memCom.DataValue{Valid: true, OtherVal: unsafe.Pointer(&requestTime)}, memstore.IgnoreCount)
			if isPointValid[i] {
				pointLiveVP.SetDataValue(i, memCom.DataValue{Valid: true, OtherVal: unsafe.Pointer(&points[i])}, memstore.IgnoreCount)
			} else {
				pointLiveVP.SetDataValue(i, memCom.NullDataValue, memstore.IgnoreCount)
			}
		}

		tripsTableShard := &memstore.TableShard{
			Schema: tripsSchema,
			ArchiveStore: &memstore.ArchiveStore{
				CurrentVersion: memstore.NewArchiveStoreVersion(0, shard),
			},
			LiveStore: &memstore.LiveStore{
				Batches: map[int32]*memstore.LiveBatch{
					memstore.BaseBatchID: {
						Batch: memstore.Batch{
							RWMutex: &sync.RWMutex{},
							Columns: []memCom.VectorParty{
								tripTimeLiveVP,
								pointLiveVP,
							},
						},
					},
				},
				LastReadRecord: memstore.RecordID{BatchID: memstore.BaseBatchID, Index: 5},
			},
			HostMemoryManager: mockMemoryManager,
		}

		// prepare geofence table
		geofenceSchema := &memstore.TableSchema{
			Schema: metaCom.Table{
				Name: "geofence",
				Config: metaCom.TableConfig{
					InitialPrimaryKeyNumBuckets: 1,
				},
			},
			ColumnIDs:             map[string]int{"geofence_uuid": 0, "shape": 1},
			ValueTypeByColumn:     []memCom.DataType{memCom.UUID, memCom.GeoShape},
			PrimaryKeyColumnTypes: []memCom.DataType{memCom.UUID},
			PrimaryKeyBytes:       16,
		}
		shapeUUIDs := []string{"00000192F23D460DBE60400C32EA0667", "00001A3F088047D79343894698F221AB", "0000334BB6B0420986175F20F3FBF90D"}
		shapes := []memCom.GeoShapeGo{
			{
				Polygons: [][]memCom.GeoPointGo{
					{
						{1, 1}, {1, -1}, {-1, -1}, {-1, 1}, {1, 1},
					},
				},
			},
			{
				Polygons: [][]memCom.GeoPointGo{
					{
						{3, 3}, {2, 2}, {4, 2}, {3, 3},
					},
				},
			},
			{
				Polygons: [][]memCom.GeoPointGo{
					{
						{0, 6}, {3, 6}, {3, 3}, {0, 3}, {0, 6},
					},
					{
						{1, 5}, {2, 5}, {2, 4}, {1, 4}, {1, 5},
					},
				},
			},
		}
		shapeUUIDLiveVP := memstore.NewLiveVectorParty(3, memCom.UUID, memCom.NullDataValue, mockMemoryManager)
		shapeUUIDLiveVP.Allocate(false)
		shapeLiveVP := memstore.NewLiveVectorParty(3, memCom.GeoShape, memCom.NullDataValue, mockMemoryManager)
		shapeLiveVP.Allocate(false)

		geoFenceTableShard := &memstore.TableShard{
			Schema:            geofenceSchema,
			HostMemoryManager: mockMemoryManager,
		}
		geoFenceLiveStore := memstore.NewLiveStore(10, geoFenceTableShard)
		geoFenceLiveStore.Batches = map[int32]*memstore.LiveBatch{
			memstore.BaseBatchID: {
				Batch: memstore.Batch{
					RWMutex: &sync.RWMutex{},
					Columns: []memCom.VectorParty{
						shapeUUIDLiveVP,
						shapeLiveVP,
					},
				},
			},
		}
		geoFenceLiveStore.LastReadRecord = memstore.RecordID{BatchID: memstore.BaseBatchID, Index: 4}
		geoFenceTableShard.LiveStore = geoFenceLiveStore
		for i := 0; i < 3; i++ {
			uuidValue, _ := memCom.ValueFromString(shapeUUIDs[i], memCom.UUID)
			shapeUUIDLiveVP.SetDataValue(i, uuidValue, memstore.IgnoreCount)
			shapeLiveVP.SetDataValue(i, memCom.DataValue{Valid: true, GoVal: &shapes[i]}, memstore.IgnoreCount)
			key, err := memstore.GetPrimaryKeyBytes([]memCom.DataValue{uuidValue}, 16)
			Ω(err).Should(BeNil())
			geoFenceLiveStore.PrimaryKey.FindOrInsert(
				key,
				memstore.RecordID{
					BatchID: memstore.BaseBatchID,
					Index:   uint32(i)},
				0)
		}

		mockMemStore := new(memMocks.MemStore)
		mockMemStore.On("GetTableShard", "geofence", 0).Run(func(args mock.Arguments) {
			geoFenceTableShard.Users.Add(1)
		}).Return(geoFenceTableShard, nil).Once()

		mockMemStore.On("GetTableShard", "trips", 0).Run(func(args mock.Arguments) {
			tripsTableShard.Users.Add(1)
		}).Return(tripsTableShard, nil).Once()

		mockMemStore.On("RLock").Return()
		mockMemStore.On("RUnlock").Return()

		qc := AQLQueryContext{
			Query: &AQLQuery{
				Table: "trips",
				Dimensions: []Dimension{
					{Expr: "request_at"},
				},
				Measures: []Measure{
					{Expr: "count(1)"},
				},
				TimeFilter: TimeFilter{
					Column: "request_at",
					From:   "0",
					To:     "86400",
				},
			},
			TableScanners: []*TableScanner{
				{
					Columns: []int{0, 1},
					ColumnsByIDs: map[int]int{
						0: 0,
						1: 1,
					},
					ColumnUsages: map[int]columnUsage{
						0: columnUsedByAllBatches,
						1: columnUsedByAllBatches,
					},
					Schema:              tripsSchema,
					ArchiveBatchIDStart: 0,
					ArchiveBatchIDEnd:   1,
					Shards:              []int{0},
				},
				{
					ColumnsByIDs: map[int]int{
						0: 0,
						1: 1,
					},
					ColumnUsages: map[int]columnUsage{
						1: columnUsedByAllBatches,
					},
					Schema: geofenceSchema,
				},
			},
			OOPK: OOPKContext{
				DimRowBytes:          5,
				DimensionVectorIndex: []int{0},
				NumDimsPerDimWidth:   queryCom.DimCountsPerDimWidth{0, 0, 1, 0, 0},
				TimeFilters: [2]expr.Expr{
					&expr.BinaryExpr{
						ExprType: expr.Boolean,
						Op:       expr.GTE,
						LHS: &expr.VarRef{
							Val:      "request_at",
							ExprType: expr.Unsigned,
							TableID:  0,
							ColumnID: 0,
							DataType: memCom.Uint32,
						},
						RHS: &expr.NumberLiteral{
							Int:      0,
							Expr:     strconv.FormatInt(0, 10),
							ExprType: expr.Unsigned,
						},
					},
					&expr.BinaryExpr{
						ExprType: expr.Boolean,
						Op:       expr.LT,
						LHS: &expr.VarRef{
							Val:      "request_at",
							ExprType: expr.Unsigned,
							TableID:  0,
							ColumnID: 0,
							DataType: memCom.Uint32,
						},
						RHS: &expr.NumberLiteral{
							Int:      86400,
							Expr:     strconv.FormatInt(86400, 10),
							ExprType: expr.Unsigned,
						},
					},
				},
				Dimensions: []expr.Expr{
					&expr.VarRef{
						Val:      "request_at",
						ExprType: expr.Unsigned,
						TableID:  0,
						ColumnID: 0,
						DataType: memCom.Uint32,
					},
				},
				AggregateType: 1,
				MeasureBytes:  4,
				Measure: &expr.NumberLiteral{
					Val:      1,
					Int:      1,
					Expr:     "1",
					ExprType: expr.Unsigned,
				},
				geoIntersection: &geoIntersection{
					shapeTableID:  1,
					shapeColumnID: 1,
					pointColumnID: 1,
					shapeUUIDs:    shapeUUIDs,
					inOrOut:       false,
					dimIndex:      -1,
				},
			},
			fromTime: &alignedTime{time.Unix(0, 0), "s"},
			toTime:   &alignedTime{time.Unix(86400, 0), "s"},
		}

		qc.ProcessQuery(mockMemStore)
		Ω(qc.Error).Should(BeNil())
		qc.Postprocess()
		Ω(qc.Error).Should(BeNil())
		bs, err := json.Marshal(qc.Results)
		Ω(err).Should(BeNil())
		Ω(bs).Should(MatchJSON(
			`{
				"0": 1
			}`,
		))
		qc.ReleaseHostResultsBuffers()
	})

	ginkgo.It("evaluateGeoIntersectJoin should work", func() {
		mockMemoryManager := new(memComMocks.HostMemoryManager)
		mockMemoryManager.On("ReportUnmanagedSpaceUsageChange", mock.Anything).Return()

		// prepare trip table
		tripsSchema := &memstore.TableSchema{
			Schema: metaCom.Table{
				Name: "trips",
			},
			ColumnIDs:             map[string]int{"request_at": 0, "request_point": 1},
			ValueTypeByColumn:     []memCom.DataType{memCom.Uint32, memCom.GeoPoint},
			PrimaryKeyColumnTypes: []memCom.DataType{memCom.Uint32},
			PrimaryKeyBytes:       4,
		}
		tripTimeLiveVP := memstore.NewLiveVectorParty(5, memCom.Uint32, memCom.NullDataValue, mockMemoryManager)
		tripTimeLiveVP.Allocate(false)
		pointLiveVP := memstore.NewLiveVectorParty(5, memCom.GeoPoint, memCom.NullDataValue, mockMemoryManager)
		pointLiveVP.Allocate(false)

		// 5 points (0,0),(3,2.5),(1.5, 3.5),(1.5,4.5),null
		//            in   in 2    in 3       out      out
		points := [4][2]float32{{0, 0}, {3, 2.5}, {1.5, 3.5}, {1.5, 4.5}}
		isPointValid := [5]bool{true, true, true, true, false}
		for i := 0; i < 5; i++ {
			requestTime := uint32(0)
			tripTimeLiveVP.SetDataValue(i, memCom.DataValue{Valid: true, OtherVal: unsafe.Pointer(&requestTime)}, memstore.IgnoreCount)
			if isPointValid[i] {
				pointLiveVP.SetDataValue(i, memCom.DataValue{Valid: true, OtherVal: unsafe.Pointer(&points[i])}, memstore.IgnoreCount)
			} else {
				pointLiveVP.SetDataValue(i, memCom.NullDataValue, memstore.IgnoreCount)
			}
		}

		tripsTableShard := &memstore.TableShard{
			Schema: tripsSchema,
			ArchiveStore: &memstore.ArchiveStore{
				CurrentVersion: memstore.NewArchiveStoreVersion(0, shard),
			},
			LiveStore: &memstore.LiveStore{
				Batches: map[int32]*memstore.LiveBatch{
					memstore.BaseBatchID: {
						Batch: memstore.Batch{
							RWMutex: &sync.RWMutex{},
							Columns: []memCom.VectorParty{
								tripTimeLiveVP,
								pointLiveVP,
							},
						},
					},
				},
				LastReadRecord: memstore.RecordID{BatchID: memstore.BaseBatchID, Index: 5},
			},
			HostMemoryManager: mockMemoryManager,
		}

		// prepare geofence table
		geofenceSchema := &memstore.TableSchema{
			Schema: metaCom.Table{
				Name: "geofence",
				Config: metaCom.TableConfig{
					InitialPrimaryKeyNumBuckets: 1,
				},
			},
			ColumnIDs:             map[string]int{"geofence_uuid": 0, "shape": 1},
			ValueTypeByColumn:     []memCom.DataType{memCom.UUID, memCom.GeoShape},
			PrimaryKeyColumnTypes: []memCom.DataType{memCom.UUID},
			PrimaryKeyBytes:       16,
		}
		shapeUUIDs := []string{"00000192F23D460DBE60400C32EA0667", "00001A3F088047D79343894698F221AB", "0000334BB6B0420986175F20F3FBF90D"}
		shapes := []memCom.GeoShapeGo{
			{
				Polygons: [][]memCom.GeoPointGo{
					{
						{1, 1}, {1, -1}, {-1, -1}, {-1, 1}, {1, 1},
					},
				},
			},
			{
				Polygons: [][]memCom.GeoPointGo{
					{
						{3, 3}, {2, 2}, {4, 2}, {3, 3},
					},
				},
			},
			{
				Polygons: [][]memCom.GeoPointGo{
					{
						{0, 6}, {3, 6}, {3, 3}, {0, 3}, {0, 6},
					},
					{
						{1, 5}, {2, 5}, {2, 4}, {1, 4}, {1, 5},
					},
				},
			},
		}
		shapeUUIDLiveVP := memstore.NewLiveVectorParty(3, memCom.UUID, memCom.NullDataValue, mockMemoryManager)
		shapeUUIDLiveVP.Allocate(false)
		shapeLiveVP := memstore.NewLiveVectorParty(3, memCom.GeoShape, memCom.NullDataValue, mockMemoryManager)
		shapeLiveVP.Allocate(false)

		geoFenceTableShard := &memstore.TableShard{
			Schema:            geofenceSchema,
			HostMemoryManager: mockMemoryManager,
		}
		geoFenceLiveStore := memstore.NewLiveStore(10, geoFenceTableShard)
		geoFenceLiveStore.Batches = map[int32]*memstore.LiveBatch{
			memstore.BaseBatchID: {
				Batch: memstore.Batch{
					RWMutex: &sync.RWMutex{},
					Columns: []memCom.VectorParty{
						shapeUUIDLiveVP,
						shapeLiveVP,
					},
				},
			},
		}
		geoFenceLiveStore.LastReadRecord = memstore.RecordID{BatchID: memstore.BaseBatchID, Index: 4}
		geoFenceTableShard.LiveStore = geoFenceLiveStore
		for i := 0; i < 3; i++ {
			uuidValue, _ := memCom.ValueFromString(shapeUUIDs[i], memCom.UUID)
			shapeUUIDLiveVP.SetDataValue(i, uuidValue, memstore.IgnoreCount)
			shapeLiveVP.SetDataValue(i, memCom.DataValue{Valid: true, GoVal: &shapes[i]}, memstore.IgnoreCount)
			key, err := memstore.GetPrimaryKeyBytes([]memCom.DataValue{uuidValue}, 16)
			Ω(err).Should(BeNil())
			geoFenceLiveStore.PrimaryKey.FindOrInsert(
				key,
				memstore.RecordID{
					BatchID: memstore.BaseBatchID,
					Index:   uint32(i)},
				0)
		}

		mockMemStore := new(memMocks.MemStore)
		mockMemStore.On("GetTableShard", "geofence", 0).Run(func(args mock.Arguments) {
			geoFenceTableShard.Users.Add(1)
		}).Return(geoFenceTableShard, nil).Once()

		mockMemStore.On("GetTableShard", "trips", 0).Run(func(args mock.Arguments) {
			tripsTableShard.Users.Add(1)
		}).Return(tripsTableShard, nil).Once()

		mockMemStore.On("RLock").Return()
		mockMemStore.On("RUnlock").Return()

		qc := AQLQueryContext{
			Query: &AQLQuery{
				Table: "trips",
				Dimensions: []Dimension{
					{Expr: "request_at"},
					{Expr: "geo_uuid"},
				},
				Measures: []Measure{
					{Expr: "count(1)"},
				},
				TimeFilter: TimeFilter{
					Column: "request_at",
					From:   "0",
					To:     "86400",
				},
			},
			TableScanners: []*TableScanner{
				{
					Columns: []int{0, 1},
					ColumnsByIDs: map[int]int{
						0: 0,
						1: 1,
					},
					ColumnUsages: map[int]columnUsage{
						0: columnUsedByAllBatches,
						1: columnUsedByAllBatches,
					},
					Schema:              tripsSchema,
					ArchiveBatchIDStart: 0,
					ArchiveBatchIDEnd:   1,
					Shards:              []int{0},
				},
				{
					ColumnsByIDs: map[int]int{
						0: 0,
						1: 1,
					},
					ColumnUsages: map[int]columnUsage{
						1: columnUsedByAllBatches,
					},
					Schema: geofenceSchema,
				},
			},
			OOPK: OOPKContext{
				DimRowBytes:          7,
				DimensionVectorIndex: []int{0, 1},
				NumDimsPerDimWidth:   queryCom.DimCountsPerDimWidth{0, 0, 1, 0, 1},
				TimeFilters: [2]expr.Expr{
					&expr.BinaryExpr{
						ExprType: expr.Boolean,
						Op:       expr.GTE,
						LHS: &expr.VarRef{
							Val:      "request_at",
							ExprType: expr.Unsigned,
							TableID:  0,
							ColumnID: 0,
							DataType: memCom.Uint32,
						},
						RHS: &expr.NumberLiteral{
							Int:      0,
							Expr:     strconv.FormatInt(0, 10),
							ExprType: expr.Unsigned,
						},
					},
					&expr.BinaryExpr{
						ExprType: expr.Boolean,
						Op:       expr.LT,
						LHS: &expr.VarRef{
							Val:      "request_at",
							ExprType: expr.Unsigned,
							TableID:  0,
							ColumnID: 0,
							DataType: memCom.Uint32,
						},
						RHS: &expr.NumberLiteral{
							Int:      86400,
							Expr:     strconv.FormatInt(86400, 10),
							ExprType: expr.Unsigned,
						},
					},
				},
				Dimensions: []expr.Expr{
					&expr.VarRef{
						Val:      "request_at",
						ExprType: expr.Unsigned,
						TableID:  0,
						ColumnID: 0,
						DataType: memCom.Uint32,
					},
					&expr.VarRef{
						Val:      "geo_uuid",
						ExprType: expr.GeoShape,
						TableID:  0,
						ColumnID: 0,
						DataType: memCom.Uint8,
					},
				},
				AggregateType: 1,
				MeasureBytes:  4,
				Measure: &expr.NumberLiteral{
					Val:      1,
					Int:      1,
					Expr:     "1",
					ExprType: expr.Unsigned,
				},
				geoIntersection: &geoIntersection{
					shapeTableID:  1,
					shapeColumnID: 1,
					pointColumnID: 1,
					shapeUUIDs:    shapeUUIDs,
					dimIndex:      1,
					inOrOut:       true,
				},
			},
			fromTime: &alignedTime{time.Unix(0, 0), "s"},
			toTime:   &alignedTime{time.Unix(86400, 0), "s"},
		}

		qc.ProcessQuery(mockMemStore)
		Ω(qc.Error).Should(BeNil())
		qc.Postprocess()
		Ω(qc.Error).Should(BeNil())
		bs, err := json.Marshal(qc.Results)
		Ω(err).Should(BeNil())
		Ω(bs).Should(MatchJSON(
			`{
				"0": {
					"00000192F23D460DBE60400C32EA0667": 1,
					"00001A3F088047D79343894698F221AB": 1,
					"0000334BB6B0420986175F20F3FBF90D": 1
				}
			}`,
		))
		qc.ReleaseHostResultsBuffers()
	})

	ginkgo.It("shouldSkipLiveBatch should work", func() {
		qc := &AQLQueryContext{}

		column0 := new(memComMocks.LiveVectorParty)
		column0.On("GetMinMaxValue").Return(uint32(10), uint32(50))
		column1 := new(memComMocks.LiveVectorParty)
		column1.On("GetMinMaxValue").Return(uint32(50), uint32(99))

		batch := &memstore.LiveBatch{
			Batch: memstore.Batch{
				RWMutex: &sync.RWMutex{},
				Columns: []memCom.VectorParty{
					column0,
					column1,
					nil,
				},
			},
		}

		// No candidate filter.
		Ω(qc.shouldSkipLiveBatch(batch)).Should(BeFalse())

		qc.OOPK.MainTableCommonFilters = append(qc.OOPK.MainTableCommonFilters, &expr.UnaryExpr{})

		// UnaryExpr does not match.
		Ω(qc.shouldSkipLiveBatch(batch)).Should(BeFalse())

		// we will swap lhs and rhs.
		// we will need skip this batch as column 0 does not pass the check.
		qc.OOPK.MainTableCommonFilters = append(qc.OOPK.MainTableCommonFilters, &expr.BinaryExpr{
			Op: expr.LT,
			RHS: &expr.VarRef{
				ColumnID: 0,
				DataType: memCom.Uint32,
			},
			LHS: &expr.NumberLiteral{
				Int: 100,
			},
		})

		Ω(qc.shouldSkipLiveBatch(batch)).Should(BeTrue())

		// We will pass this check
		qc.OOPK.MainTableCommonFilters = []expr.Expr{&expr.BinaryExpr{
			Op: expr.GT,
			LHS: &expr.VarRef{
				ColumnID: 1,
				DataType: memCom.Uint32,
			},
			RHS: &expr.NumberLiteral{
				Int: 50,
			},
		}}
		Ω(qc.shouldSkipLiveBatch(batch)).Should(BeFalse())

		// This filter is not uint32.
		qc.OOPK.MainTableCommonFilters = append(qc.OOPK.MainTableCommonFilters, &expr.BinaryExpr{
			Op: expr.LT,
			LHS: &expr.VarRef{
				ColumnID: 0,
				DataType: memCom.Uint16,
			},
			RHS: &expr.NumberLiteral{
				Int: 5,
			},
		})

		Ω(qc.shouldSkipLiveBatch(batch)).Should(BeFalse())

		// column 2 is nil, so we should skip.
		qc.OOPK.MainTableCommonFilters = append(qc.OOPK.MainTableCommonFilters, &expr.BinaryExpr{
			Op: expr.LT,
			LHS: &expr.VarRef{
				ColumnID: 2,
				DataType: memCom.Uint32,
			},
			RHS: &expr.NumberLiteral{
				Int: 5,
			},
		})

		Ω(qc.shouldSkipLiveBatch(batch)).Should(BeTrue())

		qc.OOPK.MainTableCommonFilters = nil

		// we will fail to pass time filter.
		qc.OOPK.TimeFilters = [2]expr.Expr{
			&expr.BinaryExpr{
				Op: expr.GTE,
				LHS: &expr.VarRef{
					ColumnID: 0,
					DataType: memCom.Uint32,
				},
				RHS: &expr.NumberLiteral{
					Int: 100,
				},
			},
			&expr.BinaryExpr{
				Op: expr.LT,
				LHS: &expr.VarRef{
					ColumnID: 0,
					DataType: memCom.Uint32,
				},
				RHS: &expr.NumberLiteral{
					Int: 200,
				},
			},
		}

		Ω(qc.shouldSkipLiveBatch(batch)).Should(BeTrue())
	})

	ginkgo.It("evaluateGeoPoint query should work", func() {
		mockMemoryManager := new(memComMocks.HostMemoryManager)
		mockMemoryManager.On("ReportUnmanagedSpaceUsageChange", mock.Anything).Return()

		// prepare trip table
		tripsSchema := &memstore.TableSchema{
			Schema: metaCom.Table{
				Name: "trips",
			},
			ColumnIDs:             map[string]int{"request_at": 0, "request_point": 1},
			ValueTypeByColumn:     []memCom.DataType{memCom.Uint32, memCom.GeoPoint},
			PrimaryKeyColumnTypes: []memCom.DataType{memCom.Uint32},
			PrimaryKeyBytes:       4,
		}
		tripTimeLiveVP := memstore.NewLiveVectorParty(6, memCom.Uint32, memCom.NullDataValue, mockMemoryManager)
		tripTimeLiveVP.Allocate(false)
		pointLiveVP := memstore.NewLiveVectorParty(6, memCom.GeoPoint, memCom.NullDataValue, mockMemoryManager)
		pointLiveVP.Allocate(false)

		points := [5][2]float32{{0, 0}, {1.5, 3.5}, {1.5, 3.5}, {37.617994, -122.386177}, {37.617994, -122.386177}}
		isPointValid := [6]bool{true, true, true, true, true, false}
		for i := 0; i < 6; i++ {
			requestTime := uint32(0)
			tripTimeLiveVP.SetDataValue(i, memCom.DataValue{Valid: true, OtherVal: unsafe.Pointer(&requestTime)}, memstore.IgnoreCount)
			if isPointValid[i] {
				pointLiveVP.SetDataValue(i, memCom.DataValue{Valid: true, OtherVal: unsafe.Pointer(&points[i])}, memstore.IgnoreCount)
			} else {
				pointLiveVP.SetDataValue(i, memCom.NullDataValue, memstore.IgnoreCount)
			}
		}

		tripsTableShard := &memstore.TableShard{
			Schema: tripsSchema,
			ArchiveStore: &memstore.ArchiveStore{
				CurrentVersion: memstore.NewArchiveStoreVersion(0, shard),
			},
			LiveStore: &memstore.LiveStore{
				Batches: map[int32]*memstore.LiveBatch{
					memstore.BaseBatchID: {
						Batch: memstore.Batch{
							RWMutex: &sync.RWMutex{},
							Columns: []memCom.VectorParty{
								tripTimeLiveVP,
								pointLiveVP,
							},
						},
					},
				},
				LastReadRecord: memstore.RecordID{BatchID: memstore.BaseBatchID, Index: 5},
			},
			HostMemoryManager: mockMemoryManager,
		}

		mockMemStore := new(memMocks.MemStore)

		mockMemStore.On("GetTableShard", "trips", 0).Run(func(args mock.Arguments) {
			tripsTableShard.Users.Add(1)
		}).Return(tripsTableShard, nil).Once()

		mockMemStore.On("RLock").Return()
		mockMemStore.On("RUnlock").Return()

		qc := AQLQueryContext{
			Query: &AQLQuery{
				Table: "trips",
				Dimensions: []Dimension{
					{Expr: "request_at"},
				},
				Measures: []Measure{
					{Expr: "count(1)"},
				},
			},
			TableScanners: []*TableScanner{
				{
					Columns: []int{0, 1},
					ColumnsByIDs: map[int]int{
						0: 0,
						1: 1,
					},
					ColumnUsages: map[int]columnUsage{
						0: columnUsedByAllBatches,
						1: columnUsedByAllBatches,
					},
					Schema:              tripsSchema,
					ArchiveBatchIDStart: 0,
					ArchiveBatchIDEnd:   1,
					Shards:              []int{0},
				},
			},
			OOPK: OOPKContext{
				DimRowBytes:          5,
				DimensionVectorIndex: []int{0},
				NumDimsPerDimWidth:   queryCom.DimCountsPerDimWidth{0, 0, 1, 0, 0},
				Prefilters: []expr.Expr{
					&expr.BinaryExpr{
						Op:       expr.EQ,
						LHS:      &expr.VarRef{Val: "request_point", ColumnID: 1, TableID: 0, ExprType: expr.GeoPoint, DataType: memCom.GeoPoint},
						RHS:      &expr.GeopointLiteral{Val: [2]float32{37.617994, -122.386177}},
						ExprType: expr.Boolean,
					},
				},
				Dimensions: []expr.Expr{
					&expr.VarRef{
						Val:      "request_at",
						ExprType: expr.Unsigned,
						TableID:  0,
						ColumnID: 0,
						DataType: memCom.Uint32,
					},
				},
				AggregateType: 1,
				MeasureBytes:  4,
				Measure: &expr.NumberLiteral{
					Val:      1,
					Int:      1,
					Expr:     "1",
					ExprType: expr.Unsigned,
				},
			},
			fromTime: &alignedTime{time.Unix(0, 0), "s"},
			toTime:   &alignedTime{time.Unix(86400, 0), "s"},
		}

		qc.ProcessQuery(mockMemStore)
		Ω(qc.Error).Should(BeNil())
		qc.Postprocess()
		Ω(qc.Error).Should(BeNil())
		bs, err := json.Marshal(qc.Results)
		Ω(err).Should(BeNil())
		Ω(bs).Should(MatchJSON(
			`{
				"0": 2
			}`,
		))
		qc.ReleaseHostResultsBuffers()
	})

	ginkgo.It("ProcessQuery for non-aggregation query should work", func() {
		shard.ArchiveStore.CurrentVersion.Batches[0] = archiveBatch1
		qc := &AQLQueryContext{}
		q := &AQLQuery{
			Table: table,
			Dimensions: []Dimension{
				{Expr: "c0"},
				{Expr: "c1"},
				{Expr: "c2"},
			},
			Measures: []Measure{
				{Expr: "1"},
			},
			TimeFilter: TimeFilter{
				Column: "c0",
				From:   "1970-01-01",
				To:     "1970-01-02",
			},
			Limit: 20,
		}
		qc.Query = q

		qc = q.Compile(memStore, false, nil)
		Ω(qc.Error).Should(BeNil())
		qc.calculateMemoryRequirement(memStore)
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
			"headers": ["c0", "c1", "c2"],
			"matrixData": [
				["100", "0", "1"],
          		["110", "1", "NULL" ],
          		["120", "NULL", "1.2"],
          		["130", "0", "1.3"],
          		["100", "0", "NULL"],
          		["110", "1", "1.1"],
          		["120", "0", "1.2"],
          		["0", "NULL", "NULL"],
          		["10", "NULL", "1.1"],
          		["20", "NULL", "1.2"],
          		["30", "0", "1.3"],
          		["40", "1", "NULL"]
			]
		  }`))

		bc := qc.OOPK.currentBatch
		// Check whether pointers are properly cleaned up.
		Ω(len(qc.OOPK.foreignTables)).Should(BeZero())
		Ω(qc.cudaStreams[0]).Should(BeZero())
		Ω(qc.cudaStreams[1]).Should(BeZero())

		Ω(bc.dimensionVectorD[0]).Should(BeZero())
		Ω(bc.dimensionVectorD[1]).Should(BeZero())

		Ω(bc.dimIndexVectorD[0]).Should(BeZero())
		Ω(bc.dimIndexVectorD[1]).Should(BeZero())

		Ω(bc.hashVectorD[0]).Should(BeZero())
		Ω(bc.hashVectorD[1]).Should(BeZero())

		Ω(bc.measureVectorD[0]).Should(BeZero())
		Ω(bc.measureVectorD[1]).Should(BeZero())

		Ω(bc.resultSize).Should(BeZero())
		Ω(bc.resultCapacity).Should(BeZero())

		Ω(len(bc.columns)).Should(BeZero())

		Ω(bc.indexVectorD).Should(BeZero())
		Ω(bc.predicateVectorD).Should(BeZero())

		Ω(bc.size).Should(BeZero())

		Ω(len(bc.foreignTableRecordIDsD)).Should(BeZero())
		Ω(len(bc.exprStackD)).Should(BeZero())

		Ω(qc.OOPK.measureVectorH).Should(BeZero())
		Ω(qc.OOPK.dimensionVectorH).Should(BeZero())

		Ω(qc.OOPK.hllVectorD).Should(BeZero())
		Ω(qc.OOPK.hllDimRegIDCountD).Should(BeZero())
	})

	ginkgo.It("ProcessQuery should work for query without regular filters", func() {
		shard.ArchiveStore.CurrentVersion.Batches[0] = archiveBatch1
		qc := &AQLQueryContext{}
		q := &AQLQuery{
			Table: table,
			Dimensions: []Dimension{
				{Expr: "0"},
			},
			Measures: []Measure{
				{Expr: "count(*)"},
			},
			TimeFilter: TimeFilter{
				Column: "c0",
				From:   "1970-01-01",
				To:     "1970-01-02",
			},
		}
		qc.Query = q
		qc = q.Compile(memStore, false, nil)
		Ω(qc.Error).Should(BeNil())
		qc.ProcessQuery(memStore)
		Ω(qc.Error).Should(BeNil())

		qc.Postprocess()
		qc.ReleaseHostResultsBuffers()
		bs, err := json.Marshal(qc.Results)
		Ω(err).Should(BeNil())

		Ω(bs).Should(MatchJSON(` {
			"0": 12
		  }`))
	})

	ginkgo.It("initializeNonAggResponse should work", func() {
		qc := &AQLQueryContext{
			Query: &AQLQuery{
				Dimensions: []Dimension{
					{Expr: "foo"},
				},
			},
		}
		qc.isNonAggregationQuery = true

		w := httptest.NewRecorder()
		qc.responseWriter = w

		qc.initializeNonAggResponse()
		Ω(w.Body.String()).Should(Equal(`{"results":[{"headers":["foo"],"matrixData":[`))
	})
})
