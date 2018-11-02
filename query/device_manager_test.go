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
	"code.uber.internal/data/ares/memstore"
	"code.uber.internal/data/ares/query/common"
	"code.uber.internal/data/ares/query/expr"
	"code.uber.internal/data/ares/utils"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"sync"
	"time"
)

var _ = ginkgo.Describe("device_manager", func() {
	var deviceManager *DeviceManager
	var leastMemStrategy deviceChooseStrategy
	freeMemory := []int{400, 2000, 3000}
	queryCounts := []int{1, 1, 0}
	deviceCount := 3

	ginkgo.BeforeEach(func() {
		// create DeviceInfoMap
		deviceInfoArray := make([]*DeviceInfo, deviceCount)
		for device := 0; device < deviceCount; device++ {
			deviceInfo := DeviceInfo{
				DeviceID:             device,
				QueryCount:           queryCounts[device],
				TotalAvailableMemory: 3000,
				FreeMemory:           freeMemory[device],
				QueryMemoryUsageMap:  make(map[*AQLQuery]int, 0),
			}
			deviceInfoArray[device] = &deviceInfo
		}

		deviceManager = &DeviceManager{
			DeviceInfos:        deviceInfoArray,
			Timeout:            5,
			MaxAvailableMemory: 3000,
		}

		deviceManager.deviceAvailable = sync.NewCond(deviceManager)
		leastMemStrategy = leastQueryCountAndMemoryStrategy{
			deviceManager: deviceManager,
		}
	})

	ginkgo.AfterEach(func() {
		utils.ResetClockImplementation()
	})

	ginkgo.It("leastMemStrategy should work", func() {
		deviceManager.strategy = leastMemStrategy
		queries := [5]*AQLQuery{{}, {}, {}, {}}
		devices := [5]int{}
		// case 1: w/o device hint, return the device w/ least query count.
		devices[0] = deviceManager.findDevice(queries[0], 1000, -1)
		Ω(devices[0]).Should(Equal(2))
		// 400 2000 2000
		// 1 1 1

		// case 2: w/ device hint, return the device if it meets the requirement
		devices[1] = deviceManager.findDevice(queries[1], 1000, 2)
		Ω(devices[1]).Should(Equal(2))
		// 400 2000 1000
		// 1 1 2

		// case 3: w/ device hint, but cannot meet the requirements
		devices[2] = deviceManager.findDevice(queries[2], 1500, 0)
		Ω(devices[2]).Should(Equal(1))
		// 400 500 1000
		// 1 2 2

		// case 4: unable to find a device that meets the requirement
		devices[3] = deviceManager.findDevice(queries[3], 2000, 0)
		Ω(devices[3]).Should(Equal(-1))
		// 400 500 1000
		// 1 2 2

		// case 5: return device with least query count and least available memory.
		devices[4] = deviceManager.findDevice(queries[4], 500, 0)
		Ω(devices[4]).Should(Equal(1))
		// 400 0 1000
		// 1 3 2

		Ω(deviceManager.DeviceInfos[0].FreeMemory).Should(BeEquivalentTo(400))
		Ω(deviceManager.DeviceInfos[1].FreeMemory).Should(BeEquivalentTo(0))
		Ω(deviceManager.DeviceInfos[2].FreeMemory).Should(BeEquivalentTo(1000))

		Ω(deviceManager.DeviceInfos[0].QueryCount).Should(BeEquivalentTo(1))
		Ω(deviceManager.DeviceInfos[1].QueryCount).Should(BeEquivalentTo(3))
		Ω(deviceManager.DeviceInfos[2].QueryCount).Should(BeEquivalentTo(2))

		for i := range devices {
			deviceManager.ReleaseReservedMemory(devices[i], queries[i])
		}

		for device, deviceInfo := range deviceManager.DeviceInfos {
			Ω(deviceInfo.FreeMemory).Should(Equal(freeMemory[device]))
			Ω(deviceInfo.QueryCount).Should(Equal(queryCounts[device]))
			Ω(deviceInfo.QueryMemoryUsageMap).Should(BeEmpty())
		}
	})

	ginkgo.It("query queuing should work", func() {
		deviceManager = &DeviceManager{
			DeviceInfos: []*DeviceInfo{
				{
					DeviceID:             0,
					QueryCount:           0,
					TotalAvailableMemory: 1000,
					FreeMemory:           500,
					QueryMemoryUsageMap:  make(map[*AQLQuery]int, 0),
				},
			},
			Timeout:            5,
			MaxAvailableMemory: 1000,
		}

		leastQueryCountAndMemStrategy := leastQueryCountAndMemoryStrategy{
			deviceManager: deviceManager,
		}

		deviceManager.strategy = leastQueryCountAndMemStrategy
		deviceManager.deviceAvailable = sync.NewCond(deviceManager)

		queries := [3]*AQLQuery{{}, {}, {}}
		devices := [4]int{}
		timeout := 3

		utils.SetCurrentTime(time.Unix(0, 0))

		// first assign 500 bytes to a query 1.
		devices[0] = deviceManager.FindDevice(queries[0], 500, -1, timeout)
		Ω(devices[0]).Should(Equal(0))
		Ω(deviceManager.DeviceInfos[0].FreeMemory).Should(Equal(0))

		wg := sync.WaitGroup{}
		wg.Add(2)
		// query 2:
		// requires 500 bytes, needs to wait for release of query 1.
		go func() {
			defer wg.Done()
			devices[1] = deviceManager.FindDevice(queries[1], 500, -1, timeout)
			deviceManager.ReleaseReservedMemory(devices[1], queries[1])
		}()

		// query 3:
		// requires 1000 bytes, will timeout.
		go func() {
			defer wg.Done()
			devices[2] = deviceManager.FindDevice(queries[2], 1000, -1, timeout)
			deviceManager.ReleaseReservedMemory(devices[2], queries[2])
		}()

		<-time.NewTimer(time.Second).C
		// trigger broadcast, query 1 should finish now.
		deviceManager.ReleaseReservedMemory(devices[0], queries[0])

		<-time.NewTimer(time.Second).C
		utils.SetCurrentTime(time.Unix(10, 0))
		// trigger another broadcast, query 2 will timeout.
		device := deviceManager.FindDevice(queries[0], 500, -1, timeout)
		Ω(device).Should(Equal(0))
		deviceManager.ReleaseReservedMemory(device, queries[0])

		// wait for two go routines.
		wg.Wait()
		Ω(deviceManager.DeviceInfos[0].FreeMemory).Should(Equal(500))
		Ω(devices[1]).Should(Equal(0))
		Ω(devices[2]).Should(Equal(-1))
		// query 4:
		// requi0res 2000 bytes, exceeds max device memory.
		device = deviceManager.FindDevice(queries[2], 2000, -1, timeout)
		Ω(device).Should(Equal(-1))
	})

	ginkgo.It("estimate memory usage", func() {
		testFactory := memstore.TestFactoryT{
			RootPath:   "../testing/data",
			FileSystem: utils.OSFileSystem{},
		}
		batch110, err := testFactory.ReadLiveBatch("archiving/batch-110")
		Ω(err).Should(BeNil())
		liveBatch := &memstore.LiveBatch{
			Capacity: 5,
			Batch: memstore.Batch{
				Columns: batch110.Columns,
			},
		}

		batchArchive0, err := testFactory.ReadArchiveBatch("archiving/archiveBatch0")
		Ω(err).Should(BeNil())
		archiveBatch := &memstore.ArchiveBatch{
			Batch: memstore.Batch{
				Columns: batchArchive0.Columns,
			},
			Size: 5,
		}

		qc := AQLQueryContext{
			TableScanners: []*TableScanner{
				{
					Columns: []int{0, 2, 1},
					ColumnUsages: map[int]columnUsage{
						0: columnUsedByAllBatches,
						1: columnUsedByAllBatches | columnUsedByPrefilter,
						2: columnUsedByAllBatches,
					},
					EqualityPrefilterValues: []uint32{1},
				},
			},
			OOPK: OOPKContext{
				NumDimsPerDimWidth: common.DimCountsPerDimWidth{0, 0, 1, 0, 0},
				DimRowBytes:        5,
				MeasureBytes:       4,
				foreignTables:      []*foreignTable{{}},
				MainTableCommonFilters: []expr.Expr{
					&expr.BinaryExpr{
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
					},
				},
				Dimensions: []expr.Expr{
					&expr.BinaryExpr{
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
					},
				},
				Measure: &expr.BinaryExpr{
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
			},
		}

		// expression
		memUsage := qc.estimateExpressionEvaluationMemUsage(20)
		Ω(memUsage).Should(Equal(20 * 5 * 2))

		// sort reduce
		memUsage = estimateSortReduceMemUsage(20)
		Ω(memUsage).Should(Equal(720))

		// batch processing
		memUsage = qc.estimateMemUsageForBatch(20, 128+128+128)
		Ω(memUsage).Should(Equal(1204))

		// live batch columns + processing
		memUsage = qc.estimateLiveBatchMemoryUsage(liveBatch)
		Ω(memUsage).Should(Equal(589))

		// archive batch columns + processing
		memUsage = qc.estimateArchiveBatchMemoryUsage(archiveBatch, false)
		Ω(memUsage).Should(Equal(489))
	})
})
