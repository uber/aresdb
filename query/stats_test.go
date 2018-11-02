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
	"code.uber.internal/data/ares/utils"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"sort"
	"time"
	"unsafe"
)

var _ = ginkgo.Describe("Stats", func() {
	ginkgo.AfterEach(func() {
		utils.ResetClockImplementation()
	})

	ginkgo.It("Sort should work for stageSummaryStatsSlice", func() {
		stageStats := stageSummaryStatsSlice{
			&oopkStageSummaryStats{
				name:  "a",
				total: 1,
			},
			&oopkStageSummaryStats{
				name:  "b",
				total: 2,
			},
			&oopkStageSummaryStats{
				name:  "c",
				total: 3,
			},
		}
		Ω(stageStats.Len()).Should(Equal(3))
		Ω(stageStats.Less(0, 1)).Should(BeTrue())
		Ω(stageStats.Less(1, 0)).Should(BeFalse())
		stageStats.Swap(0, 1)
		Ω(stageStats[0].name).Should(BeEquivalentTo("b"))
		Ω(stageStats[1].name).Should(BeEquivalentTo("a"))
		sort.Sort(sort.Reverse(stageStats))
		Ω(stageStats[0].name).Should(BeEquivalentTo("c"))
		Ω(stageStats[1].name).Should(BeEquivalentTo("b"))
		Ω(stageStats[2].name).Should(BeEquivalentTo("a"))
	})

	ginkgo.It("DataSource implementations should work for oopkQueryStats", func() {
		queryStats := oopkQueryStats{
			stageStats: []*oopkStageSummaryStats{
				{name: "a"},
				{avg: 1, max: 1, min: 1, total: 2, percentage: 1.0, count: 2, name: "b"},
			},
		}

		Ω(queryStats.NumRows()).Should(Equal(2))
		Ω(len(queryStats.ColumnHeaders())).Should(Equal(7))
		Ω(queryStats.GetValue(1, 0)).Should(BeEquivalentTo("b"))
		Ω(queryStats.GetValue(1, 1)).Should(Equal(1.0))
		Ω(queryStats.GetValue(1, 2)).Should(Equal(1.0))
		Ω(queryStats.GetValue(1, 3)).Should(Equal(1.0))
		Ω(queryStats.GetValue(1, 4)).Should(Equal(2))
		Ω(queryStats.GetValue(1, 5)).Should(Equal(2.0))
		Ω(queryStats.GetValue(1, 6)).Should(Equal("100.00%"))
	})

	ginkgo.It("applyBatchStats should work", func() {
		queryStats := oopkQueryStats{
			stageStats: []*oopkStageSummaryStats{
				{name: "a"},
				{avg: 1, max: 1, min: 1, total: 2, percentage: 1.0, count: 2, name: "b"},
			},
		}

		queryStats.applyBatchStats(oopkBatchStats{
			batchSize:        10,
			bytesTransferred: 200,
			numTransferCalls: 10,
		})

		Ω(queryStats).Should(Equal(oopkQueryStats{
			stageStats: []*oopkStageSummaryStats{
				{name: "a", max: 0, min: 0, avg: 0, count: 0, total: 0, percentage: 0},
				{name: "b", max: 1, min: 1, avg: 1, count: 2, total: 2, percentage: 1},
			},
			Name2Stage:       nil,
			TotalTiming:      0,
			NumBatches:       1,
			NumRecords:       10,
			BytesTransferred: 200,
			NumTransferCalls: 10,
		}))
		queryStats.writeToLog()
	})

	ginkgo.It("reportTimingForCurrentBatch and reportTiming should work", func() {
		qc := AQLQueryContext{Debug: true}
		qc.OOPK.LiveBatchStats = oopkQueryStats{
			Name2Stage: make(map[stageName]*oopkStageSummaryStats),
		}
		qc.OOPK.currentBatch.stats = oopkBatchStats{
			timings: make(map[stageName]float64),
		}

		var stream unsafe.Pointer
		utils.SetClockImplementation(func() time.Time {
			return time.Unix(0, 0)
		})
		start := utils.Now()
		utils.SetClockImplementation(func() time.Time {
			return time.Unix(1, 0)
		})
		qc.reportTimingForCurrentBatch(stream, &start, dimEvalTiming)
		utils.ResetClockImplementation()
		Ω(qc.OOPK.currentBatch.stats.timings[dimEvalTiming]).Should(Equal(1000.0))

		utils.SetClockImplementation(func() time.Time {
			return time.Unix(2, 0)
		})
		qc.reportTiming(stream, &start, prepareForeignTableTiming)
		Ω(*qc.OOPK.LiveBatchStats.Name2Stage[prepareForeignTableTiming]).Should(Equal(oopkStageSummaryStats{
			name:  prepareForeignTableTiming,
			max:   1000.0,
			min:   1000.0,
			count: 1,
			total: 1000.0,
		}))
	})

	ginkgo.It("reportBatch should work", func() {
		qc := AQLQueryContext{Debug: true}
		qc.OOPK.LiveBatchStats = oopkQueryStats{
			Name2Stage: make(map[stageName]*oopkStageSummaryStats),
		}

		qc.OOPK.ArchiveBatchStats = oopkQueryStats{
			Name2Stage: make(map[stageName]*oopkStageSummaryStats),
		}

		qc.OOPK.currentBatch.stats = oopkBatchStats{
			timings: map[stageName]float64{
				filterEvalTiming: 100,
			},
		}

		qc.reportBatch(true)
		Ω(qc.OOPK.ArchiveBatchStats).Should(Equal(oopkQueryStats{
			stageStats: nil,
			Name2Stage: map[stageName]*oopkStageSummaryStats{
				"filterEval": {name: "filterEval", max: 100, min: 100, avg: 0, count: 1, total: 100, percentage: 0},
			},
			TotalTiming:      100,
			NumBatches:       1,
			NumRecords:       0,
			BytesTransferred: 0,
			NumTransferCalls: 0,
		}))

		qc.reportBatch(false)
		Ω(qc.OOPK.LiveBatchStats).Should(Equal(oopkQueryStats{
			stageStats: nil,
			Name2Stage: map[stageName]*oopkStageSummaryStats{
				"filterEval": {name: "filterEval", max: 100, min: 100, avg: 0, count: 1, total: 100, percentage: 0},
			},
			TotalTiming:      100,
			NumBatches:       1,
			NumRecords:       0,
			BytesTransferred: 0,
			NumTransferCalls: 0,
		}))
	})
})
