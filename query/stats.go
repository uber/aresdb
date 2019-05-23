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
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"time"
	"unsafe"

	"github.com/uber/aresdb/memutils"
	"github.com/uber/aresdb/utils"
)

// stageName represents each query stage.
type stageName string

const (
	prepareForeignTableTiming     stageName = "prepareForeignTable"
	transferTiming                          = "transfer"
	prepareForFilteringTiming               = "prepareForFiltering"
	initIndexVectorTiming                   = "initIndexVector"
	filterEvalTiming                        = "filterEval"
	prepareForeignRecordIDsTiming           = "prepareForeignRecordIDs"
	foreignTableFilterEvalTiming            = "foreignTableFilterEval"
	geoIntersectEvalTiming                  = "geoIntersectEval"
	prepareForDimAndMeasureTiming           = "prepareForDimAndMeasure"
	dimEvalTiming                           = "dimEval"
	measureEvalTiming                       = "measureEval"
	hllEvalTiming                           = "hllEval"
	sortEvalTiming                          = "sortEval"
	reduceEvalTiming                        = "reduceEval"
	expandEvalTiming                        = "expandEval"
	cleanupTiming                           = "cleanUpEval"
	resultTransferTiming                    = "resultTransfer"
	resultFlushTiming                       = "resultFlush"
	finalCleanupTiming                      = "finalCleanUp"
)

// oopkBatchStats stores stats for a single batch execution.
type oopkBatchStats struct {
	// Store timings for each stage of a single batch.
	timings map[stageName]float64
	// totalTiming for this batch.
	totalTiming      float64
	batchID          int32
	batchSize        int
	bytesTransferred int
	numTransferCalls int
}

// oopkStageSummaryStats stores running info for each stage.
type oopkStageSummaryStats struct {
	name       stageName
	max        float64
	min        float64
	avg        float64
	count      int
	total      float64
	percentage float64
}

// MarshalJSON marshals the message to JSON in a custom way.
func (s *oopkStageSummaryStats) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.total)
}

type stageSummaryStatsSlice []*oopkStageSummaryStats

// Len implements sort.Sort interface for stageSummaryStatsSlice.
func (s stageSummaryStatsSlice) Len() int {
	return len(s)
}

// Swap implements sort.Sort interface for stageSummaryStatsSlice.
func (s stageSummaryStatsSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Less implements sort.Sort interface for stageSummaryStatsSlice.
func (s stageSummaryStatsSlice) Less(i, j int) bool {
	return s[i].total < s[j].total
}

// oopkQueryStats stores the overall stats for a query.
type oopkQueryStats struct {
	// stats for each stage. Sorted by total time.
	stageStats []*oopkStageSummaryStats
	// mapping from stage name to stage stats.
	Name2Stage map[stageName]*oopkStageSummaryStats `json:"stages"`

	// Total timing for all query stages **including transfer**.
	TotalTiming float64 `json:"latency"`

	// Total number of batches.
	NumBatches int `json:"batches"`
	// Total number of records processed on GPU.
	// A record could represent multiple data record if firstColumn is compressed.
	NumRecords int `json:"records"`

	// For archive batch, we skip process empty batch. For live batch, we will skip it
	// if its min or max value does not pass main table filters or time filters.
	NumBatchSkipped int `json:"numBatchSkipped"`

	// Stats for input data transferred via PCIe.
	BytesTransferred int `json:"tranBytes"`
	NumTransferCalls int `json:"tranCalls"`
}

// NumRows implements the utils.TableDataSource for stats.
func (stats oopkQueryStats) NumRows() int {
	return len(stats.stageStats)
}

// GetValue implements the utils.TableDataSource for stats. **Notes** row boundary
// are not checked!
func (stats oopkQueryStats) GetValue(row, col int) interface{} {
	rowValue := stats.stageStats[row]
	switch col {
	case 0:
		return rowValue.name
	case 1:
		return rowValue.avg
	case 2:
		return rowValue.max
	case 3:
		return rowValue.min
	case 4:
		return rowValue.count
	case 5:
		return rowValue.total
	case 6:
		return fmt.Sprintf("%.2f%%", rowValue.percentage*100)
	}
	return nil
}

// ColumnHeaders implements the utils.TableDataSource for stats.
func (stats oopkQueryStats) ColumnHeaders() []string {
	return []string{"stage", "avg", "max", "minCallName", "count", "total", "percentage"}
}

// reportTimingForCurrentBatch will first wait for current cuda stream if the debug mode is set and change the timing stat accordingly.
// It will add to the total timing as well. Therefore this function should only be called one time for each stage.
func (qc *AQLQueryContext) reportTimingForCurrentBatch(stream unsafe.Pointer, start *time.Time, name stageName) {
	if qc.Debug {
		memutils.WaitForCudaStream(stream, qc.Device)
		now := utils.Now()
		value := now.Sub(*start).Seconds() * 1000
		qc.OOPK.currentBatch.stats.timings[name] = value
		qc.OOPK.currentBatch.stats.totalTiming += value
		*start = now
	}
}

// reportTiming is similar to reportTimingForCurrentBatch except that it modifies the query stats for the
// whole query. It's usually should be called once for each stage
func (qc *AQLQueryContext) reportTiming(stream unsafe.Pointer, start *time.Time, name stageName) {
	if qc.Debug {
		if stream != nil {
			memutils.WaitForCudaStream(stream, qc.Device)
		}
		now := utils.Now()
		value := now.Sub(*start).Seconds() * 1000
		queryStats := &qc.OOPK.LiveBatchStats
		queryStats.applyStageStats(name, value)
		*start = now
	}
}

// applyStageStats applies the stage stats to the overall query stats and compute max,minCallName and total for that
// stage.
func (stats *oopkQueryStats) applyStageStats(name stageName, value float64) {
	if _, ok := stats.Name2Stage[name]; !ok {
		stats.Name2Stage[name] = &oopkStageSummaryStats{name: name, max: -1, min: math.MaxFloat64}
	}
	stageStats := stats.Name2Stage[name]
	stageStats.max = math.Max(stageStats.max, value)
	stageStats.min = math.Min(stageStats.max, value)
	stageStats.count++
	stageStats.total += value
	stats.TotalTiming += value
}

// applyBatchStats applies the current batch stats onto the overall query stats. It computes information
// like max, minCallName, average for each stage as well as the percentage.
func (stats *oopkQueryStats) applyBatchStats(batchStats oopkBatchStats) {
	for name, value := range batchStats.timings {
		stats.applyStageStats(name, value)
	}
	stats.NumBatches++
	stats.NumRecords += batchStats.batchSize
	stats.BytesTransferred += batchStats.bytesTransferred
	stats.NumTransferCalls += batchStats.numTransferCalls
}

// writeToLog writes the summary stats for this query in a tabular format to logger.
func (stats *oopkQueryStats) writeToLog() {
	if stats.NumBatches+stats.NumBatchSkipped > 0 {
		// Compute average and percentage.
		stats.stageStats = make([]*oopkStageSummaryStats, 0, len(stats.Name2Stage))
		for _, oopkStageStats := range stats.Name2Stage {
			oopkStageStats.avg = oopkStageStats.total / float64(stats.NumBatches)
			oopkStageStats.percentage = oopkStageStats.total / stats.TotalTiming
			stats.stageStats = append(stats.stageStats, oopkStageStats)
		}
		sort.Sort(sort.Reverse(stageSummaryStatsSlice(stats.stageStats)))

		utils.GetQueryLogger().Infof("Total timing: %f", stats.TotalTiming)
		utils.GetQueryLogger().Infof("Num batches: %d", stats.NumBatches)
		utils.GetQueryLogger().Infof("Num batches skipped: %d", stats.NumBatchSkipped)
		// Create tabular output.
		summary := utils.WriteTable(stats)
		utils.GetQueryLogger().Info("\n" + summary)
	}
}

// reportBatch will report OOPK batch related stats to the query logger.
func (qc *AQLQueryContext) reportBatch(isArchiveBatch bool) {
	if qc.Debug {
		batchType := "live batch"
		if isArchiveBatch {
			batchType = "archive batch"
		}
		stats := qc.OOPK.currentBatch.stats
		utils.GetQueryLogger().
			With(
				"timings", stats.timings,
				"total", stats.totalTiming,
				"batchID", stats.batchID,
				"batchSize", stats.batchSize,
				"batchType", batchType,
			).Infof("Query stats")
		if isArchiveBatch {
			qc.OOPK.ArchiveBatchStats.applyBatchStats(stats)
		} else {
			qc.OOPK.LiveBatchStats.applyBatchStats(stats)
		}
	}
}
