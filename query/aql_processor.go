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
	"math"
	"unsafe"

	"encoding/binary"
	"encoding/json"
	"github.com/uber/aresdb/memstore"
	memCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/memutils"
	queryCom "github.com/uber/aresdb/query/common"
	"github.com/uber/aresdb/query/expr"
	"github.com/uber/aresdb/utils"
	"time"
)

const (
	hllQueryRequiredMemoryInMB = 10 * 1024
)

// batchTransferExecutor defines the type of the functor to transfer a live batch or a archive batch
// from host memory to device memory. hostVPs will be the columns to be released after transfer. startRow
// is used to slice the vector party.
type batchTransferExecutor func(stream unsafe.Pointer) (deviceColumns []deviceVectorPartySlice,
	hostVPs []memCom.VectorParty, firstColumn, startRow, totalBytes, numTransfers, sizeAfterPrefilter int)

// customFilterExecutor is the functor to apply custom filters depends on the batch type. For archive batch,
// the custom filter will be the time filter and will only be applied to first or last batch. For live batch,
// the custom filters will be the cutoff time filter if cutoff is larger than 0, pre-filters and time filters.
type customFilterExecutor func(stream unsafe.Pointer)

// ProcessQuery processes the compiled query and executes it on GPU.
func (qc *AQLQueryContext) ProcessQuery(memStore memstore.MemStore) {
	defer func() {
		if r := recover(); r != nil {
			// find out exactly what the error was and set err
			switch x := r.(type) {
			case string:
				qc.Error = utils.StackError(nil, x)
			case error:
				qc.Error = utils.StackError(x, "Panic happens when processing query")
			default:
				qc.Error = utils.StackError(nil, "Panic happens when processing query %v", x)
			}
			utils.GetLogger().Error("Releasing device memory after panic")
			qc.Release()
		}
	}()

	qc.cudaStreams[0] = memutils.CreateCudaStream(qc.Device)
	qc.cudaStreams[1] = memutils.CreateCudaStream(qc.Device)
	qc.OOPK.currentBatch.device = qc.Device
	qc.OOPK.LiveBatchStats = oopkQueryStats{
		Name2Stage: make(map[stageName]*oopkStageSummaryStats),
	}
	qc.OOPK.ArchiveBatchStats = oopkQueryStats{
		Name2Stage: make(map[stageName]*oopkStageSummaryStats),
	}

	previousBatchExecutor := NewDummyBatchExecutor()

	start := utils.Now()
	for joinTableID, join := range qc.Query.Joins {
		qc.prepareForeignTable(memStore, joinTableID, join)
		if qc.Error != nil {
			return
		}
	}
	qc.reportTiming(qc.cudaStreams[0], &start, prepareForeignTableTiming)

	qc.prepareTimezoneTable(memStore)
	if qc.Error != nil {
		return
	}

	// prepare geo intersection
	if qc.OOPK.geoIntersection != nil {
		shapeExists := qc.prepareForGeoIntersect(memStore)
		if qc.Error != nil {
			return
		}
		if !shapeExists {
			// if no shape exist and geo check for point in shape
			// no need to continue processing batch
			if qc.OOPK.geoIntersection.inOrOut {
				return
			}
			// if no shape exist and geo check for point not in shape
			// no need to do geo intersection
			qc.OOPK.geoIntersection = nil
		}
	}

	qc.initializeNonAggResponse()

	qc.initResultFlushContext()

	for _, shardID := range qc.TableScanners[0].Shards {
		previousBatchExecutor = qc.processShard(memStore, shardID, previousBatchExecutor)
		if qc.Error != nil {
			return
		}
		if qc.OOPK.done {
			break
		}
	}

	// query execution for last batch.
	qc.runBatchExecutor(previousBatchExecutor, true)

	// this code snippet does the followings:
	// 1. write stats to log.
	// 2. allocate host buffer for result and copy the result from device to host.
	// 3. clean up device status buffers if no panic.
	if qc.Debug {
		qc.OOPK.LiveBatchStats.writeToLog()
		qc.OOPK.ArchiveBatchStats.writeToLog()
	}

	start = utils.Now()
	if qc.Error == nil {
		// Copy the result to host memory.
		qc.OOPK.ResultSize = qc.OOPK.currentBatch.resultSize
		if qc.OOPK.IsHLL() {
			qc.HLLQueryResult, qc.Error = qc.PostprocessAsHLLData()
		} else {
			if !qc.IsNonAggregationQuery {
				// copy dimensions
				qc.OOPK.dimensionVectorH = memutils.HostAlloc(qc.OOPK.ResultSize * qc.OOPK.DimRowBytes)
				asyncCopyDimensionVector(qc.OOPK.dimensionVectorH, qc.OOPK.currentBatch.dimensionVectorD[0].getPointer(), qc.OOPK.ResultSize, 0,
					qc.OOPK.NumDimsPerDimWidth, qc.OOPK.ResultSize, qc.OOPK.currentBatch.resultCapacity,
					memutils.AsyncCopyDeviceToHost, qc.cudaStreams[0], qc.Device)
				// copy measures
				qc.OOPK.measureVectorH = memutils.HostAlloc(qc.OOPK.ResultSize * qc.OOPK.MeasureBytes)
				memutils.AsyncCopyDeviceToHost(
					qc.OOPK.measureVectorH, qc.OOPK.currentBatch.measureVectorD[0].getPointer(),
					qc.OOPK.ResultSize*qc.OOPK.MeasureBytes, qc.cudaStreams[0], qc.Device)
				memutils.WaitForCudaStream(qc.cudaStreams[0], qc.Device)
			}
		}
	}
	qc.reportTiming(qc.cudaStreams[0], &start, resultTransferTiming)
	qc.cleanUpDeviceStatus()
	qc.reportTiming(nil, &start, finalCleanupTiming)
}

func (qc *AQLQueryContext) processShard(memStore memstore.MemStore, shardID int, previousBatchExecutor BatchExecutor) BatchExecutor {
	var liveRecordsProcessed, archiveRecordsProcessed, liveBatchProcessed, archiveBatchProcessed, liveBytesTransferred, archiveBytesTransferred int
	shard, err := memStore.GetTableShard(qc.Query.Table, shardID)
	if err != nil {
		qc.Error = utils.StackError(err, "failed to get shard %d for table %s",
			shardID, qc.Query.Table)
		return previousBatchExecutor
	}
	defer shard.Users.Done()

	var archiveStore *memstore.ArchiveStoreVersion
	var cutoff uint32
	if shard.Schema.Schema.IsFactTable {
		archiveStore = shard.ArchiveStore.GetCurrentVersion()
		defer archiveStore.Users.Done()
		cutoff = archiveStore.ArchivingCutoff
	}

	// Process live batches.
	if qc.toTime == nil || cutoff < uint32(qc.toTime.Time.Unix()) {
		batchIDs, numRecordsInLastBatch := shard.LiveStore.GetBatchIDs()
		for i, batchID := range batchIDs {
			if qc.OOPK.done {
				break
			}
			batch := shard.LiveStore.GetBatchForRead(batchID)
			if batch == nil {
				continue
			}

			// For now, dimension table does not persist min and max therefore
			// we can only skip live batch for fact table.
			// TODO: Persist min/max/numTrues when snapshotting.
			if shard.Schema.Schema.IsFactTable && qc.shouldSkipLiveBatch(batch) {
				batch.RUnlock()
				qc.OOPK.LiveBatchStats.NumBatchSkipped++
				continue
			}

			liveBatchProcessed++
			size := batch.Capacity
			if i == len(batchIDs)-1 {
				size = numRecordsInLastBatch
			}
			liveRecordsProcessed += size
			previousBatchExecutor = qc.processBatch(&batch.Batch,
				batchID,
				size,
				qc.transferLiveBatch(batch, size),
				qc.liveBatchCustomFilterExecutor(cutoff), previousBatchExecutor, true)
			qc.cudaStreams[0], qc.cudaStreams[1] = qc.cudaStreams[1], qc.cudaStreams[0]
			liveBytesTransferred += qc.OOPK.currentBatch.stats.bytesTransferred
		}
	}

	// Process archive batches.
	if archiveStore != nil && (qc.fromTime == nil || cutoff > uint32(qc.fromTime.Time.Unix())) {
		scanner := qc.TableScanners[0]
		for batchID := scanner.ArchiveBatchIDStart; batchID < scanner.ArchiveBatchIDEnd; batchID++ {
			if qc.OOPK.done {
				break
			}
			archiveBatch := archiveStore.RequestBatch(int32(batchID))
			if archiveBatch.Size == 0 {
				qc.OOPK.ArchiveBatchStats.NumBatchSkipped++
				continue
			}
			isFirstOrLast := batchID == scanner.ArchiveBatchIDStart || batchID == scanner.ArchiveBatchIDEnd-1
			previousBatchExecutor = qc.processBatch(
				&archiveBatch.Batch,
				int32(batchID),
				archiveBatch.Size,
				qc.transferArchiveBatch(archiveBatch, isFirstOrLast),
				qc.archiveBatchCustomFilterExecutor(isFirstOrLast),
				previousBatchExecutor, false)
			archiveRecordsProcessed += archiveBatch.Size
			archiveBatchProcessed++
			qc.cudaStreams[0], qc.cudaStreams[1] = qc.cudaStreams[1], qc.cudaStreams[0]
			archiveBytesTransferred += qc.OOPK.currentBatch.stats.bytesTransferred
		}
	}
	utils.GetReporter(qc.Query.Table, shardID).GetCounter(utils.QueryLiveRecordsProcessed).Inc(int64(liveRecordsProcessed))
	utils.GetReporter(qc.Query.Table, shardID).GetCounter(utils.QueryArchiveRecordsProcessed).Inc(int64(archiveRecordsProcessed))
	utils.GetReporter(qc.Query.Table, shardID).GetCounter(utils.QueryLiveBatchProcessed).Inc(int64(liveBatchProcessed))
	utils.GetReporter(qc.Query.Table, shardID).GetCounter(utils.QueryArchiveBatchProcessed).Inc(int64(archiveBatchProcessed))
	utils.GetReporter(qc.Query.Table, shardID).GetCounter(utils.QueryLiveBytesTransferred).Inc(int64(liveBytesTransferred))
	utils.GetReporter(qc.Query.Table, shardID).GetCounter(utils.QueryArchiveBytesTransferred).Inc(int64(archiveBytesTransferred))

	return previousBatchExecutor
}

// Release releases all device memory it allocated. It **should only called** when any errors happens while the query is
// processed.
func (qc *AQLQueryContext) Release() {
	// release device memory for processing current batch.
	qc.OOPK.currentBatch.cleanupBeforeAggregation()
	qc.OOPK.currentBatch.swapResultBufferForNextBatch()
	qc.cleanUpDeviceStatus()
	qc.ReleaseHostResultsBuffers()
}

// CleanUpDevice cleans up the device status including
//  1. clean up the device buffer for storing results.
//  2. clean up the cuda streams
func (qc *AQLQueryContext) cleanUpDeviceStatus() {
	// clean up foreign table memory after query
	for _, foreignTable := range qc.OOPK.foreignTables {
		qc.cleanUpForeignTable(foreignTable)
	}
	qc.OOPK.foreignTables = nil

	// release geo pointers
	if qc.OOPK.geoIntersection != nil {
		deviceFreeAndSetNil(&qc.OOPK.geoIntersection.shapeLatLongs)
	}

	// Destroy streams
	memutils.DestroyCudaStream(qc.cudaStreams[0], qc.Device)
	memutils.DestroyCudaStream(qc.cudaStreams[1], qc.Device)
	qc.cudaStreams = [2]unsafe.Pointer{nil, nil}

	// Clean up the device result buffers.
	qc.OOPK.currentBatch.cleanupDeviceResultBuffers()

	// Clean up timezone lookup buffer.
	deviceFreeAndSetNil(&qc.OOPK.currentBatch.timezoneLookupD)
}

// clean up foreign table
func (qc *AQLQueryContext) cleanUpForeignTable(table *foreignTable) {
	if table != nil {
		deviceFreeAndSetNil(&table.devicePrimaryKeyPtr)
		for _, batch := range table.batches {
			for _, column := range batch {
				deviceFreeAndSetNil(&column.basePtr)
			}
		}
		table.batches = nil
	}
}

// getGeoShapeLatLongSlice format GeoShapeGo into slices of float32 for query purpose
// Lats and Longs are stored in the format as [a1,a2,...an,a1,MaxFloat32,b1,bz,...bn]
// refer to time_series_aggregate.h for GeoShape struct
func getGeoShapeLatLongSlice(shapesLats, shapesLongs []float32, gs memCom.GeoShapeGo) ([]float32, []float32, int) {
	numPoints := 0
	for i, polygon := range gs.Polygons {
		if len(polygon) > 0 && i > 0 {
			// write place holder at start of polygon
			shapesLats = append(shapesLats, math.MaxFloat32)
			shapesLongs = append(shapesLongs, math.MaxFloat32)
			// FLT_MAX as placeholder for each polygon
			numPoints++
		}
		for _, point := range polygon {
			shapesLats = append(shapesLats, point[0])
			shapesLongs = append(shapesLongs, point[1])
			numPoints++
		}
	}
	return shapesLats, shapesLongs, numPoints
}

func (qc *AQLQueryContext) prepareForGeoIntersect(memStore memstore.MemStore) (shapeExists bool) {
	tableScanner := qc.TableScanners[qc.OOPK.geoIntersection.shapeTableID]
	shapeColumnID := qc.OOPK.geoIntersection.shapeColumnID
	tableName := tableScanner.Schema.Schema.Name
	// geo table is not sharded
	shard, err := memStore.GetTableShard(tableName, 0)
	if err != nil {
		qc.Error = utils.StackError(err, "Failed to get shard for table %s, shard: %d", tableName, 0)
		return
	}
	defer shard.Users.Done()

	numPointsPerShape := make([]int32, 0, len(qc.OOPK.geoIntersection.shapeUUIDs))
	qc.OOPK.geoIntersection.validShapeUUIDs = make([]string, 0, len(qc.OOPK.geoIntersection.shapeUUIDs))
	var shapesLats, shapesLongs []float32
	var numPoints, totalNumPoints int
	for _, uuid := range qc.OOPK.geoIntersection.shapeUUIDs {
		recordID, found := shard.LiveStore.LookupKey([]string{uuid})
		if found {
			batch := shard.LiveStore.GetBatchForRead(recordID.BatchID)
			if batch != nil {
				shapeValue := batch.GetDataValue(int(recordID.Index), shapeColumnID)
				// compiler should have verified the geo column GeoShape type
				shapesLats, shapesLongs, numPoints = getGeoShapeLatLongSlice(shapesLats, shapesLongs, *(shapeValue.GoVal.(*memCom.GeoShapeGo)))
				if numPoints > 0 {
					totalNumPoints += numPoints
					numPointsPerShape = append(numPointsPerShape, int32(numPoints))
					qc.OOPK.geoIntersection.validShapeUUIDs = append(qc.OOPK.geoIntersection.validShapeUUIDs, uuid)
					shapeExists = true
				}
				batch.RUnlock()
			}
		}
	}

	if !shapeExists {
		return
	}

	numValidShapes := len(numPointsPerShape)
	shapeIndexs := make([]uint8, totalNumPoints)
	pointIndex := 0
	for shapeIndex, numPoints := range numPointsPerShape {
		for i := 0; i < int(numPoints); i++ {
			shapeIndexs[pointIndex] = uint8(shapeIndex)
			pointIndex++
		}
	}

	// allocate memory for lats, longs (float32) and numPoints (int32) device vectors
	latsPtrD := deviceAllocate(totalNumPoints*4*2+totalNumPoints, qc.Device)
	longsPtrD := latsPtrD.offset(totalNumPoints * 4)
	shapeIndexsD := longsPtrD.offset(totalNumPoints * 4)

	memutils.AsyncCopyHostToDevice(latsPtrD.getPointer(), unsafe.Pointer(&shapesLats[0]), totalNumPoints*4, qc.cudaStreams[0], qc.Device)
	memutils.AsyncCopyHostToDevice(longsPtrD.getPointer(), unsafe.Pointer(&shapesLongs[0]), totalNumPoints*4, qc.cudaStreams[0], qc.Device)
	memutils.AsyncCopyHostToDevice(shapeIndexsD.getPointer(), unsafe.Pointer(&shapeIndexs[0]), totalNumPoints, qc.cudaStreams[0], qc.Device)

	qc.OOPK.geoIntersection.shapeLatLongs = latsPtrD
	qc.OOPK.geoIntersection.numShapes = numValidShapes
	qc.OOPK.geoIntersection.totalNumPoints = totalNumPoints
	return
}

// prepare foreign table (allocate and transfer memory) before processing
func (qc *AQLQueryContext) prepareForeignTable(memStore memstore.MemStore, joinTableID int, join Join) {
	ft := qc.OOPK.foreignTables[joinTableID]
	if ft == nil {
		return
	}

	// join only support dimension table for now
	// and dimension table is not shared
	shard, err := memStore.GetTableShard(join.Table, 0)
	if err != nil {
		qc.Error = utils.StackError(err, "Failed to get shard for table %s, shard: %d", join.Table, 0)
		return
	}
	defer shard.Users.Done()

	// only need live store for dimension table
	batchIDs, numRecordsInLastBatch := shard.LiveStore.GetBatchIDs()
	ft.numRecordsInLastBatch = numRecordsInLastBatch
	deviceBatches := make([][]deviceVectorPartySlice, len(batchIDs))

	// transfer primary key
	hostPrimaryKeyData := shard.LiveStore.PrimaryKey.LockForTransfer()
	devicePrimaryKeyPtr := deviceAllocate(hostPrimaryKeyData.NumBytes, qc.Device)
	memutils.AsyncCopyHostToDevice(devicePrimaryKeyPtr.getPointer(), hostPrimaryKeyData.Data, hostPrimaryKeyData.NumBytes, qc.cudaStreams[0], qc.Device)
	memutils.WaitForCudaStream(qc.cudaStreams[0], qc.Device)
	ft.hostPrimaryKeyData = hostPrimaryKeyData
	ft.devicePrimaryKeyPtr = devicePrimaryKeyPtr
	shard.LiveStore.PrimaryKey.UnlockAfterTransfer()

	// allocate device memory
	for i, batchID := range batchIDs {
		batch := shard.LiveStore.GetBatchForRead(batchID)
		if batch == nil {
			continue
		}
		batchIndex := batchID - memstore.BaseBatchID
		deviceBatches[batchIndex] = make([]deviceVectorPartySlice, len(qc.TableScanners[joinTableID+1].Columns))

		size := batch.Capacity
		if i == len(batchIDs)-1 {
			size = numRecordsInLastBatch
		}
		for i, columnID := range qc.TableScanners[joinTableID+1].Columns {
			usage := qc.TableScanners[joinTableID+1].ColumnUsages[columnID]
			if usage&(columnUsedByAllBatches|columnUsedByLiveBatches) != 0 {
				sourceVP := batch.Columns[columnID]
				if sourceVP == nil {
					continue
				}

				hostVPSlice := sourceVP.(memstore.TransferableVectorParty).GetHostVectorPartySlice(0, size)
				deviceBatches[batchIndex][i] = hostToDeviceColumn(hostVPSlice, qc.Device)
				copyHostToDevice(hostVPSlice, deviceBatches[batchIndex][i], qc.cudaStreams[0], qc.Device)
			}
		}
		memutils.WaitForCudaStream(qc.cudaStreams[0], qc.Device)
		batch.RUnlock()
	}
	ft.batches = deviceBatches
}

// prepareTimezoneTable
func (qc *AQLQueryContext) prepareTimezoneTable(store memstore.MemStore) {
	if qc.timezoneTable.tableColumn == "" {
		return
	}

	// Timezone table
	timezoneTableName := utils.GetConfig().Query.TimezoneTable.TableName
	schema, err := store.GetSchema(timezoneTableName)
	if err != nil {
		qc.Error = err
		return
	}
	if schema == nil {
		qc.Error = utils.StackError(nil, "unknown timezone table %s", timezoneTableName)
		return
	}

	timer := utils.GetRootReporter().GetTimer(utils.TimezoneLookupTableCreationTime)
	start := utils.Now()
	defer func() {
		duration := utils.Now().Sub(start)
		timer.Record(duration)
	}()

	schema.RLock()
	defer schema.RUnlock()

	if tzDict, found := schema.EnumDicts[qc.timezoneTable.tableColumn]; found {
		lookUp := make([]int16, len(tzDict.ReverseDict))
		for i := range lookUp {
			if loc, err := time.LoadLocation(tzDict.ReverseDict[i]); err == nil {
				_, offset := time.Now().In(loc).Zone()
				lookUp[i] = int16(offset)
			} else {
				qc.Error = utils.StackError(err, "error parsing timezone")
				return
			}
		}
		sizeInBytes := binary.Size(lookUp)
		lookupPtr := deviceAllocate(sizeInBytes, qc.Device)
		memutils.AsyncCopyHostToDevice(lookupPtr.getPointer(), unsafe.Pointer(&lookUp[0]), sizeInBytes, qc.cudaStreams[0], qc.Device)
		qc.OOPK.currentBatch.timezoneLookupD = lookupPtr
		qc.OOPK.currentBatch.timezoneLookupDSize = len(lookUp)
	} else {
		qc.Error = utils.StackError(nil, "unknown timezone column %s", qc.timezoneTable.tableColumn)
		return
	}

}

// transferLiveBatch returns a functor to transfer a live batch to device memory. The size parameter will be either the
// size of the batch or num records in last batch. hostColumns will always be empty since we should not release a vector
// party of a live batch. Start row will always be zero as well.
func (qc *AQLQueryContext) transferLiveBatch(batch *memstore.LiveBatch, size int) batchTransferExecutor {
	return func(stream unsafe.Pointer) (deviceColumns []deviceVectorPartySlice, hostVPs []memCom.VectorParty,
		firstColumn, startRow, totalBytes, numTransfers, sizeAfterPrefilter int) {
		// Allocate column inputs.
		firstColumn = -1
		deviceColumns = make([]deviceVectorPartySlice, len(qc.TableScanners[0].Columns))
		for i, columnID := range qc.TableScanners[0].Columns {
			usage := qc.TableScanners[0].ColumnUsages[columnID]
			if usage&(columnUsedByAllBatches|columnUsedByLiveBatches) != 0 {
				if firstColumn < 0 {
					firstColumn = i
				}
				sourceVP := batch.Columns[columnID]
				if sourceVP == nil {
					continue
				}

				hostColumn := sourceVP.(memstore.TransferableVectorParty).GetHostVectorPartySlice(0, size)
				deviceColumns[i] = hostToDeviceColumn(hostColumn, qc.Device)
				b, t := copyHostToDevice(hostColumn, deviceColumns[i], stream, qc.Device)
				totalBytes += b
				numTransfers += t
			}
		}
		sizeAfterPrefilter = size
		return
	}
}

// liveBatchTimeFilterExecutor returns a functor to apply custom time filters to live batch.
func (qc *AQLQueryContext) liveBatchCustomFilterExecutor(cutoff uint32) customFilterExecutor {
	return func(stream unsafe.Pointer) {
		// cutoff filter evaluation.
		// only apply to fact table where cutoff > 0
		if cutoff > 0 {
			qc.OOPK.currentBatch.processExpression(
				qc.createCutoffTimeFilter(cutoff), nil,
				qc.TableScanners, qc.OOPK.foreignTables, stream, qc.Device, qc.OOPK.currentBatch.filterAction)
		}

		// time filter evaluation
		for _, filter := range qc.OOPK.TimeFilters {
			if filter != nil {
				qc.OOPK.currentBatch.processExpression(filter, nil,
					qc.TableScanners, qc.OOPK.foreignTables, stream, qc.Device, qc.OOPK.currentBatch.filterAction)
			}
		}

		// prefilter evaluation
		for _, filter := range qc.OOPK.Prefilters {
			qc.OOPK.currentBatch.processExpression(filter, nil,
				qc.TableScanners, qc.OOPK.foreignTables, stream, qc.Device, qc.OOPK.currentBatch.filterAction)
		}
	}
}

// transferArchiveBatch returns the functor to transfer an archive batch to device memory. We will need to release
// hostColumns after transfer completes.
func (qc *AQLQueryContext) transferArchiveBatch(batch *memstore.ArchiveBatch,
	isFirstOrLast bool) batchTransferExecutor {
	return func(stream unsafe.Pointer) (deviceSlices []deviceVectorPartySlice, hostVPs []memCom.VectorParty,
		firstColumn, startRow, totalBytes, numTransfers, sizeAfterPreFilter int) {
		matchedColumnUsages := columnUsedByAllBatches
		if isFirstOrLast {
			matchedColumnUsages |= columnUsedByFirstArchiveBatch | columnUsedByLastArchiveBatch
		}

		// Request columns, prefilter-slicing, allocate column inputs.
		firstColumn = -1
		hostVPs = make([]memCom.VectorParty, len(qc.TableScanners[0].Columns))
		hostSlices := make([]memCom.HostVectorPartySlice, len(qc.TableScanners[0].Columns))
		deviceSlices = make([]deviceVectorPartySlice, len(qc.TableScanners[0].Columns))
		endRow := batch.Size
		prefilterIndex := 0
		// Must iterate in reverse order to apply prefilter slicing properly.
		for i := len(qc.TableScanners[0].Columns) - 1; i >= 0; i-- {
			columnID := qc.TableScanners[0].Columns[i]
			usage := qc.TableScanners[0].ColumnUsages[columnID]

			if usage&matchedColumnUsages != 0 || usage&columnUsedByPrefilter != 0 {
				// Request/pin column from disk and wait.
				vp := batch.RequestVectorParty(columnID)
				vp.WaitForDiskLoad()

				// prefilter slicing
				startRow, endRow, hostSlices[i] = qc.prefilterSlice(vp, prefilterIndex, startRow, endRow)
				prefilterIndex++

				if usage&matchedColumnUsages != 0 {
					hostVPs[i] = vp
					firstColumn = i
					deviceSlices[i] = hostToDeviceColumn(hostSlices[i], qc.Device)
				} else {
					vp.Release()
				}
			}
		}

		for i, dstVPSlice := range deviceSlices {
			columnID := qc.TableScanners[0].Columns[i]
			usage := qc.TableScanners[0].ColumnUsages[columnID]
			if usage&matchedColumnUsages != 0 {
				srcVPSlice := hostSlices[i]
				b, t := copyHostToDevice(srcVPSlice, dstVPSlice, stream, qc.Device)
				totalBytes += b
				numTransfers += t
			}
		}
		sizeAfterPreFilter = endRow - startRow
		return
	}
}

// archiveBatchCustomFilterExecutor returns a functor to apply custom filter to first or last archive batch.
func (qc *AQLQueryContext) archiveBatchCustomFilterExecutor(isFirstOrLast bool) customFilterExecutor {
	return func(stream unsafe.Pointer) {
		if isFirstOrLast {
			for _, filter := range qc.OOPK.TimeFilters {
				if filter != nil {
					qc.OOPK.currentBatch.processExpression(filter, nil,
						qc.TableScanners, qc.OOPK.foreignTables, stream, qc.Device, qc.OOPK.currentBatch.filterAction)
				}
			}
		}
	}
}

// helper function for copy dimension vector. Returns the total size of dimension vector.
func asyncCopyDimensionVector(toDimVector, fromDimVector unsafe.Pointer, length, offset int, numDimsPerDimWidth queryCom.DimCountsPerDimWidth,
	toVectorCapacity, fromVectorCapacity int, copyFunc memutils.AsyncMemCopyFunc,
	stream unsafe.Pointer, device int) {

	ptrFrom, ptrTo := fromDimVector, toDimVector
	numNullVectors := 0
	for _, numDims := range numDimsPerDimWidth {
		numNullVectors += int(numDims)
	}

	dimBytes := 1 << uint(len(numDimsPerDimWidth)-1)
	bytesToCopy := length * dimBytes
	for _, numDim := range numDimsPerDimWidth {
		for i := 0; i < int(numDim); i++ {
			ptrTemp := utils.MemAccess(ptrTo, dimBytes*offset)
			copyFunc(ptrTemp, ptrFrom, bytesToCopy, stream, device)
			ptrTo = utils.MemAccess(ptrTo, dimBytes*toVectorCapacity)
			ptrFrom = utils.MemAccess(ptrFrom, dimBytes*fromVectorCapacity)
		}
		dimBytes >>= 1
		bytesToCopy = length * dimBytes
	}

	// copy null bytes
	for i := 0; i < numNullVectors; i++ {
		ptrTemp := utils.MemAccess(ptrTo, offset)
		copyFunc(ptrTemp, ptrFrom, length, stream, device)
		ptrTo = utils.MemAccess(ptrTo, toVectorCapacity)
		ptrFrom = utils.MemAccess(ptrFrom, fromVectorCapacity)
	}
}

// dimValueVectorSize returns the size of final dim value vector on host side.
func dimValResVectorSize(resultSize int, numDimsPerDimWidth queryCom.DimCountsPerDimWidth) int {
	totalDims := 0
	for _, numDims := range numDimsPerDimWidth {
		totalDims += int(numDims)
	}

	dimBytes := 1 << uint(len(numDimsPerDimWidth)-1)
	var totalBytes int
	for _, numDims := range numDimsPerDimWidth {
		totalBytes += dimBytes * resultSize * int(numDims)
		dimBytes >>= 1
	}

	totalBytes += totalDims * resultSize
	return totalBytes
}

// cleanupDeviceResultBuffers cleans up result buffers and resets result fields.
func (bc *oopkBatchContext) cleanupDeviceResultBuffers() {
	deviceFreeAndSetNil(&bc.dimensionVectorD[0])
	deviceFreeAndSetNil(&bc.dimensionVectorD[1])

	deviceFreeAndSetNil(&bc.dimIndexVectorD[0])
	deviceFreeAndSetNil(&bc.dimIndexVectorD[1])

	deviceFreeAndSetNil(&bc.hashVectorD[0])
	deviceFreeAndSetNil(&bc.hashVectorD[1])

	deviceFreeAndSetNil(&bc.measureVectorD[0])
	deviceFreeAndSetNil(&bc.measureVectorD[1])

	bc.size = 0
	bc.resultSize = 0
	bc.resultCapacity = 0
}

// clean up memory not used in final aggregation (sort, reduce, hll)
// before aggregation happen
func (bc *oopkBatchContext) cleanupBeforeAggregation() {
	for _, column := range bc.columns {
		deviceFreeAndSetNil(&column.basePtr)
	}
	bc.columns = nil

	deviceFreeAndSetNil(&bc.indexVectorD)
	deviceFreeAndSetNil(&bc.predicateVectorD)
	deviceFreeAndSetNil(&bc.geoPredicateVectorD)

	for _, recordIDsVector := range bc.foreignTableRecordIDsD {
		deviceFreeAndSetNil(&recordIDsVector)
	}
	bc.foreignTableRecordIDsD = nil

	for _, stackFrame := range bc.exprStackD {
		deviceFreeAndSetNil(&stackFrame[0])
	}
	bc.exprStackD = nil
}

// swapResultBufferForNextBatch swaps the two
// sets of dim/measure/hash vectors to get ready for the next batch.
func (bc *oopkBatchContext) swapResultBufferForNextBatch() {
	bc.size = 0
	bc.dimensionVectorD[0], bc.dimensionVectorD[1] = bc.dimensionVectorD[1], bc.dimensionVectorD[0]
	bc.measureVectorD[0], bc.measureVectorD[1] = bc.measureVectorD[1], bc.measureVectorD[0]
	bc.hashVectorD[0], bc.hashVectorD[1] = bc.hashVectorD[1], bc.hashVectorD[0]
}

// prepareForFiltering prepares the input and the index vectors for filtering.
func (bc *oopkBatchContext) prepareForFiltering(
	columns []deviceVectorPartySlice, firstColumn int, startRow int, stream unsafe.Pointer) {
	bc.columns = columns
	bc.startRow = startRow

	if firstColumn >= 0 {
		bc.size = columns[firstColumn].length
		// Allocate twice of the size to save number of allocations of temporary index vector.
		bc.indexVectorD = deviceAllocate(bc.size*4, bc.device)
		bc.predicateVectorD = deviceAllocate(bc.size, bc.device)
		bc.baseCountD = columns[firstColumn].counts.offset(columns[firstColumn].countStartIndex * 4)
	}
	bc.stats.batchSize = bc.size
}

// prepareForDimAndMeasureEval ensures that dim/measure vectors have enough
// capacity for bc.resultSize+bc.size.
func (bc *oopkBatchContext) prepareForDimAndMeasureEval(
	dimRowBytes int, measureBytes int, numDimsPerDimWidth queryCom.DimCountsPerDimWidth, isHLL bool, stream unsafe.Pointer) {
	if bc.resultSize+bc.size > bc.resultCapacity {
		oldCapacity := bc.resultCapacity

		bc.resultCapacity = bc.resultSize + bc.size
		// Extra budget for future proofing.
		bc.resultCapacity += bc.resultCapacity / 8

		bc.dimensionVectorD = bc.reallocateResultBuffers(bc.dimensionVectorD, dimRowBytes, stream, func(to, from unsafe.Pointer) {
			asyncCopyDimensionVector(to, from, bc.resultSize, 0,
				numDimsPerDimWidth, bc.resultCapacity, oldCapacity,
				memutils.AsyncCopyDeviceToDevice, stream, bc.device)
		})

		// uint32_t for index value
		bc.dimIndexVectorD = bc.reallocateResultBuffers(bc.dimIndexVectorD, 4, stream, nil)
		// uint64_t for hash value
		// Note: only when aggregate function is hll, we need to reuse vector[0]
		if isHLL {
			bc.hashVectorD = bc.reallocateResultBuffers(bc.hashVectorD, 8, stream, func(to, from unsafe.Pointer) {
				memutils.AsyncCopyDeviceToDevice(to, from, bc.resultSize*8, stream, bc.device)
			})
		} else {
			bc.hashVectorD = bc.reallocateResultBuffers(bc.hashVectorD, 8, stream, nil)
		}

		bc.measureVectorD = bc.reallocateResultBuffers(bc.measureVectorD, measureBytes, stream, func(to, from unsafe.Pointer) {
			memutils.AsyncCopyDeviceToDevice(to, from, bc.resultSize*measureBytes, stream, bc.device)
		})
	}
}

// reallocateResultBuffers reallocates the result buffer pair to size
// resultCapacity*unitBytes and copies resultSize*unitBytes from input[0] to output[0].
func (bc *oopkBatchContext) reallocateResultBuffers(
	input [2]devicePointer, unitBytes int, stream unsafe.Pointer, copyFunc func(to, from unsafe.Pointer)) (output [2]devicePointer) {

	output = [2]devicePointer{
		deviceAllocate(bc.resultCapacity*unitBytes, bc.device),
		deviceAllocate(bc.resultCapacity*unitBytes, bc.device),
	}

	if copyFunc != nil {
		copyFunc(output[0].getPointer(), input[0].getPointer())
	}

	deviceFreeAndSetNil(&input[0])
	deviceFreeAndSetNil(&input[1])
	return
}

// doProfile checks the corresponding profileName against query parameter
// and do cuda profiling for this action if name matches.
func (qc *AQLQueryContext) doProfile(action func(), profileName string, stream unsafe.Pointer) {
	if qc.Profiling == profileName {
		// explicit waiting for cuda stream to avoid profiling previous actions.
		memutils.WaitForCudaStream(stream, qc.Device)
		utils.GetQueryLogger().Infof("Starting cuda profiler for %s", profileName)
		memutils.CudaProfilerStart()
		defer func() {
			// explicit waiting for cuda stream to wait for completion of current action.
			memutils.WaitForCudaStream(stream, qc.Device)
			utils.GetQueryLogger().Infof("Stopping cuda profiler for %s", profileName)
			memutils.CudaProfilerStop()
		}()
	}
	action()
}

// processBatch allocates device memory and starts async input data
// transferring to device memory. It then invokes previousBatchExecutor
// asynchronously to process the previous batch. When both async operations
// finish, it prepares for the current batch execution and returns it as
// a function closure to be invoked later. customFilterExecutor is the executor
// to apply custom filters for live batch and archive batch.
func (qc *AQLQueryContext) processBatch(
	batch *memstore.Batch, batchID int32, batchSize int, transferFunc batchTransferExecutor,
	customFilterFunc customFilterExecutor, previousBatchExecutor BatchExecutor, needToUnlockBatch bool) BatchExecutor {
	defer func() {
		if needToUnlockBatch {
			batch.RUnlock()
		}
	}()

	if qc.Debug {
		// Finish executing previous batch first to avoid timeline overlapping
		qc.runBatchExecutor(previousBatchExecutor, false)
		previousBatchExecutor = NewDummyBatchExecutor()
	}

	// reset stats.
	qc.OOPK.currentBatch.stats = oopkBatchStats{
		batchID: batchID,
		timings: make(map[stageName]float64),
	}
	start := utils.Now()

	// Async transfer.
	stream := qc.cudaStreams[0]
	deviceSlices, hostVPs, firstColumn, startRow, totalBytes, numTransfers, sizeAfterPreFilter := transferFunc(stream)
	qc.OOPK.currentBatch.stats.bytesTransferred += totalBytes
	qc.OOPK.currentBatch.stats.numTransferCalls += numTransfers

	qc.reportTimingForCurrentBatch(stream, &start, transferTiming)

	// Async execute the previous batch.
	executionDone := make(chan struct{ error }, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				var err error
				// find out exactly what the error was and set err
				switch x := r.(type) {
				case string:
					err = utils.StackError(nil, x)
				case error:
					err = utils.StackError(x, "Panic happens when executing query")
				default:
					err = utils.StackError(nil, "Panic happens when executing query %v", x)
				}
				executionDone <- struct{ error }{err}
			}
		}()
		qc.runBatchExecutor(previousBatchExecutor, false)
		executionDone <- struct{ error }{}
	}()

	// Wait for data transfer of the current batch.
	memutils.WaitForCudaStream(stream, qc.Device)

	for _, vp := range hostVPs {
		if vp != nil {
			// only archive vector party will be returned after transfer function
			vp.(memCom.ArchiveVectorParty).Release()
		}
	}

	if needToUnlockBatch {
		batch.RUnlock()
		needToUnlockBatch = false
	}

	// Wait for execution of the previous batch.
	res := <-executionDone
	if res.error != nil {
		// column data transfer for current batch is done
		// need release current batch's column data before panic
		for _, column := range deviceSlices {
			deviceFreeAndSetNil(&column.basePtr)
		}
		panic(res.error)
	}

	if qc.OOPK.done {
		// if the query is already satisfied in the middle, we can skip next batch and return
		for _, column := range deviceSlices {
			deviceFreeAndSetNil(&column.basePtr)
		}
		return NewDummyBatchExecutor()
	}

	// no prefilter slicing in livebatch, startRow is always 0
	qc.OOPK.currentBatch.size = batchSize
	qc.OOPK.currentBatch.sizeAfterPreFilter = sizeAfterPreFilter
	qc.OOPK.currentBatch.prepareForFiltering(deviceSlices, firstColumn, startRow, stream)

	qc.reportTimingForCurrentBatch(stream, &start, prepareForFilteringTiming)

	return NewBatchExecutor(qc, batchID, customFilterFunc, stream, start)
}

// prefilterSlice does the following:
// 1. binary search for prefilter values following the matched sort column order
// 2. record matched index range on these matched sort columns
// 3. binary search on unmatched compressed columns for the row number range
// 4. index slice on uncompressed columns for the row number range
// 5. align/pad all slices to be pushed
func (qc *AQLQueryContext) prefilterSlice(vp memCom.ArchiveVectorParty, prefilterIndex, startRow, endRow int) (int, int, memCom.HostVectorPartySlice) {
	startIndex, endIndex := 0, vp.GetLength()

	unmatchedColumn := false
	scanner := qc.TableScanners[0]
	if prefilterIndex < len(scanner.EqualityPrefilterValues) {
		// matched equality filter
		filterValue := scanner.EqualityPrefilterValues[prefilterIndex]
		startRow, endRow, startIndex, endIndex = vp.SliceByValue(startRow, endRow, unsafe.Pointer(&filterValue))
	} else if prefilterIndex == len(scanner.EqualityPrefilterValues) {
		// matched range filter
		// lower bound
		filterValue := scanner.RangePrefilterValues[0]
		boundaryType := scanner.RangePrefilterBoundaries[0]

		if boundaryType != noBoundary {
			lowerStartRow, lowerEndRow, lowerStartIndex, lowerEndIndex := vp.SliceByValue(startRow, endRow, unsafe.Pointer(&filterValue))
			if boundaryType == inclusiveBoundary {
				startRow, startIndex = lowerStartRow, lowerStartIndex
			} else {
				startRow, startIndex = lowerEndRow, lowerEndIndex
			}
		} else {
			// treat as unmatchedColumn when there is one range filter missing
			unmatchedColumn = true
		}

		// SliceByValue of upperBound
		filterValue = scanner.RangePrefilterValues[1]
		boundaryType = scanner.RangePrefilterBoundaries[1]
		if boundaryType != noBoundary {
			upperStartRow, upperEndRow, upperStartIndex, upperEndIndex := vp.SliceByValue(startRow, endRow, unsafe.Pointer(&filterValue))
			if boundaryType == inclusiveBoundary {
				endRow, endIndex = upperEndRow, upperEndIndex
			} else {
				endRow, endIndex = upperStartRow, upperStartIndex
			}
		} else {
			// treat as unmatchedColumn when there is one range filter missing
			unmatchedColumn = true
		}
	} else {
		unmatchedColumn = true
	}

	if unmatchedColumn {
		// unmatched columns, simply slice based on row number range
		startIndex, endIndex = vp.SliceIndex(startRow, endRow)
	}

	return startRow, endRow, vp.(memstore.TransferableVectorParty).GetHostVectorPartySlice(startIndex, endIndex-startIndex)
}

// calculateMemoryRequirement estimate memory requirement for batch data.
func (qc *AQLQueryContext) calculateMemoryRequirement(memStore memstore.MemStore) int {
	// keep track of max requirement for batch
	maxBytesRequired := 0

	//TODO(jians): hard code hll query memory requirement here for now,
	//we can track memory usage
	//based on table, dimensions, duration to do estimation
	if qc.OOPK.IsHLL() {
		return hllQueryRequiredMemoryInMB
	}

	for _, shardID := range qc.TableScanners[0].Shards {
		shard, err := memStore.GetTableShard(qc.Query.Table, shardID)
		if err != nil {
			qc.Error = utils.StackError(err, "failed to get shard %d for table %s",
				shardID, qc.Query.Table)
			return -1
		}

		var archiveStore *memstore.ArchiveStoreVersion
		var cutoff uint32
		if shard.Schema.Schema.IsFactTable {
			archiveStore = shard.ArchiveStore.GetCurrentVersion()
			cutoff = archiveStore.ArchivingCutoff
		}

		// estimate live batch memory usage
		if qc.toTime == nil || cutoff < uint32(qc.toTime.Time.Unix()) {
			batchIDs, _ := shard.LiveStore.GetBatchIDs()

			// find first non null batch and estimate.
			for _, batchID := range batchIDs {
				liveBatch := shard.LiveStore.GetBatchForRead(batchID)
				if liveBatch != nil {
					batchBytes := qc.estimateLiveBatchMemoryUsage(liveBatch)
					liveBatch.RUnlock()

					if batchBytes > maxBytesRequired {
						maxBytesRequired = batchBytes
					}
					break
				}
			}
		}

		// estimate archive batch memory usage
		if archiveStore != nil {
			if qc.fromTime == nil || cutoff > uint32(qc.fromTime.Time.Unix()) {
				scanner := qc.TableScanners[0]
				for batchID := scanner.ArchiveBatchIDStart; batchID < scanner.ArchiveBatchIDEnd; batchID++ {
					archiveBatch := archiveStore.RequestBatch(int32(batchID))
					if archiveBatch == nil || archiveBatch.Size == 0 {
						continue
					}
					isFirstOrLast := batchID == scanner.ArchiveBatchIDStart || batchID == scanner.ArchiveBatchIDEnd-1
					batchBytes := qc.estimateArchiveBatchMemoryUsage(archiveBatch, isFirstOrLast)
					if batchBytes > maxBytesRequired {
						maxBytesRequired = batchBytes
					}
				}
			}
			archiveStore.Users.Done()
		}
		shard.Users.Done()
	}

	maxBytesRequired += qc.calculateForeignTableMemUsage(memStore)
	return maxBytesRequired
}

// estimateLiveBatchMemoryUsage estimate the GPU memory usage for live batches
func (qc *AQLQueryContext) estimateLiveBatchMemoryUsage(batch *memstore.LiveBatch) int {
	columnMemUsage := 0
	for _, columnID := range qc.TableScanners[0].Columns {
		sourceVP := batch.Columns[columnID]
		if sourceVP == nil {
			continue
		}
		columnMemUsage += int(sourceVP.GetBytes())
	}
	if batch.Capacity > qc.maxBatchSizeAfterPrefilter {
		qc.maxBatchSizeAfterPrefilter = batch.Capacity
	}
	totalBytes := qc.estimateMemUsageForBatch(batch.Capacity, columnMemUsage, batch.Capacity)
	utils.GetQueryLogger().Debugf("Live batch %+v needs memory: %d", batch, totalBytes)
	return totalBytes
}

// estimateArchiveBatchMemoryUsage estimate the GPU memory usage for archive batch
func (qc *AQLQueryContext) estimateArchiveBatchMemoryUsage(batch *memstore.ArchiveBatch, isFirstOrLast bool) int {
	if batch == nil {
		return 0
	}

	columnMemUsage := 0
	var firstColumnSize int
	startRow, endRow := 0, batch.Size
	var hostSlice memCom.HostVectorPartySlice

	matchedColumnUsages := columnUsedByAllBatches
	if isFirstOrLast {
		matchedColumnUsages |= columnUsedByFirstArchiveBatch | columnUsedByLastArchiveBatch
	}

	prefilterIndex := 0
	// max number of rows after pre-filtering. used for non-agg query
	maxSizeAfterPreFilter := batch.Size
	for i := len(qc.TableScanners[0].Columns) - 1; i >= 0; i-- {
		columnID := qc.TableScanners[0].Columns[i]
		usage := qc.TableScanners[0].ColumnUsages[columnID]
		// TODO(cdavid): only read metadata when estimate query memory requirement.
		sourceVP := batch.RequestVectorParty(columnID)
		sourceVP.WaitForDiskLoad()

		if usage&matchedColumnUsages != 0 || usage&columnUsedByPrefilter != 0 {
			startRow, endRow, hostSlice = qc.prefilterSlice(sourceVP, prefilterIndex, startRow, endRow)
			if endRow-startRow < maxSizeAfterPreFilter {
				maxSizeAfterPreFilter = endRow - startRow
			}
			prefilterIndex++
			if usage&matchedColumnUsages != 0 {
				columnMemUsage += hostSlice.ValueBytes + hostSlice.NullBytes + hostSlice.CountBytes
				firstColumnSize = hostSlice.Length
			}
		}
		sourceVP.Release()
	}
	if maxSizeAfterPreFilter > qc.maxBatchSizeAfterPrefilter {
		qc.maxBatchSizeAfterPrefilter = maxSizeAfterPreFilter
	}

	totalBytes := qc.estimateMemUsageForBatch(firstColumnSize, columnMemUsage, maxSizeAfterPreFilter)
	utils.GetQueryLogger().Debugf("Archive batch %d needs memory: %d", batch.BatchID, totalBytes)
	return totalBytes
}

// estimateMemUsageForBatch calculates memory usage including:
// * Index vector
// * Predicate vector
// * Dimension
// * Measurement
// * Sort (hash/index)
// * Reduce
func (qc *AQLQueryContext) estimateMemUsageForBatch(firstColumnSize, columnMemUsage, maxSizeAfterPreFilter int) (memUsage int) {
	// 1. columnMemUsage
	memUsageBeforeAgg := columnMemUsage

	// 2. index vector memory usage (4 bytes each)
	memUsageBeforeAgg += firstColumnSize * 4

	// 3. predicate memory usage (1 byte each)
	memUsageBeforeAgg += firstColumnSize

	// 4. record id vector for foreign table (8 bytes each recordID)
	memUsageBeforeAgg += firstColumnSize * 8 * len(qc.OOPK.foreignTables)

	// 5. expression eval memory (max scratch space)
	memUsageBeforeAgg += qc.estimateExpressionEvaluationMemUsage(firstColumnSize)

	// 6. geoPredicateVector
	if qc.OOPK.geoIntersection != nil {
		memUsageBeforeAgg += firstColumnSize * 4 * 2
	}

	// 7. max(memUsageBeforeAgg, sortReduceMemoryUsage)
	memUsage = memUsageBeforeAgg
	if !qc.IsNonAggregationQuery {
		memUsage = int(math.Max(float64(memUsage), float64(estimateSortReduceMemUsage(firstColumnSize))))
	}

	// 8. Dimension vector memory usage (input + output)
	if qc.IsNonAggregationQuery {
		maxRowsPerBatch := maxSizeAfterPreFilter
		if qc.Query.Limit < maxRowsPerBatch {
			maxRowsPerBatch = qc.Query.Limit
		}
		memUsage += maxRowsPerBatch * qc.OOPK.DimRowBytes * 2
	} else {
		memUsage += firstColumnSize * qc.OOPK.DimRowBytes * 2
	}

	// 9. Measure vector memory usage (input + output)
	memUsage += firstColumnSize * qc.OOPK.MeasureBytes * 2

	return
}

// memory usage duration expression (filter, dimension, measure) evaluation
func (qc *AQLQueryContext) estimateExpressionEvaluationMemUsage(inputSize int) (memUsage int) {
	// filter expression evaluation
	for _, filter := range qc.OOPK.MainTableCommonFilters {
		_, maxExpMemUsage := estimateScratchSpaceMemUsage(filter, inputSize, true)
		utils.GetQueryLogger().Debugf("Filter %+v: maxExpMemUsage=%d", filter, maxExpMemUsage)
		memUsage = int(math.Max(float64(memUsage), float64(maxExpMemUsage)))
	}

	for _, filter := range qc.OOPK.ForeignTableCommonFilters {
		_, maxExpMemUsage := estimateScratchSpaceMemUsage(filter, inputSize, true)
		utils.GetQueryLogger().Debugf("Filter %+v: maxExpMemUsage=%d", filter, maxExpMemUsage)
		memUsage = int(math.Max(float64(memUsage), float64(maxExpMemUsage)))
	}

	// dimension expression evaluation
	for _, dimension := range qc.OOPK.Dimensions {
		_, maxExpMemUsage := estimateScratchSpaceMemUsage(dimension, inputSize, true)
		utils.GetQueryLogger().Debugf("Dimension %+v: maxExpMemUsage=%d", dimension, maxExpMemUsage)
		memUsage = int(math.Max(float64(memUsage), float64(maxExpMemUsage)))
	}

	// measure expression evaluation
	_, maxExpMemUsage := estimateScratchSpaceMemUsage(qc.OOPK.Measure, inputSize, true)
	utils.GetQueryLogger().Debugf("Measure %+v: maxExpMemUsage=%d", qc.OOPK.Measure, maxExpMemUsage)
	memUsage = int(math.Max(float64(memUsage), float64(maxExpMemUsage)))

	return memUsage
}

// Note: we only calculate Sort memory usage
// since sort memory usage is larger than reduce
// and we only care about the maximum
func estimateSortReduceMemUsage(inputSize int) (memUsage int) {
	// dimension index vector
	// 4 byte for uint32
	// 2 vectors for input and output
	memUsage += inputSize * 4 * 2
	// hash vector
	// 8 byte for uint64 hash value
	// 2 vectors for input and output
	memUsage += inputSize * 8 * 2
	// we sort dim index values as value, and hash value as key
	memUsage += inputSize * (8 + 4)
	return
}

// estimateScratchSpaceMemUsage calculates memory usage for an expression
func estimateScratchSpaceMemUsage(exp expr.Expr, firstColumnSize int, isRoot bool) (int, int) {
	var currentMemUsage int
	var maxMemUsage int

	switch e := exp.(type) {
	case *expr.ParenExpr:
		return estimateScratchSpaceMemUsage(e.Expr, firstColumnSize, isRoot)
	case *expr.UnaryExpr:
		childCurrentMemUsage, childMaxMemUsage := estimateScratchSpaceMemUsage(e.Expr, firstColumnSize, false)
		if !isRoot {
			currentMemUsage = firstColumnSize * 5
		}
		maxMemUsage = int(math.Max(float64(childCurrentMemUsage+currentMemUsage), float64(childMaxMemUsage)))
		return currentMemUsage, maxMemUsage
	case *expr.BinaryExpr:
		lhsCurrentMemUsage, lhsMaxMemUsage := estimateScratchSpaceMemUsage(e.LHS, firstColumnSize, false)
		rhsCurrentMemUsage, rhsMaxMemUsage := estimateScratchSpaceMemUsage(e.RHS, firstColumnSize, false)

		if !isRoot {
			currentMemUsage = firstColumnSize * 5
		}

		childrenMaxMemUsage := math.Max(float64(lhsMaxMemUsage), float64(rhsMaxMemUsage))
		maxMemUsage = int(math.Max(float64(currentMemUsage+lhsCurrentMemUsage+rhsCurrentMemUsage), float64(childrenMaxMemUsage)))

		return currentMemUsage, maxMemUsage
	default:
		return 0, 0
	}
}

// calculateForeignTableMemUsage returns how much device memory is needed for foreign table
func (qc *AQLQueryContext) calculateForeignTableMemUsage(memStore memstore.MemStore) int {
	var memUsage int

	for joinTableID, join := range qc.Query.Joins {
		// join only support dimension table for now
		// and dimension table is not shared
		shard, err := memStore.GetTableShard(join.Table, 0)
		if err != nil {
			qc.Error = utils.StackError(err, "Failed to get shard for table %s, shard: %d", join.Table, 0)
			return 0
		}

		// only need live store for dimension table
		batchIDs, _ := shard.LiveStore.GetBatchIDs()

		// primary key
		memUsage += int(shard.LiveStore.PrimaryKey.AllocatedBytes())

		// VPs
		for _, batchID := range batchIDs {
			batch := shard.LiveStore.GetBatchForRead(batchID)
			if batch == nil {
				continue
			}

			for _, columnID := range qc.TableScanners[joinTableID+1].Columns {
				usage := qc.TableScanners[joinTableID+1].ColumnUsages[columnID]
				if usage&(columnUsedByAllBatches|columnUsedByLiveBatches) != 0 {
					sourceVP := batch.Columns[columnID]
					if sourceVP == nil {
						continue
					}
					memUsage += int(sourceVP.GetBytes())
				}
			}
			batch.RUnlock()
		}
		shard.Users.Done()
	}

	return memUsage
}

// FindDeviceForQuery calls device manager to find a device for the query
func (qc *AQLQueryContext) FindDeviceForQuery(memStore memstore.MemStore, preferredDevice int,
	deviceManager *DeviceManager, timeout int) {
	memoryRequired := qc.calculateMemoryRequirement(memStore)
	if qc.Error != nil {
		return
	}

	qc.OOPK.DeviceMemoryRequirement = memoryRequired

	waitStart := utils.Now()
	device := deviceManager.FindDevice(qc.Query, memoryRequired, preferredDevice, timeout)
	if device == -1 {
		qc.Error = utils.StackError(nil, "Unable to find device to run this query")
	}
	qc.OOPK.DurationWaitedForDevice = utils.Now().Sub(waitStart)
	qc.Device = device
}

func (qc *AQLQueryContext) runBatchExecutor(e BatchExecutor, isLastBatch bool) {
	start := utils.Now()
	e.preExec(isLastBatch, start)

	e.filter()

	e.join()

	e.project()

	e.reduce()

	e.postExec(start)
}

// copyHostToDevice copy vector party slice to device vector party slice
func copyHostToDevice(vps memCom.HostVectorPartySlice, deviceVPSlice deviceVectorPartySlice, stream unsafe.Pointer, device int) (bytesCopied, numTransfers int) {
	if vps.ValueBytes > 0 {
		memutils.AsyncCopyHostToDevice(
			deviceVPSlice.values.getPointer(), vps.Values, vps.ValueBytes,
			stream, device)
		bytesCopied += vps.ValueBytes
		numTransfers++
	}
	if vps.NullBytes > 0 {
		memutils.AsyncCopyHostToDevice(
			deviceVPSlice.nulls.getPointer(), vps.Nulls, vps.NullBytes,
			stream, device)
		bytesCopied += vps.NullBytes
		numTransfers++
	}
	if vps.CountBytes > 0 {
		memutils.AsyncCopyHostToDevice(
			deviceVPSlice.counts.getPointer(), vps.Counts, vps.CountBytes,
			stream, device)
		bytesCopied += vps.CountBytes
		numTransfers++
	}
	return
}

func hostToDeviceColumn(hostColumn memCom.HostVectorPartySlice, device int) deviceVectorPartySlice {
	deviceColumn := deviceVectorPartySlice{
		length:          hostColumn.Length,
		valueType:       hostColumn.ValueType,
		defaultValue:    hostColumn.DefaultValue,
		valueStartIndex: hostColumn.ValueStartIndex,
		nullStartIndex:  hostColumn.NullStartIndex,
		countStartIndex: hostColumn.CountStartIndex,
	}
	totalColumnBytes := hostColumn.ValueBytes + hostColumn.NullBytes + hostColumn.CountBytes

	if totalColumnBytes > 0 {
		deviceColumn.basePtr = deviceAllocate(totalColumnBytes, device)
		if hostColumn.Counts != nil {
			deviceColumn.counts = deviceColumn.basePtr.offset(0)
		}

		if hostColumn.Nulls != nil {
			deviceColumn.nulls = deviceColumn.basePtr.offset(hostColumn.CountBytes)
		}

		deviceColumn.values = deviceColumn.basePtr.offset(
			hostColumn.NullBytes + hostColumn.CountBytes)
	}
	return deviceColumn
}

// shouldSkipLiveBatch will determine whether we can skip processing a live batch by checking time filter and
// eligible main table common filters. The batch must be non nil.
func (qc *AQLQueryContext) shouldSkipLiveBatch(b *memstore.LiveBatch) bool {
	candidatesFilters := []expr.Expr{qc.OOPK.TimeFilters[0], qc.OOPK.TimeFilters[1]}
	candidatesFilters = append(candidatesFilters, qc.OOPK.MainTableCommonFilters...)
	for _, filter := range candidatesFilters {
		if shouldSkipLiveBatchWithFilter(b, filter) {
			return true
		}
	}
	return false
}

// shouldSkipLiveBatchWithFilter will check max and min for the corresponding column against the filter express and
// determines whether we should skip processing this live batch.
// Following constraints apply:
//  1. Filter must be on main table.
//  2. Filter must be a binary expression.
//  3. OPs must be one of (EQ, GTE,GE,LTE,LE).
//  4. One side of the expr must be VarRef
//  5. Another side of the xpr must be NumericalLiteral
//  6. ColumnType must be UInt32
func shouldSkipLiveBatchWithFilter(b *memstore.LiveBatch, filter expr.Expr) bool {
	if filter == nil {
		return false
	}

	if binExpr, ok := filter.(*expr.BinaryExpr); ok {
		var columnExpr *expr.VarRef
		var numExpr *expr.NumberLiteral
		op := binExpr.Op
		switch op {
		case expr.GTE, expr.GT, expr.LT, expr.LTE, expr.EQ:
		default:
			return false
		}
		// First try lhs VarRef, rhs Num.
		lhsVarRef, lhsOK := binExpr.LHS.(*expr.VarRef)
		rhsNum, rhsOK := binExpr.RHS.(*expr.NumberLiteral)
		if lhsOK && rhsOK {
			columnExpr = lhsVarRef
			numExpr = rhsNum
		} else {
			// Then try rhs VarRef, lhs Num.
			lhsNum, lhsOK := binExpr.LHS.(*expr.NumberLiteral)
			rhsVarRef, rhsOK := binExpr.RHS.(*expr.VarRef)
			if lhsOK && rhsOK {
				// Swap column to the left and number to right.
				columnExpr = rhsVarRef
				numExpr = lhsNum
				// Invert the OP.
				switch op {
				case expr.GTE:
					op = expr.LTE
				case expr.GT:
					op = expr.LT
				case expr.LTE:
					op = expr.GTE
				case expr.LT:
					op = expr.GT
				}
			}
		}

		if columnExpr != nil && numExpr != nil {
			// Time filters and main table filters are guaranteed to be on main table.
			vp := b.Columns[columnExpr.ColumnID]
			if vp == nil {
				return true
			}

			if columnExpr.DataType != memCom.Uint32 {
				return false
			}

			num := int64(numExpr.Int)
			minUint32, maxUint32 := vp.(memCom.LiveVectorParty).GetMinMaxValue()
			min, max := int64(minUint32), int64(maxUint32)
			switch op {
			case expr.GTE:
				return max < num
			case expr.GT:
				return max <= num
			case expr.LTE:
				return min > num
			case expr.LT:
				return min >= num
			case expr.EQ:
				return min > num || max < num
			}
		}
	}
	return false
}

func (qc *AQLQueryContext) initializeNonAggResponse() {
	if qc.IsNonAggregationQuery {
		headers := make([]string, len(qc.Query.Dimensions))
		for i, dim := range qc.Query.Dimensions {
			headers[i] = dim.Expr
		}
		if qc.ResponseWriter != nil {
			headersBytes, _ := json.Marshal(headers)
			qc.ResponseWriter.Write([]byte(`{"results":[{"headers":`))
			qc.ResponseWriter.Write(headersBytes)
			qc.ResponseWriter.Write([]byte(`,"matrixData":[`))
		} else {
			qc.Results = make(queryCom.AQLQueryResult)
			qc.Results.SetHeaders(headers)
		}
	}
}
