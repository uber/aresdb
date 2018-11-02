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

package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"github.com/uber/aresdb/memstore"
	memCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/metastore"
	"github.com/uber/aresdb/utils"
	"io"
)

// DebugHandler handles debug operations.
type DebugHandler struct {
	memStore memstore.MemStore
	// For getting cutoff of a shard.
	metaStore          metastore.MetaStore
	queryHandler       *QueryHandler
	healthCheckHandler *HealthCheckHandler
}

// NewDebugHandler returns a new DebugHandler.
func NewDebugHandler(memStore memstore.MemStore, metaStore metastore.MetaStore, queryHandler *QueryHandler, healthCheckHandler *HealthCheckHandler) *DebugHandler {
	return &DebugHandler{
		memStore:           memStore,
		metaStore:          metaStore,
		queryHandler:       queryHandler,
		healthCheckHandler: healthCheckHandler,
	}
}

// Register registers http handlers.
func (handler *DebugHandler) Register(router *mux.Router) {
	router.HandleFunc("/health", handler.Health).Methods(http.MethodGet)
	router.HandleFunc("/health/{onOrOff}", handler.HealthSwitch).Methods(http.MethodPost)
	router.HandleFunc("/jobs/{jobType}", handler.ShowJobStatus).Methods(http.MethodGet)
	router.HandleFunc("/devices", handler.ShowDeviceStatus).Methods(http.MethodGet)
	router.HandleFunc("/host-memory", handler.ShowHostMemory).Methods(http.MethodGet)
	router.HandleFunc("/{table}/{shard}", handler.ShowShardMeta).Methods(http.MethodGet)
	router.HandleFunc("/{table}/{shard}/archive", handler.Archive).Methods(http.MethodPost)
	router.HandleFunc("/{table}/{shard}/backfill", handler.Backfill).Methods(http.MethodPost)
	router.HandleFunc("/{table}/{shard}/snapshot", handler.Snapshot).Methods(http.MethodPost)
	router.HandleFunc("/{table}/{shard}/purge", handler.Purge).Methods(http.MethodPost)
	router.HandleFunc("/{table}/{shard}/batches/{batch}", handler.ShowBatch).Methods(http.MethodGet)
	router.HandleFunc("/{table}/{shard}/batches/{batch}/vector-parties/{column}", handler.LoadVectorParty).Methods(http.MethodGet)
	router.HandleFunc("/{table}/{shard}/batches/{batch}/vector-parties/{column}", handler.EvictVectorParty).Methods(http.MethodDelete)
	router.HandleFunc("/{table}/{shard}/primary-keys", handler.LookupPrimaryKey).Methods(http.MethodGet)
	router.HandleFunc("/{table}/{shard}/redologs", handler.ListRedoLogs).
		Methods(http.MethodGet)
	router.HandleFunc("/{table}/{shard}/redologs/{creationTime}/upsertbatches", handler.ListUpsertBatches).
		Methods(http.MethodGet)
	router.HandleFunc("/{table}/{shard}/redologs/{creationTime}/upsertbatches/{offset}", handler.ReadUpsertBatch).
		Methods(http.MethodGet)
	router.HandleFunc("/{table}/{shard}/backfill-manager/upsertbatches/{offset}", handler.ReadBackfillQueueUpsertBatch).Methods(http.MethodGet)
}

// Health returns whether the health check is on or off
func (handler *DebugHandler) Health(w http.ResponseWriter, r *http.Request) {
	handler.healthCheckHandler.RLock()
	disabled := handler.healthCheckHandler.disable
	handler.healthCheckHandler.RUnlock()
	status := "on"
	if disabled {
		status = "off"
	}
	io.WriteString(w, status)
}

// HealthSwitch will turn on health check based on the request.
func (handler *DebugHandler) HealthSwitch(w http.ResponseWriter, r *http.Request) {
	var request HealthSwitchRequest
	var err error
	if err = ReadRequest(r, &request); err != nil {
		RespondWithBadRequest(w, err)
		return
	}

	if request.OnOrOff != "on" && request.OnOrOff != "off" {
		RespondWithBadRequest(w, errors.New("must specify on or off in the url"))
		return
	}

	handler.healthCheckHandler.Lock()
	handler.healthCheckHandler.disable = request.OnOrOff == "off"
	handler.healthCheckHandler.Unlock()
	io.WriteString(w, "OK")
}

// ShowBatch will only show batches that is present in memory, it will not request batch
// from DiskStore.
func (handler *DebugHandler) ShowBatch(w http.ResponseWriter, r *http.Request) {

	var request ShowBatchRequest
	var response ShowBatchResponse
	var err error

	err = ReadRequest(r, &request)
	if err != nil {
		RespondWithBadRequest(w, err)
		return
	}

	if request.NumRows <= 0 || request.NumRows > 100 {
		request.NumRows = 100
	}

	if request.StartRow < 0 {
		request.StartRow = 0
	}

	response.Body.StartRow = request.StartRow

	schema, err := handler.memStore.GetSchema(request.TableName)
	if err != nil {
		RespondWithBadRequest(w, ErrTableDoesNotExist)
		return
	}

	schema.RLock()
	response.Body.Vectors = make([]memCom.SlicedVector, 0, len(schema.Schema.Columns))
	response.Body.Columns = make([]string, 0, len(schema.Schema.Columns))
	for columnID, column := range schema.Schema.Columns {
		response.Body.Columns = append(response.Body.Columns, column.Name)
		response.Body.Types = append(response.Body.Types, column.Type)
		if column.Deleted {
			response.Body.Deleted = append(response.Body.Deleted, columnID)
		}
	}
	schema.RUnlock()

	var shard *memstore.TableShard
	shard, err = handler.memStore.GetTableShard(request.TableName, request.ShardID)
	if err != nil {
		RespondWithBadRequest(w, err)
		return
	}

	defer func() {
		shard.Users.Done()
		if err != nil {
			RespondWithError(w, err)
		} else {
			RespondWithJSONObject(w, response.Body)
		}
	}()

	// request archiveBatch
	if request.BatchID >= 0 {
		shard.ArchiveStore.RLock()
		currentVersion := shard.ArchiveStore.CurrentVersion
		currentVersion.Users.Add(1)
		shard.ArchiveStore.RUnlock()
		defer currentVersion.Users.Done()

		archiveBatch := currentVersion.GetBatchForRead(request.BatchID)
		if archiveBatch == nil {
			err = ErrBatchDoesNotExist
			return
		}
		defer archiveBatch.RUnlock()
		// holding archive batch lock will prevent any loading and eviction.
		response.Body.NumRows, response.Body.Vectors = readRows(archiveBatch.Columns, request.StartRow, request.NumRows)
	} else {
		// request liveBatch
		batchIDs, numRecordsInLastBatch := shard.LiveStore.GetBatchIDs()
		liveBatch := shard.LiveStore.GetBatchForRead(int32(request.BatchID))
		if liveBatch == nil {
			err = ErrBatchDoesNotExist
			return
		}
		defer liveBatch.RUnlock()

		if batchIDs[len(batchIDs)-1] == int32(request.BatchID) {
			if request.StartRow >= numRecordsInLastBatch {
				return
			}

			if request.NumRows > numRecordsInLastBatch-request.StartRow {
				request.NumRows = numRecordsInLastBatch - request.StartRow
			}
		}
		response.Body.NumRows, response.Body.Vectors = readRows(liveBatch.Columns, request.StartRow, request.NumRows)
	}

	// translate enums
	schema.RLock()
	for columnID, column := range schema.Schema.Columns {
		if !column.Deleted && column.IsEnumColumn() && columnID < len(response.Body.Vectors) {
			vector := &response.Body.Vectors[columnID]
			err = translateEnums(vector, schema.EnumDicts[column.Name].ReverseDict)
		}
	}
	schema.RUnlock()
}

func readRows(vps []memCom.VectorParty, startRow, numRows int) (n int, vectors []memCom.SlicedVector) {
	vectors = make([]memCom.SlicedVector, len(vps))
	for columnID, vp := range vps {
		if vp != nil {
			vectors[columnID] = vp.Slice(startRow, numRows)
			if len(vectors[columnID].Counts) > 0 {
				n = vectors[columnID].Counts[len(vectors[columnID].Counts)-1]
			}
		} else {
			vectors[columnID] = memCom.SlicedVector{
				Values: []interface{}{nil},
				Counts: []int{numRows},
			}
		}
	}
	return n, vectors
}

func translateEnums(vector *memCom.SlicedVector, enumCases []string) error {
	for index, value := range vector.Values {
		if value != nil {
			var id int
			switch v := value.(type) {
			case uint8:
				id = int(v)
			case uint16:
				id = int(v)
			default:
				// this should never happen
				return utils.StackError(nil, "Wrong data type for enum vector, %T", value)
			}

			// it is possible when enum change has not arrived in memory yet,
			// treat it as empty string temporarily.
			vector.Values[index] = ""
			if id < len(enumCases) {
				vector.Values[index] = enumCases[id]
			}
		}
	}
	return nil
}

// LookupPrimaryKey looks up a key in primary key for given table and shard
func (handler *DebugHandler) LookupPrimaryKey(w http.ResponseWriter, r *http.Request) {
	var request LookupPrimaryKeyRequest
	err := ReadRequest(r, &request)
	if err != nil {
		RespondWithError(w, err)
		return
	}

	shard, err := handler.memStore.GetTableShard(request.TableName, request.ShardID)
	if err != nil {
		RespondWithError(w, utils.APIError{
			Code:    http.StatusBadRequest,
			Message: err.Error(),
			Cause:   err,
		})
	}
	defer shard.Users.Done()

	keyStrs := strings.Split(request.Key, ",")
	var found bool
	var recordID memstore.RecordID
	recordID, found = shard.LiveStore.LookupKey(keyStrs)
	if !found {
		RespondWithError(w, utils.APIError{
			Code:    http.StatusNotFound,
			Message: fmt.Sprintf("Key '%s' does not exist or expired", request.Key),
		})
		return
	}
	RespondWithJSONObject(w, recordID)
}

// Archive starts an archiving process on demand.
func (handler *DebugHandler) Archive(w http.ResponseWriter, r *http.Request) {
	var request ArchiveRequest
	err := ReadRequest(r, &request)
	if err != nil {
		RespondWithBadRequest(w, err)
		return
	}

	// Just check table and shard existence.
	shard, err := handler.memStore.GetTableShard(request.TableName, request.ShardID)
	if err != nil {
		RespondWithBadRequest(w, err)
		return
	}
	shard.Users.Done()

	scheduler := handler.memStore.GetScheduler()
	go func() {
		scheduler.SubmitJob(
			scheduler.NewArchivingJob(request.TableName, request.ShardID, request.Body.Cutoff))
	}()

	RespondJSONObjectWithCode(w, http.StatusOK, "Archiving job submitted")
}

// Backfill starts an backfill process on demand.
func (handler *DebugHandler) Backfill(w http.ResponseWriter, r *http.Request) {
	var request BackfillRequest
	err := ReadRequest(r, &request)
	if err != nil {
		RespondWithBadRequest(w, err)
		return
	}

	// Just check table and shard existence.
	shard, err := handler.memStore.GetTableShard(request.TableName, request.ShardID)
	if err != nil {
		RespondWithBadRequest(w, err)
		return
	}
	shard.Users.Done()

	scheduler := handler.memStore.GetScheduler()
	go func() {
		scheduler.SubmitJob(
			scheduler.NewBackfillJob(request.TableName, request.ShardID))
	}()

	RespondJSONObjectWithCode(w, http.StatusOK, "Backfill job submitted")
}

// Snapshot starts an snapshot process on demand.
func (handler *DebugHandler) Snapshot(w http.ResponseWriter, r *http.Request) {
	var request SnapshotRequest
	err := ReadRequest(r, &request)
	if err != nil {
		RespondWithBadRequest(w, err)
		return
	}

	// Just check table and shard existence.
	shard, err := handler.memStore.GetTableShard(request.TableName, request.ShardID)
	if err != nil {
		RespondWithBadRequest(w, err)
		return
	}
	defer shard.Users.Done()

	scheduler := handler.memStore.GetScheduler()
	go func() {
		scheduler.SubmitJob(
			scheduler.NewSnapshotJob(request.TableName, request.ShardID))
	}()

	RespondJSONObjectWithCode(w, http.StatusOK, "Snapshot job submitted")
}

// Purge starts an purge process on demand.
func (handler *DebugHandler) Purge(w http.ResponseWriter, r *http.Request) {
	var request PurgeRequest
	err := ReadRequest(r, &request)
	if err != nil {
		RespondWithBadRequest(w, err)
		return
	}

	if !request.Body.SafePurge && (request.Body.BatchIDStart < 0 || request.Body.BatchIDEnd < 0 || request.Body.BatchIDStart > request.Body.BatchIDEnd) {
		RespondWithBadRequest(w, fmt.Errorf("invalid batch range, expects both to be > 0, got [%d, %d)",
			request.Body.BatchIDStart, request.Body.BatchIDEnd))
		return
	}

	shard, err := handler.memStore.GetTableShard(request.TableName, request.ShardID)
	if err != nil {
		RespondWithBadRequest(w, err)
		return
	}
	defer shard.Users.Done()

	if request.Body.SafePurge {
		retentionDays := shard.Schema.Schema.Config.RecordRetentionInDays
		if retentionDays > 0 {
			nowInDay := int(utils.Now().Unix() / 86400)
			request.Body.BatchIDStart = 0
			request.Body.BatchIDEnd = nowInDay - retentionDays
		} else {
			RespondWithBadRequest(w, utils.APIError{Message: "safe purge attempted on table with infinite retention"})
			return
		}
	}

	scheduler := handler.memStore.GetScheduler()
	go func() {
		scheduler.SubmitJob(
			scheduler.NewPurgeJob(request.TableName, request.ShardID, request.Body.BatchIDStart, request.Body.BatchIDEnd))
	}()

	RespondJSONObjectWithCode(w, http.StatusOK, "Purge job submitted")
}

// ShowShardMeta shows the metadata for a table shard. It won't show the underlying data.
func (handler *DebugHandler) ShowShardMeta(w http.ResponseWriter, r *http.Request) {
	var request ShowShardMetaRequest
	err := ReadRequest(r, &request)
	if err != nil {
		RespondWithBadRequest(w, err)
		return
	}
	shard, err := handler.memStore.GetTableShard(request.TableName, request.ShardID)
	if err != nil {
		RespondWithBadRequest(w, err)
		return
	}
	defer shard.Users.Done()
	RespondWithJSONObject(w, shard)
	return
}

// ListRedoLogs lists all the redo log files for a given shard.
func (handler *DebugHandler) ListRedoLogs(w http.ResponseWriter, r *http.Request) {
	var request ListRedoLogsRequest
	err := ReadRequest(r, &request)
	if err != nil {
		RespondWithBadRequest(w, err)
		return
	}

	shard, err := handler.memStore.GetTableShard(request.TableName, request.ShardID)
	if err != nil {
		RespondWithBadRequest(w, err)
		return
	}
	defer shard.Users.Done()
	redoLogFiles, err := shard.NewRedoLogBrowser().ListLogFiles()

	if err != nil {
		RespondWithError(w, err)
		return
	}

	response := make(ListRedoLogsResponse, len(redoLogFiles))

	for i, redoLogFile := range redoLogFiles {
		response[i] = strconv.FormatInt(redoLogFile, 10)
	}

	RespondWithJSONObject(w, response)
	return
}

// ListUpsertBatches returns offsets of upsert batches in the redo log file.
func (handler *DebugHandler) ListUpsertBatches(w http.ResponseWriter, r *http.Request) {
	var request ListUpsertBatchesRequest
	err := ReadRequest(r, &request)
	if err != nil {
		RespondWithBadRequest(w, err)
		return
	}
	shard, err := handler.memStore.GetTableShard(request.TableName, request.ShardID)
	if err != nil {
		RespondWithBadRequest(w, err)
		return
	}

	defer shard.Users.Done()
	offsets, err := shard.NewRedoLogBrowser().ListUpsertBatch(request.CreationTime)
	if err != nil {
		RespondWithError(w, err)
		return
	}
	RespondWithJSONObject(w, &offsets)
	return
}

// ReadUpsertBatch shows the records of an upsert batch given a redolog file creation time and
// upsert batch index within the file.
func (handler *DebugHandler) ReadUpsertBatch(w http.ResponseWriter, r *http.Request) {
	var request ReadRedologUpsertBatchRequest
	if err := ReadRequest(r, &request); err != nil {
		RespondWithBadRequest(w, err)
		return
	}
	shard, err := handler.memStore.GetTableShard(request.TableName, request.ShardID)
	if err != nil {
		RespondWithBadRequest(w, err)
		return
	}
	defer shard.Users.Done()

	rows, columnNames, numTotalRows, err := shard.NewRedoLogBrowser().ReadData(request.CreationTime,
		request.Offset, request.Start, request.Length)
	if err != nil {
		RespondWithError(w, err)
		return
	}
	response := ReadUpsertBatchResponse{
		Draw:            request.Draw,
		Data:            rows,
		ColumnNames:     columnNames,
		RecordsTotal:    numTotalRows,
		RecordsFiltered: numTotalRows,
	}
	RespondWithJSONObject(w, &response)
	return
}

// LoadVectorParty requests a vector party from disk if it is not already in memory
func (handler *DebugHandler) LoadVectorParty(w http.ResponseWriter, r *http.Request) {
	var request LoadVectorPartyRequest
	if err := ReadRequest(r, &request); err != nil {
		RespondWithBadRequest(w, err)
		return
	}

	if request.BatchID < 0 {
		RespondWithBadRequest(w, errors.New("Live batch vector party cannot be loaded"))
		return
	}

	shard, err := handler.memStore.GetTableShard(request.TableName, request.ShardID)
	if err != nil {
		RespondWithBadRequest(w, err)
		return
	}
	defer shard.Users.Done()

	schema := shard.Schema
	isDeleted := false
	shard.Schema.RLock()
	columnID, exist := schema.ColumnIDs[request.ColumnName]
	if columnID < len(schema.Schema.Columns) {
		isDeleted = schema.Schema.Columns[columnID].Deleted
	}
	shard.Schema.RUnlock()

	if !exist {
		RespondWithBadRequest(w, ErrColumnDoesNotExist)
		return
	}

	if isDeleted {
		RespondWithBadRequest(w, ErrColumnDeleted)
		return
	}

	version := shard.ArchiveStore.GetCurrentVersion()
	defer version.Users.Done()

	batch := version.RequestBatch(int32(request.BatchID))
	vp := batch.RequestVectorParty(columnID)
	if vp != nil {
		vp.WaitForDiskLoad()
		vp.Release()
	}
	RespondWithJSONObject(w, nil)
}

// EvictVectorParty evict a vector party from memory.
func (handler *DebugHandler) EvictVectorParty(w http.ResponseWriter, r *http.Request) {
	var request EvictVectorPartyRequest
	if err := ReadRequest(r, &request); err != nil {
		RespondWithBadRequest(w, err)
		return
	}

	if request.BatchID < 0 {
		RespondWithBadRequest(w, errors.New("live batch vector party cannot be evicted"))
		return
	}

	shard, err := handler.memStore.GetTableShard(request.TableName, request.ShardID)
	if err != nil {
		RespondWithBadRequest(w, err)
		return
	}
	defer shard.Users.Done()

	shard.Schema.RLock()
	columnID, exist := shard.Schema.ColumnIDs[request.ColumnName]
	shard.Schema.RUnlock()

	if !exist {
		RespondWithError(w, ErrColumnDoesNotExist)
		return
	}

	version := shard.ArchiveStore.GetCurrentVersion()
	defer version.Users.Done()

	batch := version.RequestBatch(int32(request.BatchID))
	// this operation is blocking and needs the user to wait
	batch.BlockingDelete(columnID)
	RespondWithJSONObject(w, nil)
}

// ShowJobStatus shows the current archive job status.
func (handler *DebugHandler) ShowJobStatus(w http.ResponseWriter, r *http.Request) {
	var request ShowJobStatusRequest
	if err := ReadRequest(r, &request); err != nil {
		RespondWithBadRequest(w, err)
		return
	}

	scheduler := handler.memStore.GetScheduler()
	scheduler.RLock()
	jsonBuffer, err := json.Marshal(scheduler.GetJobDetails(memCom.JobType(request.JobType)))
	scheduler.RUnlock()
	RespondWithJSONBytes(w, jsonBuffer, err)
	return
}

// ShowDeviceStatus shows the current scheduler status.
func (handler *DebugHandler) ShowDeviceStatus(w http.ResponseWriter, r *http.Request) {
	deviceManager := handler.queryHandler.GetDeviceManager()
	deviceManager.RLock()
	jsonBuffer, err := json.Marshal(*deviceManager)
	deviceManager.RUnlock()
	RespondWithJSONBytes(w, jsonBuffer, err)
	return
}

// ShowHostMemory shows the current host memory usage
func (handler *DebugHandler) ShowHostMemory(w http.ResponseWriter, r *http.Request) {
	memoryUsageByTableShard, err := handler.memStore.GetMemoryUsageDetails()
	if err != nil {
		RespondWithError(w, err)
		return
	}
	RespondWithJSONObject(w, memoryUsageByTableShard)
}

// ReadBackfillQueueUpsertBatch reads upsert batch inside backfill manager backfill queue
func (handler *DebugHandler) ReadBackfillQueueUpsertBatch(w http.ResponseWriter, r *http.Request) {
	var request ReadBackfillQueueUpsertBatchRequest
	err := ReadRequest(r, &request)
	if err != nil {
		RespondWithBadRequest(w, err)
		return
	}

	shard, err := handler.memStore.GetTableShard(request.TableName, request.ShardID)
	if err != nil {
		RespondWithBadRequest(w, err)
		return
	}
	data, columnNames, err := shard.LiveStore.BackfillManager.ReadUpsertBatch(int(request.Offset), request.Start, request.Length, shard.Schema)
	shard.Users.Done()

	if err != nil {
		RespondWithBadRequest(w, err)
		return
	}

	response := ReadUpsertBatchResponse{
		Data:            data,
		ColumnNames:     columnNames,
		RecordsFiltered: len(data),
		RecordsTotal:    len(data),
		Draw:            request.Draw,
	}

	RespondWithJSONObject(w, response)
	return
}
