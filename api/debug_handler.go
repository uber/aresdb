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
	"github.com/uber/aresdb/cluster/topology"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"github.com/uber/aresdb/api/common"
	mutatorCom "github.com/uber/aresdb/controller/mutators/common"
	"github.com/uber/aresdb/memstore"
	memCom "github.com/uber/aresdb/memstore/common"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/utils"
)

// DebugHandler handles debug operations.
type DebugHandler struct {
	namespace  string
	shardOwner topology.ShardOwner
	enumReader mutatorCom.EnumReader
	memStore   memstore.MemStore
	// For getting cutoff of a shard.
	metaStore          metaCom.MetaStore
	queryHandler       *QueryHandler
	healthCheckHandler *HealthCheckHandler
	bootstrapRetryChan chan bool
}

// NewDebugHandler returns a new DebugHandler.
func NewDebugHandler(
	namespace string,
	memStore memstore.MemStore,
	metaStore metaCom.MetaStore,
	queryHandler *QueryHandler,
	healthCheckHandler *HealthCheckHandler,
	shardOwner topology.ShardOwner,
	enumReader mutatorCom.EnumReader,
) *DebugHandler {
	return &DebugHandler{
		namespace:          namespace,
		enumReader:         enumReader,
		shardOwner:         shardOwner,
		memStore:           memStore,
		metaStore:          metaStore,
		queryHandler:       queryHandler,
		healthCheckHandler: healthCheckHandler,
		bootstrapRetryChan: make(chan bool),
	}
}

// Register registers http handlers.
func (handler *DebugHandler) Register(router *mux.Router) {
	router.HandleFunc("/health", utils.ApplyHTTPWrappers(handler.Health)).Methods(http.MethodGet)
	router.HandleFunc("/health/{onOrOff}", utils.ApplyHTTPWrappers(handler.HealthSwitch)).Methods(http.MethodPost)
	router.HandleFunc("/jobs/{jobType}", utils.ApplyHTTPWrappers(handler.ShowJobStatus)).Methods(http.MethodGet)
	router.HandleFunc("/devices", utils.ApplyHTTPWrappers(handler.ShowDeviceStatus)).Methods(http.MethodGet)
	router.HandleFunc("/host-memory", utils.ApplyHTTPWrappers(handler.ShowHostMemory)).Methods(http.MethodGet)
	router.HandleFunc("/shards", utils.ApplyHTTPWrappers(handler.ShowShardSet)).Methods(http.MethodGet)
	router.HandleFunc("/{table}/{shard}", utils.ApplyHTTPWrappers(handler.ShowShardMeta)).Methods(http.MethodGet)
	router.HandleFunc("/{table}/{shard}/archive", utils.ApplyHTTPWrappers(handler.Archive)).Methods(http.MethodPost)
	router.HandleFunc("/{table}/{shard}/backfill", utils.ApplyHTTPWrappers(handler.Backfill)).Methods(http.MethodPost)
	router.HandleFunc("/{table}/{shard}/snapshot", utils.ApplyHTTPWrappers(handler.Snapshot)).Methods(http.MethodPost)
	router.HandleFunc("/{table}/{shard}/purge", utils.ApplyHTTPWrappers(handler.Purge)).Methods(http.MethodPost)
	router.HandleFunc("/{table}/{shard}/batches/{batch}", utils.ApplyHTTPWrappers(handler.ShowBatch)).Methods(http.MethodGet)
	router.HandleFunc("/{table}/{shard}/batches/{batch}/vector-parties/{column}", utils.ApplyHTTPWrappers(handler.LoadVectorParty)).Methods(http.MethodGet)
	router.HandleFunc("/{table}/{shard}/batches/{batch}/vector-parties/{column}", utils.ApplyHTTPWrappers(handler.EvictVectorParty)).Methods(http.MethodDelete)
	router.HandleFunc("/{table}/{shard}/primary-keys", utils.ApplyHTTPWrappers(handler.LookupPrimaryKey)).Methods(http.MethodGet)
	router.HandleFunc("/{table}/{shard}/redologs", utils.ApplyHTTPWrappers(handler.ListRedoLogs)).
		Methods(http.MethodGet)
	router.HandleFunc("/{table}/{shard}/redologs/{creationTime}/upsertbatches", utils.ApplyHTTPWrappers(handler.ListUpsertBatches)).
		Methods(http.MethodGet)
	router.HandleFunc("/{table}/{shard}/redologs/{creationTime}/upsertbatches/{offset}", utils.ApplyHTTPWrappers(handler.ReadUpsertBatch)).
		Methods(http.MethodGet)
	router.HandleFunc("/{table}/{shard}/backfill-manager/upsertbatches/{offset}", utils.ApplyHTTPWrappers(handler.ReadBackfillQueueUpsertBatch)).Methods(http.MethodGet)
	router.HandleFunc("/bootstrap/retry", utils.ApplyHTTPWrappers(handler.BootstrapRetry)).Methods(http.MethodPost)
}

// ShowShardSet shows the shard set owned by the server
func (handler *DebugHandler) ShowShardSet(w *utils.ResponseWriter, r *http.Request) {
	shards := handler.shardOwner.GetOwnedShards()
	sort.Ints(shards)
	w.WriteObject(shards)
}

// Health returns whether the health check is on or off
func (handler *DebugHandler) Health(w *utils.ResponseWriter, r *http.Request) {
	handler.healthCheckHandler.RLock()
	disabled := handler.healthCheckHandler.disable
	handler.healthCheckHandler.RUnlock()
	status := "on"
	if disabled {
		status = "off"
	}
	w.Write([]byte(status))
}

// HealthSwitch will turn on health check based on the request.
func (handler *DebugHandler) HealthSwitch(w *utils.ResponseWriter, r *http.Request) {
	var request HealthSwitchRequest
	var err error
	if err = common.ReadRequest(r, &request); err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	if request.OnOrOff != "on" && request.OnOrOff != "off" {
		w.WriteErrorWithCode(http.StatusBadRequest, errors.New("must specify on or off in the url"))
		return
	}

	handler.healthCheckHandler.Lock()
	handler.healthCheckHandler.disable = request.OnOrOff == "off"
	handler.healthCheckHandler.Unlock()
	w.Write([]byte("OK"))
}

// ShowBatch will only show batches that is present in memory, it will not request batch
// from DiskStore.
func (handler *DebugHandler) ShowBatch(w *utils.ResponseWriter, r *http.Request) {

	var request ShowBatchRequest
	var response ShowBatchResponse
	var err error

	err = common.ReadRequest(r, &request)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
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
		w.WriteError(ErrTableDoesNotExist)
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
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	defer func() {
		shard.Users.Done()
		if err != nil {
			w.WriteError(err)
		} else {
			w.WriteObject(response.Body)
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

	schema.RLock()
	for columnID, column := range schema.Schema.Columns {
		if !column.Deleted && column.IsEnumBasedColumn() && columnID < len(response.Body.Vectors) {
			vector := &response.Body.Vectors[columnID]
			var enumCases []string
			if handler.enumReader != nil {
				// 1. use centralized enum reader
				enumCases, err = handler.enumReader.GetEnumCases(handler.namespace, schema.Schema.Name, column.Name)
				if err != nil {
					return
				}
			} else {
				// 2. use local in memory enum dict
				enumCases = schema.EnumDicts[column.Name].ReverseDict
			}
			err = translateEnums(column.IsEnumArrayColumn(), vector, enumCases)
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

func translateEnums(isEnumArray bool, vector *memCom.SlicedVector, enumCases []string) error {
	if isEnumArray {
		return tranlateEnumsArray(vector, enumCases)
	}
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
			// display raw enum value in such case
			vector.Values[index] = value
			if id < len(enumCases) {
				vector.Values[index] = enumCases[id]
			}
		}
	}
	return nil
}

func tranlateEnumsArray(vector *memCom.SlicedVector, enumCases []string) error {
	for index, value := range vector.Values {
		if value != nil {
			ids := make([]interface{}, 0)
			err := json.Unmarshal(([]byte)(value.(string)), &ids)
			if err == nil {
				for i, v := range ids {
					if v != nil {
						var id int
						switch v := v.(type) {
						// unmarshal will turn number to float64
						case float64:
							id = int(v)
						default:
							// this should never happen
							return utils.StackError(nil, "Wrong data type for enum vector, %T", value)
						}
						if id < len(enumCases) {
							ids[i] = enumCases[id]
						}

					}
				}
				newValue, err := json.Marshal(ids)
				if err == nil {
					vector.Values[index] = string(newValue)
				}
			}
		}
	}
	return nil
}

// LookupPrimaryKey looks up a key in primary key for given table and shard
func (handler *DebugHandler) LookupPrimaryKey(w *utils.ResponseWriter, r *http.Request) {
	var request LookupPrimaryKeyRequest
	err := common.ReadRequest(r, &request)
	if err != nil {
		w.WriteError(err)
		return
	}

	shard, err := handler.memStore.GetTableShard(request.TableName, request.ShardID)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}
	defer shard.Users.Done()

	keyStrs := strings.Split(request.Key, ",")
	var found bool
	var recordID memCom.RecordID
	recordID, found = shard.LiveStore.LookupKey(keyStrs)
	if !found {
		w.WriteErrorWithCode(http.StatusNotFound, fmt.Errorf("key '%s' does not exist or expired", request.Key))
		return
	}
	w.WriteObject(recordID)
}

// Archive starts an archiving process on demand.
func (handler *DebugHandler) Archive(w *utils.ResponseWriter, r *http.Request) {
	var request ArchiveRequest
	err := common.ReadRequest(r, &request)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	// Just check table and shard existence.
	shard, err := handler.memStore.GetTableShard(request.TableName, request.ShardID)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}
	shard.Users.Done()

	scheduler := handler.memStore.GetScheduler()
	err, errChan := scheduler.SubmitJob(
		scheduler.NewArchivingJob(request.TableName, request.ShardID, request.Body.Cutoff))
	if err == nil {
		go func() {
			<-errChan
		}()
		w.Write([]byte("Archiving job submitted"))
	} else {
		w.WriteErrorWithCode(http.StatusMethodNotAllowed, err)
	}
}

// Backfill starts an backfill process on demand.
func (handler *DebugHandler) Backfill(w *utils.ResponseWriter, r *http.Request) {
	var request BackfillRequest
	err := common.ReadRequest(r, &request)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	// Just check table and shard existence.
	shard, err := handler.memStore.GetTableShard(request.TableName, request.ShardID)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}
	shard.Users.Done()

	scheduler := handler.memStore.GetScheduler()
	err, errChan := scheduler.SubmitJob(
		scheduler.NewBackfillJob(request.TableName, request.ShardID))
	if err == nil {
		go func() {
			<-errChan
		}()
		w.Write([]byte("Backfill job submitted"))
	}
}

// Snapshot starts an snapshot process on demand.
func (handler *DebugHandler) Snapshot(w *utils.ResponseWriter, r *http.Request) {
	var request SnapshotRequest
	err := common.ReadRequest(r, &request)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	// Just check table and shard existence.
	shard, err := handler.memStore.GetTableShard(request.TableName, request.ShardID)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}
	defer shard.Users.Done()

	scheduler := handler.memStore.GetScheduler()
	err, errChan := scheduler.SubmitJob(
		scheduler.NewSnapshotJob(request.TableName, request.ShardID))
	if err == nil {
		go func() {
			<-errChan
		}()
		w.Write([]byte("Snapshot job submitted"))
	}
}

// Purge starts an purge process on demand.
func (handler *DebugHandler) Purge(w *utils.ResponseWriter, r *http.Request) {
	var request PurgeRequest
	err := common.ReadRequest(r, &request)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	if !request.Body.SafePurge && (request.Body.BatchIDStart < 0 || request.Body.BatchIDEnd < 0 || request.Body.BatchIDStart > request.Body.BatchIDEnd) {
		w.WriteErrorWithCode(http.StatusBadRequest, fmt.Errorf("invalid batch range, expects both to be > 0, got [%d, %d)",
			request.Body.BatchIDStart, request.Body.BatchIDEnd))
		return
	}

	shard, err := handler.memStore.GetTableShard(request.TableName, request.ShardID)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
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
			w.WriteErrorWithCode(http.StatusBadRequest, errors.New("safe purge attempted on table with infinite retention"))
			return
		}
	}

	scheduler := handler.memStore.GetScheduler()
	err, errChan := scheduler.SubmitJob(
		scheduler.NewPurgeJob(request.TableName, request.ShardID, request.Body.BatchIDStart, request.Body.BatchIDEnd))
	if err == nil {
		go func() {
			<-errChan
		}()

		w.Write([]byte("Purge job submitted"))
	}
}

// ShowShardMeta shows the metadata for a table shard. It won't show the underlying data.
func (handler *DebugHandler) ShowShardMeta(w *utils.ResponseWriter, r *http.Request) {
	var request ShowShardMetaRequest
	err := common.ReadRequest(r, &request)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}
	shard, err := handler.memStore.GetTableShard(request.TableName, request.ShardID)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}
	defer shard.Users.Done()
	w.WriteObject(shard)
}

// ListRedoLogs lists all the redo log files for a given shard.
func (handler *DebugHandler) ListRedoLogs(w *utils.ResponseWriter, r *http.Request) {
	var request ListRedoLogsRequest
	err := common.ReadRequest(r, &request)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	shard, err := handler.memStore.GetTableShard(request.TableName, request.ShardID)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}
	defer shard.Users.Done()
	redoLogFiles, err := shard.NewRedoLogBrowser().ListLogFiles()

	if err != nil {
		w.WriteError(err)
		return
	}

	response := make(ListRedoLogsResponse, len(redoLogFiles))

	for i, redoLogFile := range redoLogFiles {
		response[i] = strconv.FormatInt(redoLogFile, 10)
	}

	w.WriteObject(response)
}

// ListUpsertBatches returns offsets of upsert batches in the redo log file.
func (handler *DebugHandler) ListUpsertBatches(w *utils.ResponseWriter, r *http.Request) {
	var request ListUpsertBatchesRequest
	err := common.ReadRequest(r, &request)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}
	shard, err := handler.memStore.GetTableShard(request.TableName, request.ShardID)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	defer shard.Users.Done()
	offsets, err := shard.NewRedoLogBrowser().ListUpsertBatch(request.CreationTime)
	if err != nil {
		w.WriteError(err)
		fmt.Println("marker1 start")
		w.ResponseWriter.Header().Write(os.Stdout)
		fmt.Println("marker1 end")
		return
	}
	w.WriteObject(&offsets)
	return
}

// ReadUpsertBatch shows the records of an upsert batch given a redolog file creation time and
// upsert batch index within the file.
func (handler *DebugHandler) ReadUpsertBatch(w *utils.ResponseWriter, r *http.Request) {
	var request ReadRedologUpsertBatchRequest
	if err := common.ReadRequest(r, &request); err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}
	shard, err := handler.memStore.GetTableShard(request.TableName, request.ShardID)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}
	defer shard.Users.Done()

	rows, columnNames, numTotalRows, err := shard.NewRedoLogBrowser().ReadData(request.CreationTime,
		request.Offset, request.Start, request.Length)
	if err != nil {
		w.WriteError(err)
		return
	}
	response := ReadUpsertBatchResponse{
		Draw:            request.Draw,
		Data:            rows,
		ColumnNames:     columnNames,
		RecordsTotal:    numTotalRows,
		RecordsFiltered: numTotalRows,
	}
	w.WriteObject(&response)
	return
}

// LoadVectorParty requests a vector party from disk if it is not already in memory
func (handler *DebugHandler) LoadVectorParty(w *utils.ResponseWriter, r *http.Request) {
	var request LoadVectorPartyRequest
	if err := common.ReadRequest(r, &request); err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	if request.BatchID < 0 {
		w.WriteErrorWithCode(http.StatusBadRequest, errors.New("live batch vector party cannot be loaded"))
		return
	}

	shard, err := handler.memStore.GetTableShard(request.TableName, request.ShardID)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
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
		w.WriteError(ErrColumnDoesNotExist)
		return
	}

	if isDeleted {
		w.WriteError(ErrColumnDeleted)
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
	w.WriteObject(nil)
}

// EvictVectorParty evict a vector party from memory.
func (handler *DebugHandler) EvictVectorParty(w *utils.ResponseWriter, r *http.Request) {
	var request EvictVectorPartyRequest
	if err := common.ReadRequest(r, &request); err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	if request.BatchID < 0 {
		w.WriteErrorWithCode(http.StatusBadRequest, errors.New("live batch vector party cannot be evicted"))
		return
	}

	shard, err := handler.memStore.GetTableShard(request.TableName, request.ShardID)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}
	defer shard.Users.Done()

	shard.Schema.RLock()
	columnID, exist := shard.Schema.ColumnIDs[request.ColumnName]
	shard.Schema.RUnlock()

	if !exist {
		w.WriteError(ErrColumnDoesNotExist)
		return
	}

	version := shard.ArchiveStore.GetCurrentVersion()
	defer version.Users.Done()

	batch := version.RequestBatch(int32(request.BatchID))
	// this operation is blocking and needs the user to wait
	batch.BlockingDelete(columnID)
	w.WriteObject(nil)
}

// ShowJobStatus shows the current archive job status.
func (handler *DebugHandler) ShowJobStatus(w *utils.ResponseWriter, r *http.Request) {
	var request ShowJobStatusRequest
	if err := common.ReadRequest(r, &request); err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	scheduler := handler.memStore.GetScheduler()
	scheduler.RLock()
	jsonBuffer, err := json.Marshal(scheduler.GetJobDetails(memCom.JobType(request.JobType)))
	scheduler.RUnlock()
	w.WriteJSONBytes(jsonBuffer, err)
	return
}

// ShowDeviceStatus shows the current scheduler status.
func (handler *DebugHandler) ShowDeviceStatus(w *utils.ResponseWriter, r *http.Request) {
	deviceManager := handler.queryHandler.GetDeviceManager()
	deviceManager.RLock()
	jsonBuffer, err := json.Marshal(*deviceManager)
	deviceManager.RUnlock()

	w.WriteJSONBytes(jsonBuffer, err)
	return
}

// ShowHostMemory shows the current host memory usage
func (handler *DebugHandler) ShowHostMemory(w *utils.ResponseWriter, r *http.Request) {
	memoryUsageByTableShard, err := handler.memStore.GetMemoryUsageDetails()
	if err != nil {
		w.WriteError(err)
		return
	}
	w.WriteObject(memoryUsageByTableShard)
}

// ReadBackfillQueueUpsertBatch reads upsert batch inside backfill manager backfill queue
func (handler *DebugHandler) ReadBackfillQueueUpsertBatch(w *utils.ResponseWriter, r *http.Request) {
	var request ReadBackfillQueueUpsertBatchRequest
	err := common.ReadRequest(r, &request)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	shard, err := handler.memStore.GetTableShard(request.TableName, request.ShardID)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}
	data, columnNames, err := shard.LiveStore.BackfillManager.ReadUpsertBatch(int(request.Offset), request.Start, request.Length, shard.Schema)
	shard.Users.Done()

	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	response := ReadUpsertBatchResponse{
		Data:            data,
		ColumnNames:     columnNames,
		RecordsFiltered: len(data),
		RecordsTotal:    len(data),
		Draw:            request.Draw,
	}

	w.WriteObject(response)
	return
}

// Bootstrap will turn on bootstrap based on the request.
func (handler *DebugHandler) BootstrapRetry(w *utils.ResponseWriter, r *http.Request) {
	handler.bootstrapRetryChan <- true

	w.Write([]byte("Bootstrap retry submitted"))
}

// GetBootstrapRetryChan returns bootstrapRetryChan
func (handler *DebugHandler) GetBootstrapRetryChan() chan bool {
	return handler.bootstrapRetryChan
}

// SetBootstrapRetryChan is used for testing
func (handler *DebugHandler) SetBootstrapRetryChan(bootstrapRetryChan chan bool) {
	handler.bootstrapRetryChan = bootstrapRetryChan
}
