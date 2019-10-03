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
	"github.com/m3db/m3/src/x/sync"
	"github.com/uber/aresdb/cluster/topology"
	"net/http"

	"github.com/uber/aresdb/memstore"
	"github.com/uber/aresdb/query"
	queryCom "github.com/uber/aresdb/query/common"
	"github.com/uber/aresdb/utils"

	"time"

	"github.com/gorilla/mux"
	apiCom "github.com/uber/aresdb/api/common"
	"github.com/uber/aresdb/common"
)

// QueryHandler handles query execution.
type QueryHandler struct {
	shardOwner    topology.ShardOwner
	memStore      memstore.MemStore
	deviceManager *query.DeviceManager
	workerPool   sync.WorkerPool
}

// NewQueryHandler creates a new QueryHandler.
func NewQueryHandler(memStore memstore.MemStore, shardOwner topology.ShardOwner, cfg common.QueryConfig) *QueryHandler {
	return &QueryHandler{
		memStore:      memStore,
		shardOwner:    shardOwner,
		deviceManager: query.NewDeviceManager(cfg),
		workerPool:    sync.NewWorkerPool(utils.GetConfig().HTTP.MaxQueryConnections),
	}
}

// GetDeviceManager returns the device manager of query handler.
func (handler *QueryHandler) GetDeviceManager() *query.DeviceManager {
	return handler.deviceManager
}

// Register registers http handlers.
func (handler *QueryHandler) Register(router *mux.Router, wrappers ...utils.HTTPHandlerWrapper) {
	router.HandleFunc("/aql", utils.ApplyHTTPWrappers(handler.HandleAQL, wrappers)).Methods(http.MethodGet, http.MethodPost)
	router.HandleFunc("/sql", utils.ApplyHTTPWrappers(handler.HandleSQL, wrappers)).Methods(http.MethodGet, http.MethodPost)
}

// HandleAQL swagger:route POST /query/aql queryAQL
// query in AQL
//
// Consumes:
//    - application/json
//    - application/hll
//
// Produces:
//    - application/json
//
// Responses:
//    default: errorResponse
//        200: aqlResponse
//        400: aqlResponse
func (handler *QueryHandler) HandleAQL(w http.ResponseWriter, r *http.Request) {
	// default device to negative value to differentiate 0 from empty
	aqlRequest := apiCom.AQLRequest{Device: -1}

	if err := apiCom.ReadRequest(r, &aqlRequest); err != nil {
		apiCom.RespondWithBadRequest(w, err)
		utils.GetLogger().With(
			"error", err,
			"statusCode", http.StatusBadRequest,
		).Error("failed to parse query")
		return
	}

	available := handler.workerPool.GoIfAvailable(func() {
		handler.handleAQLInternal(aqlRequest, w, r)
	})

	if !available {
		apiCom.RespondWithError(w, apiCom.ErrQueryServiceNotAvailable)
		return
	}
}

func (handler *QueryHandler) handleAQLInternal(aqlRequest apiCom.AQLRequest, w http.ResponseWriter, r *http.Request) {
	var err error
	var duration time.Duration
	var qcs []*query.AQLQueryContext
	var statusCode int

	defer func() {
		var errStr string
		if err != nil {
			errStr = err.Error()
		}

		l := utils.GetQueryLogger().With(
			"error", errStr,
			"request", aqlRequest,
			"queries_enabled_", aqlRequest.Body.Queries,
			"duration", duration,
			"statusCode", statusCode,
			"contexts_enabled_", qcs,
			"headers", r.Header,
		)

		if statusCode == http.StatusOK {
			l.Info("All queries succeeded")
		} else {
			l.Error("Some of the queries finished with error")
		}
	}()

	if aqlRequest.Query != "" {
		// Override from query parameter
		err = json.Unmarshal([]byte(aqlRequest.Query), &aqlRequest.Body)
		if err != nil {
			statusCode = http.StatusBadRequest
			apiCom.RespondWithBadRequest(w, utils.APIError{
				Code:    http.StatusBadRequest,
				Message: ErrMsgFailedToUnmarshalRequest,
				Cause:   err,
			})
			return
		}
	}

	if aqlRequest.Body.Queries == nil {
		statusCode = http.StatusBadRequest
		apiCom.RespondWithBadRequest(w, utils.APIError{
			Code:    http.StatusBadRequest,
			Message: ErrMsgMissingParameter,
		})
		return
	}

	returnHLL := aqlRequest.Accept == utils.HTTPContentTypeHyperLogLog
	if aqlRequest.DeviceChoosingTimeout <= 0 {
		aqlRequest.DeviceChoosingTimeout = -1
	}

	queryTimer := utils.GetRootReporter().GetTimer(utils.QueryLatency)
	start := utils.Now()
	var requestResponseWriter QueryResponseWriter

	if !returnHLL && canEagerFlush(aqlRequest.Body.Queries) {
		statusCode = http.StatusOK
		aqlQuery := aqlRequest.Body.Queries[0]
		qc := &query.AQLQueryContext{
			Query:         &aqlQuery,
			ReturnHLLData: false,
			DataOnly:      aqlRequest.DataOnly != 0,
		}
		qc.Compile(handler.memStore, handler.shardOwner)
		qc.ResponseWriter = w
		if qc.Error != nil {
			err = qc.Error
			statusCode = http.StatusBadRequest
			w.WriteHeader(statusCode)
			return
		}
		// for logging purpose only
		qcs = append(qcs, qc)

		qc.FindDeviceForQuery(handler.memStore, aqlRequest.Device, handler.deviceManager, aqlRequest.DeviceChoosingTimeout)
		if qc.Error != nil {
			err = qc.Error
			statusCode = http.StatusServiceUnavailable
			w.WriteHeader(statusCode)
			return
		}
		defer handler.deviceManager.ReleaseReservedMemory(qc.Device, qc.Query)

		qc.ProcessQuery(handler.memStore)
		if qc.Error != nil {
			err = qc.Error
			utils.GetQueryLogger().With(
				"error", err,
				"query", aqlQuery,
				"context", qc,
			).Error("Error happened when processing query")
			statusCode = http.StatusInternalServerError
			return
		}

		if !qc.DataOnly {
			w.Write([]byte(`]}]`))

			if aqlRequest.Verbose > 0 {
				w.Write([]byte(`,"context":`))
				qcBytes, _ := json.Marshal(qcs)
				w.Write(qcBytes)
			}

			w.Write([]byte(`}`))
		}

		utils.GetRootReporter().GetChildCounter(map[string]string{
			"table": aqlQuery.Table,
		}, utils.QueryRowsReturned).Inc(int64(qc.ResultsRowsFlushed()))

	} else {
		requestResponseWriter = getReponseWriter(returnHLL, len(aqlRequest.Body.Queries))

		var qc *query.AQLQueryContext
		for i, aqlQuery := range aqlRequest.Body.Queries {
			qc, statusCode = handleQuery(handler.memStore, handler.shardOwner, handler.deviceManager, aqlRequest, aqlQuery)
			if aqlRequest.Verbose > 0 {
				requestResponseWriter.ReportQueryContext(qc)
			}
			if qc.Error != nil {
				requestResponseWriter.ReportError(i, aqlQuery.Table, qc.Error, statusCode)
			} else {
				requestResponseWriter.ReportResult(i, qc)
				qc.ReleaseHostResultsBuffers()
				utils.GetRootReporter().GetChildCounter(map[string]string{
					"table": aqlQuery.Table,
				}, utils.QuerySucceeded).Inc(1)
			}

			qcs = append(qcs, qc)
		}
	}
	duration = utils.Now().Sub(start)
	queryTimer.Record(duration)
	if requestResponseWriter != nil {
		requestResponseWriter.Respond(w)
		statusCode = requestResponseWriter.GetStatusCode()
	}
	return
}

func handleQuery(memStore memstore.MemStore, shardOwner topology.ShardOwner, deviceManager *query.DeviceManager, aqlRequest apiCom.AQLRequest, aqlQuery queryCom.AQLQuery) (qc *query.AQLQueryContext, statusCode int) {
	qc = &query.AQLQueryContext{
		Query:         &aqlQuery,
		ReturnHLLData: aqlRequest.Accept == utils.HTTPContentTypeHyperLogLog,
		DataOnly:      aqlRequest.DataOnly != 0,
	}
	qc.Compile(memStore, shardOwner)

	for tableName := range qc.TableSchemaByName {
		utils.GetRootReporter().GetChildCounter(map[string]string{
			"table": tableName,
		}, utils.QueryReceived).Inc(1)
	}

	if aqlRequest.Debug > 0 || aqlRequest.Profiling != "" {
		qc.Debug = true
		aqlRequest.Verbose = 1

	}
	qc.Profiling = aqlRequest.Profiling

	// Compilation error, should be bad request
	if qc.Error != nil {
		statusCode = http.StatusBadRequest
		return
	}

	// Find a device that meets the resource requirement of this query
	// Use query specified device as hint
	qc.FindDeviceForQuery(memStore, aqlRequest.Device, deviceManager, aqlRequest.DeviceChoosingTimeout)
	// Unable to find a device for the query.
	if qc.Error != nil {
		// Unable to fulfill this request due to resource not available, clients need to try sometimes later.
		statusCode = http.StatusServiceUnavailable
		return
	}
	defer deviceManager.ReleaseReservedMemory(qc.Device, qc.Query)
	// Execute.
	qc.ProcessQuery(memStore)
	if qc.Error != nil {
		utils.GetQueryLogger().With(
			"error", qc.Error,
			"query", aqlQuery,
			"context", qc,
		).Error("Error happened when processing query")
		statusCode = http.StatusInternalServerError
	} else {
		// Report
		utils.GetRootReporter().GetChildCounter(map[string]string{
			"table": aqlQuery.Table,
		}, utils.QueryRowsReturned).Inc(int64(qc.ResultsRowsFlushed()))
	}
	return
}

func getReponseWriter(returnHLL bool, nQueries int) QueryResponseWriter {
	if returnHLL {
		return NewHLLQueryResponseWriter()
	}
	return NewJSONQueryResponseWriter(nQueries)
}

// QueryResponseWriter defines the interface to write query result and error to final response.
type QueryResponseWriter interface {
	ReportError(queryIndex int, table string, err error, statusCode int)
	ReportQueryContext(*query.AQLQueryContext)
	ReportResult(int, *query.AQLQueryContext)
	Respond(w http.ResponseWriter)
	GetStatusCode() int
}

// JSONQueryResponseWriter writes query result as json.
type JSONQueryResponseWriter struct {
	response   queryCom.AQLResponse
	statusCode int
}

// NewJSONQueryResponseWriter creates a new JSONQueryResponseWriter.
func NewJSONQueryResponseWriter(nQueries int) QueryResponseWriter {
	return &JSONQueryResponseWriter{
		response: queryCom.AQLResponse{
			Results: make([]queryCom.AQLQueryResult, nQueries),
		},
		statusCode: http.StatusOK,
	}
}

// ReportError writes the error of the query to the response.
func (w *JSONQueryResponseWriter) ReportError(queryIndex int, table string, err error, statusCode int) {
	// Usually larger status code means more severe problem.
	if statusCode > w.statusCode {
		w.statusCode = statusCode
	}
	if w.response.Errors == nil {
		w.response.Errors = make([]error, len(w.response.Results))
	}
	w.response.Errors[queryIndex] = err
	utils.GetRootReporter().GetChildCounter(map[string]string{
		"table": table,
	}, utils.QueryFailed).Inc(1)
}

// ReportQueryContext writes the query context to the response.
func (w *JSONQueryResponseWriter) ReportQueryContext(qc *query.AQLQueryContext) {
	bs, _ := json.Marshal(qc)
	w.response.QueryContext = append(w.response.QueryContext, string(bs))
}

// ReportResult writes the query result to the response.
func (w *JSONQueryResponseWriter) ReportResult(queryIndex int, qc *query.AQLQueryContext) {
	qc.Postprocess()
	if qc.Error != nil {
		w.ReportError(queryIndex, qc.Query.Table, qc.Error, http.StatusInternalServerError)
	}
	w.response.Results[queryIndex] = qc.Results
}

// Respond writes the final response into ResponseWriter.
func (w *JSONQueryResponseWriter) Respond(rw http.ResponseWriter) {
	apiCom.RespondJSONObjectWithCode(rw, w.statusCode, w.response)
}

// GetStatusCode returns the status code written into response.
func (w *JSONQueryResponseWriter) GetStatusCode() int {
	return w.statusCode
}

// HLLQueryResponseWriter writes query result as application/hll. For more inforamtion, please refer to
// https://github.com/uber/aresdb/wiki/HyperLogLog.
type HLLQueryResponseWriter struct {
	response   *queryCom.HLLQueryResults
	statusCode int
}

// NewHLLQueryResponseWriter creates a new HLLQueryResponseWriter.
func NewHLLQueryResponseWriter() QueryResponseWriter {
	w := HLLQueryResponseWriter{
		response:   queryCom.NewHLLQueryResults(),
		statusCode: http.StatusOK,
	}
	return &w
}

// ReportError writes the error of the query to the response.
func (w *HLLQueryResponseWriter) ReportError(queryIndex int, table string, err error, statusCode int) {
	if statusCode > w.statusCode {
		w.statusCode = statusCode
	}
	w.response.WriteError(err)
}

// ReportQueryContext writes the query context to the response. Since the format of application/hll is not
// designed for human reading, we will ignore storing query context in response for now.
func (w *HLLQueryResponseWriter) ReportQueryContext(qc *query.AQLQueryContext) {
}

// ReportResult writes the query result to the response.
func (w *HLLQueryResponseWriter) ReportResult(queryIndex int, qc *query.AQLQueryContext) {
	w.response.WriteResult(qc.HLLQueryResult)
}

// Respond writes the final response into ResponseWriter.
func (w *HLLQueryResponseWriter) Respond(rw http.ResponseWriter) {
	rw.Header().Set("Content-Type", utils.HTTPContentTypeHyperLogLog)
	apiCom.RespondBytesWithCode(rw, w.statusCode, w.response.GetBytes())
}

// GetStatusCode returns the status code written into response.
func (w *HLLQueryResponseWriter) GetStatusCode() int {
	return w.statusCode
}

// for now we only eager flush when
//    1. there's only 1 query in the request
//    2. the query is non aggregate query
func canEagerFlush(queries []queryCom.AQLQuery) bool {
	if len(queries) != 1 {
		return false
	}

	aqlQuery := queries[0]
	return len(aqlQuery.Measures) == 1 && aqlQuery.Measures[0].Expr == "1"
}
