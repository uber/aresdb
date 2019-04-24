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
	"net/http"

	"github.com/uber/aresdb/memstore"
	"github.com/uber/aresdb/query"
	queryCom "github.com/uber/aresdb/query/common"
	"github.com/uber/aresdb/utils"

	"time"

	"github.com/gorilla/mux"
	"github.com/uber/aresdb/common"
)

// QueryHandler handles query execution.
type QueryHandler struct {
	memStore      memstore.MemStore
	deviceManager *query.DeviceManager
}

// NewQueryHandler creates a new QueryHandler.
func NewQueryHandler(memStore memstore.MemStore, cfg common.QueryConfig) *QueryHandler {
	return &QueryHandler{
		memStore:      memStore,
		deviceManager: query.NewDeviceManager(cfg),
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
	aqlRequest := AQLRequest{Device: -1}

	if err := ReadRequest(r, &aqlRequest); err != nil {
		RespondWithBadRequest(w, err)
		utils.GetLogger().With(
			"error", err,
			"statusCode", http.StatusBadRequest,
		).Error("failed to parse query")
		return
	}

	handler.handleAQLInternal(aqlRequest, w, r)
}

func (handler *QueryHandler) handleAQLInternal(aqlRequest AQLRequest, w http.ResponseWriter, r *http.Request) {
	var err error
	var duration time.Duration
	var qcs []*query.AQLQueryContext
	var statusCode int

	defer func() {
		var errStr string
		if err != nil {
			errStr = err.Error()
		}

		if statusCode == http.StatusOK {
			utils.GetLogger().With(
				"error", errStr,
				"request", aqlRequest,
				"queries_enabled_", aqlRequest.Body.Queries,
				"duration", duration,
				"statusCode", statusCode,
				"contexts_enabled_", qcs,
				"headers", r.Header,
			).Info("All queries succeeded")
		} else {
			utils.GetLogger().With(
				"error", errStr,
				"request", aqlRequest,
				"queries_enabled_", aqlRequest.Body.Queries,
				"duration", duration,
				"statusCode", statusCode,
				"contexts_enabled_", qcs,
				"headers", r.Header,
			).Error("Some of the queries finished with error")
		}

	}()

	if aqlRequest.Query != "" {
		// Override from query parameter
		err = json.Unmarshal([]byte(aqlRequest.Query), &aqlRequest.Body)
		if err != nil {
			statusCode = http.StatusBadRequest
			RespondWithBadRequest(w, utils.APIError{
				Code:    http.StatusBadRequest,
				Message: ErrMsgFailedToUnmarshalRequest,
				Cause:   err,
			})
			return
		}
	}

	if aqlRequest.Body.Queries == nil {
		statusCode = http.StatusBadRequest
		RespondWithBadRequest(w, utils.APIError{
			Code:    http.StatusBadRequest,
			Message: ErrMsgMissingParameter,
		})
		return
	}

	returnHLL := aqlRequest.Accept == ContentTypeHyperLogLog
	requestResponseWriter := getMultiReponseWriter(returnHLL, len(aqlRequest.Body.Queries))

	queryTimer := utils.GetRootReporter().GetTimer(utils.QueryLatency)
	start := utils.Now()
	var qc *query.AQLQueryContext
	for i, aqlQuery := range aqlRequest.Body.Queries {
		qc, statusCode = handleQuery(handler.memStore, handler.deviceManager, aqlRequest, aqlQuery)
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
	duration = utils.Now().Sub(start)
	queryTimer.Record(duration)
	requestResponseWriter.Respond(w)
	statusCode = requestResponseWriter.GetStatusCode()
	return
}

func handleQuery(memStore memstore.MemStore, deviceManager *query.DeviceManager, aqlRequest AQLRequest, aqlQuery query.AQLQuery) (qc *query.AQLQueryContext, statusCode int) {
	qc = aqlQuery.Compile(memStore, aqlRequest.Accept == ContentTypeHyperLogLog)

	for tableName := range qc.TableSchemaByName {
		utils.GetRootReporter().GetChildCounter(map[string]string{
			"table": tableName,
		}, utils.QueryReceived).Inc(1)
	}

	if aqlRequest.Debug > 0 || aqlRequest.Profiling != "" {
		qc.Debug = true
	}
	qc.Profiling = aqlRequest.Profiling

	// Compilation error, should be bad request
	if qc.Error != nil {
		statusCode = http.StatusBadRequest
		return
	}

	deviceChoosingTimeout := -1
	if aqlRequest.DeviceChoosingTimeout > 0 {
		deviceChoosingTimeout = aqlRequest.DeviceChoosingTimeout
	}
	// Find a device that meets the resource requirement of this query
	// Use query specified device as hint
	qc.FindDeviceForQuery(memStore, aqlRequest.Device, deviceManager, int(deviceChoosingTimeout))
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
		}, utils.QueryRowsReturned).Inc(int64(qc.OOPK.ResultSize))
	}
	return
}

func getMultiReponseWriter(returnHLL bool, nQueries int) MultiQueryResponseWriter {
	if returnHLL {
		return NewHLLMultiQueryResponseWriter()
	}
	return NewJSONMultiQueryResponseWriter(nQueries)
}

// MultiQueryResponseWriter defines the interface to write query result and error to final response.
type MultiQueryResponseWriter interface {
	ReportError(queryIndex int, table string, err error, statusCode int)
	ReportQueryContext(*query.AQLQueryContext)
	ReportResult(int, *query.AQLQueryContext)
	Respond(w http.ResponseWriter)
	GetStatusCode() int
}

// JSONMultiQueryResponseWriter writes query result as json.
type JSONMultiQueryResponseWriter struct {
	response   query.AQLResponse
	statusCode int
}

// NewJSONQueryResponseWriter creates a new JSONMultiQueryResponseWriter.
func NewJSONMultiQueryResponseWriter(nQueries int) MultiQueryResponseWriter {
	return &JSONMultiQueryResponseWriter{
		response: query.AQLResponse{
			Results: make([]queryCom.AQLQueryResult, nQueries),
		},
		statusCode: http.StatusOK,
	}
}

// ReportError writes the error of the query to the response.
func (w *JSONMultiQueryResponseWriter) ReportError(queryIndex int, table string, err error, statusCode int) {
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
func (w *JSONMultiQueryResponseWriter) ReportQueryContext(qc *query.AQLQueryContext) {
	w.response.QueryContext = append(w.response.QueryContext, qc)
}

// ReportResult writes the query result to the response.
func (w *JSONMultiQueryResponseWriter) ReportResult(queryIndex int, qc *query.AQLQueryContext) {
	qc.Results = qc.Postprocess()
	if qc.Error != nil {
		w.ReportError(queryIndex, qc.Query.Table, qc.Error, http.StatusInternalServerError)
	}
	w.response.Results[queryIndex] = qc.Results
}

// Respond writes the final response into ResponseWriter.
func (w *JSONMultiQueryResponseWriter) Respond(rw http.ResponseWriter) {
	RespondJSONObjectWithCode(rw, w.statusCode, w.response)
}

// GetStatusCode returns the status code written into response.
func (w *JSONMultiQueryResponseWriter) GetStatusCode() int {
	return w.statusCode
}

// HLLMultiQueryResponseWriter writes query result as application/hll. For more inforamtion, please refer to
// https://github.com/uber/aresdb/wiki/HyperLogLog.
type HLLMultiQueryResponseWriter struct {
	response   *query.HLLQueryResults
	statusCode int
}

// NewHLLQueryResponseWriter creates a new HLLMultiQueryResponseWriter.
func NewHLLMultiQueryResponseWriter() MultiQueryResponseWriter {
	w := HLLMultiQueryResponseWriter{
		response:   query.NewHLLQueryResults(),
		statusCode: http.StatusOK,
	}
	return &w
}

// ReportError writes the error of the query to the response.
func (w *HLLMultiQueryResponseWriter) ReportError(queryIndex int, table string, err error, statusCode int) {
	if statusCode > w.statusCode {
		w.statusCode = statusCode
	}
	w.response.WriteError(err)
}

// ReportQueryContext writes the query context to the response. Since the format of application/hll is not
// designed for human reading, we will ignore storing query context in response for now.
func (w *HLLMultiQueryResponseWriter) ReportQueryContext(qc *query.AQLQueryContext) {
}

// ReportResult writes the query result to the response.
func (w *HLLMultiQueryResponseWriter) ReportResult(queryIndex int, qc *query.AQLQueryContext) {
	w.response.WriteResult(qc.HLLQueryResult)
}

// Respond writes the final response into ResponseWriter.
func (w *HLLMultiQueryResponseWriter) Respond(rw http.ResponseWriter) {
	rw.Header().Set("Content-Type", ContentTypeHyperLogLog)
	RespondBytesWithCode(rw, w.statusCode, w.response.GetBytes())
}

// GetStatusCode returns the status code written into response.
func (w *HLLMultiQueryResponseWriter) GetStatusCode() int {
	return w.statusCode
}
