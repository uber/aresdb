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

	"github.com/gorilla/mux"
	"github.com/uber-common/bark"
	"github.com/uber/aresdb/common"
	"time"
)

// QueryHandler handles query execution.
type QueryHandler struct {
	memStore     memstore.MemStore
	deviceManger *query.DeviceManager
}

// NewQueryHandler creates a new QueryHandler.
func NewQueryHandler(memStore memstore.MemStore, cfg common.QueryConfig) *QueryHandler {
	return &QueryHandler{
		memStore:     memStore,
		deviceManger: query.NewDeviceManager(cfg),
	}
}

// GetDeviceManager returns the device manager of query handler.
func (handler *QueryHandler) GetDeviceManager() *query.DeviceManager {
	return handler.deviceManger
}

// Register registers http handlers.
func (handler *QueryHandler) Register(router *mux.Router, wrappers ...utils.HTTPHandlerWrapper) {
	router.HandleFunc("/aql", utils.ApplyHTTPWrappers(handler.HandleAQL, wrappers)).Methods(http.MethodGet, http.MethodPost)
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
	var err error
	var duration time.Duration
	var qcs []*query.AQLQueryContext
	var statusCode int

	defer func() {
		var errStr string
		if err != nil {
			errStr = err.Error()
		}

		l := utils.GetQueryLogger().WithFields(bark.Fields{
			"error":             errStr,
			"request":           aqlRequest,
			"queries_enabled_":  aqlRequest.Body.Queries,
			"duration":          duration,
			"statusCode":        statusCode,
			"contexts_enabled_": qcs,
			"headers":           r.Header,
		})

		if statusCode == http.StatusOK {
			l.Info("All queries succeeded")
		} else {
			l.Error("Some of the queries finished with error")
		}

	}()

	if err = ReadRequest(r, &aqlRequest); err != nil {
		statusCode = http.StatusBadRequest
		RespondWithBadRequest(w, err)
		return
	}

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

	// -1 means no deviceChoosingTimeout setting from request.
	deviceChoosingTimeout := -1
	if aqlRequest.DeviceChoosingTimeout > 0 {
		deviceChoosingTimeout = aqlRequest.DeviceChoosingTimeout
	}

	returnHLL := r.Header.Get("Accept") == ContentTypeHyperLogLog

	queryResponseWriter := getReponseWriter(returnHLL, len(aqlRequest.Body.Queries))
	queryTimer := utils.GetRootReporter().GetTimer(utils.QueryLatency)
	start := utils.Now()
	for i, q := range aqlRequest.Body.Queries {
		// Compile
		qc := q.Compile(handler.memStore, returnHLL)
		qcs = append(qcs, qc)

		for tableName := range qc.TableSchemaByName {
			utils.GetRootReporter().GetChildCounter(map[string]string{
				"table": tableName,
			}, utils.QueryReceived).Inc(1)
		}

		if aqlRequest.Verbose > 0 {
			queryResponseWriter.ReportQueryContext(qc)
		}

		if aqlRequest.Debug > 0 || aqlRequest.Profiling != "" {
			qc.Debug = true
		}
		qc.Profiling = aqlRequest.Profiling

		// Compilation error, should be bad request
		if qc.Error != nil {
			queryResponseWriter.ReportError(i, q.Table, qc.Error, http.StatusBadRequest)
			continue
		}

		// Find a device that meets the resource requirement of this query
		// Use query specified device as hint
		qc.FindDeviceForQuery(handler.memStore, aqlRequest.Device, handler.deviceManger, int(deviceChoosingTimeout))
		// Unable to find a device for the query.
		if qc.Error != nil {
			// Unable to fulfill this request due to resource not available, clients need to try sometimes later.
			queryResponseWriter.ReportError(i, q.Table, qc.Error, http.StatusServiceUnavailable)
			continue
		}
		defer handler.deviceManger.ReleaseReservedMemory(qc.Device, qc.Query)
		// Execute.
		qc.ProcessQuery(handler.memStore)
		if qc.Error != nil {
			utils.GetQueryLogger().WithFields(bark.Fields{
				"error":   qc.Error,
				"request": aqlRequest,
				"context": qc,
			}).Error("Error happened when processing query")
			queryResponseWriter.ReportError(i, q.Table, qc.Error, http.StatusInternalServerError)
		} else {
			// Postprocess
			utils.GetRootReporter().GetChildCounter(map[string]string{
				"table": q.Table,
			}, utils.QueryRowsReturned).Inc(int64(qc.OOPK.ResultSize))

			queryResponseWriter.ReportResult(i, qc)
			qc.ReleaseHostResultsBuffers()
			utils.GetRootReporter().GetChildCounter(map[string]string{
				"table": q.Table,
			}, utils.QuerySucceeded).Inc(1)
		}
	}
	duration = utils.Now().Sub(start)
	queryTimer.Record(duration)
	queryResponseWriter.Respond(w)
	statusCode = queryResponseWriter.GetStatusCode()
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
	response   query.AQLResponse
	statusCode int
}

// NewJSONQueryResponseWriter creates a new JSONQueryResponseWriter.
func NewJSONQueryResponseWriter(nQueries int) QueryResponseWriter {
	return &JSONQueryResponseWriter{
		response: query.AQLResponse{
			Results: make([]queryCom.AQLTimeSeriesResult, nQueries),
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
	w.response.QueryContext = append(w.response.QueryContext, qc)
}

// ReportResult writes the query result to the response.
func (w *JSONQueryResponseWriter) ReportResult(queryIndex int, qc *query.AQLQueryContext) {
	qc.Results = qc.Postprocess()
	w.response.Results[queryIndex] = qc.Results
}

// Respond writes the final response into ResponseWriter.
func (w *JSONQueryResponseWriter) Respond(rw http.ResponseWriter) {
	RespondJSONObjectWithCode(rw, w.statusCode, w.response)
}

// GetStatusCode returns the status code written into response.
func (w *JSONQueryResponseWriter) GetStatusCode() int {
	return w.statusCode
}

// HLLQueryResponseWriter writes query result as application/hll. For more inforamtion, please refer to
// https://github.com/uber/aresdb/wiki/HyperLogLog.
type HLLQueryResponseWriter struct {
	response   *query.HLLQueryResults
	statusCode int
}

// NewHLLQueryResponseWriter creates a new HLLQueryResponseWriter.
func NewHLLQueryResponseWriter() QueryResponseWriter {
	w := HLLQueryResponseWriter{
		response:   query.NewHLLQueryResults(),
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
	rw.Header().Set("Content-Type", ContentTypeHyperLogLog)
	RespondBytesWithCode(rw, w.statusCode, w.response.GetBytes())
}

// GetStatusCode returns the status code written into response.
func (w *HLLQueryResponseWriter) GetStatusCode() int {
	return w.statusCode
}
