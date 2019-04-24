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
	"github.com/uber/aresdb/query"
	queryCom "github.com/uber/aresdb/query/common"
	"github.com/uber/aresdb/utils"
	"time"
	"net/http"
)

// HandleSQL swagger:route POST /query/sql querySQL
// query in SQL
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
func (handler *QueryHandler) HandleSQL(w http.ResponseWriter, r *http.Request) {
	sqlRequest := SQLRequest{Device: -1}

	var err error
	var duration time.Duration
	var qc *query.AQLQueryContext
	var statusCode int
	defer func() {
		var errStr string
		if err != nil {
			errStr = err.Error()
		}

		if statusCode == http.StatusOK {
			utils.GetQueryLogger().With(
				"error", errStr,
				"request", sqlRequest,
				"duration", duration,
				"statusCode", statusCode,
				"context", qc,
				"headers", r.Header,
			).Info("Query succeeded")
		} else {
			utils.GetQueryLogger().With(
				"error", errStr,
				"request", sqlRequest,
				"duration", duration,
				"statusCode", statusCode,
				"context", qc,
				"headers", r.Header,
			).Error("Query finished with error")
		}

	}()

	if err = ReadRequest(r, &sqlRequest); err != nil {
		RespondWithBadRequest(w, err)
		return
	}

	// for options only, to reuse handleQuery
	aqlRequest := AQLRequest{
		Device:                sqlRequest.Device,
		Verbose:               sqlRequest.Verbose,
		Debug:                 sqlRequest.Debug,
		Profiling:             sqlRequest.Profiling,
		DeviceChoosingTimeout: sqlRequest.DeviceChoosingTimeout,
		Accept: sqlRequest.Accept,
		Origin: sqlRequest.Origin,
	}

	requestResponseWriter := getSingleResponseWriter(aqlRequest.Accept == ContentTypeHyperLogLog)

	queryTimer := utils.GetRootReporter().GetTimer(utils.QueryLatency)
	start := utils.Now()
	if sqlRequest.Body.Query != "" {
		var parsedAQLQuery *query.AQLQuery
		parsedAQLQuery, err = query.Parse(sqlRequest.Body.Query, utils.GetLogger())
		if err != nil {
			RespondWithBadRequest(w, err)
			return
		}
		qc, statusCode = handleQuery(handler.memStore, handler.deviceManager, aqlRequest, *parsedAQLQuery)
		if aqlRequest.Verbose > 0 {
			requestResponseWriter.ReportQueryContext(qc)
		}
		if qc.Error != nil {
			requestResponseWriter.ReportError(qc.Query.Table, qc.Error, statusCode)
		} else {
			requestResponseWriter.ReportResult(qc)
			qc.ReleaseHostResultsBuffers()
			utils.GetRootReporter().GetChildCounter(map[string]string{
				"table": qc.Query.Table,
			}, utils.QuerySucceeded).Inc(1)
		}
	}
	duration = utils.Now().Sub(start)
	queryTimer.Record(duration)
	requestResponseWriter.Respond(w)
	statusCode = requestResponseWriter.GetStatusCode()
}

func getSingleResponseWriter(returnHLL bool) SingleQueryResponseWriter {
	if returnHLL {
		return NewHLLSingleQueryResponseWriter()
	}
	return NewJSONSingleQueryResponseWriter()
}

// SingleQueryResponseWriter defines the interface to write query result and error to final response.
type SingleQueryResponseWriter interface {
	ReportError(table string, err error, statusCode int)
	ReportQueryContext(*query.AQLQueryContext)
	ReportResult(*query.AQLQueryContext)
	Respond(w http.ResponseWriter)
	GetStatusCode() int
}

// JSONMultiQueryResponseWriter writes query result as json.
type JSONSingleQueryResponseWriter struct {
	response struct {
		Result     queryCom.AQLQueryResult `json:"result"`
		Err        error `json:"error,omitempty"`
		Qc         *query.AQLQueryContext `json:"context,omitempty"`
	}

	statusCode int
}

// NewJSONSingleQueryResponseWriter creates a new JSONMultiQueryResponseWriter.
func NewJSONSingleQueryResponseWriter() SingleQueryResponseWriter {
	return &JSONSingleQueryResponseWriter{
		statusCode: http.StatusOK,
	}
}

// ReportError writes the error of the query to the response.
func (w *JSONSingleQueryResponseWriter) ReportError(table string, err error, statusCode int) {
	// Usually larger status code means more severe problem.
	if statusCode > w.statusCode {
		w.statusCode = statusCode
	}
	w.response.Err = err
	utils.GetRootReporter().GetChildCounter(map[string]string{
		"table": table,
	}, utils.QueryFailed).Inc(1)
}

// ReportQueryContext writes the query context to the response.
func (w *JSONSingleQueryResponseWriter) ReportQueryContext(qc *query.AQLQueryContext) {
	w.response.Qc = qc
}

// ReportResult writes the query result to the response.
func (w *JSONSingleQueryResponseWriter) ReportResult(qc *query.AQLQueryContext) {
	qc.Results = qc.Postprocess()
	if qc.Error != nil {
		w.ReportError(qc.Query.Table, qc.Error, http.StatusInternalServerError)
	}
	w.response.Result = qc.Results
}

// Respond writes the final response into ResponseWriter.
func (w *JSONSingleQueryResponseWriter) Respond(rw http.ResponseWriter) {
	RespondJSONObjectWithCode(rw, w.statusCode, w.response)
}

// GetStatusCode returns the status code written into response.
func (w *JSONSingleQueryResponseWriter) GetStatusCode() int {
	return w.statusCode
}

// HLLMultiQueryResponseWriter writes query result as application/hll. For more inforamtion, please refer to
// https://github.com/uber/aresdb/wiki/HyperLogLog.
type HLLSingleQueryResponseWriter struct {
	response   *query.HLLQueryResults
	statusCode int
}

// NewHLLSingleQueryResponseWriter creates a new HLLMultiQueryResponseWriter.
func NewHLLSingleQueryResponseWriter() SingleQueryResponseWriter {
	w := HLLSingleQueryResponseWriter{
		response:   query.NewHLLQueryResults(),
		statusCode: http.StatusOK,
	}
	return &w
}

// ReportError writes the error of the query to the response.
func (w *HLLSingleQueryResponseWriter) ReportError(table string, err error, statusCode int) {
	if statusCode > w.statusCode {
		w.statusCode = statusCode
	}
	w.response.WriteError(err)
}

// ReportQueryContext writes the query context to the response. Since the format of application/hll is not
// designed for human reading, we will ignore storing query context in response for now.
func (w *HLLSingleQueryResponseWriter) ReportQueryContext(qc *query.AQLQueryContext) {
}

// ReportResult writes the query result to the response.
func (w *HLLSingleQueryResponseWriter) ReportResult(qc *query.AQLQueryContext) {
	w.response.WriteResult(qc.HLLQueryResult)
}

// Respond writes the final response into ResponseWriter.
func (w *HLLSingleQueryResponseWriter) Respond(rw http.ResponseWriter) {
	rw.Header().Set("Content-Type", ContentTypeHyperLogLog)
	RespondBytesWithCode(rw, w.statusCode, w.response.GetBytes())
}

// GetStatusCode returns the status code written into response.
func (w *HLLSingleQueryResponseWriter) GetStatusCode() int {
	return w.statusCode
}
