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
	"net/http"
	"time"
	"github.com/uber/aresdb/query"
	"github.com/uber/aresdb/utils"
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
	var qcs []*query.AQLQueryContext
	var statusCode int

	defer func() {
		var errStr string
		if err != nil {
			errStr = err.Error()
		}

		l := utils.GetQueryLogger().With(
			"error", errStr,
			"request", sqlRequest,
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

	if err = ReadRequest(r, &sqlRequest); err != nil {
		statusCode = http.StatusBadRequest
		RespondWithBadRequest(w, err)
		return
	}

	reqContext := RequestContext{
		Device: sqlRequest.Device,
		Verbose: sqlRequest.Verbose,
		Debug: sqlRequest.Debug,
		Profiling: sqlRequest.Profiling,
		DeviceChoosingTimeout: sqlRequest.DeviceChoosingTimeout,
		Accept: sqlRequest.Accept,
		Origin: sqlRequest.Origin,
	}

	if sqlRequest.Body.Queries == nil {
		statusCode = http.StatusBadRequest
		RespondWithBadRequest(w, utils.APIError{
			Code:    http.StatusBadRequest,
			Message: ErrMsgMissingParameter,
		})
		return
	}

	returnHLL := sqlRequest.Accept == ContentTypeHyperLogLog
	requestResponseWriter := getReponseWriter(returnHLL, len(sqlRequest.Body.Queries))

	queryTimer := utils.GetRootReporter().GetTimer(utils.QueryLatency)
	start := utils.Now()
	var parsedAQLQuery *query.AQLQuery
	var qc *query.AQLQueryContext
	for i, sqlQuery := range sqlRequest.Body.Queries {
		parsedAQLQuery, err = query.Parse(sqlQuery, utils.GetLogger())
		if err != nil {
			statusCode = http.StatusBadRequest
			requestResponseWriter.ReportError(i, "", err, statusCode)
		}

		qc, statusCode = handleQuery(handler.memStore, handler.deviceManager, reqContext, *parsedAQLQuery)
		if reqContext.Verbose > 0 {
			requestResponseWriter.ReportQueryContext(qc)
		}
		if qc.Error != nil {
			requestResponseWriter.ReportError(i, qc.Query.Table, qc.Error, statusCode)
		} else {
			requestResponseWriter.ReportResult(i, qc)
			qc.ReleaseHostResultsBuffers()
			utils.GetRootReporter().GetChildCounter(map[string]string{
				"table": qc.Query.Table,
			}, utils.QuerySucceeded).Inc(1)
		}

		qcs = append(qcs, qc)
	}
	duration = utils.Now().Sub(start)
	queryTimer.Record(duration)
	requestResponseWriter.Respond(w)
	statusCode = requestResponseWriter.GetStatusCode()
}
