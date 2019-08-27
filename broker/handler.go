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

package broker

import (
	"context"
	"fmt"
	"github.com/gorilla/mux"
	apiCom "github.com/uber/aresdb/api/common"
	"github.com/uber/aresdb/broker/common"
	queryCom "github.com/uber/aresdb/query/common"
	"github.com/uber/aresdb/query/sql"
	"github.com/uber/aresdb/utils"
	"net/http"
	"sync/atomic"
)

type QueryHandler struct {
	exec          common.QueryExecutor
	nextRequestID int64
	instanceID    string
}

func NewQueryHandler(executor common.QueryExecutor, instanceID string) QueryHandler {
	return QueryHandler{
		exec:       executor,
		instanceID: instanceID,
	}
}

func (handler *QueryHandler) Register(router *mux.Router, wrappers ...utils.HTTPHandlerWrapper) {
	router.HandleFunc("/sql", utils.ApplyHTTPWrappers(handler.HandleSQL, wrappers)).Methods(http.MethodPost)
	router.HandleFunc("/aql", utils.ApplyHTTPWrappers(handler.HandleAQL, wrappers)).Methods(http.MethodPost)
}

func (handler *QueryHandler) HandleSQL(w http.ResponseWriter, r *http.Request) {
	utils.GetRootReporter().GetCounter(utils.SQLQueryReceivedBroker).Inc(1)
	var queryReqeust BrokerSQLRequest

	start := utils.Now()
	var err error
	defer func() {
		duration := utils.Now().Sub(start)
		utils.GetRootReporter().GetTimer(utils.QueryLatencyBroker).Record(duration)
		if err != nil {
			utils.GetRootReporter().GetCounter(utils.QueryFailedBroker).Inc(1)
			utils.GetLogger().With(
				"error", err,
				"request", queryReqeust).Error("Error happened when processing request")
		} else {
			utils.GetRootReporter().GetCounter(utils.QuerySucceededBroker).Inc(1)
			utils.GetLogger().With("request", queryReqeust).Info("Request succeeded")
		}
	}()

	err = apiCom.ReadRequest(r, &queryReqeust)
	if err != nil {
		apiCom.RespondWithError(w, err)
		return
	}

	sqlParseStart := utils.Now()
	var aql *queryCom.AQLQuery
	aql, err = sql.Parse(queryReqeust.Body.Query, utils.GetLogger())
	utils.GetRootReporter().GetTimer(utils.SQLParsingLatencyBroker).Record(utils.Now().Sub(sqlParseStart))
	if err != nil {
		apiCom.RespondWithError(w, err)
		return
	}

	err = handler.exec.Execute(context.Background(), handler.getReqestID(), aql, queryReqeust.Accept == utils.HTTPContentTypeHyperLogLog, w)
	if err != nil {
		apiCom.RespondWithError(w, err)
		return
	}
	return
}

func (handler *QueryHandler) HandleAQL(w http.ResponseWriter, r *http.Request) {
	var queryReqeust BrokerAQLRequest
	utils.GetRootReporter().GetCounter(utils.AQLQueryReceivedBroker).Inc(1)

	start := utils.Now()
	var err error
	defer func() {
		duration := utils.Now().Sub(start)
		utils.GetRootReporter().GetTimer(utils.QueryLatencyBroker).Record(duration)
		if err != nil {
			utils.GetRootReporter().GetCounter(utils.QueryFailedBroker).Inc(1)
			utils.GetLogger().With(
				"error", err,
				"request", queryReqeust).Error("Error happened when processing request")
		} else {
			utils.GetRootReporter().GetCounter(utils.QuerySucceededBroker).Inc(1)
			utils.GetLogger().With("request", queryReqeust).Info("Request succeeded")
		}
	}()

	err = apiCom.ReadRequest(r, &queryReqeust)
	if err != nil {
		apiCom.RespondWithError(w, err)
		return
	}

	err = handler.exec.Execute(context.TODO(), handler.getReqestID(), &queryReqeust.Body.Query, queryReqeust.Accept == utils.HTTPContentTypeHyperLogLog, w)
	if err != nil {
		apiCom.RespondWithError(w, err)
		return
	}
	return
}

func (handler *QueryHandler) getReqestID() string {
	newID := atomic.AddInt64(&handler.nextRequestID, 1)
	return fmt.Sprintf("%s_%d", handler.instanceID, newID)
}

// BrokerSQLRequest represents SQL query request. Debug mode will
// run **each batch** in synchronized mode and report time
// for each step.
// swagger:parameters querySQL
type BrokerSQLRequest struct {
	// in: query
	Verbose int `query:"verbose,optional" json:"verbose"`
	// in: query
	Debug int `query:"debug,optional" json:"debug"`
	// in: header
	Accept string `header:"Accept,optional" json:"accept"`
	// in: header
	Origin string `header:"Rpc-Caller,optional" json:"origin"`
	// in: body
	Body struct {
		Query string `json:"query"`
	} `body:""`
}

// BrokerAQLRequest represents AQL query request. Debug mode will
// run **each batch** in synchronized mode and report time
// for each step.
// swagger:parameters querySQL
type BrokerAQLRequest struct {
	// in: query
	Verbose int `query:"verbose,optional" json:"verbose"`
	// in: query
	Debug int `query:"debug,optional" json:"debug"`
	// in: header
	Accept string `header:"Accept,optional" json:"accept"`
	// in: header
	Origin string `header:"Rpc-Caller,optional" json:"origin"`
	// in: body
	Body struct {
		Query queryCom.AQLQuery `json:"query"`
	} `body:""`
}
