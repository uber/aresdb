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
	"github.com/gorilla/mux"
	apiCom "github.com/uber/aresdb/api/common"
	"github.com/uber/aresdb/broker/common"
	"github.com/uber/aresdb/utils"
	"net/http"
)

type QueryHandler struct {
	exec common.QueryExecutor
}

func NewQueryHandler(executor common.QueryExecutor) QueryHandler {
	return QueryHandler{
		exec: executor,
	}
}

func (handler *QueryHandler) Register(router *mux.Router, wrappers ...utils.HTTPHandlerWrapper) {
	router.HandleFunc("/", utils.ApplyHTTPWrappers(handler.HandleQuery, wrappers)).Methods(http.MethodPost)
}

func (handler *QueryHandler) HandleQuery(w http.ResponseWriter, r *http.Request) {
	var queryReqeust BrokerQueryRequest
	err := apiCom.ReadRequest(r, &queryReqeust)
	if err != nil {
		apiCom.RespondWithError(w, err)
		return
	}

	err = handler.exec.Execute(context.TODO(), queryReqeust.Body.Query, w)
	if err != nil {
		apiCom.RespondWithError(w, err)
		return
	}
	// TODO: logging and metrics
	return
}

// SQLRequest represents SQL query request. Debug mode will
// run **each batch** in synchronized mode and report time
// for each step.
// swagger:parameters querySQL
type BrokerQueryRequest struct {
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
