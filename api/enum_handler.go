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
	"github.com/gorilla/mux"
	"github.com/uber/aresdb/api/common"
	"github.com/uber/aresdb/memstore"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/utils"
	"net/http"
)

// EnumHandler handlers enum rw
type EnumHandler struct {
	memStore  memstore.MemStore
	metastore metaCom.MetaStore
}

// NewEnumHandler returns a new enum handler
func NewEnumHandler(memStore memstore.MemStore, metastore metaCom.MetaStore) *EnumHandler {
	return &EnumHandler{
		memStore:  memStore,
		metastore: metastore,
	}
}

// Register regists paths
func (handler *EnumHandler) Register(router *mux.Router, wrappers ...utils.HTTPHandlerWrapper) {
	router.HandleFunc("/tables/{table}/columns/{column}/enum-cases", utils.ApplyHTTPWrappers(handler.ListEnumCases, wrappers)).Methods(http.MethodGet)
	router.HandleFunc("/tables/{table}/columns/{column}/enum-cases", utils.ApplyHTTPWrappers(handler.AddEnumCase, wrappers)).Methods(http.MethodPost)
}

// ListEnumCases swagger:route GET /schema/tables/{table}/columns/{column}/enum-cases listEnumCases
// list existing enumCases for given table and column
//
// Responses:
//    default: errorResponse
//        200: listEnumCasesResponse
func (handler *EnumHandler) ListEnumCases(w http.ResponseWriter, r *http.Request) {
	var listEnumCasesRequest ListEnumCasesRequest
	var listEnumCasesResponse ListEnumCasesResponse

	err := common.ReadRequest(r, &listEnumCasesRequest)
	if err != nil {
		common.RespondWithError(w, err)
		return
	}

	tableSchema, err := handler.memStore.GetSchema(listEnumCasesRequest.TableName)
	if err != nil {
		common.RespondWithError(w, ErrTableDoesNotExist)
		return
	}

	tableSchema.RLock()
	enumDict, columnExist := tableSchema.EnumDicts[listEnumCasesRequest.ColumnName]
	if !columnExist {
		tableSchema.RUnlock()
		common.RespondWithError(w, ErrColumnDoesNotExist)
		return
	}

	listEnumCasesResponse.JSONBuffer, err = json.Marshal(enumDict.ReverseDict)
	tableSchema.RUnlock()

	common.RespondWithJSONBytes(w, listEnumCasesResponse.JSONBuffer, err)
}

// AddEnumCase swagger:route POST /schema/tables/{table}/columns/{column}/enum-cases addEnumCase
// add an enum case to given column of given table
// return the id of the enum
//
// Responses:
//    default: errorResponse
//        200: addEnumCaseResponse
func (handler *EnumHandler) AddEnumCase(w http.ResponseWriter, r *http.Request) {
	var addEnumCaseRequest AddEnumCaseRequest
	var addEnumCaseResponse AddEnumCaseResponse

	err := common.ReadRequest(r, &addEnumCaseRequest)
	if err != nil {
		common.RespondWithError(w, err)
		return
	}

	addEnumCaseResponse.Body, err = handler.metastore.ExtendEnumDict(addEnumCaseRequest.TableName, addEnumCaseRequest.ColumnName, addEnumCaseRequest.Body.EnumCases)
	if err != nil {
		// TODO: need mapping from metaStore error to api error
		// for metaStore error might also be user error
		common.RespondWithError(w, err)
		return
	}

	common.RespondWithJSONObject(w, addEnumCaseResponse.Body)
}
