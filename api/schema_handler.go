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
	"github.com/uber/aresdb/metastore"
	"net/http"

	apiCom "github.com/uber/aresdb/api/common"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/utils"

	"github.com/gorilla/mux"
)

// SchemaHandler handles schema http requests.
type SchemaHandler struct {
	// all write requests will go to metaStore.
	metaStore metaCom.MetaStore
}

// NewSchemaHandler will create a new SchemaHandler with memStore and metaStore.
func NewSchemaHandler(metaStore metaCom.MetaStore) *SchemaHandler {
	return &SchemaHandler{
		metaStore: metaStore,
	}
}

// Register registers http handlers.
func (handler *SchemaHandler) Register(router *mux.Router, wrappers ...utils.HTTPHandlerWrapper2) {
	router.HandleFunc("/tables", utils.ApplyHTTPWrappers2(handler.ListTables, wrappers...)).Methods(http.MethodGet)
	router.HandleFunc("/tables", utils.ApplyHTTPWrappers2(handler.AddTable, wrappers...)).Methods(http.MethodPost)
	router.HandleFunc("/tables/{table}", utils.ApplyHTTPWrappers2(handler.GetTable, wrappers...)).Methods(http.MethodGet)
	router.HandleFunc("/tables/{table}", utils.ApplyHTTPWrappers2(handler.DeleteTable, wrappers...)).Methods(http.MethodDelete)
	router.HandleFunc("/tables/{table}", utils.ApplyHTTPWrappers2(handler.UpdateTableConfig, wrappers...)).Methods(http.MethodPut)
	router.HandleFunc("/tables/{table}/columns", utils.ApplyHTTPWrappers2(handler.AddColumn, wrappers...)).Methods(http.MethodPost)
	router.HandleFunc("/tables/{table}/columns/{column}", utils.ApplyHTTPWrappers2(handler.UpdateColumn, wrappers...)).Methods(http.MethodPut)
	router.HandleFunc("/tables/{table}/columns/{column}", utils.ApplyHTTPWrappers2(handler.DeleteColumn, wrappers...)).Methods(http.MethodDelete)
}

// RegisterForDebug register handlers for debug port
func (handler *SchemaHandler) RegisterForDebug(router *mux.Router, wrappers ...utils.HTTPHandlerWrapper2) {
	router.HandleFunc("/tables", utils.ApplyHTTPWrappers2(handler.ListTables, wrappers...)).Methods(http.MethodGet)
	router.HandleFunc("/tables/{table}", utils.ApplyHTTPWrappers2(handler.GetTable, wrappers...)).Methods(http.MethodGet)
}

// ListTables swagger:route GET /schema/tables listTables
// List all table schemas
// Consumes:
//    - application/json
//
// Produces:
//    - application/json
//
// Responses:
//    default: errorResponse
//        200: stringArrayResponse
func (handler *SchemaHandler) ListTables(w *utils.ResponseWriter, r *http.Request) {

	response := apiCom.NewStringArrayResponse()

	tables, err := handler.metaStore.ListTables()
	if err != nil {
		w.WriteError(err)
		return
	}
	for _, tableName := range tables {
		response.Body = append(response.Body, tableName)
	}

	w.WriteObject(response.Body)
}

// GetTable swagger:route GET /schema/tables/{table} getTable
// get the table schema for specific table name
//
// Consumes:
//    - application/json
//
// Produces:
//    - application/json
//
// Responses:
//    default: errorResponse
//        200: getTableResponse
func (handler *SchemaHandler) GetTable(w *utils.ResponseWriter, r *http.Request) {
	var getTableRequest GetTableRequest
	var getTableResponse GetTableResponse

	err := apiCom.ReadRequest(r, &getTableRequest, w)
	if err != nil {
		w.WriteError(err)
		return
	}

	table, err := handler.metaStore.GetTable(getTableRequest.TableName)
	if err != nil {
		if err.Error() == metaCom.ErrTableDoesNotExist.Error() {
			w.WriteErrorWithCode(http.StatusNotFound, err)
			return
		}
	}
	getTableResponse.JSONBuffer, err = json.Marshal(table)
	w.WriteJSONBytes(getTableResponse.JSONBuffer, err)
}

// AddTable swagger:route POST /schema/tables addTable
// add table to table collections
//
// Consumes:
//    - application/json
//
// Responses:
//    default: errorResponse
//        200: noContentResponse
func (handler *SchemaHandler) AddTable(w *utils.ResponseWriter, r *http.Request) {

	var addTableRequest AddTableRequest
	// add default table configs first
	addTableRequest.Body.Config = metastore.DefaultTableConfig
	err := apiCom.ReadRequest(r, &addTableRequest, w)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	newTable := addTableRequest.Body
	err = handler.metaStore.CreateTable(&newTable)
	if err != nil {
		w.WriteError(err)
		return
	}
	w.WriteObject(nil)
}

// UpdateTableConfig swagger:route PUT /schema/tables/{table} updateTableConfig
// update config of the specified table
//
// Consumes:
//    - application/json
//
// Responses:
//    default: errorResponse
//        200: noContentResponse
func (handler *SchemaHandler) UpdateTableConfig(w *utils.ResponseWriter, r *http.Request) {
	var request UpdateTableConfigRequest
	err := apiCom.ReadRequest(r, &request, w)
	if err != nil {
		w.WriteError(err)
		return
	}

	err = handler.metaStore.UpdateTableConfig(request.TableName, request.Body)
	if err != nil {
		w.WriteError(err)
		return
	}
	w.WriteObject(nil)
}

// DeleteTable swagger:route DELETE /schema/tables/{table} deleteTable
// delete table from metaStore
//
// Responses:
//    default: errorResponse
//        200: noContentResponse
func (handler *SchemaHandler) DeleteTable(w *utils.ResponseWriter, r *http.Request) {
	var deleteTableRequest DeleteTableRequest
	err := apiCom.ReadRequest(r, &deleteTableRequest, w)
	if err != nil {
		w.WriteError(err)
		return
	}

	err = handler.metaStore.DeleteTable(deleteTableRequest.TableName)
	if err != nil {
		// TODO: need mapping from metaStore error to api error
		/// for metaStore error might also be user error
		w.WriteError(err)
		return
	}

	w.WriteObject(nil)
}

// AddColumn swagger:route POST /schema/tables/{table}/columns addColumn
// add a single column to existing table
//
// Consumes:
//    - application/json
//
// Responses:
//    default: errorResponse
//        200: noContentResponse
func (handler *SchemaHandler) AddColumn(w *utils.ResponseWriter, r *http.Request) {
	var addColumnRequest AddColumnRequest
	err := apiCom.ReadRequest(r, &addColumnRequest, w)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	err = handler.metaStore.AddColumn(addColumnRequest.TableName, addColumnRequest.Body.Column, addColumnRequest.Body.AddToArchivingSortOrder)
	// TODO: validate column
	// might better do in metaStore and here needs to return either user error or server error
	if err != nil {
		// TODO: need mapping from metaStore error to api error
		/// for metaStore error might also be user error
		w.WriteError(err)
		return
	}

	w.WriteObject(nil)
}

// UpdateColumn swagger:route PUT /schema/tables/{table}/columns/{column} updateColumn
// update specified column
//
// Consumes:
//    - application/json
//
// Responses:
//    default: errorResponse
//        200: noContentResponse
func (handler *SchemaHandler) UpdateColumn(w *utils.ResponseWriter, r *http.Request) {
	var updateColumnRequest UpdateColumnRequest
	err := apiCom.ReadRequest(r, &updateColumnRequest, w)
	if err != nil {
		w.WriteError(err)
		return
	}

	if err = handler.metaStore.UpdateColumn(updateColumnRequest.TableName,
		updateColumnRequest.ColumnName, updateColumnRequest.Body); err != nil {
		// TODO: need mapping from metaStore error to api error
		// for metaStore error might also be user error
		w.WriteError(err)
		return
	}

	w.WriteObject(nil)
}

// DeleteColumn swagger:route DELETE /schema/tables/{table}/columns/{column} deleteColumn
// delete columns from existing table
//
// Responses:
//    default: errorResponse
//        200: noContentResponse
func (handler *SchemaHandler) DeleteColumn(w *utils.ResponseWriter, r *http.Request) {
	var deleteColumnRequest DeleteColumnRequest

	err := apiCom.ReadRequest(r, &deleteColumnRequest, w)
	if err != nil {
		w.WriteError(err)
		return
	}

	err = handler.metaStore.DeleteColumn(deleteColumnRequest.TableName, deleteColumnRequest.ColumnName)
	// TODO: validate whether table exists and specified columns does not belong to primary key or time column
	// might be better for metaStore to do this and return specified error type
	if err != nil {
		// TODO: need mapping from metaStore error to api error
		// for metaStore error might also be user error
		w.WriteError(err)
		return
	}

	w.WriteObject(nil)
}
