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

	"github.com/uber/aresdb/metastore"
	"github.com/uber/aresdb/utils"

	"fmt"
	"github.com/gorilla/mux"
	memCom "github.com/uber/aresdb/memstore/common"
)

// SchemaHandler handles schema http requests.
type SchemaHandler struct {
	// all write requests will go to metaStore.
	metaStore metastore.MetaStore
	// if isClusterMode, disable all write operations from handler
	isClusterMode bool
}

// NewSchemaHandler will create a new SchemaHandler with memStore and metaStore.
func NewSchemaHandler(metaStore metastore.MetaStore, isClusterMode bool) *SchemaHandler {
	return &SchemaHandler{
		metaStore:     metaStore,
		isClusterMode: isClusterMode,
	}
}

// Register registers http handlers.
func (handler *SchemaHandler) Register(router *mux.Router, wrappers ...utils.HTTPHandlerWrapper) {
	router.HandleFunc("/tables", utils.ApplyHTTPWrappers(handler.ListTables, wrappers)).Methods(http.MethodGet)
	router.HandleFunc("/tables", utils.ApplyHTTPWrappers(handler.AddTable, wrappers)).Methods(http.MethodPost)
	router.HandleFunc("/tables/{table}", utils.ApplyHTTPWrappers(handler.GetTable, wrappers)).Methods(http.MethodGet)
	router.HandleFunc("/tables/{table}", utils.ApplyHTTPWrappers(handler.DeleteTable, wrappers)).Methods(http.MethodDelete)
	router.HandleFunc("/tables/{table}", utils.ApplyHTTPWrappers(handler.UpdateTableConfig, wrappers)).Methods(http.MethodPut)
	router.HandleFunc("/tables/{table}/columns", utils.ApplyHTTPWrappers(handler.AddColumn, wrappers)).Methods(http.MethodPost)
	router.HandleFunc("/tables/{table}/columns/{column}", utils.ApplyHTTPWrappers(handler.UpdateColumn, wrappers)).Methods(http.MethodPut)
	router.HandleFunc("/tables/{table}/columns/{column}", utils.ApplyHTTPWrappers(handler.DeleteColumn, wrappers)).Methods(http.MethodDelete)
}

// RegisterForDebug register handlers for debug port
func (handler *SchemaHandler) RegisterForDebug(router *mux.Router, wrappers ...utils.HTTPHandlerWrapper) {
	router.HandleFunc("/tables", utils.ApplyHTTPWrappers(handler.ListTables, wrappers)).Methods(http.MethodGet)
	router.HandleFunc("/tables/{table}", utils.ApplyHTTPWrappers(handler.GetTable, wrappers)).Methods(http.MethodGet)
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
func (handler *SchemaHandler) ListTables(w http.ResponseWriter, r *http.Request) {

	response := NewStringArrayResponse()

	tables, err := handler.metaStore.ListTables()
	if err != nil {
		RespondWithError(w, err)
		return
	}
	for _, tableName := range tables {
		response.Body = append(response.Body, tableName)
	}

	RespondWithJSONObject(w, response.Body)
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
func (handler *SchemaHandler) GetTable(w http.ResponseWriter, r *http.Request) {
	var getTableRequest GetTableRequest
	var getTableResponse GetTableResponse

	err := ReadRequest(r, &getTableRequest)
	if err != nil {
		RespondWithError(w, err)
		return
	}

	table, err := handler.metaStore.GetTable(getTableRequest.TableName)
	if err != nil {
		if err.Error() == metastore.ErrTableDoesNotExist.Error() {
			RespondBytesWithCode(w, http.StatusNotFound, []byte(err.Error()))
			return
		}
	}
	getTableResponse.JSONBuffer, err = json.Marshal(table)

	RespondWithJSONBytes(w, getTableResponse.JSONBuffer, err)
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
func (handler *SchemaHandler) AddTable(w http.ResponseWriter, r *http.Request) {
	if handler.isClusterMode {
		RespondJSONObjectWithCode(w, http.StatusMethodNotAllowed, nil)
	}

	var addTableRequest AddTableRequest
	err := ReadRequest(r, &addTableRequest)
	if err != nil {
		RespondWithBadRequest(w, err)
		return
	}

	validator := metastore.NewTableSchameValidator()
	validator.SetNewTable(addTableRequest.Body)
	err = validator.Validate()
	if err != nil {
		RespondWithBadRequest(w, err)
	}

	err = handler.metaStore.CreateTable(&addTableRequest.Body)
	if err != nil {
		RespondWithError(w, err)
		return
	}

	RespondWithJSONObject(w, nil)
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
func (handler *SchemaHandler) UpdateTableConfig(w http.ResponseWriter, r *http.Request) {
	if handler.isClusterMode {
		RespondJSONObjectWithCode(w, http.StatusMethodNotAllowed, nil)
	}

	var request UpdateTableConfigRequest
	err := ReadRequest(r, &request)
	if err != nil {
		RespondWithBadRequest(w, err)
		return
	}

	err = handler.metaStore.UpdateTableConfig(request.TableName, request.Body)
	if err != nil {
		RespondWithError(w, err)
		return
	}

	RespondWithJSONObject(w, nil)
}

// DeleteTable swagger:route DELETE /schema/tables/{table} deleteTable
// delete table from metaStore
//
// Responses:
//    default: errorResponse
//        200: noContentResponse
func (handler *SchemaHandler) DeleteTable(w http.ResponseWriter, r *http.Request) {
	if handler.isClusterMode {
		RespondJSONObjectWithCode(w, http.StatusMethodNotAllowed, nil)
	}

	var deleteTableRequest DeleteTableRequest
	err := ReadRequest(r, &deleteTableRequest)
	if err != nil {
		RespondWithError(w, err)
		return
	}

	err = handler.metaStore.DeleteTable(deleteTableRequest.TableName)
	if err != nil {
		// TODO: need mapping from metaStore error to api error
		/// for metaStore error might also be user error
		RespondWithError(w, err)
		return
	}

	RespondWithJSONObject(w, nil)
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
func (handler *SchemaHandler) AddColumn(w http.ResponseWriter, r *http.Request) {
	if handler.isClusterMode {
		RespondJSONObjectWithCode(w, http.StatusMethodNotAllowed, nil)
	}

	var addColumnRequest AddColumnRequest
	err := ReadRequest(r, &addColumnRequest)
	if err != nil {
		RespondWithError(w, err)
		return
	}

	// validate data type
	if dataType := memCom.DataTypeFromString(addColumnRequest.Body.Type); dataType == memCom.Unknown {
		RespondWithBadRequest(w, fmt.Errorf("unknown data type: %s", addColumnRequest.Body.Type))
		return
	}

	// validate default value
	if addColumnRequest.Body.DefaultValue != nil {
		err = metastore.ValidateDefaultValue(*addColumnRequest.Body.DefaultValue, addColumnRequest.Body.Type)
		if err != nil {
			RespondWithBadRequest(w, err)
			return
		}
	}

	err = handler.metaStore.AddColumn(addColumnRequest.TableName, addColumnRequest.Body.Column, addColumnRequest.Body.AddToArchivingSortOrder)
	// TODO: validate column
	// might better do in metaStore and here needs to return either user error or server error
	if err != nil {
		// TODO: need mapping from metaStore error to api error
		/// for metaStore error might also be user error
		RespondWithError(w, err)
		return
	}

	RespondWithJSONObject(w, nil)
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
func (handler *SchemaHandler) UpdateColumn(w http.ResponseWriter, r *http.Request) {
	if handler.isClusterMode {
		RespondJSONObjectWithCode(w, http.StatusMethodNotAllowed, nil)
	}

	var updateColumnRequest UpdateColumnRequest

	err := ReadRequest(r, &updateColumnRequest)
	if err != nil {
		RespondWithError(w, err)
		return
	}

	if err = handler.metaStore.UpdateColumn(updateColumnRequest.TableName,
		updateColumnRequest.ColumnName, updateColumnRequest.Body); err != nil {
		// TODO: need mapping from metaStore error to api error
		// for metaStore error might also be user error
		RespondWithError(w, err)
		return
	}

	RespondWithJSONObject(w, nil)
}

// DeleteColumn swagger:route DELETE /schema/tables/{table}/columns/{column} deleteColumn
// delete columns from existing table
//
// Responses:
//    default: errorResponse
//        200: noContentResponse
func (handler *SchemaHandler) DeleteColumn(w http.ResponseWriter, r *http.Request) {
	if handler.isClusterMode {
		RespondJSONObjectWithCode(w, http.StatusMethodNotAllowed, nil)
	}

	var deleteColumnRequest DeleteColumnRequest

	err := ReadRequest(r, &deleteColumnRequest)
	if err != nil {
		RespondWithError(w, err)
		return
	}

	err = handler.metaStore.DeleteColumn(deleteColumnRequest.TableName, deleteColumnRequest.ColumnName)
	// TODO: validate whether table exists and specified columns does not belong to primary key or time column
	// might be better for metaStore to do this and return specified error type
	if err != nil {
		// TODO: need mapping from metaStore error to api error
		// for metaStore error might also be user error
		RespondWithError(w, err)
		return
	}

	RespondWithJSONObject(w, nil)
}
