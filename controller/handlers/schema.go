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

package handlers

import (
	"github.com/uber/aresdb/utils"
	"net/http"

	"github.com/gorilla/mux"
	apiCom "github.com/uber/aresdb/api/common"
	mutatorCom "github.com/uber/aresdb/controller/mutators/common"
	"github.com/uber/aresdb/metastore"
	metaCom "github.com/uber/aresdb/metastore/common"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

const (
	forceKey = "force"
	logMsg   = "Schema request"
)

// SchemaHandlerParams defineds parameters needed to initialize schema handler
type SchemaHandlerParams struct {
	fx.In

	TableSchemaMutator mutatorCom.TableSchemaMutator
	EnumMutator        mutatorCom.EnumMutator
	Logger             *zap.SugaredLogger
}

// SchemaHandler serves schema requests
type SchemaHandler struct {
	tableSchemaMutator mutatorCom.TableSchemaMutator
	enumMutator        mutatorCom.EnumMutator
	logger             *zap.SugaredLogger
}

// NewSchemaHandler creates a new schema handler
func NewSchemaHandler(p SchemaHandlerParams) SchemaHandler {
	return SchemaHandler{
		tableSchemaMutator: p.TableSchemaMutator,
		enumMutator:        p.EnumMutator,
		logger:             p.Logger,
	}
}

// Register adds paths to router
func (h SchemaHandler) Register(router *mux.Router, wrappers ...utils.HTTPHandlerWrapper) {
	router.HandleFunc("/{namespace}/tables", utils.ApplyHTTPWrappers(h.AddTable, wrappers...)).Methods(http.MethodPost)
	router.HandleFunc("/{namespace}/tables/{table}", utils.ApplyHTTPWrappers(h.GetTable, wrappers...)).Methods(http.MethodGet)
	router.HandleFunc("/{namespace}/tables", utils.ApplyHTTPWrappers(h.GetTables, wrappers...)).Methods(http.MethodGet)
	router.HandleFunc("/{namespace}/tables/{table}", utils.ApplyHTTPWrappers(h.DeleteTable, wrappers...)).Methods(http.MethodDelete)
	router.HandleFunc("/{namespace}/tables/{table}", utils.ApplyHTTPWrappers(h.UpdateTable, wrappers...)).Methods(http.MethodPut)
	router.HandleFunc("/{namespace}/tables/{table}/columns/{column}/enum-cases", utils.ApplyHTTPWrappers(h.GetEnumCases, wrappers...)).Methods(http.MethodGet)
	router.HandleFunc("/{namespace}/tables/{table}/columns/{column}/enum-cases", utils.ApplyHTTPWrappers(h.ExtendEnumCases, wrappers...)).Methods(http.MethodPost)
	router.HandleFunc("/{namespace}/hash", utils.ApplyHTTPWrappers(h.GetHash, wrappers...)).Methods(http.MethodGet)
}

// AddTable  swagger:route POST /schema/{namespace}/tables addTable
// adds a new table
//
// Consumes:
//		- application/json
func (h SchemaHandler) AddTable(w *utils.ResponseWriter, r *http.Request) {
	var req AddTableRequest
	req.Body.Config = metastore.DefaultTableConfig
	err := apiCom.ReadRequest(r, &req, w)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	forceUpdate := r.URL.Query().Get(forceKey)

	err = h.tableSchemaMutator.CreateTable(req.Namespace, &req.Body, forceUpdate == "true")
	if err != nil {
		statusCode := http.StatusInternalServerError
		if err == mutatorCom.ErrNamespaceDoesNotExist || err == metaCom.ErrTableAlreadyExist {
			statusCode = http.StatusBadRequest
		}
		w.WriteErrorWithCode(statusCode, err)
		return
	}

	w.WriteObject(nil)
}

// GetTable swagger:route GET /schema/{namespace}/tables/{table} getTable
// returns table schema of given table name
func (h SchemaHandler) GetTable(w *utils.ResponseWriter, r *http.Request) {
	var req GetTableRequest
	err := apiCom.ReadRequest(r, &req, w)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	table, err := h.tableSchemaMutator.GetTable(req.Namespace, req.TableName)
	if err != nil {
		statusCode := http.StatusInternalServerError
		if mutatorCom.IsNonExist(err) {
			statusCode = http.StatusNotFound
			err = metaCom.ErrTableDoesNotExist
		}
		w.WriteErrorWithCode(statusCode, err)
		return
	}
	w.WriteObject(table)
}

// GetTables swagger:route GET /schema/{namespace}/tables getTables
// returns schema of all tables
func (h SchemaHandler) GetTables(w *utils.ResponseWriter, r *http.Request) {
	var getTablesRequest GetTablesRequest
	err := apiCom.ReadRequest(r, &getTablesRequest, w)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	// TODO: call membership mutator to update instance status
	//if getTablesRequest.InstanceName != "" {
	//	...
	//}

	tableNames, err := h.tableSchemaMutator.ListTables(getTablesRequest.Namespace)
	if err != nil {
		statusCode := http.StatusInternalServerError
		if err == metaCom.ErrTableDoesNotExist {
			statusCode = http.StatusBadRequest
		}
		w.WriteErrorWithCode(statusCode, err)
		return
	}

	tables := make([]metaCom.Table, len(tableNames))
	for i, tableName := range tableNames {
		table, err := h.tableSchemaMutator.GetTable(getTablesRequest.Namespace, tableName)
		if err != nil {
			w.WriteError(err)
			return
		}
		tables[i] = *table
	}
	w.WriteObject(tables)
}

// DeleteTable swagger:route DELETE /schema/{namespace}/tables/{table} deleteTable
// deletes an existing table
func (h SchemaHandler) DeleteTable(w *utils.ResponseWriter, r *http.Request) {
	var deleteTableRequest DeleteTableRequest
	err := apiCom.ReadRequest(r, &deleteTableRequest, w)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	err = h.tableSchemaMutator.DeleteTable(deleteTableRequest.Namespace, deleteTableRequest.TableName)
	if err != nil {
		statusCode := http.StatusInternalServerError
		if err == metaCom.ErrTableDoesNotExist {
			statusCode = http.StatusBadRequest
		}
		w.WriteErrorWithCode(statusCode, err)
		return
	}
	w.WriteObject(nil)
}

// UpdateTable swagger:route PUT /schema/{namespace}/tables/{table} updateTable
// updates existing table's schema and config
//
// Consumes:
//    - application/json
func (h SchemaHandler) UpdateTable(w *utils.ResponseWriter, r *http.Request) {
	var updateTableRequest UpdateTableRequest
	updateTableRequest.Body.Config = metastore.DefaultTableConfig
	err := apiCom.ReadRequest(r, &updateTableRequest, w)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	forceUpdate := r.URL.Query().Get(forceKey)

	h.logger.Infow(logMsg,
		"endpoint", "update",
		"event", "received",
		"force", forceUpdate)

	err = h.tableSchemaMutator.UpdateTable(updateTableRequest.Namespace, updateTableRequest.Body, forceUpdate == "true")
	if err != nil {
		w.WriteErrorWithCode(http.StatusInternalServerError, err)
		return
	}
	w.WriteObject(updateTableRequest.Body)
}

// GetHash swagger:route GET /schema/{namespace}/hash getSchemaHash
// returns hash of all table schemas in a namespace, hash change means there's a change in any of the schemas
func (h SchemaHandler) GetHash(w *utils.ResponseWriter, r *http.Request) {
	var req GetHashRequest
	err := apiCom.ReadRequest(r, &req, w)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	// TODO: call membership mutator to update instance status
	//if getTablesRequest.InstanceName != "" {
	//	...
	//}

	hash, err := h.tableSchemaMutator.GetHash(req.Namespace)
	if err != nil {
		statusCode := http.StatusInternalServerError
		if mutatorCom.IsNonExist(err) {
			statusCode = http.StatusNotFound
			err = metaCom.ErrTableDoesNotExist
		}
		w.WriteErrorWithCode(statusCode, err)
		return
	}
	w.WriteJSONBytesWithCode(http.StatusOK, []byte(hash), nil)
}

// ExtendEnumCases swagger:route POST /schema/{namespace}/tables/{table}/columns/{column}/enum-cases extendEnumCases
// returns enum ids for given enum cases
func (h SchemaHandler) ExtendEnumCases(w *utils.ResponseWriter, r *http.Request) {
	var req ExtendEnumCaseRequest
	err := apiCom.ReadRequest(r, &req, w)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	enumIDs, err := h.enumMutator.ExtendEnumCases(req.Namespace, req.TableName, req.Column, req.Body)
	if err != nil {
		if err == ErrTableDoesNotExist || err == ErrColumnDoesNotExist {
			w.WriteErrorWithCode(http.StatusBadRequest, err)
		} else {
			w.WriteError(err)
		}
		return
	}
	w.WriteObject(enumIDs)
}

// GetEnumCases swagger:route GET /schema/{namespace}/tables/{table}/columns/{column}/enum-cases getEnumCases
// returns all enum cases for given table column
func (h SchemaHandler) GetEnumCases(w *utils.ResponseWriter, r *http.Request) {
	var req GetEnumCaseRequest
	err := apiCom.ReadRequest(r, &req, w)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	enumCases, err := h.enumMutator.GetEnumCases(req.Namespace, req.TableName, req.Column)
	if err != nil {
		if err == ErrTableDoesNotExist || err == ErrColumnDoesNotExist {
			w.WriteErrorWithCode(http.StatusBadRequest, err)
		} else {
			w.WriteError(err)
		}
		return
	}
	w.WriteObject(enumCases)
}
