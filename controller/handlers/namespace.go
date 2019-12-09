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
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// NamespaceHandlerParams defines parameters needed to initialize namespace handler
type NamespaceHandlerParams struct {
	fx.In

	NamespaceMutator mutatorCom.NamespaceMutator
	Logger           *zap.SugaredLogger
}

// NamespaceHandler serves namespace requests
type NamespaceHandler struct {
	namespaceMutator mutatorCom.NamespaceMutator
	logger           *zap.SugaredLogger
}

// NewNamespaceHandler creates a new namespace handler
func NewNamespaceHandler(p NamespaceHandlerParams) NamespaceHandler {
	return NamespaceHandler{
		namespaceMutator: p.NamespaceMutator,
		logger:           p.Logger,
	}
}

// Register adds paths to router
func (h NamespaceHandler) Register(router *mux.Router, wrappers ...utils.HTTPHandlerWrapper) {
	router.HandleFunc("/namespaces", utils.ApplyHTTPWrappers(h.CreateNamespace, wrappers...)).Methods(http.MethodPost)
	router.HandleFunc("/namespaces", utils.ApplyHTTPWrappers(h.ListNamespaces, wrappers...)).Methods(http.MethodGet)
}

// CreateNamespace swagger:route POST /namespaces createNamespace
// adds a new namespace
//
// Consumes:
//    - application/json
func (h NamespaceHandler) CreateNamespace(w *utils.ResponseWriter, r *http.Request) {
	var req CreateNamespaceRequest
	err := apiCom.ReadRequest(r, &req, w.SetRequest)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	err = h.namespaceMutator.CreateNamespace(req.Body.Namespace)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}
	w.WriteObject(nil)
}

// ListNamespaces swagger:route GET /namespaces listNamespaces
// returns all namespaces
//
// Produces:
//    - application/json
func (h NamespaceHandler) ListNamespaces(w *utils.ResponseWriter, r *http.Request) {
	var err error

	var namespaces []string
	namespaces, err = h.namespaceMutator.ListNamespaces()
	if err != nil {
		w.WriteError(err)
		return
	}
	w.WriteObject(namespaces)
}
