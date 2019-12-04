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
	"fmt"
	apiCom "github.com/uber/aresdb/api/common"
	"github.com/uber/aresdb/utils"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/uber-go/tally"
	mutatorCom "github.com/uber/aresdb/controller/mutators/common"
	metaCom "github.com/uber/aresdb/metastore/common"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// MembershipHandlerParams defineds parameters needed to initialize schema handler
type MembershipHandlerParams struct {
	fx.In

	MembershipMutator mutatorCom.MembershipMutator
	Logger            *zap.SugaredLogger
	Scope             tally.Scope
}

// MembershipHandler serves schema requests
type MembershipHandler struct {
	membershipMutator mutatorCom.MembershipMutator
	logger            *zap.SugaredLogger
	scope             tally.Scope
}

// NewMembershipHandler creates a new schema handler
func NewMembershipHandler(p MembershipHandlerParams) MembershipHandler {
	return MembershipHandler{
		membershipMutator: p.MembershipMutator,
		logger:            p.Logger,
		scope:             p.Scope,
	}
}

// Register adds paths to router
func (h MembershipHandler) Register(router *mux.Router, wrappers ...utils.HTTPHandlerWrapper) {
	router.HandleFunc("/{namespace}/instances", utils.ApplyHTTPWrappers(h.Join, wrappers...)).Methods(http.MethodPost)
	router.HandleFunc("/{namespace}/instances/{instance}", utils.ApplyHTTPWrappers(h.GetInstance, wrappers...)).Methods(http.MethodGet)
	router.HandleFunc("/{namespace}/instances", utils.ApplyHTTPWrappers(h.GetInstances, wrappers...)).Methods(http.MethodGet)
	router.HandleFunc("/{namespace}/instances/{instance}", utils.ApplyHTTPWrappers(h.Leave, wrappers...)).Methods(http.MethodDelete)
	router.HandleFunc("/{namespace}/hash", utils.ApplyHTTPWrappers(h.GetHash, wrappers...)).Methods(http.MethodGet)
}

// Join adds a instance
func (h MembershipHandler) Join(w *utils.ResponseWriter, r *http.Request) {
	if h.membershipMutator == nil {
		w.WriteError(ErrNotImplemented)
		return
	}
	var req JoinRequest
	err := apiCom.ReadRequest(r, &req, w.SetRequest)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}
	err = h.membershipMutator.Join(req.Namespace, req.Body)
	if err != nil {
		statusCode := http.StatusInternalServerError
		if err == mutatorCom.ErrNamespaceDoesNotExist || err == mutatorCom.ErrInstanceAlreadyExist {
			statusCode = http.StatusBadRequest
		}
		w.WriteErrorWithCode(statusCode, err)
		return
	}
	w.WriteObject(nil)
}

// GetInstance swagger:route GET /membership/{namespace}/instances/{instance} getInstance
// returns an instance
func (h MembershipHandler) GetInstance(w *utils.ResponseWriter, r *http.Request) {
	if h.membershipMutator == nil {
		w.WriteError(ErrNotImplemented)
		return
	}
	var req GetInstanceRequest

	err := apiCom.ReadRequest(r, &req, w.SetRequest)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	instance, err := h.membershipMutator.GetInstance(req.Namespace, req.InstanceName)
	if err != nil {
		statusCode := http.StatusInternalServerError
		if mutatorCom.IsNonExist(err) {
			statusCode = http.StatusBadRequest
			err = metaCom.ErrTableDoesNotExist
		}
		w.WriteErrorWithCode(statusCode, err)
		return
	}

	resp := GetInstanceResponse{Instance{fmt.Sprintf("%s:%d", instance.Host, instance.Port)}}
	w.WriteObject(resp)
}

// GetInstances swagger:route GET /membership/{namespace}/instances getInstances
// returns all instances
func (h MembershipHandler) GetInstances(w *utils.ResponseWriter, r *http.Request) {
	if h.membershipMutator == nil {
		w.WriteError(ErrNotImplemented)
		return
	}
	var req GetInstancesRequest
	err := apiCom.ReadRequest(r, &req, w.SetRequest)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	instances, err := h.membershipMutator.GetInstances(req.Namespace)
	if err != nil {
		statusCode := http.StatusInternalServerError
		w.WriteErrorWithCode(statusCode, err)
		return
	}

	instancesMap := make(map[string]Instance)
	for _, instance := range instances {
		instancesMap[instance.Name] = Instance{fmt.Sprintf("%s:%d", instance.Host, instance.Port)}
	}
	resp := GetInstancesResponse{instancesMap}
	w.WriteObject(resp)
}

// Leave deletes an existing instance
func (h MembershipHandler) Leave(w *utils.ResponseWriter, r *http.Request) {
	if h.membershipMutator == nil {
		w.WriteError(ErrNotImplemented)
		return
	}
	var req LeaveRequest
	err := apiCom.ReadRequest(r, &req, w.SetRequest)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	err = h.membershipMutator.Leave(req.Namespace, req.InstanceName)
	if err != nil {
		statusCode := http.StatusInternalServerError
		if err == mutatorCom.ErrInstanceDoesNotExist {
			statusCode = http.StatusBadRequest
		}
		w.WriteErrorWithCode(statusCode, err)
		return
	}
	w.WriteObject(nil)
}

// GetHash swagger:route GET /membership/{namespace}/hash getMembershipHash
// returns hash of all instances in a namespace
func (h MembershipHandler) GetHash(w *utils.ResponseWriter, r *http.Request) {
	if h.membershipMutator == nil {
		w.WriteError(ErrNotImplemented)
		return
	}
	var req GetHashRequest

	err := apiCom.ReadRequest(r, &req, w.SetRequest)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	hash, err := h.membershipMutator.GetHash(req.Namespace)
	if err != nil {
		statusCode := http.StatusInternalServerError
		if mutatorCom.IsNonExist(err) {
			statusCode = http.StatusBadRequest
			err = metaCom.ErrTableDoesNotExist
		}
		w.WriteErrorWithCode(statusCode, err)
		return
	}
	w.WriteJSONBytesWithCode(http.StatusOK, []byte(hash), nil)
}
