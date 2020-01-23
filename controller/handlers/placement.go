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
	"net/http"

	"github.com/gorilla/mux"
	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
	"github.com/m3db/m3/src/cluster/placement"
	apiCom "github.com/uber/aresdb/api/common"
	mutatorCom "github.com/uber/aresdb/controller/mutators/common"
	"github.com/uber/aresdb/utils"
)

// PlacementHandler handles placement requests
type PlacementHandler struct {
	placementMutator mutatorCom.PlacementMutator
}

// NewPlacementHandler creates placement handler
func NewPlacementHandler(mutator mutatorCom.PlacementMutator) PlacementHandler {
	return PlacementHandler{
		placementMutator: mutator,
	}
}

// Register adds paths to router
func (h PlacementHandler) Register(router *mux.Router, wrappers ...utils.HTTPHandlerWrapper) {
	router.HandleFunc("/{namespace}/datanode", utils.ApplyHTTPWrappers(h.Get, wrappers...)).Methods(http.MethodGet)
	router.HandleFunc("/{namespace}/datanode/available", utils.ApplyHTTPWrappers(h.MarkNamespaceAvailable, wrappers...)).Methods(http.MethodPost)
	router.HandleFunc("/{namespace}/datanode/init", utils.ApplyHTTPWrappers(h.Init, wrappers...)).Methods(http.MethodPost)
	router.HandleFunc("/{namespace}/datanode/instances", utils.ApplyHTTPWrappers(h.Add, wrappers...)).Methods(http.MethodPost)
	router.HandleFunc("/{namespace}/datanode/instances", utils.ApplyHTTPWrappers(h.Replace, wrappers...)).Methods(http.MethodPut)
	router.HandleFunc("/{namespace}/datanode/instances", utils.ApplyHTTPWrappers(h.Remove, wrappers...)).Methods(http.MethodDelete)
	router.HandleFunc("/{namespace}/datanode/instances/{instance}/available", utils.ApplyHTTPWrappers(h.MarkInstanceAvailable, wrappers...)).Methods(http.MethodPost)
}

func newInstancesFromProto(instancepbs []placementpb.Instance) ([]placement.Instance, error) {
	instances := make([]placement.Instance, 0, len(instancepbs))
	for _, instancepb := range instancepbs {
		instance, err := placement.NewInstanceFromProto(&instancepb)
		if err != nil {
			return nil, err
		}
		instances = append(instances, instance)
	}
	return instances, nil
}

// Init initialize new placement
func (h *PlacementHandler) Init(rw *utils.ResponseWriter, r *http.Request) {
	var req InitPlacementRequest
	err := apiCom.ReadRequest(r, &req, rw.SetRequest)
	if err != nil {
		rw.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	newInstances, err := newInstancesFromProto(req.Body.NewInstances)
	if err != nil {
		rw.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	plm, err := h.placementMutator.BuildInitialPlacement(req.Namespace, req.Body.NumShards, req.Body.NumReplica, newInstances)
	if err != nil {
		rw.WriteError(err)
		return
	}
	respondWithPlacement(plm, rw)
}

// Get get the current placement
func (h *PlacementHandler) Get(rw *utils.ResponseWriter, r *http.Request) {
	var req NamespaceRequest
	err := apiCom.ReadRequest(r, &req, rw.SetRequest)
	if err != nil {
		rw.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}
	plm, err := h.placementMutator.GetCurrentPlacement(req.Namespace)
	if err != nil {
		rw.WriteError(err)
		return
	}
	respondWithPlacement(plm, rw)
}

// Add adds new instances
func (h *PlacementHandler) Add(rw *utils.ResponseWriter, r *http.Request) {
	var req AddInstancesRequest
	err := apiCom.ReadRequest(r, &req, rw.SetRequest)
	if err != nil {
		rw.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	instances, err := newInstancesFromProto(req.Body.NewInstances)
	if err != nil {
		rw.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	// validate all shards available in placement before adding instance
	plm, err := h.placementMutator.AddInstance(req.Namespace, instances)
	if err != nil {
		rw.WriteError(err)
		return
	}
	respondWithPlacement(plm, rw)
}

// Replace replace existing instances within placement with new instances
func (h *PlacementHandler) Replace(rw *utils.ResponseWriter, r *http.Request) {
	var req ReplaceInstanceRequest
	err := apiCom.ReadRequest(r, &req, rw.SetRequest)
	if err != nil {
		rw.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	newInstances, err := newInstancesFromProto(req.Body.NewInstances)
	if err != nil {
		rw.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	// validate all shards are available before replace instance
	plm, err := h.placementMutator.ReplaceInstance(req.Namespace, req.Body.LeavingInstances, newInstances)
	if err != nil {
		rw.WriteError(err)
		return
	}
	respondWithPlacement(plm, rw)
}

// Remove remove instance from placement
func (h *PlacementHandler) Remove(rw *utils.ResponseWriter, r *http.Request) {
	var req RemoveInstanceRequest
	err := apiCom.ReadRequest(r, &req, rw.SetRequest)
	if err != nil {
		rw.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	if len(req.Body.LeavingInstances) != 1 {
		rw.WriteError(ErrRemoveOneInstance)
		return
	}

	// validate all shards are available before replace instance
	plm, err := h.placementMutator.RemoveInstance(req.Namespace, req.Body.LeavingInstances)
	if err != nil {
		rw.WriteError(err)
		return
	}
	respondWithPlacement(plm, rw)
}

// MarkNamespaceAvailable marks all instance/shards in placement as available
func (h *PlacementHandler) MarkNamespaceAvailable(rw *utils.ResponseWriter, r *http.Request) {
	var req NamespaceRequest
	err := apiCom.ReadRequest(r, &req, rw.SetRequest)
	if err != nil {
		rw.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	plm, err := h.placementMutator.MarkNamespaceAvailable(req.Namespace)
	if err != nil {
		rw.WriteError(err)
		return
	}
	respondWithPlacement(plm, rw)
}

// MarkInstanceAvailable marks one instance as available
func (h *PlacementHandler) MarkInstanceAvailable(rw *utils.ResponseWriter, r *http.Request) {
	var req MarkAvailableRequest
	err := apiCom.ReadRequest(r, &req, rw.SetRequest)
	if err != nil {
		rw.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	var p placement.Placement
	if req.Body.AllShards {
		p, err = h.placementMutator.MarkInstanceAvailable(req.Namespace, req.Instance)
	} else {
		p, err = h.placementMutator.MarkShardsAvailable(req.Namespace, req.Instance, req.Body.Shards)
	}
	if err != nil {
		rw.WriteError(err)
		return
	}
	respondWithPlacement(p, rw)
}

func respondWithPlacement(p placement.Placement, rw *utils.ResponseWriter) {
	pb, err := p.Proto()
	if err != nil {
		rw.WriteError(utils.StackError(err, "failed to marshal placement"))
		return
	}
	rw.WriteObject(pb)
}
