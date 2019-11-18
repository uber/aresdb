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
	"net/http"

	"github.com/gorilla/mux"
	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/cluster/kvstore"
	"github.com/uber/aresdb/utils"
	"go.uber.org/zap"
)

// PlacementHandler handles placement requests
type PlacementHandler struct {
	client *kvstore.EtcdClient
	logger *zap.SugaredLogger
	scope  tally.Scope
}

// NewPlacementHandler creates placement handler
func NewPlacementHandler(logger *zap.SugaredLogger, scope tally.Scope, client *kvstore.EtcdClient) PlacementHandler {
	return PlacementHandler{
		client: client,
		logger: logger,
		scope:  scope,
	}
}

// Register adds paths to router
func (h PlacementHandler) Register(router *mux.Router, wrappers ...utils.HTTPHandlerWrapper2) {
	router.HandleFunc("/{namespace}/datanode", utils.ApplyHTTPWrappers2(h.Get, wrappers...)).Methods(http.MethodGet)
	router.HandleFunc("/{namespace}/datanode/available", utils.ApplyHTTPWrappers2(h.MarkNamespaceAvailable, wrappers...)).Methods(http.MethodPost)
	router.HandleFunc("/{namespace}/datanode/init", utils.ApplyHTTPWrappers2(h.Init, wrappers...)).Methods(http.MethodPost)
	router.HandleFunc("/{namespace}/datanode/instances", utils.ApplyHTTPWrappers2(h.Add, wrappers...)).Methods(http.MethodPost)
	router.HandleFunc("/{namespace}/datanode/instances", utils.ApplyHTTPWrappers2(h.Replace, wrappers...)).Methods(http.MethodPut)
	router.HandleFunc("/{namespace}/datanode/instances", utils.ApplyHTTPWrappers2(h.Remove, wrappers...)).Methods(http.MethodDelete)
	router.HandleFunc("/{namespace}/datanode/instances/{instance}/available", utils.ApplyHTTPWrappers2(h.MarkInstanceAvailable, wrappers...)).Methods(http.MethodPost)
}

func (h *PlacementHandler) getServiceID(namespace string) services.ServiceID {
	return services.NewServiceID().
		SetEnvironment(h.client.Environment).
		SetZone(h.client.Zone).
		SetName(utils.DataNodeServiceName(namespace))
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

func (h *PlacementHandler) placementOptions() placement.Options {
	return placement.NewOptions().
		SetInstrumentOptions(instrument.NewOptions().
			SetLogger(h.logger.Desugar()).SetMetricsScope(h.scope)).
		SetValidZone(h.client.Zone).
		// if we specify more than one new instance, we want to add them all
		SetAddAllCandidates(true).
		// for now we want to make sure replacement does not affect existing instances not being replaced
		SetAllowPartialReplace(false)
}

func validateAllAvailable(p placement.Placement) error {
	for _, instance := range p.Instances() {
		if !instance.IsAvailable() {
			return utils.APIError{Code: http.StatusBadRequest, Message: fmt.Sprintf("instance %s is not available", instance.ID())}
		}
	}
	return nil
}

func validateInitPlacementReq(req InitPlacementRequest) error {
	// check number of shards
	numShards := req.Body.NumShards
	if (numShards & (^(-numShards))) != 0 {
		return ErrInvalidNumShards
	}
	return nil
}

// Init initialize new placement
func (h *PlacementHandler) Init(rw *utils.ResponseWriter, r *http.Request) {
	if h.client == nil {
		rw.WriteError(ErrEtcdNotAvailable)
		return
	}
	var req InitPlacementRequest
	err := ReadRequest(r, &req, rw)
	if err != nil {
		rw.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	if err := validateInitPlacementReq(req); err != nil {
		rw.WriteError(err)
		return
	}

	newInstances, err := newInstancesFromProto(req.Body.NewInstances)
	if err != nil {
		rw.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	placementSvc, err := h.client.Services.PlacementService(h.getServiceID(req.Namespace), h.placementOptions())
	if err != nil {
		rw.WriteError(utils.StackError(err, ErrMsgFailedToGetPlacementService))
		return
	}

	p, err := placementSvc.BuildInitialPlacement(newInstances, req.Body.NumShards, req.Body.NumReplica)
	if err != nil {
		rw.WriteError(utils.StackError(err, ErrMsgFailedToBuildInitialPlacement))
		return
	}
	respondWithPlacement(p, rw)
}

// Get get the current placement
func (h *PlacementHandler) Get(rw *utils.ResponseWriter, r *http.Request) {
	if h.client == nil {
		rw.WriteError(ErrEtcdNotAvailable)
		return
	}
	var req NamespaceRequest
	err := ReadRequest(r, &req, rw)
	if err != nil {
		rw.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}
	placementSvc, err := h.client.Services.PlacementService(h.getServiceID(req.Namespace), h.placementOptions())
	if err != nil {
		rw.WriteError(utils.StackError(err, ErrMsgFailedToGetPlacementService))
		return
	}
	p, err := placementSvc.Placement()
	if err != nil {
		rw.WriteError(utils.StackError(err, ErrMsgFailedToGetCurrentPlacement))
		return
	}
	respondWithPlacement(p, rw)
}

// Add adds new instances
func (h *PlacementHandler) Add(rw *utils.ResponseWriter, r *http.Request) {
	if h.client == nil {
		rw.WriteError(ErrEtcdNotAvailable)
		return
	}
	var req AddInstancesRequest
	err := ReadRequest(r, &req, rw)
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
	placementSvc, err := h.client.Services.PlacementService(h.getServiceID(req.Namespace),
		h.placementOptions().SetValidateFnBeforeUpdate(validateAllAvailable))
	if err != nil {
		rw.WriteError(utils.StackError(err, ErrMsgFailedToGetPlacementService))
		return
	}

	p, _, err := placementSvc.AddInstances(instances)
	if err != nil {
		rw.WriteError(utils.StackError(err, ErrMsgFailedToAddInstance))
		return
	}
	respondWithPlacement(p, rw)
}

// Replace replace existing instances within placement with new instances
func (h *PlacementHandler) Replace(rw *utils.ResponseWriter, r *http.Request) {
	if h.client == nil {
		rw.WriteError(ErrEtcdNotAvailable)
		return
	}
	var req ReplaceInstanceRequest
	err := ReadRequest(r, &req, rw)
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
	placementSvc, err := h.client.Services.PlacementService(h.getServiceID(req.Namespace),
		h.placementOptions().SetValidateFnBeforeUpdate(validateAllAvailable))
	if err != nil {
		rw.WriteError(utils.StackError(err, ErrMsgFailedToGetPlacementService))
		return
	}

	p, _, err := placementSvc.ReplaceInstances(req.Body.LeavingInstances, newInstances)
	if err != nil {
		rw.WriteError(utils.StackError(err, ErrMsgFailedToReplaceInstance))
		return
	}

	respondWithPlacement(p, rw)
}

// Remove remove instance from placement
func (h *PlacementHandler) Remove(rw *utils.ResponseWriter, r *http.Request) {
	if h.client == nil {
		rw.WriteError(ErrEtcdNotAvailable)
		return
	}
	var req RemoveInstanceRequest
	err := ReadRequest(r, &req, rw)
	if err != nil {
		rw.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	if len(req.Body.LeavingInstances) != 1 {
		rw.WriteError(ErrRemoveOneInstance)
		return
	}

	// validate all shards are available before replace instance
	placementSvc, err := h.client.Services.PlacementService(h.getServiceID(req.Namespace),
		h.placementOptions().SetValidateFnBeforeUpdate(validateAllAvailable))
	if err != nil {
		rw.WriteError(utils.StackError(err, ErrMsgFailedToGetPlacementService))
		return
	}

	p, err := placementSvc.RemoveInstances(req.Body.LeavingInstances)
	if err != nil {
		rw.WriteError(utils.StackError(err, ErrMsgFailedToRemoveInstance))
		return
	}
	respondWithPlacement(p, rw)
}

// MarkNamespaceAvailable marks all instance/shards in placement as available
func (h *PlacementHandler) MarkNamespaceAvailable(rw *utils.ResponseWriter, r *http.Request) {
	var req NamespaceRequest
	err := ReadRequest(r, &req, rw)
	if err != nil {
		rw.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	placementSvc, err := h.client.Services.PlacementService(h.getServiceID(req.Namespace), h.placementOptions())
	if err != nil {
		rw.WriteError(utils.StackError(err, ErrMsgFailedToGetPlacementService))
		return
	}

	p, err := placementSvc.MarkAllShardsAvailable()
	if err != nil {
		rw.WriteError(utils.StackError(err, ErrMsgFailedToMarkAvailable))
		return
	}

	respondWithPlacement(p, rw)
}

// MarkInstanceAvailable marks one instance as available
func (h *PlacementHandler) MarkInstanceAvailable(rw *utils.ResponseWriter, r *http.Request) {
	var req MarkAvailableRequest
	err := ReadRequest(r, &req, rw)
	if err != nil {
		rw.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	placementSvc, err := h.client.Services.PlacementService(h.getServiceID(req.Namespace), h.placementOptions())
	if err != nil {
		rw.WriteError(utils.StackError(err, ErrMsgFailedToGetPlacementService))
		return
	}

	var p placement.Placement
	if req.Body.AllShards {
		p, err = placementSvc.MarkInstanceAvailable(req.Instance)
	} else {
		p, err = placementSvc.MarkShardsAvailable(req.Instance, req.Body.Shards...)
	}

	if err != nil {
		rw.WriteError(utils.StackError(err, ErrMsgFailedToMarkAvailable))
		return
	}
	respondWithPlacement(p, rw)
}

func respondWithPlacement(p placement.Placement, rw *utils.ResponseWriter) {
	pb, err := p.Proto()
	if err != nil {
		rw.WriteError(utils.StackError(err, ErrMsgFailedToMarshalPlacement))
		return
	}
	rw.WriteObject(pb)
}
