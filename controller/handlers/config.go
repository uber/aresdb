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
	"github.com/m3db/m3/src/cluster/services"
	"github.com/uber-go/tally"
	apiCom "github.com/uber/aresdb/api/common"
	"github.com/uber/aresdb/cluster/kvstore"
	mutatorCom "github.com/uber/aresdb/controller/mutators/common"
	"github.com/uber/aresdb/utils"
	aresUtils "github.com/uber/aresdb/utils"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// ConfigHandlerParams defines params needed to initialize ConfigHandler
type ConfigHandlerParams struct {
	fx.In

	Logger        *zap.SugaredLogger
	Scope         tally.Scope
	JobMutator    mutatorCom.JobMutator
	SchemaMutator mutatorCom.TableSchemaMutator
	EtcdClient    *kvstore.EtcdClient
}

// ConfigHandler serves requests for job configurations
type ConfigHandler struct {
	logger *zap.SugaredLogger
	scope  tally.Scope

	jobMutator    mutatorCom.JobMutator
	schemaMutator mutatorCom.TableSchemaMutator

	etcdClient *kvstore.EtcdClient
}

// NewConfigHandler creates a new ConfigHandler
func NewConfigHandler(p ConfigHandlerParams) ConfigHandler {
	return ConfigHandler{
		logger:        p.Logger,
		scope:         p.Scope,
		jobMutator:    p.JobMutator,
		schemaMutator: p.SchemaMutator,
		etcdClient:    p.EtcdClient,
	}
}

// Register adds paths to router
func (h ConfigHandler) Register(router *mux.Router, wrappers ...utils.HTTPHandlerWrapper) {
	router.HandleFunc("/{namespace}/jobs/{job}", utils.ApplyHTTPWrappers(h.GetJob, wrappers...)).Methods(http.MethodGet)
	router.HandleFunc("/{namespace}/jobs", utils.ApplyHTTPWrappers(h.GetJobs, wrappers...)).Methods(http.MethodGet)
	router.HandleFunc("/{namespace}/jobs/{job}", utils.ApplyHTTPWrappers(h.DeleteJob, wrappers...)).Methods(http.MethodDelete)
	router.HandleFunc("/{namespace}/jobs/{job}", utils.ApplyHTTPWrappers(h.UpdateJob, wrappers...)).Methods(http.MethodPut)
	router.HandleFunc("/{namespace}/jobs", utils.ApplyHTTPWrappers(h.AddJob, wrappers...)).Methods(http.MethodPost)
	router.HandleFunc("/{namespace}/hash", utils.ApplyHTTPWrappers(h.GetHash, wrappers...)).Methods(http.MethodGet)
}

func (h ConfigHandler) getNumShards(namespace string) (int, error) {
	if h.etcdClient != nil {
		serviceID := services.NewServiceID().
			SetEnvironment(h.etcdClient.Environment).
			SetZone(h.etcdClient.Zone).
			SetName(aresUtils.DataNodeServiceName(namespace))
		placementSvc, err := h.etcdClient.Services.PlacementService(serviceID, nil)
		if err != nil {
			return 0, ErrFailedToFetchPlacement
		}
		plmt, err := placementSvc.Placement()
		if err != nil {
			return 0, ErrFailedToFetchPlacement
		}
		return plmt.NumShards(), nil
	}
	return 0, nil
}

// GetJob swagger:route GET /config/{namespace}/jobs/{job} getJob
// gets job config by name
func (h ConfigHandler) GetJob(w *utils.ResponseWriter, r *http.Request) {
	var req GetJobRequest
	err := apiCom.ReadRequest(r, &req, w)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	job, err := h.jobMutator.GetJob(req.Namespace, req.JobName)
	if err != nil {
		statusCode := http.StatusInternalServerError
		if mutatorCom.IsNonExist(err) {
			statusCode = http.StatusNotFound
			err = mutatorCom.ErrJobConfigDoesNotExist
		}
		w.WriteErrorWithCode(statusCode, err)
		return
	}

	table, err := h.schemaMutator.GetTable(req.Namespace, job.AresTableConfig.Name)
	if err != nil {
		w.WriteErrorWithCode(
			http.StatusInternalServerError,
			ErrFailedToFetchTableSchemaForJobConfig)
		return
	}
	job.AresTableConfig.Table = table

	numShards, err := h.getNumShards(req.Namespace)
	if err != nil {
		w.WriteError(err)
		return
	}
	job.NumShards = numShards

	w.WriteObject(job)
}

// GetJobs swagger:route GET /config/{namespace}/jobs getJobs
// returns all jobs config
func (h ConfigHandler) GetJobs(w *utils.ResponseWriter, r *http.Request) {
	var req GetJobsRequest
	err := apiCom.ReadRequest(r, &req, w)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	jobs, err := h.jobMutator.GetJobs(req.Namespace)
	if err != nil {
		statusCode := http.StatusInternalServerError
		if mutatorCom.IsNonExist(err) {
			statusCode = http.StatusNotFound
			err = mutatorCom.ErrJobConfigDoesNotExist
		}
		w.WriteErrorWithCode(statusCode, err)
		return
	}

	numShards, err := h.getNumShards(req.Namespace)
	if err != nil {
		w.WriteError(err)
		return
	}

	numValidJobs := 0
	for _, job := range jobs {
		table, err := h.schemaMutator.GetTable(req.Namespace, job.AresTableConfig.Name)
		if err != nil {
			// ignore job with table not found
			if mutatorCom.IsNonExist(err) {
				continue
			}
			err = ErrFailedToFetchTableSchemaForJobConfig
			w.WriteErrorWithCode(http.StatusInternalServerError, err)
			return
		}
		jobs[numValidJobs] = job
		jobs[numValidJobs].AresTableConfig.Table = table
		jobs[numValidJobs].NumShards = numShards
		numValidJobs++
	}
	w.WriteObject(jobs[:numValidJobs])
}

// DeleteJob swagger:route DELETE /config/{namespace}/jobs/{job} deleteJob
// deletes a job
func (h ConfigHandler) DeleteJob(w *utils.ResponseWriter, r *http.Request) {
	var req DeleteJobRequest
	err := apiCom.ReadRequest(r, &req, w)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	err = h.jobMutator.DeleteJob(req.Namespace, req.JobName)
	if err != nil {
		statusCode := http.StatusInternalServerError
		if err == mutatorCom.ErrJobConfigDoesNotExist {
			statusCode = http.StatusBadRequest
		}
		w.WriteErrorWithCode(statusCode, err)
		return
	}
	w.WriteObject(nil)
}

// UpdateJob swagger:route PUT /config/{namespace}/jobs/{job} updateJob
// updates job config
//
// Consumes:
//    - application/json
func (h ConfigHandler) UpdateJob(w *utils.ResponseWriter, r *http.Request) {
	var req UpdateJobRequest
	err := apiCom.ReadRequest(r, &req, w)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	err = h.jobMutator.UpdateJob(req.Namespace, req.Body)
	if err != nil {
		w.WriteErrorWithCode(http.StatusInternalServerError, err)
		return
	}
	w.WriteObject(req.Body)
}

// AddJob swagger:route POST /config/{namespace}/jobs addJob
// adds a new job
//
// Consumes:
//    - application/json
func (h ConfigHandler) AddJob(w *utils.ResponseWriter, r *http.Request) {
	var req AddJobRequest
	err := apiCom.ReadRequest(r, &req, w)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	err = h.jobMutator.AddJob(req.Namespace, req.Body)
	if err != nil {
		statusCode := http.StatusInternalServerError
		if err == mutatorCom.ErrNamespaceDoesNotExist || err == mutatorCom.ErrJobConfigAlreadyExist {
			statusCode = http.StatusBadRequest
		}
		w.WriteErrorWithCode(statusCode, err)
		return
	}

	w.WriteObject(nil)
}

// GetHash swagger:route GET /config/{namespace}/hash getJobsHash
// returns hash that will be different if any job changed
func (h ConfigHandler) GetHash(w *utils.ResponseWriter, r *http.Request) {
	var req GetHashRequest
	err := apiCom.ReadRequest(r, &req, w)
	if err != nil {
		w.WriteErrorWithCode(http.StatusBadRequest, err)
		return
	}

	hash, err := h.jobMutator.GetHash(req.Namespace)
	if err != nil {
		statusCode := http.StatusInternalServerError
		if mutatorCom.IsNonExist(err) {
			statusCode = http.StatusNotFound
			err = mutatorCom.ErrJobConfigDoesNotExist
		}
		w.WriteErrorWithCode(statusCode, err)
		return
	}

	w.WriteJSONBytesWithCode(http.StatusOK, []byte(hash), nil)
}
