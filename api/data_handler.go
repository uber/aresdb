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
	"github.com/m3db/m3/src/x/sync"
	"net/http"

	"github.com/uber/aresdb/api/common"
	"github.com/uber/aresdb/memstore"
	memCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/utils"

	"github.com/gorilla/mux"
)

// DataHandler handles data ingestion requests from the ingestion pipeline.
type DataHandler struct {
	memStore   memstore.MemStore
	workerPool sync.WorkerPool
}

// NewDataHandler creates a new DataHandler.
func NewDataHandler(memStore memstore.MemStore, maxConcurrentRequests int) *DataHandler {
	workerPool := sync.NewWorkerPool(maxConcurrentRequests)
	workerPool.Init()
	return &DataHandler{
		memStore:   memStore,
		workerPool: workerPool,
	}
}

// Register registers http handlers.
func (handler *DataHandler) Register(router *mux.Router, wrappers ...utils.HTTPHandlerWrapper) {
	router.HandleFunc("/{table}/{shard}", utils.ApplyHTTPWrappers(handler.PostData, wrappers)).Methods(http.MethodPost)
}

// PostData swagger:route POST /data/{table}/{shard} postData
// Post new data batch to a existing table shard
// Consumes:
//    - application/upsert-data
//
// Responses:
//    default: errorResponse
//        200: noContentResponse
func (handler *DataHandler) PostData(w http.ResponseWriter, r *http.Request) {
	var postDataRequest PostDataRequest
	err := common.ReadRequest(r, &postDataRequest)
	if err != nil {
		common.RespondWithError(w, err)
		return
	}

	upsertBatch, err := memCom.NewUpsertBatch(postDataRequest.Body)
	if err != nil {
		common.RespondWithBadRequest(w, err)
		return
	}

	done := make(chan struct{})
	available := handler.workerPool.GoIfAvailable(func() {
		defer close(done)
		err = handler.memStore.HandleIngestion(postDataRequest.TableName, postDataRequest.Shard, upsertBatch)
		if err != nil {
			common.RespondWithError(w, err)
			return
		}
		common.RespondWithJSONObject(w, nil, false)
	})

	if !available {
		common.RespondWithError(w, common.ErrIngestionServiceNotAvailable)
		return
	}
	<-done
}
