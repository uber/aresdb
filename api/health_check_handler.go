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
	"github.com/uber/aresdb/api/common"
	"github.com/uber/aresdb/utils"
	"io"
	"net/http"
	"sync"
)

// HealthCheckHandler http handler for health check.
type HealthCheckHandler struct {
	sync.RWMutex
	// This flag controls whether returns 200 health or 503 service unavaible in health check handler.
	// Useful when server is lagging behind too much so developper manually call an API in debug handler
	// to disable the health check.
	disable bool
}

// NewHealthCheckHandler return a new http handler for health check.
func NewHealthCheckHandler() *HealthCheckHandler {
	return &HealthCheckHandler{}
}

// HealthCheck is the HealthCheck endpoint.
func (handler *HealthCheckHandler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	handler.RLock()
	disabled := handler.disable
	handler.RUnlock()
	if disabled {
		common.RespondBytesWithCode(w, http.StatusServiceUnavailable, []byte("Health check disabled"))
	} else {
		io.WriteString(w, "OK")
	}
}

// Version is the Version check endpoint.
func (handler *HealthCheckHandler) Version(w http.ResponseWriter, r *http.Request) {
	io.WriteString(w, utils.GetConfig().Version)
}
