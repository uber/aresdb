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
)

// HealthHandler handles health check requests
type HealthHandler struct{}

// NewHealthHandler returns a new health handler
func NewHealthHandler() HealthHandler {
	return HealthHandler{}
}

// Register adds paths to router
func (h HealthHandler) Register(router *mux.Router) {
	router.HandleFunc("/health", h.Health)
}

// Health serves health requests
func (h HealthHandler) Health(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}
