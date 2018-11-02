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
	"net/http"
)

// PanicHandler is a wrapper handler for handling panic in http request handling
type PanicHandler struct {
	handler http.Handler
}

// WithPanicHandling will apply panic handler to regular http handler.
func WithPanicHandling(handler http.Handler) PanicHandler {
	return PanicHandler{
		handler: handler,
	}
}

// ServeHTTP serves http request for PanicHandler.
func (handler PanicHandler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	defer func() {
		if r := recover(); r != nil {
			rw.WriteHeader(http.StatusInternalServerError)
			switch e := r.(type) {
			case error:
				rw.Write([]byte(e.Error()))
			case string:
				rw.Write([]byte(e))
			default:
				rw.Write([]byte("Unknown Panic"))
			}
		}
	}()
	handler.handler.ServeHTTP(rw, r)
}
