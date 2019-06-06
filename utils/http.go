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

package utils

import (
	"fmt"
	"github.com/uber/aresdb/common"
	"golang.org/x/net/netutil"
	"net"
	"net/http"
	"strconv"
	"time"
)

const (
	HTTPContentTypeHeaderKey = "content-type"
	HTTPContentTypeApplicationJson = "application/json"
)

// HTTPHandlerWrapper wraps context aware httpHandler
type HTTPHandlerWrapper func(handler http.HandlerFunc) http.HandlerFunc

var epoch = time.Unix(0, 0).Format(time.RFC1123)

var noCacheHeaders = map[string]string{
	"Expires":         epoch,
	"Cache-Control":   "no-cache, private, max-age=0",
	"Pragma":          "no-cache",
	"X-Accel-Expires": "0",
}

var etagHeaders = []string{
	"ETag",
	"If-Modified-Since",
	"If-Match",
	"If-None-Match",
	"If-Range",
	"If-Unmodified-Since",
}

// NoCache sets no cache headers and removes any ETag headers that may have been set.
func NoCache(h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		for _, v := range etagHeaders {
			if r.Header.Get(v) != "" {
				r.Header.Del(v)
			}
		}

		for k, v := range noCacheHeaders {
			w.Header().Set(k, v)
		}

		h.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

// responseWriter is the wrapper over http.ResponseWriter to store status code. Unfortunately from built in
// http.ResponseWriter we cannot get status code.
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

// newResponseWriter returns ResponseWriter with default status code status ok.
func newResponseWriter(w http.ResponseWriter) *responseWriter {
	return &responseWriter{w, http.StatusOK}
}

// WriteHeader stores the status code as well as write the status code to original response writer.
func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// WithMetricsFunc will send stats like latency, rps and returning status code after the http handler finishes.
// It has to be applied to the actual handler function who serves the http request.
func WithMetricsFunc(h http.HandlerFunc) http.HandlerFunc {
	funcName := GetFuncName(h)

	fn := func(w http.ResponseWriter, r *http.Request) {
		origin := GetOrigin(r)
		stopWatch := GetRootReporter().GetChildTimer(map[string]string{
			metricsTagHandler: funcName,
			metricsTagOrigin:  origin,
		}, HTTPHandlerLatency).Start()
		defer stopWatch.Stop()
		rw := newResponseWriter(w)
		h.ServeHTTP(rw, r)
		GetRootReporter().GetChildCounter(map[string]string{
			metricsTagHandler:    funcName,
			metricsTagStatusCode: strconv.Itoa(rw.statusCode),
			metricsTagOrigin:     origin,
		}, HTTPHandlerCall).Inc(1)
	}
	return fn
}

// GetOrigin returns the caller of the request.
func GetOrigin(r *http.Request) string {
	origin := r.Header.Get("RPC-Caller")
	if origin == "" {
		origin = r.Header.Get("X-Uber-Origin")
	}

	if origin == "" {
		origin = "UNKNOWN"
	}
	return origin
}

// NoopHTTPWrapper does nothing; used for testing
func NoopHTTPWrapper(h http.HandlerFunc) http.HandlerFunc {
	return h
}

// ApplyHTTPWrappers apply wrappers according to the order
func ApplyHTTPWrappers(handler http.HandlerFunc, wrappers []HTTPHandlerWrapper) http.HandlerFunc {
	h := handler
	for _, wrapper := range wrappers {
		h = wrapper(h)
	}
	return h
}

// LimitServe will start a http server on the port with the handler and at most maxConnection concurrent connections.
func LimitServe(port int, handler http.Handler, httpCfg common.HTTPConfig) {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		GetLogger().Fatal(err)
	}
	defer listener.Close()

	listener = netutil.LimitListener(listener, httpCfg.MaxConnections)
	server := &http.Server{
		ReadTimeout:  time.Duration(httpCfg.ReadTimeOutInSeconds) * time.Second,
		WriteTimeout: time.Duration(httpCfg.WriteTimeOutInSeconds) * time.Second,
		Handler:      handler,
	}
	GetLogger().Fatal(server.Serve(listener))
}
