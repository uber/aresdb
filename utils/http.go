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
	"encoding/json"
	"fmt"
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/common"
	"go.uber.org/zap"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"golang.org/x/net/netutil"
	"net"
	"net/http"
	"strconv"
	"time"
)

const (
	HTTPContentTypeHeaderKey     = "content-type"
	HTTPAcceptTypeHeaderKey      = "accept"
	HTTPAcceptEncodingHeaderKey  = "Accept-Encoding"
	HTTPContentEncodingHeaderKey = "Content-Encoding"

	HTTPContentTypeApplicationJson = "application/json"
	HTTPContentTypeApplicationGRPC = "application/grpc"
	// HTTPContentTypeUpsertBatch defines the upsert data content type.
	HTTPContentTypeUpsertBatch = "application/upsert-data"
	// HTTPContentTypeHyperLogLog defines the hyperloglog query result content type.
	HTTPContentTypeHyperLogLog = "application/hll"
	HTTPContentEncodingGzip    = "gzip"
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
		Handler:      h2c.NewHandler(handler, &http2.Server{}),
	}
	GetLogger().Fatal(server.Serve(listener))
}

// LimitServe will start a http server on the port with the handler and at most maxConnection concurrent connections.
func LimitServeAsync(port int, handler http.Handler, httpCfg common.HTTPConfig) (chan error, *http.Server) {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		GetLogger().Fatal(err)
	}

	listener = netutil.LimitListener(listener, httpCfg.MaxConnections)
	server := &http.Server{
		ReadTimeout:  time.Duration(httpCfg.ReadTimeOutInSeconds) * time.Second,
		WriteTimeout: time.Duration(httpCfg.WriteTimeOutInSeconds) * time.Second,
		Handler:      h2c.NewHandler(handler, &http2.Server{}),
	}
	errChan := make(chan error)
	go func() {
		defer listener.Close()
		errChan <- server.Serve(listener)
	}()
	return errChan, server
}

//TODO: port over from controller part of consolidation
type HandlerFunc func(rw *ResponseWriter, r *http.Request)

// HTTPHandlerWrapper2 wraps http handler function
type HTTPHandlerWrapper2 func(handler HandlerFunc) HandlerFunc

// ApplyHTTPWrappers2 apply wrappers according to the order
func ApplyHTTPWrappers2(handler HandlerFunc, wrappers ...HTTPHandlerWrapper2) http.HandlerFunc {
	h := handler
	for _, wrapper := range wrappers {
		h = wrapper(h)
	}

	return func(writer http.ResponseWriter, request *http.Request) {
		rw := NewResponseWriter(writer)
		h(rw, request)
	}
}

// MetricsLoggingMiddleWareProvider provides middleware for metrics and logger for http requests
type MetricsLoggingMiddleWareProvider struct {
	scope  tally.Scope
	logger *zap.SugaredLogger
}

// NewMetricsLoggingMiddleWareProvider creates metrics and logging middleware provider
func NewMetricsLoggingMiddleWareProvider(scope tally.Scope, logger *zap.SugaredLogger) MetricsLoggingMiddleWareProvider {
	return MetricsLoggingMiddleWareProvider{
		scope:  scope,
		logger: logger,
	}
}

// WithLogging plug in metrics middleware
func (p *MetricsLoggingMiddleWareProvider) WithMetrics(next HandlerFunc) HandlerFunc {
	funcName := GetFuncName(next)
	return func(rw *ResponseWriter, r *http.Request) {
		origin := GetOrigin(r)
		stopWatch := p.scope.Tagged(map[string]string{
			metricsTagHandler:   funcName,
			metricsTagOrigin: origin,
		}).Timer(scopeNameHTTPHandlerLatency).Start()
		next(rw, r)
		stopWatch.Stop()
		p.scope.Tagged(map[string]string{
			metricsTagHandler:    funcName,
			metricsTagOrigin:     origin,
			metricsTagStatusCode: strconv.Itoa(rw.statusCode),
		}).Counter(scopeNameHTTPHandlerLatency).Inc(1)
	}
}

// WithLogging plug in logging middleware
func (p *MetricsLoggingMiddleWareProvider) WithLogging(next HandlerFunc) HandlerFunc {
	return func(rw *ResponseWriter, r *http.Request) {
		next(rw, r)
		if rw.err != nil {
			p.logger.With(
				"request", rw.req,
				"status", rw.statusCode,
				"error", rw.err,
				"method", r.Method,
				"name", r.URL.Path,
			).Errorf("request failed")
		} else {
			p.logger.With(
				"request", rw.req,
				"name", r.URL.Path,
			).Infof("request succeeded")
		}
	}
}

func setCommonHeaders(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")
}

// ErrorResponse represents error response.
// swagger:response errorResponse
type ErrorResponse struct {
	//in: body
	Body APIError
}

// ResponseWriter decorates http.ResponseWriter
type ResponseWriter struct {
	http.ResponseWriter
	statusCode int
	req        interface{}
	err        error
}

// NewResponseWriter returns response writer with status code 200
func NewResponseWriter(rw http.ResponseWriter) *ResponseWriter {
	return &ResponseWriter{
		statusCode:     http.StatusOK,
		ResponseWriter: rw,
	}
}

// WriteRequest write request
func (s *ResponseWriter) WriteRequest(req interface{}) {
	s.req = req
}

// WriteHeader implements http.ResponseWriter WriteHeader for write status code
func (s *ResponseWriter) WriteHeader(code int) {
	s.statusCode = code
	s.ResponseWriter.WriteHeader(code)
}

// Write implements http.ResponseWriter Write for write bytes
func (s *ResponseWriter) Write(bts []byte) (int, error) {
	setCommonHeaders(s)
	return s.ResponseWriter.Write(bts)
}

// WriteJSONBytes write json bytes with default status ok
func (s *ResponseWriter) WriteJSONBytes(jsonBytes []byte, marshalErr error) {
	s.WriteJSONBytesWithCode(http.StatusOK, jsonBytes, marshalErr)
}

// WriteJSONBytesWithCode write json bytes and marshal error to response
func (s *ResponseWriter) WriteJSONBytesWithCode(code int, jsonBytes []byte, marshalErr error) {
	if marshalErr != nil {
		resp := ErrorResponse{}
		resp.Body.Cause = marshalErr
		code = http.StatusInternalServerError
		resp.Body.Code = code
		resp.Body.Message = "failed to marshal object"
		// ignore this error since this should not happen
		jsonBytes, _ = json.Marshal(resp)
	}
	s.Header().Set("Content-Type", "application/json")
	s.WriteHeader(code)
	_, _ = s.Write(jsonBytes)
}

// WriteObject write json object to response
func (s *ResponseWriter) WriteObject(obj interface{}) {
	s.WriteObjectWithCode(http.StatusOK, obj)
}

// WriteObjectWithCode serialize object and write code
func (s *ResponseWriter) WriteObjectWithCode(code int, obj interface{}) {
	if obj != nil {
		jsonBytes, err := json.Marshal(obj)
		s.WriteJSONBytesWithCode(code, jsonBytes, err)
	}
}

// WriteErrorWithCode writes error with specific code
func (s *ResponseWriter) WriteErrorWithCode(code int, err error) {
	s.err = err
	var errorResponse ErrorResponse
	errorResponse.Body.Code = code
	if e, ok := err.(APIError); ok {
		errorResponse.Body = e
	} else {
		errorResponse.Body.Message = err.Error()
		errorResponse.Body.Cause = err
	}
	s.WriteObjectWithCode(errorResponse.Body.Code, errorResponse)
}

// WriteError write error to response
func (s *ResponseWriter) WriteError(err error) {
	s.WriteErrorWithCode(http.StatusInternalServerError, err)
}
