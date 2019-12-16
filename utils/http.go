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
	"compress/gzip"
	"encoding/json"
	"fmt"
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/common"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"golang.org/x/net/netutil"
	"net"
	"net/http"
	"strconv"
	"time"
)

const (
	HTTPContentTypeHeaderKey     = "Content-Type"
	HTTPAcceptTypeHeaderKey      = "Accept"
	HTTPAcceptEncodingHeaderKey  = "Accept-Encoding"
	HTTPContentEncodingHeaderKey = "Content-Encoding"

	HTTPContentTypeApplicationJson = "application/json"
	HTTPContentTypeApplicationGRPC = "application/grpc"
	// HTTPContentTypeUpsertBatch defines the upsert data content type.
	HTTPContentTypeUpsertBatch = "application/upsert-data"
	// HTTPContentTypeHyperLogLog defines the hyperloglog query result content type.
	HTTPContentTypeHyperLogLog = "application/hll"
	HTTPContentEncodingGzip    = "gzip"

	// CompressionThreshold is the min number of bytes beyond which we will compress json payload
	CompressionThreshold = 1 << 10
)

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

// LimitServeAsync will start a http server on the port with the handler and at most maxConnection concurrent connections.
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

// HandlerFunc defines http handler function
type HandlerFunc func(rw *ResponseWriter, r *http.Request)

// HTTPHandlerWrapper wraps http handler function
type HTTPHandlerWrapper func(handler HandlerFunc) HandlerFunc

// ApplyHTTPWrappers apply wrappers according to the order
func ApplyHTTPWrappers(handler HandlerFunc, wrappers ...HTTPHandlerWrapper) http.HandlerFunc {
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
	logger common.Logger
}

// NewMetricsLoggingMiddleWareProvider creates metrics and logging middleware provider
func NewMetricsLoggingMiddleWareProvider(scope tally.Scope, logger common.Logger) MetricsLoggingMiddleWareProvider {
	return MetricsLoggingMiddleWareProvider{
		scope:  scope,
		logger: logger,
	}
}

// WithMetrics plug in metrics middleware
func (p *MetricsLoggingMiddleWareProvider) WithMetrics(next HandlerFunc) HandlerFunc {
	funcName := GetFuncName(next)
	return func(rw *ResponseWriter, r *http.Request) {
		origin := GetOrigin(r)
		stopWatch := p.scope.Tagged(map[string]string{
			metricsTagHandler: funcName,
			metricsTagOrigin:  origin,
		}).Timer(scopeNameHTTPHandlerLatency).Start()
		next(rw, r)
		stopWatch.Stop()
		p.scope.Tagged(map[string]string{
			metricsTagHandler:    funcName,
			metricsTagOrigin:     origin,
			metricsTagStatusCode: strconv.Itoa(rw.statusCode),
		}).Counter(scopeNameHTTPHandlerCall).Inc(1)
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
			).Debug("request succeeded")
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

// SetRequest set unmarshalled request body to response writer for logging purpose
func (s *ResponseWriter) SetRequest(req interface{}) {
	s.req = req
}

// WriteHeader implements http.ResponseWriter WriteHeader for write status code
func (s *ResponseWriter) WriteHeader(code int) {
	if code > 0 {
		s.statusCode = code
		s.ResponseWriter.WriteHeader(code)
	}
}

// WriteBytes implements http.ResponseWriter Write for write bytes
func (s *ResponseWriter) WriteBytes(bts []byte) {
	setCommonHeaders(s)
	s.Write(bts)
}

// WriteBytesWithCode writes bytes with code
func (s *ResponseWriter) WriteBytesWithCode(code int, bts []byte) {
	setCommonHeaders(s)
	s.WriteHeader(code)
	if bts != nil {
		s.Write(bts)
	}
}

// WriteJSONBytes write json bytes with default status ok
func (s *ResponseWriter) WriteJSONBytes(jsonBytes []byte, marshalErr error) {
	s.WriteJSONBytesWithCode(http.StatusOK, jsonBytes, marshalErr)
}

// WriteJSONBytesWithCode write json bytes and marshal error to response
func (s *ResponseWriter) WriteJSONBytesWithCode(code int, jsonBytes []byte, marshalErr error) {
	s.Header().Set(HTTPContentTypeHeaderKey, HTTPContentTypeApplicationJson)

	if marshalErr != nil {
		jsonMarshalErrorResponse := ErrorResponse{}
		code = http.StatusInternalServerError
		jsonMarshalErrorResponse.Body.Code = code
		jsonMarshalErrorResponse.Body.Message = "failed to marshal object"
		jsonMarshalErrorResponse.Body.Cause = marshalErr
		// ignore this error since this should not happen
		jsonBytes, _ = json.Marshal(jsonMarshalErrorResponse.Body)
	}

	if jsonBytes == nil {
		return
	}

	// try best effort write with gzip compression
	willCompress := len(jsonBytes) > CompressionThreshold
	if willCompress {
		gw, err := gzip.NewWriterLevel(s, gzip.BestSpeed)
		if err == nil {
			defer gw.Close()

			s.Header().Set(HTTPContentEncodingHeaderKey, HTTPContentEncodingGzip)
			setCommonHeaders(s)
			s.WriteHeader(code)
			_, _ = gw.Write(jsonBytes)
			return
		}
	}

	// default to normal json response
	s.WriteBytesWithCode(code, jsonBytes)
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
	} else {
		s.WriteBytesWithCode(code, nil)
	}
}

// WriteErrorWithCode writes error with specific code
func (s *ResponseWriter) WriteErrorWithCode(code int, err error) {
	s.err = err
	var errorResponse ErrorResponse
	if e, ok := err.(APIError); ok {
		errorResponse.Body = e
	} else {
		errorResponse.Body.Message = err.Error()
	}
	errorResponse.Body.Code = code
	s.WriteObjectWithCode(errorResponse.Body.Code, errorResponse.Body)
}

// WriteError write error to response
func (s *ResponseWriter) WriteError(err error) {
	if e, ok := err.(APIError); ok {
		s.WriteErrorWithCode(e.Code, err)
	} else {
		s.WriteErrorWithCode(http.StatusInternalServerError, err)
	}
}
