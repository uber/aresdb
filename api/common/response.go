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

package common

import (
	"compress/gzip"
	"encoding/json"
	"net/http"

	"github.com/uber/aresdb/utils"
)

const (
	// CompressionThreshold is the min number of bytes beyond which we will compress json payload
	CompressionThreshold = 1 << 10
)

// NoContentResponse represents Response with no content.
// swagger:response noContentResponse
type NoContentResponse struct{}

// StringArrayResponse represents string array response.
// swagger:response stringArrayResponse
type StringArrayResponse struct {
	//in: body
	Body []string
}

// NewStringArrayResponse creates a StringArrayResponse.
func NewStringArrayResponse() StringArrayResponse {
	return StringArrayResponse{
		Body: []string{},
	}
}

// Respond responds with object with success status
func Respond(w http.ResponseWriter, obj interface{}) {
	RespondJSONObjectWithCode(w, http.StatusOK, obj)
}

// RespondJSONObjectWithCode with specified code and object.
func RespondJSONObjectWithCode(w http.ResponseWriter, code int, obj interface{}) {
	setCommonHeaders(w)
	var err error
	var jsonBytes []byte
	if obj != nil {
		jsonBytes, err = json.Marshal(obj)
	}
	writeJSONBytes(w, jsonBytes, err, code)
}

// RespondBytesWithCode with specified code and bytes.
func RespondBytesWithCode(w http.ResponseWriter, code int, bs []byte) {
	setCommonHeaders(w)
	w.WriteHeader(code)
	if bs != nil {
		w.Write(bs)
	}
}

// writeJSONBytes write jsonBytes to response if err is nil otherwise respond
// with a ErrFailedToJSONMarshalResponseBody.
func writeJSONBytes(w http.ResponseWriter, jsonBytes []byte, err error, code int) {
	var gw *gzip.Writer
	if err != nil {
		RespondWithError(w, ErrFailedToJSONMarshalResponseBody)
	}
	w.Header().Set(utils.HTTPContentTypeHeaderKey, utils.HTTPContentTypeApplicationJson)
	willCompress := len(jsonBytes) > CompressionThreshold
	if willCompress {
		w.Header().Set(utils.HTTPContentEncodingHeaderKey, utils.HTTPContentEncodingGzip)
	}
	w.WriteHeader(code)
	if jsonBytes != nil {
		if willCompress {
			gw, err = gzip.NewWriterLevel(w, gzip.BestSpeed)
			if err != nil {
				RespondWithError(w, ErrFailedToJSONMarshalResponseBody)
				return
			}
			defer gw.Close()
			gw.Write(jsonBytes)
		} else {
			w.Write(jsonBytes)
		}

	}
}

// RespondWithJSONBytes writes json bytes to response.
func RespondWithJSONBytes(w http.ResponseWriter, jsonBytes []byte, err error) {
	setCommonHeaders(w)
	writeJSONBytes(w, jsonBytes, err, http.StatusOK)
}

// RespondWithJSONObject marshals the object into json bytes and write the bytes
// into response.
func RespondWithJSONObject(w http.ResponseWriter, jsonObj interface{}) {
	RespondJSONObjectWithCode(w, http.StatusOK, jsonObj)
}

// setCommonHeaders writes no cache headers.
func setCommonHeaders(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")
}

// RespondWithError responds with error.
func RespondWithError(w http.ResponseWriter, err error) {
	var errorResponse utils.ErrorResponse
	if e, ok := err.(utils.APIError); ok {
		errorResponse = utils.ErrorResponse{
			Body: e,
		}
	} else {
		errorResponse = utils.ErrorResponse{
			Body: utils.APIError{
				Code:    http.StatusInternalServerError,
				Message: err.Error(),
			},
		}
	}
	RespondJSONObjectWithCode(w, errorResponse.Body.Code, errorResponse.Body)
}

// RespondWithBadRequest responds with StatusBadRequest as code.
func RespondWithBadRequest(w http.ResponseWriter, err error) {

	if e, ok := err.(utils.APIError); ok {
		e.Code = http.StatusBadRequest
		RespondWithError(w, e)
		return
	}

	RespondWithError(w, utils.APIError{
		Code:    http.StatusBadRequest,
		Cause:   err,
		Message: err.Error(),
	})
}
