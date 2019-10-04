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
	"net/http"

	"github.com/uber/aresdb/utils"
)

var (
	// ErrMsgFailedToUnmarshalRequest represents error message for unmarshal error.
	ErrMsgFailedToUnmarshalRequest = "Bad request: failed to unmarshal request body"
	// ErrMsgMissingParameter represents error message for missing params error.
	ErrMsgMissingParameter = "Bad request: missing/invalid parameter"
	// ErrMsgFailedToReadRequestBody represents error message for unable to read request body error.
	ErrMsgFailedToReadRequestBody = "Bad request: failed to read request body"
	// ErrMsgNonExistentTable represents error message for table does not exist
	ErrMsgNonExistentTable = "Bad request: table does not exist"
	// ErrMsgNonExistentColumn represents error message for column does not exist
	ErrMsgNonExistentColumn = "Bad request: column does not exist"
	// ErrMsgDeletedColumn represents error message for column is already deleted
	ErrMsgDeletedColumn = "Bad request: column is already deleted"
	// ErrMsgNotImplemented represents error message for method not implemented.
	ErrMsgNotImplemented = "Not implemented"
	// ErrMsgFailedToJSONMarshalResponseBody respresents error message for failure to marshal
	// response body into json.
	ErrMsgFailedToJSONMarshalResponseBody = "Failed to marshal the response body into json"
	// ErrMissingParameter represents api error for missing parameter
	ErrMissingParameter = utils.APIError{
		Code:    http.StatusBadRequest,
		Message: ErrMsgMissingParameter,
	}
	// ErrNotImplemented represents api error for method not implemented.
	ErrNotImplemented = utils.APIError{
		Code:    http.StatusNotImplemented,
		Message: ErrMsgNotImplemented,
	}
	// ErrTableDoesNotExist represents api error for table does not exist.
	ErrTableDoesNotExist = utils.APIError{
		Code:    http.StatusBadRequest,
		Message: ErrMsgNonExistentTable,
	}
	// ErrColumnDoesNotExist represents api error for column does not exist.
	ErrColumnDoesNotExist = utils.APIError{
		Code:    http.StatusBadRequest,
		Message: ErrMsgNonExistentColumn,
	}
	// ErrColumnDeleted represents api error for column is already deleted.
	ErrColumnDeleted = utils.APIError{
		Code:    http.StatusBadRequest,
		Message: ErrMsgDeletedColumn,
	}
	// ErrBatchDoesNotExist represents api error for batch does not exist.
	ErrBatchDoesNotExist = utils.APIError{
		Code:    http.StatusBadRequest,
		Message: "Bad request: batch does not exist",
	}
	// ErrFailedToJSONMarshalResponseBody represents the api error for failure to marshal
	// response body into json.
	ErrFailedToJSONMarshalResponseBody = utils.APIError{
		Code:    http.StatusInternalServerError,
		Message: ErrMsgFailedToJSONMarshalResponseBody,
	}

	// ErrQueryServiceNotAvailable represents the api error for failed to run query
	// due to to much pending queries executing
	ErrQueryServiceNotAvailable = utils.APIError{
		Code: http.StatusServiceUnavailable,
		Message: "Service unavailable: too many concurrent queries, please try again later",
	}

	// ErrIngestionServiceNotAvailable represents the api error
	// due to to much pending ingestion requests executing
	ErrIngestionServiceNotAvailable = utils.APIError{
		Code: http.StatusServiceUnavailable,
		Message: "Service unavailable: too many concurrent ingestion requests, please try again later",
	}

)
