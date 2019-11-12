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

// TODO: refactor api pkg and import from ares directly
// copied from ares/api/error.go since importing api pkg requires memstore which
// requires binaries from C code.
package handlers

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
	// ErrMsgFailedToBuildInitialPlacement represents error message for initial placement failure
	ErrMsgFailedToBuildInitialPlacement = "failed build initial placement"
	// ErrMsgFailedToGetPlacementService represents error message for failure in getting placement service
	ErrMsgFailedToGetPlacementService = "failed to get placement service"
	// ErrMsgFailedToGetCurrentPlacement represents error message for failure in getting current placement
	ErrMsgFailedToGetCurrentPlacement = "failed to get current placement"
	// ErrMsgFailedToAddInstance represents error message for failure in adding instance
	ErrMsgFailedToAddInstance = "failed to add instance to placement"
	// ErrMsgFailedToReplaceInstance represents error message for failure in replacing instance
	ErrMsgFailedToReplaceInstance = "failed to replace instance to placement"
	// ErrMsgFailedToMarkAvailable represents error message for failure in marking instance or shard available
	ErrMsgFailedToMarkAvailable = "failed to mark instance/shards available"
	// ErrMsgFailedToMarshalPlacement represents error message for failure to marshal placement
	ErrMsgFailedToMarshalPlacement = "failed to marshal placement"
	// ErrMsgFailedToRemoveInstance represents error message for failure to remove instance from a placement
	ErrMsgFailedToRemoveInstance = "failed to remove instance from placement"

	// ErrRemoveOneInstance represents error message for not removing one instance at a time
	ErrRemoveOneInstance = utils.APIError{
		Code:    http.StatusBadRequest,
		Message: "expected to remove one instance at a time",
	}

	// ErrInvalidNumShards represents invalid number of shards
	ErrInvalidNumShards = utils.APIError{
		Code:    http.StatusBadRequest,
		Message: "invalid number of shards, should be exponential of 2",
	}

	// ErrMissingParameter represents error for missing parameter
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
	// ErrFailedToFetchTableSchemaForJobConfig represents the api error for failure to
	// fetch table schema for job config.
	ErrFailedToFetchTableSchemaForJobConfig = utils.APIError{
		Code:    http.StatusInternalServerError,
		Message: "Failed to fetch table schema for job config",
	}
	// ErrEtcdNotAvailable represents the api error for service unavailable
	ErrEtcdNotAvailable = utils.APIError{
		Code:    http.StatusNotFound,
		Message: "Etcd not available",
	}
	// ErrFailedToFetchPlacement represents the api error for failure to
	// fetch placement for job config
	ErrFailedToFetchPlacement = utils.APIError{
		Code:    http.StatusInternalServerError,
		Message: "Failed to fetch placement for job config",
	}
)
