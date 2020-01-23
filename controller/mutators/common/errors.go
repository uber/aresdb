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
	"errors"
	"github.com/uber/aresdb/utils"
	"net/http"

	"github.com/m3db/m3/src/cluster/kv"
)

// NotExist represents not exist error
type NotExist string

func (e NotExist) Error() string {
	return string(e)
}

var (
	// ErrNamespaceAlreadyExists indicates namespace already exists
	ErrNamespaceAlreadyExists = errors.New("Namespace already exists")
	// ErrInvalidJobConfig indicates job config is invalid
	ErrInvalidJobConfig = errors.New("Job config is invalid")
	// ErrIllegalJobConfigVersion indicates job config version is illegal
	ErrIllegalJobConfigVersion = errors.New("Job config version is illegal")
	// ErrJobConfigAlreadyExist indicates job config already exists
	ErrJobConfigAlreadyExist = errors.New("Job config already exists")
	// ErrIngestionAssignmentAlreadyExist indicates an ingestion assignment already exists
	ErrIngestionAssignmentAlreadyExist = errors.New("Ingestion assignment already exists")
	// ErrInstanceAlreadyExist indicates an instance already exists
	ErrInstanceAlreadyExist = errors.New("Instance already exists")

	// ErrJobConfigDoesNotExist indicates job config does not exist
	ErrJobConfigDoesNotExist = NotExist("Job config does not exist")
	// ErrIngestionAssignmentDoesNotExist indicats an assignment does not exist
	ErrIngestionAssignmentDoesNotExist = NotExist("Ingestion assignment does not exist")
	// ErrNamespaceDoesNotExist indicates namespace does not exist
	ErrNamespaceDoesNotExist = NotExist("Namespace does not exist")
	// ErrInstanceDoesNotExist indicates an instance does not exist
	ErrInstanceDoesNotExist = NotExist("Instance does not exist")
	// ErrSubscriberDoesNotExist indicates an subscriber does not exist
	ErrSubscriberDoesNotExist = NotExist("Subscriber does not exist")

	// ErrMsgFailedToBuildInitialPlacement represents error message for initial placement failure
	ErrMsgFailedToBuildInitialPlacement = "failed build initial placement"
	// ErrMsgFailedToGetPlacementService represents error message for failure in getting placement service
	ErrMsgFailedToGetPlacementService = "failed to get placement service"
	// ErrMsgFailedToGetCurrentPlacement represents error message for failure in getting current placement
	ErrMsgFailedToGetCurrentPlacement = "failed to get current placement"
	// ErrMsgPlacementNotExist represents error message for placement does not exist
	ErrMsgPlacementNotExist = "placement does not exist"
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

	// ErrInvalidNumShards represents invalid number of shards
	ErrInvalidNumShards = utils.APIError{
		Code:    http.StatusBadRequest,
		Message: "invalid number of shards, should be exponential of 2",
	}

)

// IsNonExist check whether error is non exist error
func IsNonExist(err error) bool {
	if _, ok := err.(NotExist); ok {
		return true
	}
	return err == kv.ErrNotFound
}
