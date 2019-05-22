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
	"github.com/uber/aresdb/controller/models"
)

// Validate check job config should be valid
func Validate(newJobConfig, oldJobConfig *models.JobConfig) (err error) {
	err = validateJobConfig(newJobConfig)
	if err != nil {
		return
	}
	if oldJobConfig != nil {
		err = validateJobUpdate(newJobConfig, oldJobConfig)
	}
	return
}

// check job config should contains all fields
func validateJobConfig(job *models.JobConfig) (err error) {
	if job.Name == "" || job.Kafka.Cluster == "" || job.Kafka.Topic == "" || job.Table.Name == "" {
		err = ErrInvalidJobConfig
	}
	return
}

func validateJobUpdate(newJobConfig, oldJobConfig *models.JobConfig) (err error) {
	// TODO
	return
}
