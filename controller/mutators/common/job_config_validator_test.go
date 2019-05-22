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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJobConfigValidator(t *testing.T) {
	oldJob := models.JobConfig{
		Name:    "demand",
		Version: 0,
		AresTableConfig: models.TableConfig{
			Name: "rta_table1",
		},
		StreamingConfig: models.KafkaConfig{
			Topic:   "demand_topic1",
			Cluster: "demand_cluster",
		},
	}

	job := models.JobConfig{
		Name:    "demand",
		Version: 1,
		AresTableConfig: models.TableConfig{
			Name: "rta_table1",
		},
		StreamingConfig: models.KafkaConfig{
			Topic:   "demand_topic1",
			Cluster: "demand_cluster",
		},
	}

	t.Run("validate job config should work", func(t *testing.T) {
		err := Validate(&oldJob, nil)
		assert.NoError(t, err)

		err = Validate(&job, &oldJob)
		assert.NoError(t, err)
	})

	t.Run("invalid job config should not work", func(t *testing.T) {
		invalid := models.JobConfig{
			Name:    "demand",
			Version: 0,
		}
		err := Validate(&invalid, nil)
		assert.EqualError(t,  err, "Job config is invalid")
	})
}
