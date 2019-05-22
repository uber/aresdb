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
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/controller/cluster"
	"github.com/uber/aresdb/controller/mutators/common"
	"go.uber.org/config"
	"go.uber.org/zap"
)

// IngestionAssignmentTaskParams defines all parameters needed to create an IngestionAssignmentTask
type IngestionAssignmentTaskParams struct {
	ConfigProvider config.Provider
	Logger         *zap.SugaredLogger
	Scope          tally.Scope

	EtcdClient         *cluster.EtcdClient
	NamespaceMutator   common.NamespaceMutator
	JobMutator         common.JobMutator
	SchemaMutator      common.TableSchemaMutator
	AssignmentsMutator common.IngestionAssignmentMutator
	SubscriberMutator  common.SubscriberMutator
}

// Task is the interface for a long running task
type Task interface {
	Run()
	Done()
}
