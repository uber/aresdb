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

package broker

import (
	"github.com/uber/aresdb/controller/client"
	memCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/broker/common"
	"github.com/uber/aresdb/metastore"
	"github.com/uber/aresdb/utils"
	"sync"
)

const (
	schemaFetchInterval = 10
)



type schemaManagerImpl struct {
	sync.RWMutex

	controllerCli client.ControllerClient
	mutators      map[string]*BrokerSchemaMutator
}

// NewSchemaManager returns a new SchemaManager instance
func NewSchemaManager(controllerCli client.ControllerClient) common.SchemaManager {
	return &schemaManagerImpl{
		controllerCli: controllerCli,
	}
}

// Run initializes all schema and starts jobs to sync from controller
func (sm *schemaManagerImpl) Run() {
	// TODO add job to get new namespace
	namespaces, err := sm.controllerCli.GetNamespaces()
	if err != nil {
		utils.GetLogger().Fatal("Failed to fetch namespaces from controller", err)
	}

	for _, namespace := range namespaces {
		mutator := NewBrokerSchemaMutator()
		job := metastore.NewSchemaFetchJob(schemaFetchInterval, mutator, metastore.NewTableSchameValidator(), sm.controllerCli, namespace, "")
		job.FetchSchema()
		go job.Run()
		sm.mutators[namespace] = mutator
	}
}

func (sm *schemaManagerImpl) GetTableSchemaReader(namespace string) (tableSchemaReader memCom.TableSchemaReader, err error) {
	var exists bool
	tableSchemaReader, exists = sm.mutators[namespace]
	if !exists {
		err = utils.StackError(nil, "namespace doesn't exist")
		return
	}
	return
}
