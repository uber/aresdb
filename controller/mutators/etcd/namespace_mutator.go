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
package etcd

import (
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/uber/aresdb/controller/cluster"
	pb "github.com/uber/aresdb/controller/generated/proto"
	"github.com/uber/aresdb/controller/mutators/common"
	"github.com/uber/aresdb/utils"
)

// NewNamespaceMutator returns a new NamespaceMutator
func NewNamespaceMutator(etcdStore kv.TxnStore) common.NamespaceMutator {
	return &namespaceMutatorImpl{
		txnStore: etcdStore,
	}
}

type namespaceMutatorImpl struct {
	txnStore kv.TxnStore
}

func (m *namespaceMutatorImpl) CreateNamespace(namespace string) (err error) {
	nsList, nsListVersion, err := readEntityList(m.txnStore, utils.NamespaceListKey())
	if common.IsNonExist(err) {
		nsListVersion = kv.UninitializedVersion
		nsList = pb.EntityList{}
	} else if err != nil {
		return err
	}

	nsList, _, exist := addEntity(nsList, namespace)
	if exist {
		return common.ErrNamespaceAlreadyExists
	}

	return cluster.NewTransaction().
		AddKeyValue(utils.NamespaceListKey(), nsListVersion, &nsList).
		// pre create schema, job, job assignments list key
		AddKeyValue(utils.SchemaListKey(namespace), kv.UninitializedVersion, &pb.EntityList{}).
		AddKeyValue(utils.JobListKey(namespace), kv.UninitializedVersion, &pb.EntityList{}).
		AddKeyValue(utils.JobAssignmentsListKey(namespace), kv.UninitializedVersion, &pb.EntityList{}).
		WriteTo(m.txnStore)
}

func (m *namespaceMutatorImpl) ListNamespaces() ([]string, error) {
	entityList, _, err := readEntityList(m.txnStore, utils.NamespaceListKey())
	if err != nil {
		return nil, err
	}
	result := make([]string, len(entityList.Entities))
	for i, ns := range entityList.Entities {
		result[i] = ns.Name
	}
	return result, nil
}
