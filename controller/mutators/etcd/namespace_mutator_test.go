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
	"github.com/uber/aresdb/controller/mutators/common"
	"testing"

	"github.com/stretchr/testify/assert"
	pb "github.com/uber/aresdb/controller/generated/proto"
	"github.com/uber/aresdb/utils"
)

func TestNamespaceMutator(t *testing.T) {
	t.Run("list namespaces should work", func(t *testing.T) {
		// test setup
		cleanUp, port := utils.SetUpEtcdTestServer(t)
		defer cleanUp()

		client := utils.SetUpEtcdTestClient(t, port)
		etcdStore, err := client.Txn()
		assert.NoError(t, err)

		_, err = etcdStore.Set(utils.NamespaceListKey(), &pb.EntityList{
			Entities: []*pb.EntityName{
				{
					Name: "ns1",
				},
			},
		})
		assert.NoError(t, err)

		// test
		namespaceMutator := NewNamespaceMutator(etcdStore)
		res, err := namespaceMutator.ListNamespaces()
		assert.NoError(t, err)
		assert.Equal(t, []string{"ns1"}, res)
	})

	t.Run("create namespace should work", func(t *testing.T) {
		// test setup
		cleanUp, port := utils.SetUpEtcdTestServer(t)
		defer cleanUp()

		client := utils.SetUpEtcdTestClient(t, port)
		etcdStore, err := client.Txn()
		assert.NoError(t, err)

		_, err = etcdStore.Set(utils.NamespaceListKey(), &pb.EntityList{
			Entities: []*pb.EntityName{
				{
					Name: "ns1",
				},
			},
		})
		assert.NoError(t, err)

		// test
		namespaceMutator := NewNamespaceMutator(etcdStore)
		err = namespaceMutator.CreateNamespace("ns1")
		assert.EqualError(t, err, common.ErrNamespaceAlreadyExists.Error())

		err = namespaceMutator.CreateNamespace("ns2")
		assert.NoError(t, err)

		_, err = etcdStore.Get(utils.JobListKey("ns2"))
		assert.NoError(t, err)
		_, err = etcdStore.Get(utils.SchemaListKey("ns2"))
		assert.NoError(t, err)
	})
}
