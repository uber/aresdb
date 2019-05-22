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
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	pb "github.com/uber/aresdb/controller/generated/proto"
	"github.com/uber/aresdb/controller/mutators/mocks"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/utils"
)

func TestEnumMutator(t *testing.T) {
	testTable := metaCom.Table{
		Name: "test",
		Columns: []metaCom.Column{
			{
				Name:    "c1",
				Type:    metaCom.BigEnum,
				Deleted: false,
			},
		},
	}

	t.Run("Extend and get enum cases", func(t *testing.T) {
		// test setup
		cleanUp, port := utils.SetUpEtcdTestServer(t)
		defer cleanUp()

		client := utils.SetUpEtcdTestClient(t, port)
		txnStore, err := client.Txn()
		assert.NoError(t, err)

		_, err = txnStore.Set(utils.EnumNodeListKey("ns1", "test", 0, 0), &pb.EnumNodeList{
			NumEnumNodes: 1,
		})
		assert.NoError(t, err)
		_, err = txnStore.Set(utils.EnumNodeKey("ns1", "test", 0, 0, 0), &pb.EnumCases{
			Cases: []string{},
		})
		assert.NoError(t, err)

		schemaMutator := &mocks.TableSchemaMutator{}
		// test
		enumMutator := NewEnumMutator(txnStore, schemaMutator)
		schemaMutator.On("GetTable", "ns1", "test").Return(&testTable, nil)
		enumCases, err := enumMutator.GetEnumCases("ns1", "test", "c1")
		assert.NoError(t, err)
		assert.Empty(t, enumCases)

		enumIDs, err := enumMutator.ExtendEnumCases("ns1", "test", "c1", []string{"a", "b"})
		assert.NoError(t, err)
		assert.Equal(t, enumIDs, []int{0, 1})

		enumIDs, err = enumMutator.ExtendEnumCases("ns1", "test", "c1", []string{"c", "b", "a"})
		assert.NoError(t, err)
		assert.Equal(t, enumIDs, []int{2, 1, 0})

		enumCases, err = enumMutator.GetEnumCases("ns1", "test", "c1")
		assert.NoError(t, err)
		assert.Equal(t, enumCases, []string{"a", "b", "c"})

		existingNumEnums := len(enumCases)

		// test more than 1 node
		newCases := make([]string, 0)
		for i := 0; i < maxEnumCasePerNode; i++ {
			newCases = append(newCases, strconv.Itoa(existingNumEnums+i))
		}

		enumIDs, err = enumMutator.ExtendEnumCases("ns1", "test", "c1", newCases)
		if assert.NoError(t, err) {
			for i := 0; i < maxEnumCasePerNode; i++ {
				assert.Equal(t, enumIDs[i], existingNumEnums+i)
			}
		}

		enumCases, err = enumMutator.GetEnumCases("ns1", "test", "c1")
		if assert.NoError(t, err) && assert.Len(t, enumCases, existingNumEnums+maxEnumCasePerNode) {
			for i := existingNumEnums; i < maxEnumCasePerNode; i++ {
				assert.Equal(t, enumCases[i], strconv.Itoa(i))
			}
		}
	})
}
