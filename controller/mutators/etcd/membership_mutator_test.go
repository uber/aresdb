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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3/src/cluster/kv/mem"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/stretchr/testify/assert"
	"github.com/uber/aresdb/cluster/kvstore"
	"github.com/uber/aresdb/controller/models"
)

func TestMembershipMutator(t *testing.T) {
	placementInstance1 := placement.
		NewInstance().
		SetHostname("host1").
		SetPort(9374).
		SetID("inst1")

	placementInstance2 := placement.
		NewInstance().
		SetHostname("host2").
		SetPort(9374).
		SetID("inst2")

	instance1 := models.Instance{
		Name: "inst1",
		Host: "host1",
		Port: 9374,
	}

	instance2 := models.Instance{
		Name: "inst2",
		Host: "host2",
		Port: 9374,
	}

	t.Run("read ops should work", func(t *testing.T) {
		// test setup

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		clusterService := services.NewMockServices(ctrl)
		heartbeatService := services.NewMockHeartbeatService(ctrl)
		clusterService.EXPECT().HeartbeatService(gomock.Any()).Return(heartbeatService, nil).AnyTimes()

		txnStore := mem.NewStore()

		etcdClient := &kvstore.EtcdClient{
			ServiceName: "ares-controller",
			Environment: "test",
			Zone:        "local",
			Services:    clusterService,
			TxnStore:    txnStore,
		}

		// test
		heartbeatService.EXPECT().Get().
			Return([]string{"inst1"}, nil).Times(1)
		heartbeatService.EXPECT().GetInstances().
			Return([]placement.Instance{placementInstance1}, nil).Times(1)
		membershipMutator := NewMembershipMutator(etcdClient)
		res, err := membershipMutator.GetInstances("ns1")
		assert.NoError(t, err)
		assert.Len(t, res, 1)
		assert.Equal(t, instance1, res[0])

		hash1, err := membershipMutator.GetHash("ns1")
		assert.NoError(t, err)

		heartbeatService.EXPECT().Get().
			Return([]string{"inst1", "inst2"}, nil).Times(1)
		heartbeatService.EXPECT().GetInstances().
			Return([]placement.Instance{placementInstance1, placementInstance2}, nil).Times(1)

		res, err = membershipMutator.GetInstances("ns1")
		assert.NoError(t, err)
		assert.Len(t, res, 2)
		assert.Equal(t, instance1, res[0])
		assert.Equal(t, instance2, res[1])

		hash2, err := membershipMutator.GetHash("ns1")
		assert.NoError(t, err)
		assert.NotEqual(t, hash1, hash2)
	})
}
