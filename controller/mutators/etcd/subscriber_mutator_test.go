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
	"github.com/m3db/m3/src/cluster/services"
	"github.com/stretchr/testify/assert"
	"github.com/uber/aresdb/cluster/kvstore"
	"github.com/uber/aresdb/controller/models"
)

func TestSubscriberMutator(t *testing.T) {
	subscriber1 := models.Subscriber{
		Name: "session1",
	}

	subscriber2 := models.Subscriber{
		Name: "session2",
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
		heartbeatService.EXPECT().Get().Return([]string{"session1"}, nil).Times(2)
		subscriberMutator := NewSubscriberMutator(etcdClient)
		res, err := subscriberMutator.GetSubscribers("ns1")
		assert.NoError(t, err)
		assert.Len(t, res, 1)
		assert.Equal(t, subscriber1, res[0])

		hash1, err := subscriberMutator.GetHash("ns1")
		assert.NoError(t, err)

		heartbeatService.EXPECT().Get().Return([]string{"session1", "session2"}, nil).Times(2)

		res, err = subscriberMutator.GetSubscribers("ns1")
		assert.NoError(t, err)
		assert.Len(t, res, 2)
		assert.Equal(t, subscriber1, res[0])
		assert.Equal(t, subscriber2, res[1])

		hash2, err := subscriberMutator.GetHash("ns1")
		assert.NoError(t, err)
		assert.NotEqual(t, hash1, hash2)
	})
}
