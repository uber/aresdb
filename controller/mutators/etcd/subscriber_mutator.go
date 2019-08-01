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
	"github.com/m3db/m3/src/cluster/services"
	"github.com/uber/aresdb/cluster/kvstore"
	"github.com/uber/aresdb/controller/models"
	"github.com/uber/aresdb/controller/mutators/common"
	"github.com/uber/aresdb/utils"
	"strconv"
	"unsafe"
)

type subscriberMutator struct {
	etcdClient *kvstore.EtcdClient
}

// NewSubscriberMutator creates new subscriber mutator based on etcd
func NewSubscriberMutator(etcdClient *kvstore.EtcdClient) common.SubscriberMutator {
	return &subscriberMutator{
		etcdClient: etcdClient,
	}
}

// GetSubscriber returns a subscriber
func (s *subscriberMutator) GetSubscriber(namespace, subscriberName string) (subscriber models.Subscriber, err error) {
	serviceID := services.NewServiceID().
		SetName(utils.SubscriberServiceName(namespace)).
		SetEnvironment(s.etcdClient.Environment).
		SetZone(s.etcdClient.Zone)

	hbtSvc, err := s.etcdClient.Services.HeartbeatService(serviceID)
	if err != nil {
		err = utils.StackError(err, "failed to get heartbeat service for namespace: %s", namespace)
		return
	}

	ids, err := hbtSvc.Get()
	if err != nil {
		err = utils.StackError(err, "failed to get instances for namespace: %s", namespace)
		return
	}

	for _, id := range ids {
		if id == subscriberName {
			return models.Subscriber{
				Name: id,
			}, nil
		}
	}

	return subscriber, common.ErrSubscriberDoesNotExist
}

// GetSubscribers returns a list of subscribers
func (s *subscriberMutator) GetSubscribers(namespace string) (subscribers []models.Subscriber, err error) {
	serviceID := services.NewServiceID().
		SetName(utils.SubscriberServiceName(namespace)).
		SetEnvironment(s.etcdClient.Environment).
		SetZone(s.etcdClient.Zone)

	hbtSvc, err := s.etcdClient.Services.HeartbeatService(serviceID)
	if err != nil {
		err = utils.StackError(err, "failed to get heartbeat service for namespace: %s", namespace)
		return
	}

	ids, err := hbtSvc.Get()
	if err != nil {
		err = utils.StackError(err, "failed to get instances for namespace: %s", namespace)
		return
	}

	for _, id := range ids {
		subscribers = append(subscribers, models.Subscriber{
			Name: id,
		})
	}

	return
}

// GetHash returns hash of all subscribers
func (s *subscriberMutator) GetHash(namespace string) (string, error) {
	serviceID := services.NewServiceID().
		SetName(utils.SubscriberServiceName(namespace)).
		SetEnvironment(s.etcdClient.Environment).
		SetZone(s.etcdClient.Zone)

	hbtSvc, err := s.etcdClient.Services.HeartbeatService(serviceID)
	if err != nil {
		return "", utils.StackError(err, "failed to get heartbeat service for namespace: %s", namespace)
	}

	ids, err := hbtSvc.Get()
	if err != nil {
		return "", utils.StackError(err, "failed to get instances for namespace: %s", namespace)
	}

	hash := uint32(1)
	for _, id := range ids {
		strByts := []byte(id)
		hash += hash*31 + utils.Murmur3Sum32(unsafe.Pointer(&strByts[0]), len(strByts), 0)
	}

	return strconv.Itoa(int(hash)), nil
}
