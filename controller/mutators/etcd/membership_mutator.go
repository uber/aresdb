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
	"errors"
	"strconv"
	"unsafe"

	"github.com/m3db/m3/src/cluster/services"
	"github.com/uber/aresdb/cluster/kvstore"
	"github.com/uber/aresdb/controller/models"
	"github.com/uber/aresdb/controller/mutators/common"
	"github.com/uber/aresdb/utils"
)

// NewMembershipMutator creates new MembershipMutator
func NewMembershipMutator(etcdClient *kvstore.EtcdClient) common.MembershipMutator {
	return membershipMutatorImpl{
		etcdClient: etcdClient,
	}
}

type membershipMutatorImpl struct {
	etcdClient *kvstore.EtcdClient
}

func (mm membershipMutatorImpl) Join(namespace string, instance models.Instance) error {
	return errors.New("join not supported in etcd version of membership mutator")
}

func (mm membershipMutatorImpl) Leave(namespace, instanceName string) error {
	return errors.New("leave not supported in etcd version of membership mutator")
}

func (mm membershipMutatorImpl) GetInstance(namespace, instanceName string) (instance models.Instance, err error) {
	serviceID := services.NewServiceID().
		SetName(utils.DataNodeServiceName(namespace)).
		SetEnvironment(mm.etcdClient.Environment).
		SetZone(mm.etcdClient.Zone)

	hbtService, err := mm.etcdClient.Services.HeartbeatService(serviceID)
	if err != nil {
		err = utils.StackError(err, "failed to get heartbeat service, namespace: %s", namespace)
	}

	instances, err := hbtService.GetInstances()
	if err != nil {
		err = utils.StackError(err, "failed to get instances, namespace: %s", namespace)
	}

	for _, instance := range instances {
		if instance.ID() == instanceName {
			return models.Instance{
				Name: instance.ID(),
				Host: instance.Hostname(),
				Port: instance.Port(),
			}, nil
		}
	}

	return instance, common.ErrInstanceDoesNotExist
}

func (mm membershipMutatorImpl) GetInstances(namespace string) ([]models.Instance, error) {
	serviceID := services.NewServiceID().
		SetName(utils.DataNodeServiceName(namespace)).
		SetEnvironment(mm.etcdClient.Environment).
		SetZone(mm.etcdClient.Zone)

	hbtService, err := mm.etcdClient.Services.HeartbeatService(serviceID)
	if err != nil {
		return nil, utils.StackError(err, "failed to get heartbeat service, namespace: %s", namespace)
	}

	instances, err := hbtService.GetInstances()
	if err != nil {
		return nil, utils.StackError(err, "failed to get instances, namespace: %s", namespace)
	}

	result := make([]models.Instance, 0, len(instances))
	for _, instance := range instances {
		result = append(result, models.Instance{
			Name: instance.ID(),
			Host: instance.Hostname(),
			Port: instance.Port(),
		})
	}
	return result, nil
}

// GetHash returns hash that will be different if any instance changed
func (mm membershipMutatorImpl) GetHash(namespace string) (string, error) {
	serviceID := services.NewServiceID().
		SetName(utils.DataNodeServiceName(namespace)).
		SetEnvironment(mm.etcdClient.Environment).
		SetZone(mm.etcdClient.Zone)

	hbtService, err := mm.etcdClient.Services.HeartbeatService(serviceID)
	if err != nil {
		return "", utils.StackError(err, "failed to get heartbeat service, namespace: %s", namespace)
	}

	ids, err := hbtService.Get()
	if err != nil {
		return "", utils.StackError(err, "failed to get service ids, namespace: %s", namespace)
	}

	hash := uint32(1)
	for _, id := range ids {
		strByts := []byte(id)
		hash += hash*31 + utils.Murmur3Sum32(unsafe.Pointer(&strByts[0]), len(strByts), 0)
	}
	return strconv.Itoa(int(hash)), nil
}
