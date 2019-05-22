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
	"encoding/json"
	"github.com/uber/aresdb/controller/cluster"
	pb "github.com/uber/aresdb/controller/generated/proto"
	"github.com/uber/aresdb/controller/models"
	"github.com/uber/aresdb/controller/mutators/common"
	"strconv"

	"github.com/m3db/m3/src/cluster/kv"
	"github.com/uber/aresdb/utils"
)

// NewIngestionAssignmentMutator creates new IngestionAssignmentMutator
func NewIngestionAssignmentMutator(etcdStore kv.TxnStore) common.IngestionAssignmentMutator {
	return &ingestionAssignmentMutatorImpl{
		etcdStore: etcdStore,
	}
}

type ingestionAssignmentMutatorImpl struct {
	etcdStore kv.TxnStore
}

// GetIngestionAssignment gets IngestionAssignment config by name
func (j *ingestionAssignmentMutatorImpl) GetIngestionAssignment(namespace, name string) (ingestionAssignment models.IngestionAssignment, err error) {
	var entityConfig pb.EntityConfig
	_, err = readValue(j.etcdStore, utils.JobAssignmentsKey(namespace, name), &entityConfig)
	if err != nil || entityConfig.Tomstoned {
		return ingestionAssignment, common.ErrIngestionAssignmentDoesNotExist
	}

	err = json.Unmarshal(entityConfig.Config, &ingestionAssignment)
	return
}

// GetIngestionAssignments returns all IngestionAssignments config
func (j *ingestionAssignmentMutatorImpl) GetIngestionAssignments(namespace string) (ingestionAssignments []models.IngestionAssignment, err error) {
	jobAssignments, _, err := readEntityList(j.etcdStore, utils.JobAssignmentsListKey(namespace))
	if err != nil {
		return nil, err
	}

	ingestionAssignments = make([]models.IngestionAssignment, 0, len(jobAssignments.Entities))
	for _, subscriber := range jobAssignments.Entities {
		ingestionAssignment, err := j.GetIngestionAssignment(namespace, subscriber.Name)
		if common.IsNonExist(err) {
			continue
		}
		if err != nil {
			return nil, err
		}

		ingestionAssignments = append(ingestionAssignments, ingestionAssignment)
	}
	return
}

// DeleteIngestionAssignment deletes a IngestionAssignment
func (j *ingestionAssignmentMutatorImpl) DeleteIngestionAssignment(namespace, name string) error {
	entityList, entityListVersion, err := readEntityList(j.etcdStore, utils.JobAssignmentsListKey(namespace))
	if err != nil {
		return err
	}

	entityList, found := deleteEntity(entityList, name)
	if !found {
		return common.ErrIngestionAssignmentDoesNotExist
	}
	var entityConfig pb.EntityConfig
	configVersion, err := readValue(j.etcdStore, utils.JobAssignmentsKey(namespace, name), &entityConfig)
	if common.IsNonExist(err) || (err == nil && entityConfig.Tomstoned) {
		return common.ErrIngestionAssignmentDoesNotExist
	} else if err != nil {
		return err
	}

	entityConfig.Tomstoned = true
	return cluster.NewTransaction().
		AddKeyValue(utils.JobAssignmentsListKey(namespace), entityListVersion, &entityList).
		AddKeyValue(utils.JobAssignmentsKey(namespace, name), configVersion, &entityConfig).
		WriteTo(j.etcdStore)
}

// UpdateIngestionAssignment updates IngestionAssignment config
func (j *ingestionAssignmentMutatorImpl) UpdateIngestionAssignment(namespace string, ingestionAssignment models.IngestionAssignment) error {
	entityList, entityListVersion, err := readEntityList(j.etcdStore, utils.JobAssignmentsListKey(namespace))
	if err != nil {
		return err
	}

	entityList, found := updateEntity(entityList, ingestionAssignment.Subscriber)
	if !found {
		return common.ErrIngestionAssignmentDoesNotExist
	}
	var entityConfig pb.EntityConfig
	configVersion, err := readValue(j.etcdStore, utils.JobAssignmentsKey(namespace, ingestionAssignment.Subscriber), &entityConfig)
	if common.IsNonExist(err) || (err == nil && entityConfig.Tomstoned) {
		return common.ErrIngestionAssignmentDoesNotExist
	} else if err != nil {
		return err
	}
	entityConfig.Config, err = json.Marshal(ingestionAssignment)
	if err != nil {
		return err
	}

	return cluster.NewTransaction().
		AddKeyValue(utils.JobAssignmentsListKey(namespace), entityListVersion, &entityList).
		AddKeyValue(utils.JobAssignmentsKey(namespace, ingestionAssignment.Subscriber), configVersion, &entityConfig).
		WriteTo(j.etcdStore)
}

// AddIngestionAssignment adds a new IngestionAssignment
func (j *ingestionAssignmentMutatorImpl) AddIngestionAssignment(namespace string, ingestionAssignment models.IngestionAssignment) error {
	entityList, entityListVersion, err := readEntityList(j.etcdStore, utils.JobAssignmentsListKey(namespace))
	if err != nil {
		return err
	}

	entityList, incarnation, found := addEntity(entityList, ingestionAssignment.Subscriber)
	if found {
		return common.ErrIngestionAssignmentAlreadyExist
	}

	var entityConfig pb.EntityConfig
	configVersion := kv.UninitializedVersion
	if incarnation > 0 {
		configVersion, err = readValue(j.etcdStore, utils.JobAssignmentsKey(namespace, ingestionAssignment.Subscriber), &entityConfig)
		if err != nil {
			return err
		}
		if !entityConfig.Tomstoned {
			return common.ErrIngestionAssignmentAlreadyExist
		}
	}
	entityConfig.Tomstoned = false
	entityConfig.Config, err = json.Marshal(ingestionAssignment)
	if err != nil {
		return err
	}

	return cluster.NewTransaction().
		AddKeyValue(utils.JobAssignmentsListKey(namespace), entityListVersion, &entityList).
		AddKeyValue(utils.JobAssignmentsKey(namespace, ingestionAssignment.Subscriber), configVersion, &entityConfig).
		WriteTo(j.etcdStore)
}

// GetHash returns hash that will be different if ingestionAssignment for subscriber changed
func (j *ingestionAssignmentMutatorImpl) GetHash(namespace, subscriber string) (string, error) {
	entityList, _, err := readEntityList(j.etcdStore, utils.JobAssignmentsListKey(namespace))
	if err != nil {
		return "", err
	}

	lastUpdatedAt := ""
	_, found := find(entityList, subscriber, func(name *pb.EntityName) {
		lastUpdatedAt = strconv.FormatInt(name.LastUpdatedAt, 10)
	})

	if !found {
		return "", common.ErrIngestionAssignmentDoesNotExist
	}
	return lastUpdatedAt, nil
}
