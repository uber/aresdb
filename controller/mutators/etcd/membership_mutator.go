package etcd

import (
	"encoding/json"
	"github.com/uber/aresdb/cluster/kvstore"

	"github.com/m3db/m3/src/cluster/kv"
	modelspb "github.com/uber/aresdb/controller/generated/proto"
	"github.com/uber/aresdb/controller/models"
	"github.com/uber/aresdb/controller/mutators/common"
	"github.com/uber/aresdb/utils"
)

// NewMembershipMutator creates new MembershipMutator
func NewMembershipMutator(etcdStore kv.TxnStore) common.MembershipMutator {
	return membershipMutatorImpl{
		txnStore: etcdStore,
	}
}

type membershipMutatorImpl struct {
	txnStore kv.TxnStore
}

func (mm membershipMutatorImpl) Join(namespace string, instance models.Instance) error {
	instance.State = models.Active

	instanceList, instanceListVersion, err := readEntityList(mm.txnStore, utils.InstanceListKey(namespace))
	if err != nil {
		return err
	}

	instanceList, incarnation, exist := addEntity(instanceList, instance.Name)
	if exist {
		return common.ErrInstanceAlreadyExist
	}

	var entityConfig modelspb.EntityConfig
	configVersion := kv.UninitializedVersion
	if incarnation > 0 {
		configVersion, err = readValue(mm.txnStore, utils.InstanceKey(namespace, instance.Name), &entityConfig)
		if err != nil {
			return err
		}
		if !entityConfig.Tomstoned {
			return common.ErrInstanceAlreadyExist
		}
	}

	entityConfig.Config, err = json.Marshal(instance)
	if err != nil {
		return err
	}

	return kvstore.NewTransaction().
		AddKeyValue(utils.InstanceListKey(namespace), instanceListVersion, &instanceList).
		AddKeyValue(utils.InstanceKey(namespace, instance.Name), configVersion, &entityConfig).
		WriteTo(mm.txnStore)
}

func (mm membershipMutatorImpl) GetInstance(namespace, instanceName string) (instance models.Instance, err error) {
	var entityConfig modelspb.EntityConfig
	_, err = readValue(mm.txnStore, utils.InstanceKey(namespace, instanceName), &entityConfig)
	if common.IsNonExist(err) || (err == nil && entityConfig.Tomstoned) {
		return instance, common.ErrInstanceDoesNotExist
	} else if err != nil {
		return instance, err
	}
	err = json.Unmarshal(entityConfig.Config, &instance)
	return
}

func (mm membershipMutatorImpl) GetInstances(namespace string) ([]models.Instance, error) {
	entityList, _, err := readEntityList(mm.txnStore, utils.InstanceListKey(namespace))
	if err != nil {
		return nil, err
	}

	instances := make([]models.Instance, 0)
	for _, entity := range entityList.Entities {
		var instance models.Instance
		instance, err = mm.GetInstance(namespace, entity.Name)
		if common.IsNonExist(err) {
			continue
		}

		if err != nil {
			return nil, err
		}

		instances = append(instances, instance)
	}

	return instances, nil
}

func (mm membershipMutatorImpl) Leave(namespace, instanceName string) error {
	instanceList, listVersion, err := readEntityList(mm.txnStore, utils.InstanceListKey(namespace))
	if err != nil {
		return err
	}

	instanceList, found := deleteEntity(instanceList, instanceName)
	if !found {
		return common.ErrInstanceDoesNotExist
	}

	var entityConfig modelspb.EntityConfig
	configVersion, err := readValue(mm.txnStore, utils.InstanceKey(namespace, instanceName), &entityConfig)
	if common.IsNonExist(err) || (err == nil && entityConfig.Tomstoned) {
		return common.ErrInstanceDoesNotExist
	} else if err != nil {
		return err
	}

	entityConfig.Tomstoned = true

	return kvstore.NewTransaction().
		AddKeyValue(utils.InstanceListKey(namespace), listVersion, &instanceList).
		AddKeyValue(utils.InstanceKey(namespace, instanceName), configVersion, &entityConfig).
		WriteTo(mm.txnStore)
}

// GetHash returns hash that will be different if any instance changed
func (mm membershipMutatorImpl) GetHash(namespace string) (string, error) {
	return getHash(mm.txnStore, utils.InstanceListKey(namespace))
}
