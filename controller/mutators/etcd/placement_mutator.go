package etcd

import (
	"fmt"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/uber/aresdb/cluster/kvstore"
	"github.com/uber/aresdb/controller/mutators/common"
	"github.com/uber/aresdb/utils"
	"net/http"
)

type placementMutator struct {
	client *kvstore.EtcdClient
}

func (p *placementMutator) getServiceID(namespace string) services.ServiceID {
	return services.NewServiceID().
		SetEnvironment(p.client.Environment).
		SetZone(p.client.Zone).
		SetName(utils.DataNodeServiceName(namespace))
}

func validateAllAvailable(p placement.Placement) error {
	for _, instance := range p.Instances() {
		if !instance.IsAvailable() {
			return utils.APIError{Code: http.StatusBadRequest, Message: fmt.Sprintf("instance %s is not available", instance.ID())}
		}
	}
	return nil
}

// NewPlacementMutator creates mutator for placement
func NewPlacementMutator(client *kvstore.EtcdClient) common.PlacementMutator {
	return &placementMutator{
		client: client,
	}
}

func checkNumShardsIsPowerOfTwo(numShards int) bool {
	return (numShards & (^(-numShards))) == 0
}

func (p *placementMutator) placementOptions() placement.Options {
	return placement.NewOptions().
		SetInstrumentOptions(instrument.NewOptions()).
		SetValidZone(p.client.Zone).
		// if we specify more than one new instance, we want to add them all
		SetAddAllCandidates(true).
		// for now we want to make sure replacement does not affect existing instances not being replaced
		SetAllowPartialReplace(false)
}

func (p *placementMutator) BuildInitialPlacement(namespace string, numShards int, numReplica int, instances []placement.Instance) (placement.Placement, error) {
	if !checkNumShardsIsPowerOfTwo(numShards) {
		return nil, common.ErrInvalidNumShards
	}
	serviceID := p.getServiceID(namespace)
	placementSvc, err := p.client.Services.PlacementService(serviceID, p.placementOptions())
	if err != nil {
		return nil, utils.StackError(err, common.ErrMsgFailedToGetPlacementService)
	}
	plm, err := placementSvc.BuildInitialPlacement(instances, numShards, numReplica)
	if err != nil {
		return nil, utils.StackError(err, common.ErrMsgFailedToBuildInitialPlacement)
	}

	return plm, nil
}

func (p *placementMutator) GetCurrentPlacement(namespace string) (placement.Placement, error) {
	serviceID := p.getServiceID(namespace)
	placementSvc, err := p.client.Services.PlacementService(serviceID, p.placementOptions())
	if err != nil {
		return nil, utils.StackError(err, common.ErrMsgFailedToGetPlacementService)
	}
	plm, err := placementSvc.Placement()
	if err != nil {
		if err == kv.ErrNotFound {
			return nil, common.ErrPlacementDoesNotExist
		}
		return nil, utils.StackError(err, common.ErrMsgFailedToGetCurrentPlacement)
	}
	return plm, nil
}

func (p *placementMutator) AddInstance(namespace string, instances []placement.Instance) (placement.Placement, error) {
	serviceID := p.getServiceID(namespace)
	placementSvc, err := p.client.Services.PlacementService(serviceID,
		p.placementOptions().SetValidateFnBeforeUpdate(validateAllAvailable))
	if err != nil {
		return nil, utils.StackError(err, common.ErrMsgFailedToGetPlacementService)
	}
	plm, _, err := placementSvc.AddInstances(instances)
	if err != nil {
		if err == kv.ErrNotFound {
			return nil, common.ErrPlacementDoesNotExist
		}
		return nil, utils.StackError(err, common.ErrMsgFailedToAddInstance)
	}
	return plm, nil
}

func (p *placementMutator) ReplaceInstance(namespace string, leavingInstances []string, newInstances []placement.Instance) (placement.Placement, error) {
	serviceID := p.getServiceID(namespace)
	placementSvc, err := p.client.Services.PlacementService(serviceID,
		p.placementOptions().SetValidateFnBeforeUpdate(validateAllAvailable))
	if err != nil {
		return nil, utils.StackError(err, common.ErrMsgFailedToGetPlacementService)
	}
	plm, _, err := placementSvc.ReplaceInstances(leavingInstances, newInstances)
	if err != nil {
		if err == kv.ErrNotFound {
			return nil, common.ErrPlacementDoesNotExist
		}
		return nil, utils.StackError(err, common.ErrMsgFailedToReplaceInstance)
	}
	return plm, nil
}

func (p *placementMutator) RemoveInstance(namespace string, leavingInstances []string) (placement.Placement, error) {
	serviceID := p.getServiceID(namespace)
	placementSvc, err := p.client.Services.PlacementService(serviceID,
		p.placementOptions().SetValidateFnBeforeUpdate(validateAllAvailable))
	if err != nil {
		return nil, utils.StackError(err, common.ErrMsgFailedToGetPlacementService)
	}
	plm, err := placementSvc.RemoveInstances(leavingInstances)
	if err != nil {
		if err == kv.ErrNotFound {
			return nil, common.ErrPlacementDoesNotExist
		}
		return nil, utils.StackError(err, common.ErrMsgFailedToRemoveInstance)
	}
	return plm, nil
}

func (p *placementMutator) MarkNamespaceAvailable(namespace string) (placement.Placement, error) {
	serviceID := p.getServiceID(namespace)
	placementSvc, err := p.client.Services.PlacementService(serviceID, p.placementOptions())
	if err != nil {
		return nil, utils.StackError(err, common.ErrMsgFailedToGetPlacementService)
	}
	plm, err := placementSvc.MarkAllShardsAvailable()
	if err != nil {
		if err == kv.ErrNotFound {
			return nil, common.ErrPlacementDoesNotExist
		}
		return nil, utils.StackError(err, common.ErrMsgFailedToMarkAvailable)
	}
	return plm, nil
}

func (p *placementMutator) MarkInstanceAvailable(namespace string, instance string) (placement.Placement, error) {
	serviceID := p.getServiceID(namespace)
	placementSvc, err := p.client.Services.PlacementService(serviceID, p.placementOptions())
	if err != nil {
		return nil, utils.StackError(err, common.ErrMsgFailedToGetPlacementService)
	}
	plm, err := placementSvc.MarkInstanceAvailable(instance)
	if err != nil {
		if err == kv.ErrNotFound {
			return nil, common.ErrPlacementDoesNotExist
		}
		return nil, utils.StackError(err, common.ErrMsgFailedToMarkAvailable)
	}
	return plm, nil
}

func (p *placementMutator) MarkShardsAvailable(namespace string, instance string, shards []uint32) (placement.Placement, error) {
	serviceID := p.getServiceID(namespace)
	placementSvc, err := p.client.Services.PlacementService(serviceID, p.placementOptions())
	if err != nil {
		return nil, utils.StackError(err, common.ErrMsgFailedToGetPlacementService)
	}
	plm, err := placementSvc.MarkShardsAvailable(instance, shards...)
	if err != nil {
		if err == kv.ErrNotFound {
			return nil, common.ErrPlacementDoesNotExist
		}
		return nil, utils.StackError(err, common.ErrMsgFailedToMarkAvailable)
	}
	return plm, nil
}
