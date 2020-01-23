package etcd

import (
	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/kv/mem"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/cluster/shard"
	"github.com/stretchr/testify/assert"
	"github.com/uber/aresdb/cluster/kvstore"
	"testing"
)

// m3ClientMock mocks m3client
type m3ClientMock struct {
	kvStore kv.TxnStore
	services services.Services
}

func (c *m3ClientMock) Services(opts services.OverrideOptions) (services.Services, error) {
	return c.services, nil
}

func (c *m3ClientMock) KV() (kv.Store, error) {
	return c.kvStore, nil
}

func (c *m3ClientMock) Txn() (kv.TxnStore, error) {
	return c.kvStore, nil
}

func (c *m3ClientMock) Store(opts kv.OverrideOptions) (kv.Store, error) {
	return c.kvStore, nil
}

func (c *m3ClientMock) TxnStore(opts kv.OverrideOptions) (kv.TxnStore, error) {
	return c.kvStore, nil
}

func TestPlacementMutator(t *testing.T) {
	t.Run("Should work for placement handler", func(t *testing.T) {
		txnStore := mem.NewStore()
		clusterServices, err := services.NewServices(
			services.NewOptions().
				SetKVGen(func(zone string) (store kv.Store, e error) {
					return txnStore, nil
				}).
				SetHeartbeatGen(func(sid services.ServiceID) (service services.HeartbeatService, e error) {
					return nil, nil
				}).SetLeaderGen(func(sid services.ServiceID, opts services.ElectionOptions) (service services.LeaderService, e error) {
				return nil, nil
			}),
		)
		assert.NoError(t, err)
		clusterClient := &m3ClientMock{kvStore: txnStore, services: clusterServices}
		client := kvstore.EtcdClient{
			Zone:          "test",
			Environment:   "test",
			ServiceName:   "test",
			ClusterClient: clusterClient,
			TxnStore:      txnStore,
			Services:      clusterServices,
		}

		instancepb0 := placementpb.Instance{
			Id: "0",
			IsolationGroup: "rack-a",
			Zone: "test",
			Hostname: "host0",
			Endpoint: "http://host0:9374",
			Weight: 1,
			Port: 9374,
		}

		instancepb1 := placementpb.Instance {
			Id: "1",
			IsolationGroup: "rack-b",
			Zone: "test",
			Hostname: "host1",
			Endpoint: "http://host1:9374",
			Weight: 1,
			Port: 9374,
		}

		instancepb2 := placementpb.Instance {
			Id: "2",
			IsolationGroup: "rack-a",
			Zone: "test",
			Hostname: "host2",
			Endpoint: "http://host2:9374",
			Weight: 1,
			Port: 9374,
		}

		placementMutator := NewPlacementMutator(&client)

		// Tests Start
		testNamespace := "test"
		initInstances := make([]placement.Instance, 2)
		initInstances[0], _ = placement.NewInstanceFromProto(&instancepb0)
		initInstances[1], _ = placement.NewInstanceFromProto(&instancepb1)

		// 1. initialize placement
		plmt, err := placementMutator.BuildInitialPlacement(testNamespace, 2, 2, initInstances)
		assert.NoError(t, err)
		initPlacement, _ := plmt.Proto()

		assert.Equal(t, plmt.NumShards(), 2)
		assert.Equal(t, plmt.NumInstances(), 2)
		assert.Equal(t, plmt.ReplicaFactor(), 2)
		assert.True(t, plmt.IsSharded())
		assert.False(t, plmt.IsMirrored())
		instance0, exist := plmt.Instance("0")
		assert.True(t, exist)
		instance1, exist := plmt.Instance("1")
		assert.True(t, exist)
		assert.True(t, instance0.IsInitializing())
		assert.True(t, instance1.IsInitializing())


		// 2. get the current placement
		plmt, err = placementMutator.GetCurrentPlacement(testNamespace)
		assert.NoError(t, err)
		pb0, err := plmt.Proto()
		assert.NoError(t, err)
		assert.Equal(t, pb0, initPlacement)

		// 3. mark namespace as available
		plmt, err = placementMutator.MarkNamespaceAvailable(testNamespace)
		assert.NoError(t, err)
		assert.NoError(t, err)
		instance0, _ = plmt.Instance("0")
		assert.True(t, instance0.IsAvailable())
		instance1, _ = plmt.Instance("1")
		assert.True(t, instance1.IsAvailable())

		// 4. replace instance 0 with new instance 2 in with the same isolation group
		instance2, _ := placement.NewInstanceFromProto(&instancepb2)
		plmt, err = placementMutator.ReplaceInstance(testNamespace, []string{"0"}, []placement.Instance{instance2})
		assert.NoError(t, err)
		assert.NoError(t, err)

		assert.Equal(t, plmt.NumInstances(), 3)
		instance0, _ = plmt.Instance("0")
		instance2, _ = plmt.Instance("2")
		assert.True(t, instance0.IsLeaving(), true)
		assert.True(t, instance2.IsInitializing(), true)

		// 5. mark available for instance 2
		plmt, err = placementMutator.MarkInstanceAvailable(testNamespace, "2")
		assert.NoError(t, err)
		assert.Equal(t, plmt.NumInstances(), 2)
		instance1, _ = plmt.Instance("1")
		instance2, _ = plmt.Instance("2")
		assert.True(t, instance1.IsAvailable(), true)
		assert.True(t, instance2.IsAvailable(), true)

		// 6. add instance 0 back
		instance0, _ = placement.NewInstanceFromProto(&instancepb0)
		plmt, err = placementMutator.AddInstance(testNamespace, []placement.Instance{instance0})
		assert.NoError(t, err)
		assert.Equal(t, plmt.NumInstances(), 3)
		instance0, _ = plmt.Instance("0")
		instance1, _ = plmt.Instance("1")
		instance2, _ = plmt.Instance("2")
		var newShard shard.Shard
		var found bool
		// should have either shard0 or shard1 assigned to newly added instance
		if newShard, found = instance0.Shards().Shard(0); !found {
			newShard, found = instance0.Shards().Shard(1)
		}
		assert.True(t, found)
		// should have new shard initializing
		assert.Equal(t, newShard.State(), shard.Initializing)

		// 6. mark available for instance 0
		plmt, err = placementMutator.MarkInstanceAvailable(testNamespace, "0")
		assert.NoError(t, err)
		assert.Equal(t, plmt.NumInstances(), 3)
		instance0, _ = plmt.Instance("0")
		instance1, _ = plmt.Instance("1")
		instance2, _ = plmt.Instance("2")
		assert.True(t, instance0.IsAvailable())
		assert.True(t, instance1.IsAvailable())
		assert.True(t, instance2.IsAvailable())

		// 7. remove instance 0 from the placement
		plmt, err = placementMutator.RemoveInstance(testNamespace, []string{"0"})
		assert.NoError(t, err)
		assert.Equal(t, plmt.NumInstances(), 3)
		instance0, _ = plmt.Instance("0")
		instance1, _ = plmt.Instance("1")
		instance2, _ = plmt.Instance("2")
		assert.True(t, instance0.IsLeaving(), true)
		assert.True(t, instance1.Shards().Contains(0))
		assert.True(t, instance1.Shards().Contains(1))
		assert.True(t, instance2.Shards().Contains(0))
		assert.True(t, instance2.Shards().Contains(1))

		// 8. mark shards as available
		instance1, exist = plmt.Instance("1")
		assert.True(t, exist)
		instance2, exist = plmt.Instance("2")
		assert.True(t, exist)

		initShards := instance1.Shards().ShardsForState(shard.Initializing)
		ids := make([]uint32, 0)
		for _, shard := range initShards {
			ids = append(ids, shard.ID())
		}
		plmt, err = placementMutator.MarkShardsAvailable(testNamespace, instance1.ID(), ids)
		assert.NoError(t, err)

		initShards = instance2.Shards().ShardsForState(shard.Initializing)
		ids = make([]uint32, 0)
		for _, shard := range initShards {
			ids = append(ids, shard.ID())
		}
		plmt, err = placementMutator.MarkShardsAvailable(testNamespace, instance2.ID(), ids)
		assert.NoError(t, err)

		for _, instance := range plmt.Instances() {
			assert.True(t, instance.IsAvailable())
		}
	})
}