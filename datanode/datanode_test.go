package datanode

import (
	"github.com/uber/aresdb/controller/mutators/mocks"
	"os"

	"github.com/m3db/m3/src/cluster/client/etcd"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
	"github.com/uber/aresdb/cluster/topology"
	"github.com/uber/aresdb/common"
	"github.com/uber/aresdb/datanode/bootstrap"
	"github.com/uber/aresdb/memstore"

	m3Shard "github.com/m3db/m3/src/cluster/shard"
	aresShard "github.com/uber/aresdb/cluster/shard"
	memStoreMocks "github.com/uber/aresdb/memstore/mocks"
)

var _ = ginkgo.Describe("datanode", func() {

	ginkgo.It("NewDataNode", func() {
		host0 := topology.NewHost("instance0", "http://host0:9374")
		host1 := topology.NewHost("instance1", "http://host1:9374")
		staticMapOption := topology.NewStaticOptions().
			SetReplicas(1).
			SetHostShardSets([]topology.HostShardSet{
				topology.NewHostShardSet(host0, aresShard.NewShardSet([]m3Shard.Shard{
					m3Shard.NewShard(0),
				})),
				topology.NewHostShardSet(host1, aresShard.NewShardSet([]m3Shard.Shard{
					m3Shard.NewShard(0),
				})),
			}).SetShardSet(aresShard.NewShardSet([]m3Shard.Shard{
			m3Shard.NewShard(0),
		}))
		staticTopology, _ := topology.NewStaticInitializer(staticMapOption).Init()

		enumReader := &mocks.EnumReader{}

		dataNode, err := NewDataNode(
			"instance0",
			staticTopology,
			enumReader,
			NewOptions().
			SetServerConfig(common.AresServerConfig{
				Port:            9374,
				DebugPort:       43202,
				RootPath:        "/tmp/datanode-root",
				TotalMemorySize: 1 << 20,
				SchedulerOff:    false,
				DiskStore:       common.DiskStoreConfig{WriteSync: true},
				HTTP:            common.HTTPConfig{MaxConnections: 300, ReadTimeOutInSeconds: 20, WriteTimeOutInSeconds: 300},
				RedoLogConfig: common.RedoLogConfig{
					DiskConfig:  common.DiskRedoLogConfig{Disabled: false},
					KafkaConfig: common.KafkaRedoLogConfig{Enabled: false},
				},
				Cluster: common.ClusterConfig{
					Enable:      true,
					Distributed: true,
					Namespace:   "test",
					InstanceID:  "instance0",
					Controller:  &common.ControllerConfig{Address: "localhost:6708"},
					Etcd: etcd.Configuration{
						Zone:    "local",
						Env:     "test",
						Service: "ares-datanode",
						ETCDClusters: []etcd.ClusterConfig{
							{
								Zone:      "local",
								Endpoints: []string{"127.0.0.1:2379"},
							},
						},
					},
					HeartbeatConfig: common.HeartbeatConfig{Timeout: 10, Interval: 1},
				},
			}))
		Ω(err).Should(BeNil())
		Ω(dataNode).ShouldNot(BeNil())
		os.RemoveAll("/tmp/datanode-root")
	})

	ginkgo.It("checkShardReadiness", func() {
		mockMemStore := new(memStoreMocks.MemStore)
		dataNode := dataNode{
			memStore: mockMemStore,
		}

		t1s0 := memstore.TableShard{
			BootstrapState: bootstrap.BootstrapNotStarted,
		}
		t1s1 := memstore.TableShard{
			BootstrapState: bootstrap.Bootstrapped,
		}
		t1s2 := memstore.TableShard{
			BootstrapState: bootstrap.BootstrapNotStarted,
		}
		t2s0 := memstore.TableShard{
			BootstrapState: bootstrap.Bootstrapped,
		}
		t2s1 := memstore.TableShard{
			BootstrapState: bootstrap.Bootstrapped,
		}
		t2s2 := memstore.TableShard{
			BootstrapState: bootstrap.Bootstrapped,
		}

		mockMemStore.On("GetTableShard", "t1", 0).
			Run(func(args mock.Arguments) {
				t1s0.Users.Add(1)
			}).
			Return(&t1s0, nil).Once()

		mockMemStore.On("GetTableShard", "t1", 1).
			Run(func(args mock.Arguments) {
				t1s1.Users.Add(1)
			}).
			Return(&t1s1, nil).Once()

		mockMemStore.On("GetTableShard", "t1", 2).
			Run(func(args mock.Arguments) {
				t1s2.Users.Add(1)
			}).
			Return(&t1s2, nil).Once()

		mockMemStore.On("GetTableShard", "t2", 0).
			Run(func(args mock.Arguments) {
				t2s0.Users.Add(1)
			}).
			Return(&t2s0, nil).Once()

		mockMemStore.On("GetTableShard", "t2", 1).
			Run(func(args mock.Arguments) {
				t2s1.Users.Add(1)
			}).
			Return(&t2s1, nil).Once()

		mockMemStore.On("GetTableShard", "t2", 2).
			Run(func(args mock.Arguments) {
				t2s2.Users.Add(1)
			}).
			Return(&t2s2, nil).Once()

		shards := []uint32{0, 1, 2}
		Ω(dataNode.checkShardReadiness([]string{"t1", "t2"}, shards)).Should(Equal(1))
		Ω(shards[0]).Should(Equal(uint32(1)))
	})
})
