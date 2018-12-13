package distributed

import (
	"encoding/json"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/uber/aresdb/common"
	metaMocks "github.com/uber/aresdb/metastore/mocks"
	"os"
)

var _ = ginkgo.Describe("MembershipManager", func() {
	ginkgo.It("MembershipManager should work", func() {
		mockMetaStore := metaMocks.MetaStore{}
		cfg := common.AresServerConfig{
			Clients: common.ClientsConfig{
				ZK: &common.ZKConfig{
					ZKs:            "localhost:2181",
					TimeoutSeconds: 1,
				},
				Controller: &common.ControllerConfig{},
			},
			Cluster: common.ClusterConfig{
				Enable:       true,
				ClusterName:  "cluster1",
				InstanceName: "instance1",
			},
			Port: 1234,
		}

		ts, err := zk.StartTestCluster(1, nil, nil)
		if err != nil {
			panic(err)
		}
		conn, _, err := ts.ConnectAll()
		if err != nil {
			panic(err)
		}
		acls := zk.WorldACL(zk.PermAll)

		_, err = conn.Create("/ares_controller", nil, int32(0), acls)
		_, err = conn.Create("/ares_controller/cluster1", nil, int32(0), acls)
		_, err = conn.Create("/ares_controller/cluster1/instances", nil, int32(0), acls)
		_, err = conn.Create("/ares_controller/cluster1/schema", nil, int32(0), acls)
		mockMetaStore.On("ListTables").Return([]string{}, nil)

		mm := membershipManagerImpl{
			cfg:       cfg,
			zkc:       conn,
			metaStore: &mockMetaStore,
		}
		err = mm.Connect()
		Ω(err).Should(BeNil())

		// conn2 is another connection that observes zk cluster data
		conn2, _, _ := ts.ConnectAll()
		data, _, _ := conn2.Get("/ares_controller/cluster1/instances/instance1")
		hostName, _ := os.Hostname()
		var ins Instance
		json.Unmarshal(data, &ins)
		Ω(ins).Should(Equal(Instance{
			Name: "instance1",
			Host: hostName,
			Port: 1234,
		}))

		err = mm.Disconnect()
		Ω(err).Should(BeNil())

		exists, _, _ := conn2.Exists("/ares_controller/cluster1/instances/instance1")
		Ω(exists).Should(BeFalse())

		conn2.Close()
		ts.Stop()
	})

	ginkgo.It("MembershipManager should fail znode failure", func() {
		mockMetaStore := metaMocks.MetaStore{}
		cfg := common.AresServerConfig{
			Clients: common.ClientsConfig{
				ZK: &common.ZKConfig{
					ZKs:            "localhost:2181",
					TimeoutSeconds: 1,
				},
				Controller: &common.ControllerConfig{},
			},
			Cluster: common.ClusterConfig{
				Enable:       true,
				ClusterName:  "cluster1",
				InstanceName: "instance1",
			},
			Port: 1234,
		}

		ts, err := zk.StartTestCluster(1, nil, nil)
		if err != nil {
			panic(err)
		}
		conn, _, err := ts.ConnectAll()
		if err != nil {
			panic(err)
		}

		mm := membershipManagerImpl{
			cfg:       cfg,
			zkc:       conn,
			metaStore: &mockMetaStore,
		}
		err = mm.Connect()
		Ω(err).Should(Equal(zk.ErrNoNode))

		conn.Close()
		ts.Stop()
	})

	ginkgo.It("MembershipManager should fail empty instance name", func() {
		mockMetaStore := metaMocks.MetaStore{}
		cfg := common.AresServerConfig{
			Clients: common.ClientsConfig{
				ZK: &common.ZKConfig{
					ZKs:            "localhost:2181",
					TimeoutSeconds: 1,
				},
				Controller: &common.ControllerConfig{},
			},
			Cluster: common.ClusterConfig{
				Enable:      true,
				ClusterName: "cluster1",
			},
			Port: 1234,
		}

		ts, err := zk.StartTestCluster(1, nil, nil)
		if err != nil {
			panic(err)
		}
		conn, _, err := ts.ConnectAll()
		if err != nil {
			panic(err)
		}

		mm := membershipManagerImpl{
			cfg:       cfg,
			zkc:       conn,
			metaStore: &mockMetaStore,
		}
		err = mm.Connect()
		Ω(err).Should(Equal(ErrInvalidInstanceName))

		conn.Close()
		ts.Stop()
	})

	ginkgo.It("MembershipManager should fail bad zk config", func() {
		mockMetaStore := metaMocks.MetaStore{}
		cfg := common.AresServerConfig{
			Clients: common.ClientsConfig{
				ZK: &common.ZKConfig{
					ZKs:            "localhost:2181",
					TimeoutSeconds: 1,
				},
				Controller: &common.ControllerConfig{},
			},
			Cluster: common.ClusterConfig{
				Enable:      true,
				ClusterName: "cluster1",
			},
			Port: 1234,
		}

		mm := NewMembershipManager(cfg, &mockMetaStore)
		err := mm.Connect()
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("initZKConnection should work", func() {
		cfg := common.AresServerConfig{
			Clients: common.ClientsConfig{
				ZK: &common.ZKConfig{
					ZKs:            "localhost:2181",
					TimeoutSeconds: 1,
				},
			},
		}
		mm := membershipManagerImpl{
			cfg: cfg,
		}
		err := mm.initZKConnection()
		Ω(err).Should(BeNil())
		Ω(mm.zkc).ShouldNot(BeNil())
	})
})
