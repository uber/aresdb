package topology

import (
	"github.com/m3db/m3/src/cluster/client/etcd"
	"github.com/m3db/m3/src/cluster/services"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber/aresdb/utils"
)

var _ = Describe("options", func() {
	hosts := []struct {
		id     string
		addr   string
		shards []uint32
	}{
		{"h1", "h1:9000", []uint32{0}},
		{"h2", "h2:9000", []uint32{1}},
		{"h3", "h3:9000", []uint32{0}},
		{"h4", "h4:9000", []uint32{1}},
	}
	var hostShardSets []HostShardSet
	for _, h := range hosts {
		hostShardSets = append(hostShardSets,
			NewHostShardSet(NewHost(h.id, h.addr), newTestShardSet(h.shards)))
	}
	shardSet := newTestShardSet([]uint32{0, 1})

	It("staticOptions", func() {
		staticOptions := NewStaticOptions()

		sOpts := staticOptions.SetShardSet(shardSet)
		Ω(sOpts.ShardSet()).Should(Equal(shardSet))

		sOpts = staticOptions.SetHostShardSets(hostShardSets)
		Ω(sOpts.HostShardSets()).Should(Equal(hostShardSets))
	})

	It("dynamicOptions", func() {
		dynamicOptions := NewDynamicOptions()

		clusters := []etcd.Cluster{
			etcd.NewCluster().SetZone("zone1").SetEndpoints([]string{"i1"}),
			etcd.NewCluster().SetZone("zone2").SetEndpoints([]string{"i2"}),
		}
		csClient, _ := etcd.NewConfigServiceClient(etcd.NewOptions().
			SetClusters(clusters).
			SetService("test_app").
			SetZone("zone1").
			SetEnv("env"))

		dynamicOptions.SetConfigServiceClient(csClient)
		Ω(dynamicOptions.ConfigServiceClient()).Should(Equal(csClient))

		iOpts := utils.NewOptions()
		dynamicOptions.SetInstrumentOptions(iOpts)
		Ω(dynamicOptions.InstrumentOptions()).Should(Equal(iOpts))

		oOpts := services.NewOverrideOptions()
		dynamicOptions.SetServicesOverrideOptions(oOpts)
		Ω(dynamicOptions.ServicesOverrideOptions()).Should(Equal(oOpts))

		qOpts := services.NewQueryOptions()
		dynamicOptions.SetQueryOptions(qOpts)
		Ω(dynamicOptions.QueryOptions()).Should(Equal(qOpts))
	})
})
