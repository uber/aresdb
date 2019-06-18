package datanode

import (
	m3Shard "github.com/m3db/m3/src/cluster/shard"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	aresShard "github.com/uber/aresdb/cluster/shard"
	"github.com/uber/aresdb/cluster/topology"
	"github.com/uber/aresdb/datanode/generated/proto/rpc"
	"go.uber.org/zap"
)

var _ = ginkgo.Describe("peer source", func() {

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

	ginkgo.It("BorrowConnection should work", func() {
		logger := zap.NewExample()
		peerSource, err := NewPeerSource(staticTopology)
		Ω(err).Should(BeNil())
		workWithConn := 0
		err = peerSource.BorrowConnection("instance0", func(client rpc.PeerDataNodeClient) {
			logger.Info("borrowed connection")
			workWithConn++
		})
		Ω(err).Should(BeNil())
		Ω(workWithConn).Should(Equal(1))

		err = peerSource.BorrowConnection("instance3", func(client rpc.PeerDataNodeClient) {
			workWithConn++
		})
		Ω(err).ShouldNot(BeNil())
		Ω(workWithConn).Should(Equal(1))

		peerSource.Close()
	})
})
