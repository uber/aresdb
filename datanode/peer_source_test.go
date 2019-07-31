package datanode

import (
	m3Shard "github.com/m3db/m3/src/cluster/shard"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
	aresShard "github.com/uber/aresdb/cluster/shard"
	"github.com/uber/aresdb/cluster/topology"
	"github.com/uber/aresdb/common"
	"github.com/uber/aresdb/datanode/client"
	"github.com/uber/aresdb/datanode/generated/proto/rpc"
	"github.com/uber/aresdb/datanode/generated/proto/rpc/mocks"
	"google.golang.org/grpc"
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

		mockConn0 := &mocks.PeerDataNodeClient{}
		mockConn1 := &mocks.PeerDataNodeClient{}

		mockConn0.On("Health", mock.Anything, mock.Anything).Return(&rpc.HealthCheckResponse{
			Status: rpc.HealthCheckResponse_NOT_SERVING,
		}, nil)

		mockConn1.On("Health", mock.Anything, mock.Anything).Return(&rpc.HealthCheckResponse{
			Status: rpc.HealthCheckResponse_SERVING,
		}, nil)

		testClients := map[string]rpc.PeerDataNodeClient{
			"host0:9374": mockConn0,
			"host1:9374": mockConn1,
		}

		var mockDialer client.PeerConnDialer = func(target string, opts ...grpc.DialOption) (client rpc.PeerDataNodeClient, closeFn func() error, err error) {
			return testClients[target],
				func() error {
					return nil
				},
				nil
		}

		logger := common.NewLoggerFactory().GetDefaultLogger()
		peerSource, err := NewPeerSource(logger, staticTopology, mockDialer)
		Ω(err).Should(BeNil())
		workWithConn := 0
		err = peerSource.BorrowConnection([]string{"instance0", "instance1", "instance3"}, func(peerID string, client rpc.PeerDataNodeClient) {
			logger.Info("borrowed connection")
			workWithConn++
		})
		Ω(err).Should(BeNil())
		Ω(workWithConn).Should(Equal(1))

		// not able to borrow connection when no connection is serving
		workWithConn = 0
		err = peerSource.BorrowConnection([]string{"instance0", "instance3"}, func(peerID string, client rpc.PeerDataNodeClient) {
			logger.Info("borrowed connection")
			workWithConn++
		})
		Ω(err).ShouldNot(BeNil())
		Ω(workWithConn).Should(Equal(0))
	})

})
