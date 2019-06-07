package bootstrap

import (
	pb "github.com/uber/aresdb/datanode/generated/proto/rpc"
	diskMock "github.com/uber/aresdb/diskstore/mocks"
	metaMock "github.com/uber/aresdb/metastore/mocks"
	"context"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber/aresdb/metastore/common"

)

var _ = ginkgo.Describe("bootstrap server", func() {

	tableName := "table1"
	shardID := 1
	nodeID := "123"
	metaStore := &metaMock.MetaStore{}
	diskStore := &diskMock.DiskStore{}

	table := common.Table{
		Name: tableName,
		Columns: []common.Column{
			{
				Name: "col1",
				Type: "Int32",
			},
		},
		Version: 2,
	}

	server := NewPeerDataNodeServer(metaStore, diskStore)

	req := pb.StartSessionRequest{
		Table:  tableName,
		Shard:  uint32(shardID),
		Ttl:    5 * 60,
		NodeID: nodeID,
	}

	ginkgo.It("start session test", func() {
		metaStore.On("GetTable", tableName).Return(&table, nil)
		session, err := server.StartSession(context.Background(), &req)
		Ω(err).Should(BeNil())
		Ω(session).ShouldNot(BeNil())
	})

})
