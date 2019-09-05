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

package bootstrap

import (
	"context"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	pb "github.com/uber/aresdb/datanode/generated/proto/rpc"
	"github.com/uber/aresdb/diskstore"
	"github.com/uber/aresdb/metastore"
	"github.com/uber/aresdb/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"net"
	"time"
)

const bufSize = 1024 * 1024

var _ = ginkgo.Describe("bootstrap server", func() {
	factTable := "facttable1"
	dimTable := "dimtable1"

	shardID := 0
	nodeID := "123"
	metaStore, _ := metastore.NewDiskMetaStore("../../testing/data/bootstrap/metastore")
	diskStore := diskstore.NewLocalDiskStore("../../testing/data/bootstrap")

	req := pb.StartSessionRequest{
		Shard: uint32(shardID),
		Ttl:   5 * 60,
	}

	var testServer *grpc.Server
	var peerServer pb.PeerDataNodeServer
	var listener *bufconn.Listener

	connFunc := func() *grpc.ClientConn {
		ctx := context.Background()
		f := func(string, time.Duration) (net.Conn, error) {
			return listener.Dial()
		}
		conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithDialer(f), grpc.WithInsecure())
		Ω(err).Should(BeNil())
		return conn
	}

	ginkgo.BeforeEach(func() {
		// setup server
		utils.SetCurrentTime(time.Unix(18056 * 86400, 0))
		testServer = grpc.NewServer()
		peerServer = NewPeerDataNodeServer(metaStore, diskStore)
		pb.RegisterPeerDataNodeServer(testServer, peerServer)
		listener = bufconn.Listen(bufSize)

		go func() {
			testServer.Serve(listener)
		}()
	})

	ginkgo.AfterEach(func() {
		utils.ResetClockImplementation()
		testServer.Stop()
		listener.Close()
	})

	ginkgo.It("start session test", func() {
		conn := connFunc()
		defer conn.Close()

		client := pb.NewPeerDataNodeClient(conn)
		// no nodeid will fail
		session, err := client.StartSession(context.Background(), &req)
		Ω(err).ShouldNot(BeNil())

		badTable := "table_fail"
		// fail tabe
		req.Table = badTable
		req.NodeID = nodeID
		session, err = client.StartSession(context.Background(), &req)
		Ω(err).ShouldNot(BeNil())

		// working cases
		req.Table = factTable
		session, err = client.StartSession(context.Background(), &req)
		Ω(err).Should(BeNil())
		Ω(session).ShouldNot(BeNil())
		Ω(session.ID > 0).Should(BeTrue())

		req.Table = dimTable
		session, err = client.StartSession(context.Background(), &req)
		Ω(err).Should(BeNil())
		Ω(session).ShouldNot(BeNil())
		Ω(session.ID > 0).Should(BeTrue())

		// same tabe/shard/node will fail
		session, err = client.StartSession(context.Background(), &req)
		Ω(err).ShouldNot(BeNil())

		s := peerServer.(*PeerDataNodeServerImpl)
		Ω(len(s.sessions)).Should(Equal(2))
		Ω(len(s.tableShardSessions)).Should(Equal(2))
	})

	ginkgo.It("keepalive test", func() {
		conn := connFunc()
		defer conn.Close()

		client := pb.NewPeerDataNodeClient(conn)
		req.Table = factTable
		req.NodeID = nodeID

		session, err := client.StartSession(context.Background(), &req)
		Ω(err).Should(BeNil())
		Ω(session).ShouldNot(BeNil())

		keepAliveClient, err := client.KeepAlive(context.Background())
		Ω(err).Should(BeNil())
		for i := 0; i < 10; i++ {
			err = keepAliveClient.Send(&pb.Session{ID: session.ID, NodeID: nodeID})
			resp, err := keepAliveClient.Recv()
			Ω(err).Should(BeNil())
			Ω(resp.Ttl).Should(Equal(int64(300)))
			time.Sleep(time.Millisecond * 10)
		}
		err = keepAliveClient.CloseSend()
		Ω(err).Should(BeNil())

		s := peerServer.(*PeerDataNodeServerImpl)
		Ω(len(s.sessions)).Should(Equal(1))
		Ω(len(s.tableShardSessions)).Should(Equal(1))

		// sent invaild session id will be failure
		keepAliveClient, err = client.KeepAlive(context.Background())
		Ω(err).Should(BeNil())
		err = keepAliveClient.Send(&pb.Session{ID: int64(123), NodeID: nodeID})
		Ω(err).Should(BeNil())
		_, err = keepAliveClient.Recv()
		Ω(err).ShouldNot(BeNil())

		// sent invaild node id will be failure
		keepAliveClient, err = client.KeepAlive(context.Background())
		Ω(err).Should(BeNil())
		err = keepAliveClient.Send(&pb.Session{ID: session.ID})
		Ω(err).Should(BeNil())
		_, err = keepAliveClient.Recv()
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("FetchTableShardMetaData test", func() {
		conn := connFunc()
		defer conn.Close()

		client := pb.NewPeerDataNodeClient(conn)
		req.Table = factTable
		req.NodeID = nodeID

		// fact table
		session, err := client.StartSession(context.Background(), &req)
		Ω(err).Should(BeNil())
		Ω(session).ShouldNot(BeNil())

		metaReq := &pb.TableShardMetaDataRequest{
			Table:     factTable,
			Shard:     uint32(shardID),
			NodeID:    nodeID,
			SessionID: session.ID,
		}
		metaData, err := client.FetchTableShardMetaData(context.Background(), metaReq)
		Ω(err).Should(BeNil())
		Ω(metaData).ShouldNot(BeNil())
		Ω(metaData.KafkaOffset.CheckPointOffset).Should(Equal(int64(1000)))
		Ω(metaData.KafkaOffset.CommitOffset).Should(Equal(int64(10000)))
		Ω(metaData.GetFactMeta().HighWatermark).Should(Equal(uint32(1560049865)))
		Ω(metaData.GetFactMeta().BackfillCheckpoint.RedoFileID).Should(Equal(int64(1560052917)))
		Ω(metaData.GetFactMeta().BackfillCheckpoint.RedoFileOffset).Should(Equal(uint32(6894)))
		Ω(len(metaData.Batches)).Should(Equal(9))
		Ω(metaData.Batches[0].BatchID).Should(Equal(int32(18048)))
		Ω(metaData.Batches[0].Size).Should(Equal(uint32(78347676)))
		Ω(metaData.Batches[0].ArchiveVersion.ArchiveVersion).Should(Equal(uint32(1559436638)))
		Ω(metaData.Batches[0].ArchiveVersion.BackfillSeq).Should(Equal(uint32(0)))
		Ω(len(metaData.Batches[0].Vps)).Should(Equal(6))

		// dimension table
		req.Table = dimTable
		session, err = client.StartSession(context.Background(), &req)
		Ω(err).Should(BeNil())
		Ω(session).ShouldNot(BeNil())

		metaReq.Table = dimTable
		metaReq.SessionID = session.ID
		metaData, err = client.FetchTableShardMetaData(context.Background(), metaReq)
		Ω(err).Should(BeNil())
		Ω(metaData).ShouldNot(BeNil())
		Ω(metaData.KafkaOffset.CheckPointOffset).Should(Equal(int64(1000)))
		Ω(metaData.KafkaOffset.CommitOffset).Should(Equal(int64(10000)))
		Ω(metaData.GetDimensionMeta().SnapshotVersion.RedoFileID).Should(Equal(int64(1560032167)))
		Ω(metaData.GetDimensionMeta().SnapshotVersion.RedoFileOffset).Should(Equal(uint32(605)))
		Ω(metaData.GetDimensionMeta().LastBatchID).Should(Equal(int32(-2147483648)))
		Ω(metaData.GetDimensionMeta().LastBatchSize).Should(Equal(int32(603670)))
		Ω(len(metaData.Batches)).Should(Equal(1))
		Ω(len(metaData.Batches[0].Vps)).Should(Equal(5))
	})

	ginkgo.It("FetchVectorPartyRawData test", func() {
		conn := connFunc()
		defer conn.Close()

		client := pb.NewPeerDataNodeClient(conn)
		req.Table = factTable
		req.NodeID = nodeID

		// fact table
		session, err := client.StartSession(context.Background(), &req)
		Ω(err).Should(BeNil())
		Ω(session).ShouldNot(BeNil())

		dataRequest := &pb.VectorPartyRawDataRequest{
			Table:     factTable,
			Shard:     uint32(shardID),
			SessionID: session.ID,
			NodeID:    nodeID,
			BatchID:   18048,
			ColumnID:  0,
			Version: &pb.VectorPartyRawDataRequest_ArchiveVersion{
				ArchiveVersion: &pb.ArchiveVersion{
					ArchiveVersion: uint32(1559436638),
					BackfillSeq:    uint32(0),
				},
			},
		}

		fetchClient, err := client.FetchVectorPartyRawData(context.Background(), dataRequest)
		Ω(err).Should(BeNil())
		l := 0
		var rawData *pb.VectorPartyRawData
		for {
			rawData, err = fetchClient.Recv()
			if err != nil {
				break
			}
			l += len(rawData.Chunk)
		}
		Ω(l).Should(Equal(163840))
		Ω(err.Error()).Should(ContainSubstring("EOF")) // weird it not return io.EOF while msg is EOF

		// dim table
		req.Table = dimTable
		session, err = client.StartSession(context.Background(), &req)
		Ω(err).Should(BeNil())
		Ω(session).ShouldNot(BeNil())

		dataRequest = &pb.VectorPartyRawDataRequest{
			Table:     dimTable,
			Shard:     uint32(shardID),
			SessionID: session.ID,
			NodeID:    nodeID,
			BatchID:   -2147483648,
			ColumnID:  0,
			Version: &pb.VectorPartyRawDataRequest_SnapshotVersion{
				SnapshotVersion: &pb.SnapshotVersion{
					RedoFileID:     int64(1560032167),
					RedoFileOffset: uint32(605),
				},
			},
		}

		fetchClient, err = client.FetchVectorPartyRawData(context.Background(), dataRequest)
		Ω(err).Should(BeNil())
		l = 0
		for {
			rawData, err = fetchClient.Recv()
			if err != nil {
				break
			}
			l += len(rawData.Chunk)
		}
		Ω(l).Should(Equal(163839))
		Ω(err.Error()).Should(ContainSubstring("EOF"))
	})

	ginkgo.It("AcquireToken/ReleaseToken test", func() {
		s := peerServer.(*PeerDataNodeServerImpl)
		ok := s.AcquireToken(factTable, 0)
		Ω(ok).Should(BeTrue())

		conn := connFunc()
		defer conn.Close()

		client := pb.NewPeerDataNodeClient(conn)
		req.Table = factTable
		req.NodeID = nodeID
		session, err := client.StartSession(context.Background(), &req)
		Ω(err).Should(BeNil())
		Ω(len(s.sessions)).Should(Equal(1))
		Ω(len(s.tableShardSessions)).Should(Equal(1))

		// some session running, acquire token shall fail
		ok = s.AcquireToken(factTable, 0)
		Ω(ok).Should(BeFalse())

		s.cleanSession(session.ID, true)
		// no more sessions, acquire token shall success
		ok = s.AcquireToken(factTable, 0)
		Ω(ok).Should(BeTrue())
	})
})
