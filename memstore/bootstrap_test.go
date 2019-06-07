package memstore

import (
	"io"

	"github.com/golang/mock/gomock"
	m3Shard "github.com/m3db/m3/src/cluster/shard"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
	aresShard "github.com/uber/aresdb/cluster/shard"
	"github.com/uber/aresdb/cluster/topology"
	"github.com/uber/aresdb/datanode/client"
	datanodeMocks "github.com/uber/aresdb/datanode/client/mocks"
	"github.com/uber/aresdb/datanode/generated/proto/rpc"
	rpcMocks "github.com/uber/aresdb/datanode/generated/proto/rpc/mocks"
	diskMocks "github.com/uber/aresdb/diskstore/mocks"
	memCom "github.com/uber/aresdb/memstore/common"
	metaCom "github.com/uber/aresdb/metastore/common"
	metaMocks "github.com/uber/aresdb/metastore/mocks"
	testingUtils "github.com/uber/aresdb/testing"
	"github.com/uber/aresdb/utils"
)

var _ = ginkgo.Describe("table shard bootstrap", func() {
	table := "test"
	diskStore := &diskMocks.DiskStore{}
	metaStore := &metaMocks.MetaStore{}
	memStore := getFactory().NewMockMemStore()
	peerSource := &datanodeMocks.PeerSource{}

	hostMemoryManager := NewHostMemoryManager(memStore, 1<<32)
	shard := NewTableShard(&memCom.TableSchema{
		Schema: metaCom.Table{
			Name: table,
			Config: metaCom.TableConfig{
				ArchivingDelayMinutes:    500,
				ArchivingIntervalMinutes: 300,
				RedoLogRotationInterval:  10800,
				MaxRedoLogFileSize:       1 << 30,
				RecordRetentionInDays:    10,
			},
			IsFactTable:          true,
			ArchivingSortColumns: []int{1, 2},
			Columns: []metaCom.Column{
				{Deleted: false},
				{Deleted: false},
				{Deleted: false},
			},
		},
		ValueTypeByColumn: []memCom.DataType{memCom.Uint32, memCom.Bool, memCom.Float32},
		DefaultValues:     []*memCom.DataValue{&memCom.NullDataValue, &memCom.NullDataValue, &memCom.NullDataValue},
	}, metaStore, diskStore, hostMemoryManager, 0, memStore.redologManagerMaster)

	ginkgo.It("Bootstrap should work", func() {
		ctrl := gomock.NewController(utils.TestingT)
		defer ctrl.Finish()

		metaStore.On("UpdateArchivingCutoff", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		metaStore.On("UpdateRedoLogCommitOffset", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		metaStore.On("UpdateRedoLogCheckpointOffset", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		metaStore.On("UpdateBackfillProgress", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		metaStore.On("OverwriteArchiveBatchVersion", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		column0MockBuffer := &testingUtils.TestReadWriteCloser{}
		column1MockBuffer := &testingUtils.TestReadWriteCloser{}
		column2MockBuffer := &testingUtils.TestReadWriteCloser{}
		diskStore.On("OpenVectorPartyFileForWrite", "test", 0, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(column0MockBuffer, nil).Once()
		diskStore.On("OpenVectorPartyFileForWrite", "test", 1, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(column1MockBuffer, nil).Once()
		diskStore.On("OpenVectorPartyFileForWrite", "test", 2, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(column2MockBuffer, nil).Once()

		host0 := topology.NewHost("instance0", "http://host0:9374")
		host1 := topology.NewHost("instance1", "http://host1:9374")

		mockClient := &rpcMocks.PeerDataNodeClient{}
		mockSession := &rpc.Session{ID: 0}
		mockClient.On("StartSession", mock.Anything, mock.Anything).Return(mockSession, nil)
		peerSource.On("BorrowConnection", host1.ID(), mock.Anything).Run(func(args mock.Arguments) {
			fn := args.Get(1).(client.WithConnectionFn)
			fn(mockClient)
		}).Return(nil)

		mockKeepAliveStream := &rpcMocks.PeerDataNode_KeepAliveClient{}
		mockKeepAliveStream.On("Send", mock.Anything).Return(nil)
		mockKeepAliveStream.On("Recv", mock.Anything).Return(&rpc.KeepAliveResponse{ID: 0}, nil)
		mockKeepAliveStream.On("CloseSend", mock.Anything).Return(nil)
		mockClient.On("KeepAlive", mock.Anything).Return(mockKeepAliveStream, nil)

		tableShardMetaData := &rpc.TableShardMetaData{
			Table:       table,
			Shard:       0,
			Incarnation: 0,
			KafkaOffset: &rpc.KafkaOffset{
				CheckPointOffset: 10,
				CommitOffset:     20,
			},
			Meta: &rpc.TableShardMetaData_FactMeta{
				FactMeta: &rpc.FactTableShardMetaData{
					HighWatermark: 10,
					BackfillCheckpoint: &rpc.BackfillCheckpoint{
						RedoFileID:     10,
						RedoFileOffset: 10,
					},
				},
			},
			Batches: []*rpc.BatchMetaData{
				{
					BatchID: 1,
					Size:    100,
					ArchiveVersion: &rpc.ArchiveVersion{
						ArchiveVersion: 10,
						BackfillSeq:    1,
					},
					Vps: []*rpc.VectorPartyMetaData{
						{
							ColumnID: 0,
						},
						{
							ColumnID: 1,
						},
						{
							ColumnID: 2,
						},
					},
				},
			},
		}
		mockClient.On("FetchTableShardMetaData", mock.Anything, mock.Anything).Return(tableShardMetaData, nil)

		mockFetchRawDataStream0 := &rpcMocks.PeerDataNode_FetchVectorPartyRawDataClient{}
		mockFetchRawDataStream1 := &rpcMocks.PeerDataNode_FetchVectorPartyRawDataClient{}
		mockFetchRawDataStream2 := &rpcMocks.PeerDataNode_FetchVectorPartyRawDataClient{}

		mockFetchRawDataStream0.On("Recv").Return(&rpc.VectorPartyRawData{Chunk: []byte{1}}, nil).Once()
		mockFetchRawDataStream0.On("Recv").Return(&rpc.VectorPartyRawData{Chunk: []byte{2}}, nil).Once()
		mockFetchRawDataStream0.On("Recv").Return(&rpc.VectorPartyRawData{Chunk: nil}, io.EOF).Once()

		mockFetchRawDataStream1.On("Recv").Return(&rpc.VectorPartyRawData{Chunk: []byte{1}}, nil).Once()
		mockFetchRawDataStream1.On("Recv").Return(&rpc.VectorPartyRawData{Chunk: []byte{2}}, nil).Once()
		mockFetchRawDataStream1.On("Recv").Return(&rpc.VectorPartyRawData{Chunk: nil}, io.EOF).Once()

		mockFetchRawDataStream2.On("Recv").Return(&rpc.VectorPartyRawData{Chunk: []byte{1}}, nil).Once()
		mockFetchRawDataStream2.On("Recv").Return(&rpc.VectorPartyRawData{Chunk: []byte{2}}, nil).Once()
		mockFetchRawDataStream2.On("Recv").Return(&rpc.VectorPartyRawData{Chunk: nil}, io.EOF).Once()

		mockClient.On("FetchVectorPartyRawData", mock.Anything, mock.Anything).Return(mockFetchRawDataStream0, nil).Once()
		mockClient.On("FetchVectorPartyRawData", mock.Anything, mock.Anything).Return(mockFetchRawDataStream1, nil).Once()
		mockClient.On("FetchVectorPartyRawData", mock.Anything, mock.Anything).Return(mockFetchRawDataStream2, nil).Once()

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
		staticTopology, err := topology.NewStaticInitializer(staticMapOption).Init()
		Ω(err).Should(BeNil())

		topoState := &topology.StateSnapshot{
			Origin: host0,
			ShardStates: map[topology.ShardID]map[topology.HostID]topology.HostShardState{
				0: {
					"instance0": topology.HostShardState{
						Host:       host0,
						ShardState: m3Shard.Initializing,
					},
					"instance1": topology.HostShardState{
						Host:       host1,
						ShardState: m3Shard.Available,
					},
				},
			},
		}

		err = shard.Bootstrap(peerSource, host0, staticTopology, topoState)
		Ω(err).Should(BeNil())
		Ω(column0MockBuffer.Buffer.Bytes()).Should(Equal([]byte{1, 2}))
		Ω(column1MockBuffer.Buffer.Bytes()).Should(Equal([]byte{1, 2}))
		Ω(column2MockBuffer.Buffer.Bytes()).Should(Equal([]byte{1, 2}))
	})
})
