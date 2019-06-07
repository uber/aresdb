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
	diskStore := &diskMocks.DiskStore{}
	metaStore := &metaMocks.MetaStore{}
	memStore := getFactory().NewMockMemStore()
	peerSource := &datanodeMocks.PeerSource{}

	hostMemoryManager := NewHostMemoryManager(memStore, 1<<32)

	ginkgo.Describe("bootstrap should work", func() {
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
		metaStore.On("UpdateRedoLogCommitOffset", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		metaStore.On("UpdateRedoLogCheckpointOffset", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		metaStore.On("UpdateSnapshotProgress", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		metaStore.On("UpdateArchivingCutoff", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		metaStore.On("UpdateBackfillProgress", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		metaStore.On("OverwriteArchiveBatchVersion", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		mockPeerDataNodeClient := &rpcMocks.PeerDataNodeClient{}
		mockSession := &rpc.Session{ID: 0}
		mockPeerDataNodeClient.On("StartSession", mock.Anything, mock.Anything).Return(mockSession, nil)
		peerSource.On("BorrowConnection", host1.ID(), mock.Anything).Run(func(args mock.Arguments) {
			fn := args.Get(1).(client.WithConnectionFn)
			fn(mockPeerDataNodeClient)
		}).Return(nil)

		mockKeepAliveStream := &rpcMocks.PeerDataNode_KeepAliveClient{}
		mockKeepAliveStream.On("Send", mock.Anything).Return(nil)
		mockKeepAliveStream.On("Recv", mock.Anything).Return(&rpc.KeepAliveResponse{ID: 0}, nil)
		mockKeepAliveStream.On("CloseSend", mock.Anything).Return(nil)
		mockPeerDataNodeClient.On("KeepAlive", mock.Anything).Return(mockKeepAliveStream, nil)

		ginkgo.It("bootstrap should work for fact table", func() {
			table := "test_fact"
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

			ctrl := gomock.NewController(utils.TestingT)
			defer ctrl.Finish()


			column0MockBuffer := &testingUtils.TestReadWriteCloser{}
			column1MockBuffer := &testingUtils.TestReadWriteCloser{}
			column2MockBuffer := &testingUtils.TestReadWriteCloser{}

			diskStore.On("OpenVectorPartyFileForWrite", table, 0, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(column0MockBuffer, nil).Once()
			diskStore.On("OpenVectorPartyFileForWrite", table, 1, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(column1MockBuffer, nil).Once()
			diskStore.On("OpenVectorPartyFileForWrite", table, 2, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(column2MockBuffer, nil).Once()

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

			mockPeerDataNodeClient.On("FetchTableShardMetaData", mock.Anything, mock.Anything).Return(tableShardMetaData, nil).Once()

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

			mockPeerDataNodeClient.On("FetchVectorPartyRawData", mock.Anything, mock.Anything).Return(mockFetchRawDataStream0, nil).Once()
			mockPeerDataNodeClient.On("FetchVectorPartyRawData", mock.Anything, mock.Anything).Return(mockFetchRawDataStream1, nil).Once()
			mockPeerDataNodeClient.On("FetchVectorPartyRawData", mock.Anything, mock.Anything).Return(mockFetchRawDataStream2, nil).Once()

			err := shard.Bootstrap(peerSource, host0, staticTopology, topoState)
			Ω(err).Should(BeNil())
			Ω(column0MockBuffer.Buffer.Bytes()).Should(Equal([]byte{1, 2}))
			Ω(column1MockBuffer.Buffer.Bytes()).Should(Equal([]byte{1, 2}))
			Ω(column2MockBuffer.Buffer.Bytes()).Should(Equal([]byte{1, 2}))
		})

		ginkgo.It("bootstrap should work for dimension table", func() {
			table := "test_dim"
			shard := NewTableShard(&memCom.TableSchema{
				Schema: metaCom.Table{
					Name: table,
					Config: metaCom.TableConfig{
						RedoLogRotationInterval:  10800,
						MaxRedoLogFileSize:       1 << 30,
						RecordRetentionInDays:    10,
					},
					IsFactTable:          false,
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

			ctrl := gomock.NewController(utils.TestingT)
			defer ctrl.Finish()

			column0MockBuffer := &testingUtils.TestReadWriteCloser{}
			column1MockBuffer := &testingUtils.TestReadWriteCloser{}
			column2MockBuffer := &testingUtils.TestReadWriteCloser{}
			diskStore.On("OpenSnapshotVectorPartyFileForWrite", table, mock.Anything, mock.Anything, mock.Anything, mock.Anything, 0).Return(column0MockBuffer, nil).Once()
			diskStore.On("OpenSnapshotVectorPartyFileForWrite", table, mock.Anything, mock.Anything, mock.Anything, mock.Anything, 1).Return(column1MockBuffer, nil).Once()
			diskStore.On("OpenSnapshotVectorPartyFileForWrite", table, mock.Anything, mock.Anything, mock.Anything, mock.Anything, 2).Return(column2MockBuffer, nil).Once()

			tableShardMetaData := &rpc.TableShardMetaData{
				Table:       table,
				Shard:       0,
				Incarnation: 0,
				KafkaOffset: &rpc.KafkaOffset{
					CheckPointOffset: 10,
					CommitOffset:     20,
				},
				Meta: &rpc.TableShardMetaData_DimensionMeta{
					DimensionMeta: &rpc.DimensionTableShardMetaData{
						SnapshotVersion: &rpc.SnapshotVersion{
							RedoFileID: 123,
							RedoFileOffset: 1,
						},
						LastBatchID: -213,
						LastBatchSize: 10,
					},
				},
				Batches: []*rpc.BatchMetaData{
					{
						BatchID: 1,
						Size:    100,
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
			mockPeerDataNodeClient.On("FetchTableShardMetaData", mock.Anything, mock.Anything).Return(tableShardMetaData, nil).Once()

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

			mockPeerDataNodeClient.On("FetchVectorPartyRawData", mock.Anything, mock.Anything).Return(mockFetchRawDataStream0, nil).Once()
			mockPeerDataNodeClient.On("FetchVectorPartyRawData", mock.Anything, mock.Anything).Return(mockFetchRawDataStream1, nil).Once()
			mockPeerDataNodeClient.On("FetchVectorPartyRawData", mock.Anything, mock.Anything).Return(mockFetchRawDataStream2, nil).Once()

			err := shard.Bootstrap(peerSource, host0, staticTopology, topoState)
			Ω(err).Should(BeNil())
			Ω(column0MockBuffer.Buffer.Bytes()).Should(Equal([]byte{1, 2}))
			Ω(column1MockBuffer.Buffer.Bytes()).Should(Equal([]byte{1, 2}))
			Ω(column2MockBuffer.Buffer.Bytes()).Should(Equal([]byte{1, 2}))
		})
	})
})
