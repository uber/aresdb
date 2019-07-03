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
	"bytes"
	"io"
	"time"

	"github.com/golang/mock/gomock"
	m3Shard "github.com/m3db/m3/src/cluster/shard"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
	aresShard "github.com/uber/aresdb/cluster/shard"
	"github.com/uber/aresdb/cluster/topology"
	"github.com/uber/aresdb/common"
	"github.com/uber/aresdb/datanode/bootstrap"
	"github.com/uber/aresdb/datanode/client"
	datanodeMocks "github.com/uber/aresdb/datanode/client/mocks"
	"github.com/uber/aresdb/datanode/generated/proto/rpc"
	rpcMocks "github.com/uber/aresdb/datanode/generated/proto/rpc/mocks"
	diskMocks "github.com/uber/aresdb/diskstore/mocks"
	memCom "github.com/uber/aresdb/memstore/common"
	memComMocks "github.com/uber/aresdb/memstore/common/mocks"
	metaCom "github.com/uber/aresdb/metastore/common"
	metaMocks "github.com/uber/aresdb/metastore/mocks"
	"github.com/uber/aresdb/redolog"
	testingUtils "github.com/uber/aresdb/testing"
	"github.com/uber/aresdb/utils"
)

var _ = ginkgo.Describe("table shard bootstrap", func() {
	diskStore := &diskMocks.DiskStore{}
	metaStore := &metaMocks.MetaStore{}
	redoLogManagerMaster, _ := redolog.NewRedoLogManagerMaster(&common.RedoLogConfig{}, diskStore, metaStore)
	bootstrapToken := new(memComMocks.BootStrapToken)
	bootstrapToken.On("AcquireToken",  mock.Anything, mock.Anything).Return(true)
	bootstrapToken.On("ReleaseToken",  mock.Anything, mock.Anything).Return()
	memStore := NewMemStore(metaStore, diskStore, NewOptions(bootstrapToken, redoLogManagerMaster)).(*memStoreImpl)
	peerSource := &datanodeMocks.PeerSource{}

	hostMemoryManager := NewHostMemoryManager(memStore, 1<<32)

	options := bootstrap.NewOptions().SetMaxConcurrentStreamsPerTableShards(1)

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
			shardID := 0
			batchID := 1
			batchSize := 5
			archivingCutoff := 10
			backfillSeq := 1
			redoFileID := 10
			redoFileOffset := 10
			utils.SetClockImplementation(func() time.Time {
				return time.Unix(86400, 0)
			})
			defer utils.ResetClockImplementation()

			sorted, _ := GetFactory().ReadArchiveBatch("archiving/archiveBatch0")

			vp0BufferSorted := &bytes.Buffer{}
			vp1BufferSorted := &bytes.Buffer{}
			vp2BufferSorted := &bytes.Buffer{}
			sorted.GetVectorParty(0).Write(vp0BufferSorted)
			sorted.GetVectorParty(1).Write(vp1BufferSorted)
			sorted.GetVectorParty(2).Write(vp2BufferSorted)

			metaStore.On("UpdateArchivingCutoff", table, shardID, uint32(archivingCutoff)).Return(nil).Once()
			metaStore.On("GetArchivingCutoff", table, shardID).Return(uint32(archivingCutoff), nil).Once()
			metaStore.On("GetArchiveBatchVersion", table, shardID, batchID, uint32(archivingCutoff)).Return(uint32(archivingCutoff), uint32(backfillSeq), batchSize, nil)
			metaStore.On("UpdateBackfillProgress", table, shardID, int64(redoFileID), uint32(redoFileOffset)).Return(nil).Once()
			metaStore.On("GetBackfillProgressInfo", table, shardID).Return(int64(redoFileID), uint32(redoFileOffset), nil).Once()
			diskStore.On("ListLogFiles", table, shardID).Return([]int64{}, nil).Once()

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
					PrimaryKeyColumns:    []int{0},
					ArchivingSortColumns: []int{1, 2},
					Columns: []metaCom.Column{
						{Deleted: false, Config: metaCom.ColumnConfig{
							PreloadingDays: 1,
						}},
						{Deleted: false, Config: metaCom.ColumnConfig{
							PreloadingDays: 1,
						}},
						{Deleted: false, Config: metaCom.ColumnConfig{
							PreloadingDays: 1,
						}},
					},
				},
				ValueTypeByColumn: []memCom.DataType{memCom.Uint32, memCom.Bool, memCom.Float32},
				DefaultValues:     []*memCom.DataValue{&memCom.NullDataValue, &memCom.NullDataValue, &memCom.NullDataValue},
			}, metaStore, diskStore, hostMemoryManager, 0, memStore.options)
			shard.needPeerCopy = 1

			ctrl := gomock.NewController(utils.TestingT)
			defer ctrl.Finish()

			column0MockBuffer := &testingUtils.TestReadWriteCloser{}
			column1MockBuffer := &testingUtils.TestReadWriteCloser{}
			column2MockBuffer := &testingUtils.TestReadWriteCloser{}

			diskStore.On("OpenVectorPartyFileForWrite", table, 0, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(column0MockBuffer, nil).Once()
			diskStore.On("OpenVectorPartyFileForWrite", table, 1, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(column1MockBuffer, nil).Once()
			diskStore.On("OpenVectorPartyFileForWrite", table, 2, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(column2MockBuffer, nil).Once()

			diskStore.On("OpenVectorPartyFileForRead", table, 0, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(column0MockBuffer, nil).Once()
			diskStore.On("OpenVectorPartyFileForRead", table, 1, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(column1MockBuffer, nil).Once()
			diskStore.On("OpenVectorPartyFileForRead", table, 2, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(column2MockBuffer, nil).Once()

			tableShardMetaData := &rpc.TableShardMetaData{
				Table:       table,
				Shard:       uint32(shardID),
				Incarnation: 0,
				KafkaOffset: &rpc.KafkaOffset{
					CheckPointOffset: 10,
					CommitOffset:     20,
				},
				Meta: &rpc.TableShardMetaData_FactMeta{
					FactMeta: &rpc.FactTableShardMetaData{
						HighWatermark: uint32(archivingCutoff),
						BackfillCheckpoint: &rpc.BackfillCheckpoint{
							RedoFileID:     int64(redoFileID),
							RedoFileOffset: uint32(redoFileOffset),
						},
					},
				},
				Batches: []*rpc.BatchMetaData{
					{
						BatchID: int32(batchID),
						Size:    uint32(batchSize),
						ArchiveVersion: &rpc.ArchiveVersion{
							ArchiveVersion: uint32(archivingCutoff),
							BackfillSeq:    uint32(backfillSeq),
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

			mockFetchRawDataStream0.On("Recv").Return(&rpc.VectorPartyRawData{Chunk: vp0BufferSorted.Bytes()}, nil).Once()
			mockFetchRawDataStream0.On("Recv").Return(&rpc.VectorPartyRawData{Chunk: nil}, io.EOF).Once()

			mockFetchRawDataStream1.On("Recv").Return(&rpc.VectorPartyRawData{Chunk: vp1BufferSorted.Bytes()}, nil).Once()
			mockFetchRawDataStream1.On("Recv").Return(&rpc.VectorPartyRawData{Chunk: nil}, io.EOF).Once()

			mockFetchRawDataStream2.On("Recv").Return(&rpc.VectorPartyRawData{Chunk: vp2BufferSorted.Bytes()}, nil).Once()
			mockFetchRawDataStream2.On("Recv").Return(&rpc.VectorPartyRawData{Chunk: nil}, io.EOF).Once()

			mockPeerDataNodeClient.On("FetchVectorPartyRawData", mock.Anything, mock.Anything).Return(mockFetchRawDataStream0, nil).Once()
			mockPeerDataNodeClient.On("FetchVectorPartyRawData", mock.Anything, mock.Anything).Return(mockFetchRawDataStream1, nil).Once()
			mockPeerDataNodeClient.On("FetchVectorPartyRawData", mock.Anything, mock.Anything).Return(mockFetchRawDataStream2, nil).Once()

			err := shard.Bootstrap(peerSource, host0.ID(), staticTopology, topoState, options)
			Ω(err).Should(BeNil())
			Ω(err).Should(BeNil())
			Ω(shard.IsDiskDataAvailable()).Should(BeTrue())
			Ω(shard.IsBootstrapped()).Should(BeTrue())

			b0 := &bytes.Buffer{}
			b1 := &bytes.Buffer{}
			b2 := &bytes.Buffer{}
			batch0 := shard.ArchiveStore.CurrentVersion.GetBatchForRead(batchID)
			defer batch0.RUnlock()

			err = batch0.GetVectorParty(0).Write(b0)
			Ω(err).Should(BeNil())
			err = batch0.GetVectorParty(1).Write(b1)
			Ω(err).Should(BeNil())
			err = batch0.GetVectorParty(2).Write(b2)
			Ω(err).Should(BeNil())
			Ω(b0.Bytes()).Should(Equal(vp0BufferSorted.Bytes()))
			Ω(b1.Bytes()).Should(Equal(vp1BufferSorted.Bytes()))
			Ω(b2.Bytes()).Should(Equal(vp2BufferSorted.Bytes()))
		})

		ginkgo.It("bootstrap should work for dimension table", func() {
			table := "test_dim"
			shardID := 0
			redoFileID := 123
			redoFileOffset := 1
			lastReadBatchID := -110
			lastBatchSize := 6

			unsorted, _ := GetFactory().ReadLiveBatch("archiving/batch-110")

			vp0BufferUnsorted := &bytes.Buffer{}
			vp1BufferUnsorted := &bytes.Buffer{}
			vp2BufferUnsorted := &bytes.Buffer{}

			unsorted.GetVectorParty(0).Write(vp0BufferUnsorted)
			unsorted.GetVectorParty(1).Write(vp1BufferUnsorted)
			unsorted.GetVectorParty(2).Write(vp2BufferUnsorted)

			metaStore.On("UpdateSnapshotProgress", table, shardID, int64(redoFileID), uint32(redoFileOffset), int32(lastReadBatchID), uint32(lastBatchSize)).Return(nil).Once()
			metaStore.On("GetSnapshotProgress", table, shardID).Return(int64(redoFileID), uint32(redoFileOffset), int32(lastReadBatchID), uint32(lastBatchSize), nil).Once()
			diskStore.On("ListSnapshotBatches", table, shardID, int64(redoFileID), uint32(redoFileOffset)).Return([]int{lastReadBatchID}, nil)
			diskStore.On("ListSnapshotVectorPartyFiles", table, shardID, int64(redoFileID), uint32(redoFileOffset), lastReadBatchID).Return([]int{0, 1, 2}, nil).Once()
			diskStore.On("ListLogFiles", table, shardID).Return([]int64{}, nil).Once()

			column0MockBuffer := &testingUtils.TestReadWriteCloser{}
			column1MockBuffer := &testingUtils.TestReadWriteCloser{}
			column2MockBuffer := &testingUtils.TestReadWriteCloser{}

			diskStore.On("OpenSnapshotVectorPartyFileForWrite", table, shardID, mock.Anything, mock.Anything, mock.Anything, 0).Return(column0MockBuffer, nil).Once()
			diskStore.On("OpenSnapshotVectorPartyFileForWrite", table, shardID, mock.Anything, mock.Anything, mock.Anything, 1).Return(column1MockBuffer, nil).Once()
			diskStore.On("OpenSnapshotVectorPartyFileForWrite", table, shardID, mock.Anything, mock.Anything, mock.Anything, 2).Return(column2MockBuffer, nil).Once()

			diskStore.On("OpenSnapshotVectorPartyFileForRead", table, shardID, mock.Anything, mock.Anything, mock.Anything, 0).Return(column0MockBuffer, nil).Once()
			diskStore.On("OpenSnapshotVectorPartyFileForRead", table, shardID, mock.Anything, mock.Anything, mock.Anything, 1).Return(column1MockBuffer, nil).Once()
			diskStore.On("OpenSnapshotVectorPartyFileForRead", table, shardID, mock.Anything, mock.Anything, mock.Anything, 2).Return(column2MockBuffer, nil).Once()

			shard := NewTableShard(&memCom.TableSchema{
				Schema: metaCom.Table{
					Name: table,
					Config: metaCom.TableConfig{
						RedoLogRotationInterval: 10800,
						MaxRedoLogFileSize:      1 << 30,
						RecordRetentionInDays:   10,
					},
					IsFactTable:       false,
					PrimaryKeyColumns: []int{0},
					Columns: []metaCom.Column{
						{Deleted: false},
						{Deleted: false},
						{Deleted: false},
					},
				},
				ValueTypeByColumn: []memCom.DataType{memCom.Uint32, memCom.Bool, memCom.Float32},
				DefaultValues:     []*memCom.DataValue{&memCom.NullDataValue, &memCom.NullDataValue, &memCom.NullDataValue},
			}, metaStore, diskStore, hostMemoryManager, 0, memStore.options)
			shard.needPeerCopy = 1

			ctrl := gomock.NewController(utils.TestingT)
			defer ctrl.Finish()

			tableShardMetaData := &rpc.TableShardMetaData{
				Table:       table,
				Shard:       uint32(shardID),
				Incarnation: 0,
				KafkaOffset: &rpc.KafkaOffset{
					CheckPointOffset: 10,
					CommitOffset:     20,
				},
				Meta: &rpc.TableShardMetaData_DimensionMeta{
					DimensionMeta: &rpc.DimensionTableShardMetaData{
						SnapshotVersion: &rpc.SnapshotVersion{
							RedoFileID:     int64(redoFileID),
							RedoFileOffset: uint32(redoFileOffset),
						},
						LastBatchID:   int32(lastReadBatchID),
						LastBatchSize: int32(lastBatchSize),
					},
				},
				Batches: []*rpc.BatchMetaData{
					{
						BatchID: int32(lastReadBatchID),
						Size:    uint32(lastBatchSize),
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

			mockFetchRawDataStream0.On("Recv").Return(&rpc.VectorPartyRawData{Chunk: vp0BufferUnsorted.Bytes()}, nil).Once()
			mockFetchRawDataStream0.On("Recv").Return(&rpc.VectorPartyRawData{Chunk: nil}, io.EOF).Once()

			mockFetchRawDataStream1.On("Recv").Return(&rpc.VectorPartyRawData{Chunk: vp1BufferUnsorted.Bytes()}, nil).Once()
			mockFetchRawDataStream1.On("Recv").Return(&rpc.VectorPartyRawData{Chunk: nil}, io.EOF).Once()

			mockFetchRawDataStream2.On("Recv").Return(&rpc.VectorPartyRawData{Chunk: vp2BufferUnsorted.Bytes()}, nil).Once()
			mockFetchRawDataStream2.On("Recv").Return(&rpc.VectorPartyRawData{Chunk: nil}, io.EOF).Once()

			mockPeerDataNodeClient.On("FetchVectorPartyRawData", mock.Anything, mock.Anything).Return(mockFetchRawDataStream0, nil).Once()
			mockPeerDataNodeClient.On("FetchVectorPartyRawData", mock.Anything, mock.Anything).Return(mockFetchRawDataStream1, nil).Once()
			mockPeerDataNodeClient.On("FetchVectorPartyRawData", mock.Anything, mock.Anything).Return(mockFetchRawDataStream2, nil).Once()

			err := shard.Bootstrap(peerSource, host0.ID(), staticTopology, topoState, options)
			Ω(err).Should(BeNil())
			Ω(shard.IsDiskDataAvailable()).Should(BeTrue())
			Ω(shard.IsBootstrapped()).Should(BeTrue())

			b0 := &bytes.Buffer{}
			b1 := &bytes.Buffer{}
			b2 := &bytes.Buffer{}
			batch110 := shard.LiveStore.GetBatchForRead(int32(lastReadBatchID))
			defer batch110.RUnlock()

			err = batch110.GetVectorParty(0).Write(b0)
			Ω(err).Should(BeNil())
			err = batch110.GetVectorParty(1).Write(b1)
			Ω(err).Should(BeNil())
			err = batch110.GetVectorParty(2).Write(b2)
			Ω(err).Should(BeNil())
			Ω(b0.Bytes()).Should(Equal(vp0BufferUnsorted.Bytes()))
			Ω(b1.Bytes()).Should(Equal(vp1BufferUnsorted.Bytes()))
			Ω(b2.Bytes()).Should(Equal(vp2BufferUnsorted.Bytes()))
		})
	})
})
