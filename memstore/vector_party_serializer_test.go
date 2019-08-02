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
	"github.com/uber/aresdb/memstore/vectors"
	"io"

	"bytes"
	"github.com/uber/aresdb/diskstore/mocks"
	"github.com/uber/aresdb/memstore/common"
	memComMocks "github.com/uber/aresdb/memstore/common/mocks"
	metaCom "github.com/uber/aresdb/metastore/common"

	"fmt"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber/aresdb/utils"
)

var _ = ginkgo.Describe("vector party serializer", func() {
	var serializer vectors.VectorPartySerializer
	var snapshotSerializer vectors.VectorPartySerializer

	var writer io.WriteCloser
	var reader io.ReadCloser
	var buf *bytes.Buffer
	m := GetFactory().NewMockMemStore()
	hostMemoryManager := NewHostMemoryManager(m, 1<<32)
	var diskStore *mocks.DiskStore
	table := "test"
	var shardID, columnID, batchID int
	var batchVersion, seqNum, offset uint32
	var redoLogFile int64

	ginkgo.BeforeEach(func() {
		diskStore = new(mocks.DiskStore)
		serializer = common.NewVectorPartyArchiveSerializer(hostMemoryManager, diskStore, "test", shardID, columnID, batchID, batchVersion, seqNum)

		buf = &bytes.Buffer{}
		writer = &utils.ClosableBuffer{
			Buffer: buf,
		}

		diskStore.On("OpenVectorPartyFileForWrite",
			table, columnID, shardID, batchID, batchVersion, seqNum).Return(writer, nil)

		snapshotSerializer = common.NewVectorPartySnapshotSerializer(hostMemoryManager, diskStore, table, shardID, columnID, batchID, batchVersion, seqNum, redoLogFile, offset)

		diskStore.On("OpenSnapshotVectorPartyFileForWrite", table, shardID, redoLogFile, offset, batchID, columnID).Return(writer, nil)
	})

	ginkgo.It("mode 0 vector should work", func() {
		mode0Int8, err := GetFactory().ReadArchiveVectorParty("serializer/mode0_int8", nil)
		Ω(err).Should(BeNil())
		defer mode0Int8.SafeDestruct()

		Ω(serializer.WriteVectorParty(mode0Int8)).Should(BeNil())
		reader = &utils.ClosableReader{
			Reader: bytes.NewReader(buf.Bytes()),
		}
		diskStore.On("OpenVectorPartyFileForRead", table, columnID, shardID, batchID, batchVersion, seqNum).Return(reader, nil)

		newVP := &cVectorParty{}
		err = serializer.ReadVectorParty(newVP)
		Ω(err).Should(BeNil())
		Ω(mode0Int8.Equals(newVP)).Should(BeTrue())
	})

	ginkgo.It("mode 1 vector should work", func() {
		mode1Bool, err := GetFactory().ReadArchiveVectorParty("serializer/mode1_bool", nil)
		Ω(err).Should(BeNil())
		defer mode1Bool.SafeDestruct()

		Ω(serializer.WriteVectorParty(mode1Bool)).Should(BeNil())
		reader = &utils.ClosableReader{
			Reader: bytes.NewReader(buf.Bytes()),
		}
		diskStore.On("OpenVectorPartyFileForRead", table, columnID, shardID, batchID, batchVersion, seqNum).Return(reader, nil)
		newVP := &cVectorParty{}
		err = serializer.ReadVectorParty(newVP)
		Ω(err).Should(BeNil())
		Ω(mode1Bool.Equals(newVP)).Should(BeTrue())
	})

	ginkgo.It("mode 2 vector should work", func() {
		mode2Int8, err := GetFactory().ReadArchiveVectorParty("serializer/mode2_int8", nil)
		Ω(err).Should(BeNil())
		defer mode2Int8.SafeDestruct()

		Ω(serializer.WriteVectorParty(mode2Int8)).Should(BeNil())
		reader = &utils.ClosableReader{
			Reader: bytes.NewReader(buf.Bytes()),
		}
		diskStore.On("OpenVectorPartyFileForRead", table, columnID, shardID, batchID, batchVersion, seqNum).Return(reader, nil)
		newVP := &cVectorParty{}
		err = serializer.ReadVectorParty(newVP)
		Ω(err).Should(BeNil())
		Ω(mode2Int8.Equals(newVP)).Should(BeTrue())
	})

	ginkgo.It("mode 3 vector should work", func() {
		mode3Int8, err := GetFactory().ReadArchiveVectorParty("serializer/mode3_int8", nil)
		Ω(err).Should(BeNil())
		defer mode3Int8.SafeDestruct()

		Ω(serializer.WriteVectorParty(mode3Int8)).Should(BeNil())
		reader = &utils.ClosableReader{
			Reader: bytes.NewReader(buf.Bytes()),
		}
		diskStore.On("OpenVectorPartyFileForRead", table, columnID, shardID, batchID, batchVersion, seqNum).Return(reader, nil)

		newVP := &cVectorParty{}
		err = serializer.ReadVectorParty(newVP)
		Ω(err).Should(BeNil())
		Ω(mode3Int8.Equals(newVP)).Should(BeTrue())
	})

	ginkgo.AfterEach(func() {
	})

	ginkgo.It("mode 0 vector snapshot should work", func() {
		mode0Int8, err := GetFactory().ReadLiveVectorParty("serializer/mode0_int8")
		Ω(err).Should(BeNil())
		defer mode0Int8.SafeDestruct()

		Ω(snapshotSerializer.WriteVectorParty(mode0Int8)).Should(BeNil())
		reader = &utils.ClosableReader{
			Reader: bytes.NewReader(buf.Bytes()),
		}
		diskStore.On("OpenSnapshotVectorPartyFileForRead", table, shardID, redoLogFile, offset, batchID, columnID).Return(reader, nil)
		newVP := &cVectorParty{}
		err = snapshotSerializer.ReadVectorParty(newVP)
		Ω(err).Should(BeNil())
		Ω(mode0Int8.Equals(newVP)).Should(BeTrue())
	})

	ginkgo.It("mode 1 vector snapshot should work", func() {
		mode1Bool, err := GetFactory().ReadLiveVectorParty("serializer/mode1_bool")
		Ω(err).Should(BeNil())
		defer mode1Bool.SafeDestruct()

		Ω(snapshotSerializer.WriteVectorParty(mode1Bool)).Should(BeNil())
		reader = &utils.ClosableReader{
			Reader: bytes.NewReader(buf.Bytes()),
		}
		diskStore.On("OpenSnapshotVectorPartyFileForRead", table, shardID, redoLogFile, offset, batchID, columnID).Return(reader, nil)
		newVP := &cVectorParty{}
		err = snapshotSerializer.ReadVectorParty(newVP)
		Ω(err).Should(BeNil())
		Ω(mode1Bool.Equals(newVP)).Should(BeTrue())
	})

	ginkgo.It("mode 2 vector snapshot should work", func() {
		mode2Int8, err := GetFactory().ReadLiveVectorParty("serializer/mode2_int8")
		Ω(err).Should(BeNil())
		defer mode2Int8.SafeDestruct()

		Ω(snapshotSerializer.WriteVectorParty(mode2Int8)).Should(BeNil())
		reader = &utils.ClosableReader{
			Reader: bytes.NewReader(buf.Bytes()),
		}
		diskStore.On("OpenSnapshotVectorPartyFileForRead", table, shardID, redoLogFile, offset, batchID, columnID).Return(reader, nil)
		newVP := &cVectorParty{}
		err = snapshotSerializer.ReadVectorParty(newVP)
		Ω(err).Should(BeNil())
		Ω(mode2Int8.Equals(newVP)).Should(BeTrue())
	})

	ginkgo.It("mode 3 vector snapshot should work", func() {
		mode3Int8, err := GetFactory().ReadLiveVectorParty("serializer/mode3_int8")
		Ω(err).Should(BeNil())
		defer mode3Int8.SafeDestruct()

		Ω(snapshotSerializer.WriteVectorParty(mode3Int8)).Should(BeNil())
		reader = &utils.ClosableReader{
			Reader: bytes.NewReader(buf.Bytes()),
		}
		diskStore.On("OpenSnapshotVectorPartyFileForRead", table, shardID, redoLogFile, offset, batchID, columnID).Return(reader, nil)
		newVP := &cVectorParty{}
		err = snapshotSerializer.ReadVectorParty(newVP)
		Ω(err).Should(BeNil())
		Ω(mode3Int8.Equals(newVP)).Should(BeTrue())
	})

	ginkgo.It("CheckVectorPartySerializable test", func() {
		schema := common.NewTableSchema(&metaCom.Table{
			Name:                 "trips",
			IsFactTable:          true,
			PrimaryKeyColumns:    []int{1},
			ArchivingSortColumns: []int{3},
			Columns: []metaCom.Column{
				{
					Name: "request_at",
					Type: "Uint32",
				},
				{
					Name: "uuid",
					Type: "UUID",
				},
			},
		})
		diskStore := &mocks.DiskStore{}

		shard := NewTableShard(schema, nil, diskStore,
			NewHostMemoryManager(GetFactory().NewMockMemStore(), 1<<32), 0, m.options)
		archiveSerializer := common.NewVectorPartyArchiveSerializer(shard.HostMemoryManager, shard.diskStore, shard.Schema.Schema.Name, shard.ShardID, 0, 0, 0, 0)
		snapshotSerializer := common.NewVectorPartySnapshotSerializer(shard.HostMemoryManager, shard.diskStore, shard.Schema.Schema.Name, shard.ShardID,0, 0, 0, 0, 0, 0)

		// snapshotSerializer should always has no error
		// goLiveVectoryParty should always has no error
		columnModes := []vectors.ColumnMode{vectors.AllValuesDefault, vectors.AllValuesPresent, vectors.HasNullVector}
		nonDefaultValueCounts := []int{0, 2}
		for i := 0; i < 3; i++ {
			for _, columnMode := range columnModes {
				for _, nonDefaultValueCount := range nonDefaultValueCounts {
					if i == 0 {
						vp := &archiveVectorParty{
							cVectorParty: cVectorParty{
								baseVectorParty: baseVectorParty{
									nonDefaultValueCount: nonDefaultValueCount,
									dataType:             common.Uint32,
								},
								columnMode: columnMode,
							},
						}
						err := archiveSerializer.CheckVectorPartySerializable(vp)
						if (columnMode == vectors.AllValuesDefault && nonDefaultValueCount == 0) || (columnMode != vectors.AllValuesDefault && nonDefaultValueCount > 0) {
							Ω(err).Should(BeNil())
						} else {
							Ω(err).ShouldNot(BeNil())
						}
						err = snapshotSerializer.CheckVectorPartySerializable(vp)
						Ω(err).Should(BeNil())
					} else if i == 1 {
						vp := &cLiveVectorParty{
							cVectorParty: cVectorParty{
								baseVectorParty: baseVectorParty{
									nonDefaultValueCount: nonDefaultValueCount,
									dataType:             common.Uint32,
								},
								columnMode: columnMode,
							},
						}
						err := archiveSerializer.CheckVectorPartySerializable(vp)
						if (columnMode == vectors.AllValuesDefault && nonDefaultValueCount == 0) || (columnMode != vectors.AllValuesDefault && nonDefaultValueCount > 0) {
							Ω(err).Should(BeNil())
						} else {
							Ω(err).ShouldNot(BeNil())
						}

						err = snapshotSerializer.CheckVectorPartySerializable(vp)
						Ω(err).Should(BeNil())
					} else {
						vp := &goLiveVectorParty{
							baseVectorParty: baseVectorParty{
								nonDefaultValueCount: nonDefaultValueCount,
								dataType:             common.Uint32,
							},
						}
						err := archiveSerializer.CheckVectorPartySerializable(vp)
						Ω(err).Should(BeNil())

						err = snapshotSerializer.CheckVectorPartySerializable(vp)
						Ω(err).Should(BeNil())
					}
				}
			}
		}
	})

	ginkgo.It("vector party serializer mock test", func() {
		vp := &memComMocks.VectorParty{}
		vpErr := &memComMocks.VectorParty{}

		reader = &utils.ClosableReader{
			Reader: bytes.NewReader(buf.Bytes()),
		}
		diskStore.On("OpenSnapshotVectorPartyFileForRead", table, shardID, redoLogFile, offset, batchID, columnID).Return(reader, nil)

		vp.On("Read", reader, snapshotSerializer).Return(nil)
		err := snapshotSerializer.ReadVectorParty(vp)
		Ω(err).Should(BeNil())

		vpErr.On("Read", reader, snapshotSerializer).Return(fmt.Errorf("error"))
		err = snapshotSerializer.ReadVectorParty(vpErr)
		Ω(err).ShouldNot(BeNil())
	})
})

