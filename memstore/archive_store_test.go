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
	"errors"

	"encoding/hex"

	"unsafe"

	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
	diskStoreMocks "github.com/uber/aresdb/diskstore/mocks"
	memCom "github.com/uber/aresdb/memstore/common"
	metaCom "github.com/uber/aresdb/metastore/common"
	utilsMocks "github.com/uber/aresdb/utils/mocks"
	"sync"
)

var _ = ginkgo.Describe("archive store", func() {
	table := "table1"
	var shardID, batchID int
	var cutoff uint32 = 100

	m := getFactory().NewMockMemStore()
	hostMemoryManager := NewHostMemoryManager(m, 1<<32)

	ginkgo.It("newArchiveStoreVersion should work", func() {
		newversion := NewArchiveStoreVersion(cutoff, nil)
		Ω(newversion).ShouldNot(BeNil())
		Ω(newversion.ArchivingCutoff).Should(BeEquivalentTo(cutoff))
	})

	ginkgo.It("WriteToDisk should work", func() {
		ds := new(diskStoreMocks.DiskStore)
		archiveBatch := &ArchiveBatch{
			Batch: Batch{
				RWMutex: &sync.RWMutex{},
				Columns: []memCom.VectorParty{
					&archiveVectorParty{
						cVectorParty: cVectorParty{
							baseVectorParty: baseVectorParty{
								dataType: memCom.Bool}}},
					&archiveVectorParty{
						cVectorParty: cVectorParty{
							baseVectorParty: baseVectorParty{
								dataType: memCom.Bool}}},
				},
			},
			Version: cutoff,
			BatchID: int32(batchID),
			Shard: &TableShard{
				diskStore: ds,
				ShardID:   shardID,
				Schema: &memCom.TableSchema{
					Schema: metaCom.Table{
						Name: table,
					},
				},
			},
		}

		writer := new(utilsMocks.WriteCloser)
		writer.On("Write", mock.Anything).Return(0, nil)
		writer.On("Close").Return(nil)

		ds.On("OpenVectorPartyFileForWrite",
			table, mock.Anything, shardID,
			batchID, cutoff, uint32(0)).Return(writer, nil)
		Ω(archiveBatch.WriteToDisk()).Should(BeNil())
	})

	ginkgo.It("RequestVectorParty should work", func() {
		ds := new(diskStoreMocks.DiskStore)

		archiveBatch := &ArchiveBatch{
			Batch: Batch{
				RWMutex: &sync.RWMutex{},
			},
			Version: cutoff,
			BatchID: int32(batchID),
			Shard: &TableShard{
				diskStore: ds,
				ShardID:   shardID,
				Schema: &memCom.TableSchema{
					Schema: metaCom.Table{
						Name: table,
					},
				},
			},
		}

		testVectorParty := newArchiveVectorParty(archiveBatch.Size, memCom.Bool, memCom.NullDataValue, archiveBatch.RWMutex)
		archiveBatch.Batch.Columns = []memCom.VectorParty{
			nil,
			testVectorParty,
		}

		mockErr := errors.New("mock error")
		ds.On("OpenVectorPartyFileForRead",
			table, mock.Anything, shardID,
			batchID, cutoff).Return(nil, mockErr)

		// We won't load second column so it will not trigger an error.
		vp := archiveBatch.RequestVectorParty(1)
		Ω(vp.Equals(archiveBatch.Columns[1])).Should(BeTrue())
		// should be able to release without panic
		vp.Release()
	})

	ginkgo.It("Clone ArchiveBatch should work", func() {
		batch := &ArchiveBatch{
			Version: 1,
			SeqNum:  1,
			Size:    5,
			BatchID: 1,
			Batch: Batch{
				RWMutex: &sync.RWMutex{},
				Columns: make([]memCom.VectorParty, 3),
			},
		}
		batch.Columns = make([]memCom.VectorParty, 3)
		batch.Columns[0] = &archiveVectorParty{}
		batch.Columns[1] = &archiveVectorParty{}
		batch.Columns[2] = &archiveVectorParty{}
		cloned := batch.Clone()
		Ω(cloned.Version).Should(Equal(batch.Version))
		Ω(cloned.SeqNum).Should(Equal(batch.SeqNum))
		Ω(cloned.Size).Should(Equal(batch.Size))
		Ω(cloned.BatchID).Should(Equal(batch.BatchID))
		Ω(cloned.Columns).Should(BeEquivalentTo(batch.Columns))
	})

	ginkgo.It("BuildIndex should work", func() {
		tableSchema := &memCom.TableSchema{
			Schema: metaCom.Table{
				Name:                 "test",
				IsFactTable:          true,
				ArchivingSortColumns: []int{1, 2},
				PrimaryKeyColumns:    []int{1, 3},
				Columns: []metaCom.Column{
					{Deleted: false}, // event time col.
					{Deleted: false}, // sort col 1. pk col 1
					{Deleted: false}, // sort col 2,
					{Deleted: false}, // pk col2
				},
			},
			PrimaryKeyBytes:       2,
			PrimaryKeyColumnTypes: []memCom.DataType{memCom.Uint32, memCom.Uint8, memCom.Uint8, memCom.Uint8},
		}

		batch, err := getFactory().ReadArchiveBatch("backfill/buildIndex")
		Ω(err).Should(BeNil())
		archiveBatch := &ArchiveBatch{
			Size:  3,
			Batch: *batch,
			Shard: &TableShard{
				Schema: tableSchema,
			},
		}

		pk := memCom.NewPrimaryKey(2, false, 0, hostMemoryManager)
		err = archiveBatch.BuildIndex(tableSchema.Schema.ArchivingSortColumns, tableSchema.Schema.PrimaryKeyColumns, pk)
		row0, err := hex.DecodeString("0000")
		recordID, existing := pk.Find(row0)
		Ω(existing).Should(BeTrue())
		Ω(recordID).Should(Equal(memCom.RecordID{Index: 0}))
		row1, _ := hex.DecodeString("0001")
		recordID, existing = pk.Find(row1)
		Ω(existing).Should(BeTrue())
		Ω(recordID).Should(Equal(memCom.RecordID{Index: 1}))
		row2, _ := hex.DecodeString("0102")
		recordID, existing = pk.Find(row2)
		Ω(existing).Should(BeTrue())
		Ω(recordID).Should(Equal(memCom.RecordID{Index: 2}))
		notExistingRow, _ := hex.DecodeString("0004")
		recordID, existing = pk.Find(notExistingRow)
		Ω(existing).Should(BeFalse())
	})

	ginkgo.It("CopyOnWrite should work", func() {
		tableSchema := &memCom.TableSchema{
			Schema: metaCom.Table{
				Name:                 "test",
				IsFactTable:          true,
				ArchivingSortColumns: []int{1},
				Columns: []metaCom.Column{
					{Deleted: false}, // event time col, all values valid.
					{Deleted: false}, // sort col 1.
					{Deleted: false}, // all values null.
					{Deleted: false}, // has null vector.
				},
			},
			PrimaryKeyColumnTypes: []memCom.DataType{memCom.Uint32, memCom.Uint32, memCom.Uint32, memCom.Uint32},
			DefaultValues:         []*memCom.DataValue{&memCom.NullDataValue, &memCom.NullDataValue, &memCom.NullDataValue, &memCom.NullDataValue},
		}

		batch, err := getFactory().ReadArchiveBatch("backfill/cloneVPForWrite")
		Ω(err).Should(BeNil())
		archiveBatch := &ArchiveBatch{
			Size:  3,
			Batch: *batch,
			Shard: &TableShard{
				Schema: tableSchema,
			},
		}

		clonedVP := archiveBatch.Columns[0].(memCom.ArchiveVectorParty).CopyOnWrite(archiveBatch.Size)
		Ω(clonedVP.(*archiveVectorParty).nulls).ShouldNot(BeNil())
		Ω(clonedVP.(*archiveVectorParty).nonDefaultValueCount).Should(BeEquivalentTo(3))
		Ω(*(*[3]uint32)(unsafe.Pointer(clonedVP.(*archiveVectorParty).values.buffer))).Should(BeEquivalentTo([3]uint32{0, 1, 2}))
		Ω(*(*uint8)(unsafe.Pointer(clonedVP.(*archiveVectorParty).nulls.buffer))).Should(BeEquivalentTo(0xFF))

		// we cannot clone sorted column.
		Ω(func() { archiveBatch.Columns[1].(memCom.ArchiveVectorParty).CopyOnWrite(archiveBatch.Size) }).Should(Panic())

		clonedVP = archiveBatch.Columns[2].(memCom.ArchiveVectorParty).CopyOnWrite(archiveBatch.Size)
		Ω(clonedVP.(*archiveVectorParty).values).ShouldNot(BeNil())
		Ω(clonedVP.(*archiveVectorParty).nulls).ShouldNot(BeNil())
		Ω(clonedVP.(*archiveVectorParty).nonDefaultValueCount).Should(BeEquivalentTo(0))
		Ω(*(*[3]uint32)(clonedVP.(*archiveVectorParty).values.Buffer())).Should(BeEquivalentTo([3]uint32{0, 0, 0}))
		Ω(*(*uint8)(clonedVP.(*archiveVectorParty).nulls.Buffer())).Should(BeEquivalentTo(0x0))

		clonedVP = archiveBatch.Columns[3].(memCom.ArchiveVectorParty).CopyOnWrite(archiveBatch.Size)
		Ω(clonedVP.(*archiveVectorParty).values).ShouldNot(BeNil())
		Ω(clonedVP.(*archiveVectorParty).nulls).ShouldNot(BeNil())
		Ω(*(*[3]uint32)(unsafe.Pointer(clonedVP.(*archiveVectorParty).values.buffer))).Should(BeEquivalentTo([3]uint32{0, 0, 2}))
		Ω(*(*uint8)(unsafe.Pointer(clonedVP.(*archiveVectorParty).nulls.buffer))).Should(BeEquivalentTo(5))
		Ω(clonedVP.(*archiveVectorParty).nonDefaultValueCount).Should(BeEquivalentTo(2))
	})

	ginkgo.It("UnpinVectorParties should work", func() {
		tableSchema := &memCom.TableSchema{
			Schema: metaCom.Table{
				Name:                 "test",
				IsFactTable:          true,
				ArchivingSortColumns: []int{1},
				Columns: []metaCom.Column{
					{Deleted: false}, // event time col, all values valid.
					{Deleted: false}, // sort col 1.
					{Deleted: false}, // all values null.
					{Deleted: false}, // has null vector.
				},
			},
			PrimaryKeyColumnTypes: []memCom.DataType{memCom.Uint32, memCom.Uint32, memCom.Uint32, memCom.Uint32},
		}

		batch, err := getFactory().ReadArchiveBatch("backfill/cloneVPForWrite")
		Ω(err).Should(BeNil())
		archiveBatch := &ArchiveBatch{
			Size:  3,
			Batch: *batch,
			Shard: &TableShard{
				Schema: tableSchema,
			},
		}

		var requestedVPs []memCom.ArchiveVectorParty
		for columnID := 0; columnID < 4; columnID++ {
			requestedVP := archiveBatch.RequestVectorParty(columnID)
			requestedVP.WaitForDiskLoad()
			requestedVPs = append(requestedVPs, requestedVP)
		}

		for columnID := 0; columnID < 4; columnID++ {
			Ω(requestedVPs[columnID].(*archiveVectorParty).pins).Should(Equal(1))
		}

		UnpinVectorParties(requestedVPs)

		for columnID := 0; columnID < 4; columnID++ {
			Ω(requestedVPs[columnID].(*archiveVectorParty).pins).Should(Equal(0))
		}
	})
})
