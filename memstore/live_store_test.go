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
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber/aresdb/memstore/common"
)

var _ = ginkgo.Describe("live store", func() {
	m := getFactory().NewMockMemStore()
	mockDiskStore := CreateMockDiskStore()
	hostMemoryManager := NewHostMemoryManager(m, 1<<32)
	ginkgo.It("provides batches for reads and writes", func() {
		shard := &TableShard{
			Schema: &TableSchema{
				ValueTypeByColumn: []common.DataType{common.Uint32, common.Uint16, common.Uint8},
				DefaultValues:     []*common.DataValue{&common.NullDataValue, &common.NullDataValue, &common.NullDataValue},
			},
			diskStore:         mockDiskStore,
			HostMemoryManager: hostMemoryManager,
		}
		vs := NewLiveStore(0, shard)

		Ω(vs.LastReadRecord.BatchID).Should(Equal(BaseBatchID))
		Ω(vs.LastReadRecord.Index).Should(Equal(uint32(0)))

		b := vs.GetBatchForRead(BaseBatchID)
		Ω(b).Should(BeNil())

		b = vs.appendBatch(BaseBatchID)
		Ω(b.Capacity).Should(Equal(0))

		b = vs.GetBatchForRead(BaseBatchID)
		Ω(b).ShouldNot(BeNil())
		Ω(b.Capacity).Should(Equal(0))
		b.RUnlock()

		b = vs.GetBatchForRead(BaseBatchID + 1)
		Ω(b).Should(BeNil())
	})

	ginkgo.It("provides live batches for reads and appends", func() {
		shard := &TableShard{
			Schema: &TableSchema{
				ValueTypeByColumn: []common.DataType{common.Uint32, common.Uint16, common.Uint8},
				DefaultValues:     []*common.DataValue{&common.NullDataValue, &common.NullDataValue, &common.NullDataValue},
			},
			diskStore:         mockDiskStore,
			HostMemoryManager: hostMemoryManager,
		}
		vs := NewLiveStore(16, shard)

		vs.appendBatch(BaseBatchID)
		b := vs.GetBatchForRead(BaseBatchID)
		b.RUnlock()
		Ω(b).ShouldNot(BeNil())
		Ω(b.Capacity).Should(Equal(16))

		b = vs.appendBatch(BaseBatchID + 1)
		Ω(b.Capacity).Should(Equal(16))

		b = vs.GetBatchForRead(BaseBatchID + 1)
		b.RUnlock()
		Ω(b.Capacity).Should(Equal(16))
		b.GetOrCreateVectorParty(0, false)

		b = vs.appendBatch(BaseBatchID + 2)
		Ω(b.Capacity).Should(Equal(16))

		b = vs.GetBatchForRead(BaseBatchID + 2)
		b.RUnlock()
		Ω(b.Capacity).Should(Equal(16))

		vs.PurgeBatch(BaseBatchID + 3)
		vs.PurgeBatch(BaseBatchID + 2)
		vs.PurgeBatch(BaseBatchID + 1)

		b = vs.GetBatchForRead(BaseBatchID)
		b.RUnlock()
		Ω(b.Capacity).Should(Equal(16))

		b = vs.GetBatchForRead(BaseBatchID + 1)
		Ω(b).Should(BeNil())
	})

	ginkgo.It("provides live vectorparty for reads and writes", func() {
		shard := &TableShard{
			Schema: &TableSchema{
				ValueTypeByColumn: []common.DataType{common.Uint32, common.Uint16, common.Uint8, common.Uint8, common.Uint8, common.Bool},
				DefaultValues: []*common.DataValue{&common.NullDataValue, &common.NullDataValue, &common.NullDataValue, &common.NullDataValue,
					&common.NullDataValue, &common.NullDataValue},
			},
			diskStore:         mockDiskStore,
			HostMemoryManager: hostMemoryManager,
		}
		vs := NewLiveStore(0, shard)

		vs.appendBatch(BaseBatchID)
		b := vs.GetBatchForRead(BaseBatchID)
		b.RUnlock()
		liveVP := b.GetOrCreateVectorParty(1, false)
		Ω(liveVP.(*cLiveVectorParty).values).ShouldNot(BeNil())
		Ω(liveVP.(*cLiveVectorParty).nulls).ShouldNot(BeNil())
		Ω(liveVP.(*cLiveVectorParty).counts).Should(BeNil())

		liveVP = b.GetOrCreateVectorParty(4, false)
		Ω(liveVP.(*cLiveVectorParty).values).ShouldNot(BeNil())
		Ω(liveVP.(*cLiveVectorParty).nulls).ShouldNot(BeNil())
		Ω(liveVP.(*cLiveVectorParty).counts).Should(BeNil())

		b = vs.GetBatchForRead(BaseBatchID)
		vp := b.GetVectorParty(0)
		Ω(vp).Should(BeNil())
		vp = b.GetVectorParty(1)
		Ω(vp).ShouldNot(BeNil())
		vp = b.GetVectorParty(2)
		Ω(vp).Should(BeNil())
		vp = b.GetVectorParty(3)
		Ω(vp).Should(BeNil())
		vp = b.GetVectorParty(4)
		Ω(vp).ShouldNot(BeNil())
		vp = b.GetVectorParty(5)
		Ω(vp).Should(BeNil())
		b.RUnlock()

		vs.PurgeBatch(BaseBatchID)
	})

	ginkgo.It("get batch ids skips batches that are beyond last read batch", func() {
		shard := &TableShard{
			Schema: &TableSchema{
				ValueTypeByColumn: []common.DataType{common.Uint32, common.Uint16, common.Uint8, common.Uint8, common.Uint8, common.Bool},
				DefaultValues:     []*common.DataValue{&common.NullDataValue, &common.NullDataValue, &common.NullDataValue, &common.NullDataValue, &common.NullDataValue, &common.NullDataValue},
			},
			diskStore:         mockDiskStore,
			HostMemoryManager: hostMemoryManager,
		}
		vs := NewLiveStore(16, shard)

		vs.appendBatch(BaseBatchID)
		vs.appendBatch(BaseBatchID + 1)
		vs.appendBatch(BaseBatchID + 2)

		vs.LastReadRecord.Index = 1
		batchIDs, numRecordsInLastBatch := vs.GetBatchIDs()
		Ω(len(batchIDs)).Should(Equal(1))
		Ω(numRecordsInLastBatch).Should(Equal(1))

		vs.LastReadRecord.BatchID = BaseBatchID + 1

		batchIDs, numRecordsInLastBatch = vs.GetBatchIDs()
		Ω(len(batchIDs)).Should(Equal(2))
		Ω(numRecordsInLastBatch).Should(Equal(1))

		vs.LastReadRecord.Index = 0
		batchIDs, numRecordsInLastBatch = vs.GetBatchIDs()
		Ω(len(batchIDs)).Should(Equal(1))
		Ω(numRecordsInLastBatch).Should(Equal(16))
	})

	ginkgo.It("Looks up key in primary key", func() {
		primaryKeyDataTypes := []common.DataType{common.UUID, common.Uint32, common.Bool}
		keyBytes := 21
		pk := NewPrimaryKey(keyBytes, true, 10, hostMemoryManager)
		shard := &TableShard{
			Schema: &TableSchema{
				PrimaryKeyBytes:       keyBytes,
				PrimaryKeyColumnTypes: primaryKeyDataTypes,
			},
			diskStore:         mockDiskStore,
			HostMemoryManager: hostMemoryManager,
		}
		vs := NewLiveStore(0, shard)
		vs.PrimaryKey = pk

		key := []byte{1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1}
		recordID := RecordID{1, 1}

		pk.FindOrInsert(key, recordID, 1)
		r, found := vs.LookupKey([]string{"01000000000000000100000000000000", "1", "true"})
		Ω(found).Should(BeTrue())
		Ω(r).Should(Equal(recordID))
		pk.Destruct()
	})

	ginkgo.It("get or create batch for snapshot", func() {
		shard := &TableShard{
			Schema: &TableSchema{
				ValueTypeByColumn: []common.DataType{common.Uint32, common.Uint16, common.Uint8, common.Uint8, common.Uint8, common.Bool},
				DefaultValues: []*common.DataValue{&common.NullDataValue, &common.NullDataValue, &common.NullDataValue, &common.NullDataValue,
					&common.NullDataValue, &common.NullDataValue},
			},
			diskStore:         mockDiskStore,
			HostMemoryManager: hostMemoryManager,
		}
		vs := NewLiveStore(0, shard)
		liveBatch1 := vs.getOrCreateBatch(-1)
		Ω(liveBatch1).ShouldNot(BeNil())
		liveBatch1.Unlock()

		liveBatch2 := vs.getOrCreateBatch(0)
		Ω(liveBatch2).ShouldNot(BeNil())
		liveBatch2.Unlock()

		liveBatch3 := vs.getOrCreateBatch(-1)
		Ω(liveBatch3).ShouldNot(BeNil())
		liveBatch3.Unlock()
		Ω(liveBatch3 == liveBatch1).Should(BeTrue())
	})
})
