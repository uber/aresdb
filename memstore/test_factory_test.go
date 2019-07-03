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
	diskMocks "github.com/uber/aresdb/diskstore/mocks"
	"github.com/uber/aresdb/memstore/common"
	metaMocks "github.com/uber/aresdb/metastore/mocks"
	"sync"
)

var _ = ginkgo.Describe("test factory", func() {
	ginkgo.It("test read vector", func() {
		v, err := GetFactory().ReadVector("v0")
		Ω(err).Should(BeNil())
		Ω(v.Size).Should(BeEquivalentTo(6))

		Ω(v.GetBool(0)).Should(BeTrue())
		Ω(v.GetBool(1)).Should(BeFalse())
		// null
		Ω(v.GetBool(2)).Should(BeFalse())
		Ω(v.GetBool(3)).Should(BeFalse())
		Ω(v.GetBool(4)).Should(BeTrue())
		// null
		Ω(v.GetBool(5)).Should(BeFalse())

		_, err = GetFactory().ReadVector("not_exist")
		Ω(err).ShouldNot(BeNil())

		// invalid bool value
		_, err = GetFactory().ReadVector("invalid_bool_value")
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("test read vector party", func() {
		// test vector party
		locker := &sync.RWMutex{}
		archiveVP, err := GetFactory().ReadArchiveVectorParty("sortedVP0", locker)
		Ω(err).Should(BeNil())
		vp := archiveVP.(*archiveVectorParty)
		Ω(vp.GetLength()).Should(BeEquivalentTo(5))

		Ω(*(*uint32)(vp.values.GetValue(0))).Should(BeEquivalentTo(0))
		Ω(*(*uint32)(vp.values.GetValue(1))).Should(BeEquivalentTo(10))
		Ω(*(*uint32)(vp.values.GetValue(2))).Should(BeEquivalentTo(20))
		Ω(*(*uint32)(vp.values.GetValue(3))).Should(BeEquivalentTo(30))
		Ω(*(*uint32)(vp.values.GetValue(4))).Should(BeEquivalentTo(40))

		// test vector party with counts
		// test vector party
		archiveVP, err = GetFactory().ReadArchiveVectorParty("mergedVP1", locker)
		Ω(err).Should(BeNil())
		vp = archiveVP.(*archiveVectorParty)
		Ω(vp.GetLength()).Should(BeEquivalentTo(3))
		Ω(vp.nonDefaultValueCount).Should(BeEquivalentTo(6))

		archiveVP, err = GetFactory().ReadArchiveVectorParty("sortedVP1", locker)
		Ω(err).Should(BeNil())
		vp = archiveVP.(*archiveVectorParty)
		Ω(vp.GetLength()).Should(BeEquivalentTo(3))
		Ω(vp.nonDefaultValueCount).Should(BeEquivalentTo(2))

		// values check
		Ω(vp.nulls.GetBool(0)).Should(BeEquivalentTo(false))
		Ω(vp.values.GetBool(1)).Should(BeEquivalentTo(false))
		Ω(vp.values.GetBool(2)).Should(BeEquivalentTo(true))

		// counts check
		Ω(*(*uint32)(vp.counts.GetValue(0))).Should(BeEquivalentTo(0))
		Ω(*(*uint32)(vp.counts.GetValue(1))).Should(BeEquivalentTo(3))
		Ω(*(*uint32)(vp.counts.GetValue(2))).Should(BeEquivalentTo(4))
		Ω(*(*uint32)(vp.counts.GetValue(3))).Should(BeEquivalentTo(5))

		// invalid value length (counts length is not equal to values length+1)
		_, err = GetFactory().ReadArchiveVectorParty("invalid_value_length", locker)
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("test read batch", func() {
		batch, err := GetFactory().ReadArchiveBatch("archiveBatch")
		Ω(err).Should(BeNil())
		Ω(len(batch.Columns)).Should(BeEquivalentTo(6))
		Ω(batch.Columns[0].(*archiveVectorParty).length).Should(BeEquivalentTo(5))
		Ω(batch.Columns[0].(*archiveVectorParty).counts).Should(BeNil())
		Ω(batch.Columns[1].(*archiveVectorParty).length).Should(BeEquivalentTo(3))
		Ω(batch.Columns[1].(*archiveVectorParty).counts).ShouldNot(BeNil())
		Ω(batch.Columns[2].(*archiveVectorParty).length).Should(BeEquivalentTo(5))
		Ω(batch.Columns[2].(*archiveVectorParty).counts).ShouldNot(BeNil())
	})

	ginkgo.It("test new mock memStoreImpl", func() {
		m := GetFactory().NewMockMemStore()
		Ω(m).ShouldNot(BeNil())
		Ω(m.diskStore).Should(BeAssignableToTypeOf(new(diskMocks.DiskStore)))
		Ω(m.metaStore).Should(BeAssignableToTypeOf(new(metaMocks.MetaStore)))
	})

	ginkgo.It("test read upsert batch", func() {
		ub, err := GetFactory().ReadUpsertBatch("testReadUpsertBatch")
		Ω(err).Should(BeNil())
		Ω(ub).ShouldNot(BeNil())
		Ω(ub.NumColumns).Should(Equal(2))
		Ω(ub.NumRows).Should(Equal(3))
		Ω(ub.GetColumnType(0)).Should(Equal(common.Uint16))
		Ω(ub.GetColumnID(0)).Should(Equal(2))
		Ω(ub.GetColumnType(1)).Should(Equal(common.Bool))
		Ω(ub.GetColumnID(1)).Should(Equal(1))

		val, err := ub.GetDataValue(0, 0)
		Ω(err).Should(BeNil())
		Ω(val.Valid).Should(BeTrue())
		Ω(*(*uint16)(val.OtherVal)).Should(BeEquivalentTo(16))

		val, err = ub.GetDataValue(0, 1)
		Ω(err).Should(BeNil())
		Ω(val.Valid).Should(BeTrue())
		Ω(val.BoolVal).Should(Equal(true))

		val, err = ub.GetDataValue(1, 0)
		Ω(err).Should(BeNil())
		Ω(val.Valid).Should(BeFalse())

		val, err = ub.GetDataValue(1, 1)
		Ω(err).Should(BeNil())
		Ω(val.Valid).Should(BeTrue())
		Ω(val.BoolVal).Should(Equal(false))

		val, err = ub.GetDataValue(2, 0)
		Ω(err).Should(BeNil())
		Ω(val.Valid).Should(BeTrue())
		Ω(*(*uint16)(val.OtherVal)).Should(BeEquivalentTo(0))

		val, err = ub.GetDataValue(2, 1)
		Ω(err).Should(BeNil())
		Ω(val.Valid).Should(BeTrue())
		Ω(val.BoolVal).Should(Equal(true))
	})
})
