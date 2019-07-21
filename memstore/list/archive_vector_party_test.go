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

package list

import (
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber/aresdb/memstore"
	"github.com/uber/aresdb/memstore/common"
	"sync"
)

var _ = ginkgo.Describe("list vector party tests", func() {
	var expectedUint32LiveStoreVP, expectedBoolLiveStoreVP common.VectorParty
	var uint32ArchiveTotalBytes, boolArchiveTotalBytes int64

	ginkgo.BeforeEach(func() {
		var err error
		expectedUint32LiveStoreVP, err = GetFactory().ReadListVectorParty("live_vp_uint32")
		Ω(err).Should(BeNil())
		Ω(expectedUint32LiveStoreVP).ShouldNot(BeNil())
		uint32ArchiveTotalBytes = getCompactedTotalBytes(expectedUint32LiveStoreVP.(common.LiveVectorParty))

		expectedBoolLiveStoreVP, err = GetFactory().ReadListVectorParty("live_vp_bool")
		Ω(err).Should(BeNil())
		Ω(expectedBoolLiveStoreVP).ShouldNot(BeNil())
		boolArchiveTotalBytes = getCompactedTotalBytes(expectedBoolLiveStoreVP.(common.LiveVectorParty))
	})

	ginkgo.AfterEach(func() {
		expectedUint32LiveStoreVP.SafeDestruct()
		expectedBoolLiveStoreVP.SafeDestruct()
	})

	ginkgo.It("archiving list vector: test basics for uint32 type", func() {
		// Test basics
		listVP := NewArchiveVectorParty(4, common.Uint32, uint32ArchiveTotalBytes, &sync.RWMutex{})
		Ω(listVP.GetLength()).Should(Equal(4))
		Ω(listVP.GetBytes()).Should(BeZero())
		Ω(listVP.GetCount(1)).Should(BeEquivalentTo(1))
		Ω(listVP.GetDataType()).Should(Equal(common.Uint32))
		Ω(func() { listVP.SetCount(0, 0) }).Should(Panic())
		Ω(func() { listVP.Prune() }).ShouldNot(Panic())
		Ω(func() { listVP.CopyOnWrite(0) }).Should(Panic())
		Ω(func() { listVP.SetDataValue(0, common.NullDataValue, memstore.IgnoreCount) }).Should(Panic())
		Ω(func() { listVP.SliceByValue(0, 0, nil) }).Should(Panic())

		lowerBoundRow := 0
		upperBoundRow := 4

		lowerBoundRowSliced, upperBoundRowSliced := listVP.SliceIndex(lowerBoundRow, upperBoundRow)
		Ω(lowerBoundRowSliced).Should(Equal(lowerBoundRow))
		Ω(upperBoundRowSliced).Should(Equal(upperBoundRowSliced))
		Ω(listVP.GetNonDefaultValueCount()).Should(Equal(listVP.GetLength()))
		for i := 0; i < listVP.GetLength(); i++ {
			Ω(listVP.GetValidity(i)).Should(BeFalse())
		}

		Ω(listVP.IsList()).Should(BeTrue())
		Ω(func() { listVP.AsList() }).ShouldNot(Panic())

		// Test allocation.
		listVP.Allocate(false)
		Ω(listVP.GetBytes()).Should(BeEquivalentTo(128))

		// Test read and write
		lengthToWrite := expectedUint32LiveStoreVP.GetLength()
		Ω(lengthToWrite).Should(BeEquivalentTo(listVP.GetLength()))

		expectedVP := expectedUint32LiveStoreVP.AsList()
		for i := 0; i < lengthToWrite; i++ {
			value, valid := expectedVP.GetListValue(i)
			listVP.AsList().SetListValue(i, value, valid)
		}
		Ω(listVP.GetBytes()).Should(BeEquivalentTo(128))

		Ω(listVP.Equals(expectedUint32LiveStoreVP)).Should(BeTrue())
		// Test Destroy.
		listVP.SafeDestruct()
		Ω(listVP.GetBytes()).Should(BeZero())
	})

	ginkgo.It("archiving list vector: test basics for bool type", func() {
		// Test basics
		listVP := NewArchiveVectorParty(4, common.Bool, boolArchiveTotalBytes, &sync.RWMutex{})
		Ω(listVP.GetBytes()).Should(BeZero())

		// Test allocation.
		listVP.Allocate(false)
		Ω(listVP.GetBytes()).Should(BeEquivalentTo(128))

		// Test read and write
		lengthToWrite := expectedBoolLiveStoreVP.GetLength()
		Ω(lengthToWrite).Should(BeEquivalentTo(listVP.GetLength()))

		expectedVP := expectedBoolLiveStoreVP.AsList()
		for i := 0; i < lengthToWrite; i++ {
			value, valid := expectedVP.GetListValue(i)
			listVP.AsList().SetListValue(i, value, valid)
		}

		Ω(listVP.Equals(expectedBoolLiveStoreVP)).Should(BeTrue())
		// Test Destroy.
		listVP.SafeDestruct()
		Ω(listVP.GetBytes()).Should(BeZero())
	})
})
