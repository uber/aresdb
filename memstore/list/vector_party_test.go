package list

import (
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber/aresdb/memstore"
	"github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/utils"
	"sync"
)

// getCompactedTotalBytes returns the total bytes if we store the list values continuously without holes (but with
// paddings).
func getCompactedTotalBytes(vp common.LiveVectorParty) int64 {
	if !vp.IsList() {
		utils.GetLogger().Panic("Expect a list live vp")
	}

	listVP := vp.AsList()
	var totalBytes int64
	for i := 0; i < vp.GetLength(); i++ {
		totalBytes += int64(common.CalculateListElementBytes(vp.GetDataType(), listVP.GetElementLength(i)))
	}
	return totalBytes
}

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

	ginkgo.It("live list vector: test basics for uint32 type", func() {
		// Test basics
		listVP := NewLiveVectorParty(4, common.Uint32, nil)
		Ω(listVP.GetLength()).Should(Equal(4))
		Ω(listVP.GetBytes()).Should(BeZero())
		Ω(listVP.GetDataType()).Should(Equal(common.Uint32))
		Ω(listVP.GetDataValue(0)).Should(Equal(common.NullDataValue))
		Ω(listVP.GetDataValueByRow(0)).Should(Equal(common.NullDataValue))
		Ω(func() { listVP.GetMinMaxValue() }).Should(Panic())
		Ω(listVP.GetNonDefaultValueCount()).Should(Equal(listVP.GetLength()))
		for i := 0; i < listVP.GetLength(); i++ {
			Ω(listVP.GetValidity(i)).Should(BeTrue())
		}

		Ω(func() { listVP.GetValue(0) }).Should(Panic())
		Ω(listVP.IsList()).Should(BeTrue())
		Ω(func() { listVP.AsList() }).ShouldNot(Panic())

		// Test allocation.
		listVP.Allocate(false)
		Ω(listVP.GetBytes()).Should(BeEquivalentTo(128))

		// Test read and write
		lengthToWrite := expectedUint32LiveStoreVP.GetLength()
		Ω(lengthToWrite).Should(BeEquivalentTo(listVP.GetLength()))

		for i := 0; i < lengthToWrite; i++ {
			listVP.AsList().SetListValue(i, expectedUint32LiveStoreVP.AsList())
		}
		Ω(listVP.GetBytes()).Should(BeEquivalentTo(nativeChunkSize + 128))

		Ω(listVP.Equals(expectedUint32LiveStoreVP)).Should(BeTrue())
		// Test Destroy.
		listVP.SafeDestruct()
		Ω(listVP.GetBytes()).Should(BeZero())
	})

	ginkgo.It("live list vector: test basics for bool type", func() {
		// Test basics
		listVP := NewLiveVectorParty(4, common.Bool, nil)
		Ω(listVP.GetBytes()).Should(BeZero())

		// Test allocation.
		listVP.Allocate(false)
		Ω(listVP.GetBytes()).Should(BeEquivalentTo(128))

		// Test read and write
		lengthToWrite := expectedBoolLiveStoreVP.GetLength()
		Ω(lengthToWrite).Should(BeEquivalentTo(listVP.GetLength()))

		for i := 0; i < lengthToWrite; i++ {
			listVP.AsList().SetListValue(i, expectedBoolLiveStoreVP.AsList())
		}

		Ω(listVP.Equals(expectedBoolLiveStoreVP)).Should(BeTrue())
		// Test Destroy.
		listVP.SafeDestruct()
		Ω(listVP.GetBytes()).Should(BeZero())
	})

	ginkgo.It("archiving list vector: test basics for uint32 type", func() {
		// Test basics
		listVP := NewArchiveVectorParty(4, common.Uint32, uint32ArchiveTotalBytes, &sync.RWMutex{})
		Ω(listVP.GetLength()).Should(Equal(4))
		Ω(listVP.GetBytes()).Should(BeZero())
		Ω(listVP.GetCount(1)).Should(BeEquivalentTo(1))
		Ω(listVP.GetDataType()).Should(Equal(common.Uint32))
		Ω(listVP.GetDataValue(0)).Should(Equal(common.NullDataValue))
		Ω(listVP.GetDataValueByRow(0)).Should(Equal(common.NullDataValue))
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
			Ω(listVP.GetValidity(i)).Should(BeTrue())
		}

		Ω(listVP.IsList()).Should(BeTrue())
		Ω(func() { listVP.AsList() }).ShouldNot(Panic())

		// Test allocation.
		listVP.Allocate(false)
		Ω(listVP.GetBytes()).Should(BeEquivalentTo(128))

		// Test read and write
		lengthToWrite := expectedUint32LiveStoreVP.GetLength()
		Ω(lengthToWrite).Should(BeEquivalentTo(listVP.GetLength()))

		for i := 0; i < lengthToWrite; i++ {
			listVP.AsList().SetListValue(i, expectedUint32LiveStoreVP.AsList())
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

		for i := 0; i < lengthToWrite; i++ {
			listVP.AsList().SetListValue(i, expectedBoolLiveStoreVP.AsList())
		}

		Ω(listVP.Equals(expectedBoolLiveStoreVP)).Should(BeTrue())
		// Test Destroy.
		listVP.SafeDestruct()
		Ω(listVP.GetBytes()).Should(BeZero())
	})
})
