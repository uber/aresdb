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
	"bytes"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
	diskMock "github.com/uber/aresdb/diskstore/mocks"
	"github.com/uber/aresdb/memstore/common"
	commMock "github.com/uber/aresdb/memstore/common/mocks"
	testingUtils "github.com/uber/aresdb/testing"
	"sync"
	"unsafe"
)

func createArchiveVP() common.ArchiveVectorParty {
	upsertBatch, err := createArrayUpsertBatch()
	Ω(err).Should(BeNil())

	var totalBytes int64
	for i := 0; i < upsertBatch.NumRows; i++ {
		val, valid, _ := upsertBatch.GetValue(i, 1)
		if valid {
			reader := common.NewArrayValueReader(common.ArrayUint32, val)
			totalBytes += int64(reader.GetBytes())
		}
	}
	// store into vp
	vp := NewArchiveVectorParty(4, common.ArrayUint32, totalBytes, &sync.RWMutex{})
	vp.Allocate(false)
	for i := 0; i < upsertBatch.NumRows; i++ {
		val, valid, err := upsertBatch.GetValue(i, 1)
		Ω(err).Should(BeNil())
		vp.SetDataValue(i, common.DataValue{
			Valid:    valid,
			OtherVal: val,
			DataType: common.Uint32,
		}, common.IgnoreCount)
	}
	return vp
}

var _ = ginkgo.Describe("list vector party tests", func() {
	var expectedUint32LiveStoreVP, expectedBoolLiveStoreVP common.VectorParty
	var uint32ArchiveTotalBytes, boolArchiveTotalBytes int64

	ginkgo.BeforeEach(func() {
		var err error
		expectedUint32LiveStoreVP, err = GetFactory().ReadLiveVectorParty("list/live_vp_uint32")
		Ω(err).Should(BeNil())
		Ω(expectedUint32LiveStoreVP).ShouldNot(BeNil())
		uint32ArchiveTotalBytes = getCompactedTotalBytes(expectedUint32LiveStoreVP.(common.LiveVectorParty))

		expectedBoolLiveStoreVP, err = GetFactory().ReadLiveVectorParty("list/live_vp_bool")
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
		listVP := NewArchiveVectorParty(4, common.ArrayUint32, uint32ArchiveTotalBytes, &sync.RWMutex{})
		Ω(listVP.GetLength()).Should(Equal(4))
		Ω(listVP.GetBytes()).Should(BeZero())
		Ω(listVP.GetCount(1)).Should(BeEquivalentTo(1))
		Ω(listVP.GetDataType()).Should(Equal(common.ArrayUint32))
		Ω(func() { listVP.SetCount(0, 0) }).Should(Panic())
		Ω(func() { listVP.Prune() }).ShouldNot(Panic())
		Ω(func() { listVP.CopyOnWrite(0) }).ShouldNot(Panic())
		Ω(func() { listVP.SetDataValue(0, common.NullDataValue, common.IgnoreCount) }).Should(Panic())
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
		listVP := NewArchiveVectorParty(4, common.ArrayBool, boolArchiveTotalBytes, &sync.RWMutex{})
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

	ginkgo.It("archive list vectorparty read write should work", func() {
		vp := createArchiveVP()

		//check data in vp is correct
		// row 0
		val := vp.GetDataValue(0)
		Ω(val.Valid).Should(BeTrue())
		reader := common.NewArrayValueReader(common.Uint32, val.OtherVal)
		Ω(reader.GetLength()).Should(Equal(3))
		Ω(reader.IsItemValid(0)).Should(BeTrue())
		Ω(*(*uint32)(reader.Get(0))).Should(Equal(uint32(11)))
		Ω(reader.IsItemValid(1)).Should(BeFalse())
		Ω(reader.IsItemValid(2)).Should(BeTrue())
		Ω(*(*uint32)(reader.Get(2))).Should(Equal(uint32(13)))

		// row 1
		val = vp.GetDataValue(1)
		Ω(val.Valid).Should(BeTrue())
		reader = common.NewArrayValueReader(common.Uint32, val.OtherVal)
		Ω(reader.GetLength()).Should(Equal(3))
		Ω(reader.IsItemValid(0)).Should(BeTrue())
		Ω(*(*uint32)(reader.Get(0))).Should(Equal(uint32(21)))
		Ω(reader.IsItemValid(1)).Should(BeTrue())
		Ω(*(*uint32)(reader.Get(1))).Should(Equal(uint32(22)))
		Ω(reader.IsItemValid(2)).Should(BeFalse())

		// row 2
		val = vp.GetDataValue(2)
		Ω(val.Valid).Should(BeFalse())

		// row 3
		val = vp.GetDataValue(3)
		Ω(val.Valid).Should(BeTrue())
		reader = common.NewArrayValueReader(common.Uint32, val.OtherVal)
		Ω(reader.GetLength()).Should(Equal(3))
		Ω(reader.IsItemValid(0)).Should(BeTrue())
		Ω(*(*uint32)(reader.Get(0))).Should(Equal(uint32(41)))
		Ω(reader.IsItemValid(1)).Should(BeTrue())
		Ω(*(*uint32)(reader.Get(1))).Should(Equal(uint32(42)))
		Ω(reader.IsItemValid(2)).Should(BeTrue())
		Ω(*(*uint32)(reader.Get(2))).Should(Equal(uint32(43)))

		// save to buffer
		buf := &bytes.Buffer{}
		err := vp.Write(buf)
		Ω(err).Should(BeNil())

		// read from buffer
		newVP := NewArchiveVectorParty(4, common.ArrayUint32, 0, &sync.RWMutex{})
		err = newVP.Read(buf, nil)
		Ω(err).Should(BeNil())
		Ω(newVP.Equals(vp)).Should(BeTrue())
	})

	ginkgo.It("mix list archive/live vp read/write should work", func() {
		upsertBatch, err := createArrayUpsertBatch()
		Ω(err).Should(BeNil())

		// store into vp
		vp := NewLiveVectorParty(10, common.ArrayUint32, nil)
		vp.Allocate(false)
		for i := 0; i < upsertBatch.NumRows; i++ {
			val, valid, err := upsertBatch.GetValue(i, 1)
			Ω(err).Should(BeNil())
			vp.SetValue(i, val, valid)
		}

		// save to buffer
		buf := &bytes.Buffer{}
		err = vp.Write(buf)
		Ω(err).Should(BeNil())

		// read from buffer
		newVP := NewArchiveVectorParty(4, common.ArrayUint32, 0, &sync.RWMutex{})
		err = newVP.Read(buf, nil)
		Ω(err).Should(BeNil())
		Ω(newVP.Equals(vp)).Should(BeTrue())
	})

	ginkgo.It("LoadFromDisk should work for Array Archive VP", func() {
		vp1 := createArchiveVP()
		table := "test"
		buf := &testingUtils.TestReadWriteCloser{}

		hostMemoryManager := &commMock.HostMemoryManager{}
		hostMemoryManager.On("ReportManagedObject", mock.Anything, mock.Anything,
			mock.Anything, mock.Anything, mock.Anything).Return()

		diskStore := &diskMock.DiskStore{}
		diskStore.On("OpenVectorPartyFileForWrite", table, 1, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(buf, nil).Once()
		diskStore.On("OpenVectorPartyFileForRead", table, 1, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(buf, nil).Once()

		serializer := common.NewVectorPartyArchiveSerializer(hostMemoryManager, diskStore, table, 0, 1, 1, 1, 0)
		err := serializer.WriteVectorParty(vp1)
		Ω(err).Should(BeNil())

		vp2 := NewArchiveVectorParty(4, common.ArrayUint32, 0, &sync.RWMutex{})
		vp2.LoadFromDisk(hostMemoryManager, diskStore, table, 0, 1, 1, 1, 0)
		vp2.WaitForDiskLoad()
		Ω(vp2.Equals(vp1)).Should(BeTrue())
	})

	ginkgo.It("GetHostVectorPartySlice should work for Array Archive VP", func() {
		vp := createArchiveVP().(*ArchiveVectorParty)
		expectedHostSlice := common.HostVectorPartySlice{
			Values:            vp.values.Buffer(),
			Length:            vp.length,
			ValueType:         vp.dataType,
			ValueBytes:        128,
			Offsets:           vp.offsets.Buffer(),
			ValueOffsetAdjust: 0,
		}
		hostSlice := vp.GetHostVectorPartySlice(0, vp.length)
		Ω(hostSlice).Should(Equal(expectedHostSlice))

		newValue := uintptr(vp.values.Buffer()) + 24
		newOffset := uintptr(vp.offsets.Buffer()) + 8
		expectedHostSlice = common.HostVectorPartySlice{
			Values:            unsafe.Pointer(newValue),
			Length:            2,
			ValueType:         vp.dataType,
			ValueBytes:        24,
			Offsets:           unsafe.Pointer(newOffset),
			ValueOffsetAdjust: 24,
		}
		hostSlice = vp.GetHostVectorPartySlice(1, vp.length-2)
		Ω(hostSlice).Should(Equal(expectedHostSlice))
	})

	ginkgo.It("Array archive vector party should work in special cases", func() {
		// set data
		vp := NewArchiveVectorParty(10, common.ArrayInt16, 1024, &sync.RWMutex{})
		vp.Allocate(false)
		val := common.DataValue{
			DataType: common.ArrayInt16,
			OtherVal: nil,
			Valid: false,
		}
		vp.SetDataValue(0, val, common.IgnoreCount)

		var data uint32
		val = common.DataValue{
			DataType: common.ArrayInt16,
			OtherVal: unsafe.Pointer(&data),
			Valid: true,
		}
		vp.SetDataValue(1, val, common.IgnoreCount)
		// get data
		val = vp.GetDataValue(0)
		Ω(val.Valid).Should(BeFalse())

		val = vp.GetDataValue(1)
		Ω(val.Valid).Should(BeTrue())
		Ω(uintptr(val.OtherVal)).Should(Equal(uintptr(0)))

		// update same row with different length
		data = 2
		val = common.DataValue{
			DataType: common.ArrayInt16,
			OtherVal: unsafe.Pointer(&data),
			Valid: true,
		}
		Ω(func() {vp.SetDataValue(1, val, common.IgnoreCount)}).Should(Panic())
	})
})
