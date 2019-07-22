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
	"github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/utils"
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
		value, valid := listVP.GetListValue(i)
		if valid {
			reader := common.NewArrayValueReader(vp.GetDataType(), value)
			totalBytes += int64(common.CalculateListElementBytes(vp.GetDataType(), reader.GetLength()))
		}
	}
	return totalBytes
}

func createArrayUpsertBatch() (*common.UpsertBatch, error) {
	// initiate value into UpsertBatch
	builder := common.NewUpsertBatchBuilder()
	err := builder.AddColumn(1, common.Uint16)
	Ω(err).Should(BeNil())
	err = builder.AddColumn(2, common.ArrayInt32)
	Ω(err).Should(BeNil())
	err = builder.AddColumn(3, common.Int32)
	Ω(err).Should(BeNil())

	builder.AddRow()
	err = builder.SetValue(0, 0, 1)
	Ω(err).Should(BeNil())
	err = builder.SetValue(0, 1, "[\"11\",\"\",\"13\"]")
	Ω(err).Should(BeNil())
	err = builder.SetValue(0, 2, "101")
	Ω(err).Should(BeNil())

	builder.AddRow()
	err = builder.SetValue(1, 0, 2)
	Ω(err).Should(BeNil())
	err = builder.SetValue(1, 1, "[21,22,null]")
	Ω(err).Should(BeNil())
	err = builder.SetValue(1, 2, "102")
	Ω(err).Should(BeNil())

	builder.AddRow()
	err = builder.SetValue(2, 0, 3)
	Ω(err).Should(BeNil())
	err = builder.SetValue(2, 1, "null")
	Ω(err).Should(BeNil())
	err = builder.SetValue(2, 2, "103")
	Ω(err).Should(BeNil())

	builder.AddRow()
	err = builder.SetValue(3, 0, 4)
	Ω(err).Should(BeNil())
	err = builder.SetValue(3, 1, "41,42,43")
	Ω(err).Should(BeNil())
	err = builder.SetValue(3, 2, "104")
	Ω(err).Should(BeNil())

	upsertBatchBytes, err := builder.ToByteArray()
	Ω(err).Should(BeNil())

	return common.NewUpsertBatch(upsertBatchBytes)
}

var _ = ginkgo.Describe("list vector party tests", func() {
	var expectedUint32LiveStoreVP, expectedBoolLiveStoreVP common.VectorParty

	ginkgo.BeforeEach(func() {
		var err error
		expectedUint32LiveStoreVP, err = GetFactory().ReadListVectorParty("live_vp_uint32")
		Ω(err).Should(BeNil())
		Ω(expectedUint32LiveStoreVP).ShouldNot(BeNil())

		expectedBoolLiveStoreVP, err = GetFactory().ReadListVectorParty("live_vp_bool")
		Ω(err).Should(BeNil())
		Ω(expectedBoolLiveStoreVP).ShouldNot(BeNil())
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

		Ω(func() { listVP.GetMinMaxValue() }).Should(Panic())
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

	ginkgo.It("live list vectorparty read write should work", func() {
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

		//check data in vp is correct
		// row 0
		val, valid := vp.GetValue(0)
		Ω(valid).Should(BeTrue())
		reader := common.NewArrayValueReader(common.Uint32, val)
		Ω(reader.GetLength()).Should(Equal(3))
		Ω(reader.IsValid(0)).Should(BeTrue())
		Ω(*(*uint32)(reader.Get(0))).Should(Equal(uint32(11)))
		Ω(reader.IsValid(1)).Should(BeFalse())
		Ω(reader.IsValid(2)).Should(BeTrue())
		Ω(*(*uint32)(reader.Get(2))).Should(Equal(uint32(13)))

		// row 1
		val, valid = vp.GetValue(1)
		Ω(valid).Should(BeTrue())
		reader = common.NewArrayValueReader(common.Uint32, val)
		Ω(reader.GetLength()).Should(Equal(3))
		Ω(reader.IsValid(0)).Should(BeTrue())
		Ω(*(*uint32)(reader.Get(0))).Should(Equal(uint32(21)))
		Ω(reader.IsValid(1)).Should(BeTrue())
		Ω(*(*uint32)(reader.Get(1))).Should(Equal(uint32(22)))
		Ω(reader.IsValid(2)).Should(BeFalse())

		// row 2
		val, valid = vp.GetValue(2)
		Ω(valid).Should(BeFalse())

		// row 3
		val, valid = vp.GetValue(3)
		Ω(valid).Should(BeTrue())
		reader = common.NewArrayValueReader(common.Uint32, val)
		Ω(reader.GetLength()).Should(Equal(3))
		Ω(reader.IsValid(0)).Should(BeTrue())
		Ω(*(*uint32)(reader.Get(0))).Should(Equal(uint32(41)))
		Ω(reader.IsValid(1)).Should(BeTrue())
		Ω(*(*uint32)(reader.Get(1))).Should(Equal(uint32(42)))
		Ω(reader.IsValid(2)).Should(BeTrue())
		Ω(*(*uint32)(reader.Get(2))).Should(Equal(uint32(43)))

		buf := &bytes.Buffer{}
		err = vp.Write(buf)
		Ω(err).Should(BeNil())

		newVP := NewLiveVectorParty(10, common.ArrayUint32, nil)
		err = newVP.Read(buf, nil)
		Ω(err).Should(BeNil())
		Ω(newVP.Equals(vp)).Should(BeTrue())
	})
})
