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

package common

import (
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber/aresdb/utils"
	"time"
	"unsafe"
)

var _ = ginkgo.Describe("upsert batch builder", func() {

	ginkgo.BeforeEach(func() {
		utils.SetCurrentTime(time.Unix(10, 0))
	})

	ginkgo.AfterEach(func() {
		utils.ResetClockImplementation()
	})

	ginkgo.It("works for empty batch", func() {
		builder := NewUpsertBatchBuilder()
		// write with new version
		bufferNew, err := builder.ToByteArray()
		Ω(err).Should(BeNil())
		Ω(bufferNew).Should(Equal([]byte{1, 0, 237, 254, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0}))
	})

	ginkgo.It("works for empty row", func() {
		builder := NewUpsertBatchBuilder()
		builder.AddColumn(123, Uint8)
		buffer, err := builder.ToByteArray()
		Ω(err).Should(BeNil())
		Ω(buffer).Should(Equal([]byte{1, 0, 237, 254, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 51, 0, 0, 0, 51, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 2, 0, 123, 0, 0, 0, 0, 0, 0, 0}))
	})

	ginkgo.It("works for empty column", func() {
		builder := NewUpsertBatchBuilder()
		builder.AddRow()
		buffer, err := builder.ToByteArray()
		Ω(err).Should(BeNil())
		Ω(buffer).Should(Equal([]byte{1, 0, 237, 254, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0}))
	})

	ginkgo.It("works for one row, one col, no value", func() {
		builder := NewUpsertBatchBuilder()
		builder.AddRow()
		err := builder.AddColumn(123, Uint8)
		Ω(err).Should(BeNil())
		buffer, err := builder.ToByteArray()
		Ω(err).Should(BeNil())
		Ω(buffer).Should(Equal([]byte{1, 0, 237, 254, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 51, 0, 0, 0, 51, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 2, 0, 123, 0, 0, 0, 0, 0, 0, 0}))
	})

	ginkgo.It("works for one row, one col, one value", func() {
		builder := NewUpsertBatchBuilder()
		builder.AddRow()
		err := builder.AddColumn(123, Uint8)
		builder.SetValue(0, 0, uint8(135))
		buffer, err := builder.ToByteArray()
		Ω(err).Should(BeNil())
		Ω(buffer).Should(Equal([]byte{1, 0, 237, 254, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 51, 0, 0, 0, 57, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 2, 0, 123, 0, 1, 0, 0, 0, 0, 0, 135, 0, 0, 0, 0, 0, 0, 0}))
	})

	ginkgo.It("reset row works", func() {
		builder := NewUpsertBatchBuilder()
		builder.AddRow()
		err := builder.AddColumn(123, Uint8)
		builder.SetValue(0, 0, uint8(135))
		builder.ResetRows()
		buffer, err := builder.ToByteArray()
		Ω(err).Should(BeNil())
		Ω(buffer).Should(Equal([]byte{1, 0, 237, 254, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 51, 0, 0, 0, 51, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 2, 0, 123, 0, 0, 0, 0, 0, 0, 0}))
	})

	ginkgo.It("raises error when setting wrong value type", func() {
		builder := NewUpsertBatchBuilder()
		builder.AddRow()
		builder.AddColumn(123, Uint8)
		err := builder.SetValue(0, 0, "a")
		Ω(err).ShouldNot(BeNil())
		buffer, err := builder.ToByteArray()
		Ω(buffer).Should(Equal([]byte{1, 0, 237, 254, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 51, 0, 0, 0, 51, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 2, 0, 123, 0, 0, 0, 0, 0, 0, 0}))
	})

	ginkgo.It("last value wins", func() {
		builder := NewUpsertBatchBuilder()
		builder.AddRow()
		builder.AddRow()
		builder.AddColumn(123, Bool)
		err := builder.SetValue(0, 0, false)
		Ω(err).Should(BeNil())

		buffer, err := builder.ToByteArray()
		Ω(err).Should(BeNil())
		Ω(buffer).Should(Equal([]byte{1, 0, 237, 254, 2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 51, 0, 0, 0, 57, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 123, 0, 2, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}))

		buffer, err = builder.ToByteArray()
		Ω(err).Should(BeNil())
		Ω(buffer).Should(Equal([]byte{1, 0, 237, 254, 2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 51, 0, 0, 0, 57, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 123, 0, 2, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}))

		err = builder.SetValue(0, 0, nil)
		Ω(err).Should(BeNil())
		Ω(buffer).Should(Equal([]byte{1, 0, 237, 254, 2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 51, 0, 0, 0, 57, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 123, 0, 2, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}))

		buffer, err = builder.ToByteArray()
		Ω(err).Should(BeNil())
		Ω(buffer).Should(Equal([]byte{1, 0, 237, 254, 2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 51, 0, 0, 0, 51, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 123, 0, 0, 0, 0, 0, 0, 0}))
	})

	ginkgo.It("works for bool type", func() {
		builder := NewUpsertBatchBuilder()

		// All null bools.
		err := builder.AddColumn(123, Bool)
		Ω(err).Should(BeNil())

		// Half valid bools.
		err = builder.AddColumn(456, Bool)
		builder.AddRow()
		Ω(err).Should(BeNil())
		err = builder.SetValue(0, 1, true)

		// All valid bools.
		err = builder.AddColumn(789, Bool)
		builder.AddRow()
		Ω(err).Should(BeNil())
		err = builder.SetValue(0, 2, true)
		Ω(err).Should(BeNil())
		err = builder.SetValue(1, 2, false)
		Ω(err).Should(BeNil())

		buffer, err := builder.ToByteArray()
		Ω(err).Should(BeNil())
		Ω(buffer).Should(Equal([]byte{1, 0, 237, 254, 2, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 89, 0, 0, 0, 89, 0, 0, 0, 97, 0, 0, 0, 105, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 123, 0, 200, 1, 21, 3, 0, 2, 1, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0}))
	})

	ginkgo.It("works for array types", func() {
		builder := NewUpsertBatchBuilder()

		// All null bools.
		err := builder.AddColumn(1, Uint8)
		Ω(err).Should(BeNil())
		err = builder.AddColumn(2, ArrayInt16)
		Ω(err).Should(BeNil())
		err = builder.AddColumn(3, Int16)
		Ω(err).Should(BeNil())

		builder.AddRow()
		err = builder.SetValue(0, 0, 1)
		Ω(err).Should(BeNil())
		err = builder.SetValue(0, 1, "[11,12,13]")
		Ω(err).Should(BeNil())
		err = builder.SetValue(0, 2, "101")
		Ω(err).Should(BeNil())

		builder.AddRow()
		err = builder.SetValue(1, 0, 2)
		Ω(err).Should(BeNil())
		err = builder.SetValue(1, 1, "[21,22,23]")
		Ω(err).Should(BeNil())
		err = builder.SetValue(1, 2, "102")
		Ω(err).Should(BeNil())

		buffer, err := builder.ToByteArray()
		Ω(err).Should(BeNil())
		Ω(buffer).Should(Equal([]byte{1, 0, 237, 254, 2, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 89, 0, 0, 0, 98, 0, 0, 0, 144, 0, 0, 0, 148, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 2, 0, 16, 0, 3, 1, 16, 0, 3, 0, 1, 0, 2, 0, 3, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 2, 0, 0, 0, 0, 0, 0, 16, 0, 0, 0, 32, 0, 0, 0, 3, 0, 0, 0, 11, 0, 12, 0, 13, 0, 7, 0, 0, 0, 0, 0, 3, 0, 0, 0, 21, 0, 22, 0, 23, 0, 7, 0, 0, 0, 0, 0, 101, 0, 102, 0, 0, 0, 0, 0}))
	})

	ginkgo.It("UpdateOverWrite add column ex fail", func() {
		builder := NewUpsertBatchBuilder()
		err := builder.AddColumn(1, Uint8)
		Ω(err).Should(BeNil())
		err = builder.AddColumnWithUpdateMode(2, Uint8, UpdateWithAddition)
		Ω(err).Should(BeNil())
		err = builder.AddColumnWithUpdateMode(2, Uint8, MaxColumnUpdateMode)
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("AdditionUpdate should work", func() {
		// big enough to hold all numeric types
		var oldValue int64
		var newValue int64
		*(*int8)(unsafe.Pointer(&oldValue)) = -1
		*(*int8)(unsafe.Pointer(&newValue)) = 1
		AdditionUpdate(unsafe.Pointer(&oldValue), unsafe.Pointer(&newValue), Int8)
		Ω(*(*int8)(unsafe.Pointer(&oldValue))).Should(Equal(int8(0)))

		*(*uint8)(unsafe.Pointer(&oldValue)) = 1
		*(*uint8)(unsafe.Pointer(&newValue)) = 1
		AdditionUpdate(unsafe.Pointer(&oldValue), unsafe.Pointer(&newValue), Uint8)
		Ω(*(*uint8)(unsafe.Pointer(&oldValue))).Should(Equal(uint8(2)))

		*(*int16)(unsafe.Pointer(&oldValue)) = -256
		*(*int16)(unsafe.Pointer(&newValue)) = 256
		AdditionUpdate(unsafe.Pointer(&oldValue), unsafe.Pointer(&newValue), Int16)
		Ω(*(*int16)(unsafe.Pointer(&oldValue))).Should(Equal(int16(0)))

		*(*uint16)(unsafe.Pointer(&oldValue)) = 1
		*(*uint16)(unsafe.Pointer(&newValue)) = 256
		AdditionUpdate(unsafe.Pointer(&oldValue), unsafe.Pointer(&newValue), Uint16)
		Ω(*(*uint16)(unsafe.Pointer(&oldValue))).Should(Equal(uint16(257)))

		*(*int32)(unsafe.Pointer(&oldValue)) = -65536
		*(*int32)(unsafe.Pointer(&newValue)) = 65536
		AdditionUpdate(unsafe.Pointer(&oldValue), unsafe.Pointer(&newValue), Int32)
		Ω(*(*int32)(unsafe.Pointer(&oldValue))).Should(Equal(int32(0)))

		*(*uint32)(unsafe.Pointer(&oldValue)) = 1
		*(*uint32)(unsafe.Pointer(&newValue)) = 65536
		AdditionUpdate(unsafe.Pointer(&oldValue), unsafe.Pointer(&newValue), Uint32)
		Ω(*(*uint32)(unsafe.Pointer(&oldValue))).Should(Equal(uint32(65537)))

		*(*float32)(unsafe.Pointer(&oldValue)) = -1.0
		*(*float32)(unsafe.Pointer(&newValue)) = 1.0
		AdditionUpdate(unsafe.Pointer(&oldValue), unsafe.Pointer(&newValue), Float32)
		Ω(*(*float32)(unsafe.Pointer(&oldValue))).Should(Equal(float32(0.0)))

		*(*int64)(unsafe.Pointer(&oldValue)) = -(1 << 31) - 1
		*(*int64)(unsafe.Pointer(&newValue)) = (1 << 31) + 1
		AdditionUpdate(unsafe.Pointer(&oldValue), unsafe.Pointer(&newValue), Int64)
		Ω(*(*int64)(unsafe.Pointer(&oldValue))).Should(Equal(int64(0)))
	})

	ginkgo.It("MinMaxUpdate should work", func() {
		// big enough to hold all numeric types
		var oldValue int64
		var newValue int64
		*(*int8)(unsafe.Pointer(&oldValue)) = -1
		*(*int8)(unsafe.Pointer(&newValue)) = 1
		MinMaxUpdate(unsafe.Pointer(&oldValue), unsafe.Pointer(&newValue), Int8, CompareInt8, 1)
		Ω(*(*int8)(unsafe.Pointer(&oldValue))).Should(Equal(int8(-1)))

		MinMaxUpdate(unsafe.Pointer(&oldValue), unsafe.Pointer(&newValue), Int8, CompareInt8, -1)
		Ω(*(*int8)(unsafe.Pointer(&oldValue))).Should(Equal(int8(1)))

		*(*uint8)(unsafe.Pointer(&oldValue)) = 0
		*(*uint8)(unsafe.Pointer(&newValue)) = 1
		MinMaxUpdate(unsafe.Pointer(&oldValue), unsafe.Pointer(&newValue), Uint8, CompareUint8, 1)
		Ω(*(*uint8)(unsafe.Pointer(&oldValue))).Should(Equal(uint8(0)))

		MinMaxUpdate(unsafe.Pointer(&oldValue), unsafe.Pointer(&newValue), Uint8, CompareUint8, -1)
		Ω(*(*uint8)(unsafe.Pointer(&oldValue))).Should(Equal(uint8(1)))

		*(*int16)(unsafe.Pointer(&oldValue)) = -256
		*(*int16)(unsafe.Pointer(&newValue)) = 256
		MinMaxUpdate(unsafe.Pointer(&oldValue), unsafe.Pointer(&newValue), Int16, CompareInt16, 1)
		Ω(*(*int16)(unsafe.Pointer(&oldValue))).Should(Equal(int16(-256)))

		MinMaxUpdate(unsafe.Pointer(&oldValue), unsafe.Pointer(&newValue), Int16, CompareInt16, -1)
		Ω(*(*int16)(unsafe.Pointer(&oldValue))).Should(Equal(int16(256)))

		*(*uint16)(unsafe.Pointer(&oldValue)) = 0
		*(*uint16)(unsafe.Pointer(&newValue)) = 256
		MinMaxUpdate(unsafe.Pointer(&oldValue), unsafe.Pointer(&newValue), Uint16, CompareUint16, 1)
		Ω(*(*uint16)(unsafe.Pointer(&oldValue))).Should(Equal(uint16(0)))

		MinMaxUpdate(unsafe.Pointer(&oldValue), unsafe.Pointer(&newValue), Uint16, CompareUint16, -1)
		Ω(*(*uint16)(unsafe.Pointer(&oldValue))).Should(Equal(uint16(256)))

		*(*int32)(unsafe.Pointer(&oldValue)) = -65536
		*(*int32)(unsafe.Pointer(&newValue)) = 65536
		MinMaxUpdate(unsafe.Pointer(&oldValue), unsafe.Pointer(&newValue), Int32, CompareInt32, 1)
		Ω(*(*int32)(unsafe.Pointer(&oldValue))).Should(Equal(int32(-65536)))

		MinMaxUpdate(unsafe.Pointer(&oldValue), unsafe.Pointer(&newValue), Int32, CompareInt32, -1)
		Ω(*(*int32)(unsafe.Pointer(&oldValue))).Should(Equal(int32(65536)))

		*(*uint32)(unsafe.Pointer(&oldValue)) = 0
		*(*uint32)(unsafe.Pointer(&newValue)) = 65536
		MinMaxUpdate(unsafe.Pointer(&oldValue), unsafe.Pointer(&newValue), Uint32, CompareUint32, 1)
		Ω(*(*uint32)(unsafe.Pointer(&oldValue))).Should(Equal(uint32(0)))

		MinMaxUpdate(unsafe.Pointer(&oldValue), unsafe.Pointer(&newValue), Uint32, CompareUint32, -1)
		Ω(*(*uint32)(unsafe.Pointer(&oldValue))).Should(Equal(uint32(65536)))

		*(*int64)(unsafe.Pointer(&oldValue)) = -(1 << 31) - 1
		*(*int64)(unsafe.Pointer(&newValue)) = (1 << 31) + 1
		MinMaxUpdate(unsafe.Pointer(&oldValue), unsafe.Pointer(&newValue), Int64, CompareInt64, 1)
		Ω(*(*int64)(unsafe.Pointer(&oldValue))).Should(Equal(int64(-(1 << 31) - 1)))

		MinMaxUpdate(unsafe.Pointer(&oldValue), unsafe.Pointer(&newValue), Int64, CompareInt64, -1)
		Ω(*(*int64)(unsafe.Pointer(&oldValue))).Should(Equal(int64((1 << 31) + 1)))

		*(*float32)(unsafe.Pointer(&oldValue)) = -1.0
		*(*float32)(unsafe.Pointer(&newValue)) = 1.0
		MinMaxUpdate(unsafe.Pointer(&oldValue), unsafe.Pointer(&newValue), Float32, CompareFloat32, 1)
		Ω(*(*float32)(unsafe.Pointer(&oldValue))).Should(Equal(float32(-1.0)))

		MinMaxUpdate(unsafe.Pointer(&oldValue), unsafe.Pointer(&newValue), Float32, CompareFloat32, -1)
		Ω(*(*float32)(unsafe.Pointer(&oldValue))).Should(Equal(float32(1.0)))
	})

	ginkgo.It("upsert batch column builder setValue", func() {
		builder := columnBuilder{
			columnID:     0,
			dataType:     Uint32,
			values:       make([]interface{}, 1),
			isTimeColumn: true,
		}

		err := builder.SetValue(0, "1570489452010")
		Ω(err).Should(BeNil())

		err = builder.SetValue(0, "1570489452")
		Ω(err).Should(BeNil())

		err = builder.SetValue(0, "abcd")
		Ω(err).ShouldNot(BeNil())
	})
})
