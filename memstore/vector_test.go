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
	"unsafe"

	"github.com/uber/aresdb/memstore/common"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = ginkgo.Describe("vector", func() {
	ginkgo.It("stores 9 bools", func() {
		v := NewVector(common.Bool, 9)
		Ω(v.unitBits).Should(Equal(1))
		Ω(v.Size).Should(Equal(9))
		Ω(v.Bytes).Should(Equal(64))
		Ω(v.DataType).Should(Equal(common.Bool))

		Ω(v.GetBool(0)).Should(BeFalse())
		Ω(v.GetBool(1)).Should(BeFalse())
		Ω(v.GetBool(8)).Should(BeFalse())
		Ω(v.numTrues).Should(Equal(0))

		v.SetBool(2, true)
		Ω(v.GetBool(1)).Should(BeFalse())
		Ω(v.GetBool(2)).Should(BeTrue())
		Ω(v.GetBool(3)).Should(BeFalse())
		Ω(v.numTrues).Should(Equal(1))

		v.SetBool(8, true)
		Ω(v.GetBool(7)).Should(BeFalse())
		Ω(v.GetBool(8)).Should(BeTrue())
		Ω(v.numTrues).Should(Equal(2))

		data := *(*uint16)(v.GetValue(0))
		Ω(data).Should(Equal(uint16(0x104)))

		v.SetBool(2, false)
		Ω(v.GetBool(2)).Should(BeFalse())
		Ω(v.numTrues).Should(Equal(1))

		v.SetBool(7, false)
		Ω(v.GetBool(7)).Should(BeFalse())
		Ω(v.numTrues).Should(Equal(1))

		data = *(*uint16)(v.GetValue(0))
		Ω(data).Should(Equal(uint16(0x100)))

		v.SafeDestruct()
	})

	ginkgo.It("stores 3 uint8s", func() {
		v := NewVector(common.Uint8, 3)
		Ω(v.unitBits).Should(Equal(8))
		Ω(v.Size).Should(Equal(3))
		Ω(v.Bytes).Should(Equal(64))
		Ω(v.DataType).Should(Equal(common.Uint8))

		data := *(*uint32)(v.GetValue(0))
		Ω(data).Should(Equal(uint32(0x0)))

		value := *(*uint8)(v.GetValue(0))
		Ω(value).Should(Equal(uint8(0x0)))
		value = *(*uint8)(v.GetValue(1))
		Ω(value).Should(Equal(uint8(0x0)))
		value = *(*uint8)(v.GetValue(2))
		Ω(value).Should(Equal(uint8(0x0)))

		value = 0x12
		v.SetValue(0, unsafe.Pointer(&value))
		value = 0x34
		v.SetValue(1, unsafe.Pointer(&value))
		value = 0x56
		v.SetValue(2, unsafe.Pointer(&value))

		data = *(*uint32)(v.GetValue(0))
		Ω(data).Should(Equal(uint32(0x563412)))

		value = 0xff
		v.SetValue(1, unsafe.Pointer(&value))

		data = *(*uint32)(v.GetValue(0))
		Ω(data).Should(Equal(uint32(0x56ff12)))

		value = *(*uint8)(v.GetValue(0))
		Ω(value).Should(Equal(uint8(0x12)))
		value = *(*uint8)(v.GetValue(1))
		Ω(value).Should(Equal(uint8(0xff)))
		value = *(*uint8)(v.GetValue(2))
		Ω(value).Should(Equal(uint8(0x56)))

		v.SafeDestruct()
	})

	ginkgo.It("stores 2 uint16s", func() {
		v := NewVector(common.Uint16, 2)
		Ω(v.unitBits).Should(Equal(16))
		Ω(v.Size).Should(Equal(2))
		Ω(v.Bytes).Should(Equal(64))
		Ω(v.DataType).Should(Equal(common.Uint16))

		data := *(*uint32)(v.GetValue(0))
		Ω(data).Should(Equal(uint32(0x0)))

		value := *(*uint16)(v.GetValue(0))
		Ω(value).Should(Equal(uint16(0x0)))
		value = *(*uint16)(v.GetValue(1))
		Ω(value).Should(Equal(uint16(0x0)))

		value = 0x1234
		v.SetValue(0, unsafe.Pointer(&value))
		value = 0x5678
		v.SetValue(1, unsafe.Pointer(&value))

		data = *(*uint32)(v.GetValue(0))
		Ω(data).Should(Equal(uint32(0x56781234)))

		value = *(*uint16)(v.GetValue(0))
		Ω(value).Should(Equal(uint16(0x1234)))
		value = *(*uint16)(v.GetValue(1))
		Ω(value).Should(Equal(uint16(0x5678)))

		value = 0xbeef
		v.SetValue(1, unsafe.Pointer(&value))

		value = *(*uint16)(v.GetValue(0))
		Ω(value).Should(Equal(uint16(0x1234)))
		value = *(*uint16)(v.GetValue(1))
		Ω(value).Should(Equal(uint16(0xbeef)))

		v.SafeDestruct()
	})

	ginkgo.It("stores 2 uint32s", func() {
		v := NewVector(common.Uint32, 2)
		Ω(v.unitBits).Should(Equal(32))
		Ω(v.Size).Should(Equal(2))
		Ω(v.Bytes).Should(Equal(64))
		Ω(v.DataType).Should(Equal(common.Uint32))

		value := *(*uint32)(v.GetValue(0))
		Ω(value).Should(Equal(uint32(0x0)))
		value = *(*uint32)(v.GetValue(1))
		Ω(value).Should(Equal(uint32(0x0)))

		value = 0x232
		v.SetValue(0, unsafe.Pointer(&value))
		value = 0xdeadbeef
		v.SetValue(1, unsafe.Pointer(&value))

		value = *(*uint32)(v.GetValue(0))
		Ω(value).Should(Equal(uint32(0x232)))
		value = *(*uint32)(v.GetValue(1))
		Ω(value).Should(Equal(uint32(0xdeadbeef)))

		Ω(v.minValue).Should(Equal(uint32(0x232)))
		Ω(v.maxValue).Should(Equal(uint32(0xdeadbeef)))

		v.SafeDestruct()
	})

	ginkgo.It("stores 2 uuids", func() {
		v := NewVector(common.UUID, 2)
		Ω(v.unitBits).Should(Equal(128))
		Ω(v.Size).Should(Equal(2))
		Ω(v.Bytes).Should(Equal(64))
		Ω(v.DataType).Should(Equal(common.UUID))

		ptr := uintptr(v.GetValue(0))
		Ω(*(*uint64)(unsafe.Pointer(ptr))).Should(Equal(uint64(0x0)))
		Ω(*(*uint64)(unsafe.Pointer(ptr + 8))).Should(Equal(uint64(0x0)))
		ptr = uintptr(v.GetValue(1))
		Ω(*(*uint64)(unsafe.Pointer(ptr))).Should(Equal(uint64(0x0)))
		Ω(*(*uint64)(unsafe.Pointer(ptr + 8))).Should(Equal(uint64(0x0)))

		buf := make([]byte, 16)
		for i := range buf {
			buf[i] = byte(i)
		}
		v.SetValue(1, unsafe.Pointer(&buf[0]))

		ptr = uintptr(v.GetValue(0))
		Ω(*(*uint64)(unsafe.Pointer(ptr))).Should(Equal(uint64(0x0)))
		Ω(*(*uint64)(unsafe.Pointer(ptr + 8))).Should(Equal(uint64(0x0)))
		ptr = uintptr(v.GetValue(1))
		Ω(*(*uint64)(unsafe.Pointer(ptr))).Should(Equal(uint64(0x0706050403020100)))
		Ω(*(*uint64)(unsafe.Pointer(ptr + 8))).Should(Equal(uint64(0x0f0e0d0c0b0a0908)))

		v.SafeDestruct()
	})

	ginkgo.It("stores 2 geopoints", func() {
		v := NewVector(common.GeoPoint, 2)
		Ω(v.unitBits).Should(Equal(64))
		Ω(v.Size).Should(Equal(2))
		Ω(v.Bytes).Should(Equal(64))
		Ω(v.DataType).Should(Equal(common.GeoPoint))

		ptr := uintptr(v.GetValue(0))
		Ω(*(*[2]float32)(unsafe.Pointer(ptr))).Should(Equal([2]float32{0.0, 0.0}))
		ptr = uintptr(v.GetValue(1))
		Ω(*(*[2]float32)(unsafe.Pointer(ptr))).Should(Equal([2]float32{0.0, 0.0}))

		value1 := [2]float32{-91.1419, 30.4208}
		value2 := [2]float32{-91.1419, 30.4208}
		v.SetValue(0, unsafe.Pointer(&value1[0]))
		v.SetValue(1, unsafe.Pointer(&value2[0]))

		ptr = uintptr(v.GetValue(0))
		Ω(*(*[2]float32)(unsafe.Pointer(ptr))).Should(Equal([2]float32{-91.1419, 30.4208}))
		ptr = uintptr(v.GetValue(1))
		Ω(*(*[2]float32)(unsafe.Pointer(ptr))).Should(Equal([2]float32{-91.1419, 30.4208}))

		v.SafeDestruct()
	})

	ginkgo.It("lower/upper bound", func() {
		v := NewVector(common.Uint8, 5)
		var value uint8
		value = 0x12
		v.SetValue(0, unsafe.Pointer(&value))
		value = 0x14
		v.SetValue(1, unsafe.Pointer(&value))
		value = 0x14
		v.SetValue(2, unsafe.Pointer(&value))
		value = 0x16
		v.SetValue(3, unsafe.Pointer(&value))
		value = 0x16
		v.SetValue(4, unsafe.Pointer(&value))

		// Test over full range search.
		value = 0x11
		Ω(v.LowerBound(0, 5, unsafe.Pointer(&value))).Should(Equal(0))
		Ω(v.UpperBound(0, 5, unsafe.Pointer(&value))).Should(Equal(0))
		value = 0x12
		Ω(v.LowerBound(0, 5, unsafe.Pointer(&value))).Should(Equal(0))
		Ω(v.UpperBound(0, 5, unsafe.Pointer(&value))).Should(Equal(1))
		value = 0x13
		Ω(v.LowerBound(0, 5, unsafe.Pointer(&value))).Should(Equal(1))
		Ω(v.UpperBound(0, 5, unsafe.Pointer(&value))).Should(Equal(1))
		value = 0x14
		Ω(v.LowerBound(0, 5, unsafe.Pointer(&value))).Should(Equal(1))
		Ω(v.UpperBound(0, 5, unsafe.Pointer(&value))).Should(Equal(3))
		value = 0x16
		Ω(v.LowerBound(0, 5, unsafe.Pointer(&value))).Should(Equal(3))
		Ω(v.UpperBound(0, 5, unsafe.Pointer(&value))).Should(Equal(5))
		value = 0x17
		Ω(v.LowerBound(0, 5, unsafe.Pointer(&value))).Should(Equal(5))
		Ω(v.UpperBound(0, 5, unsafe.Pointer(&value))).Should(Equal(5))

		// Test over sub range search.
		value = 0x11
		Ω(v.LowerBound(1, 4, unsafe.Pointer(&value))).Should(Equal(1))
		Ω(v.UpperBound(1, 4, unsafe.Pointer(&value))).Should(Equal(1))
		value = 0x11
		Ω(v.LowerBound(2, 4, unsafe.Pointer(&value))).Should(Equal(2))
		Ω(v.UpperBound(2, 4, unsafe.Pointer(&value))).Should(Equal(2))
		value = 0x14
		Ω(v.LowerBound(0, 2, unsafe.Pointer(&value))).Should(Equal(1))
		Ω(v.UpperBound(0, 2, unsafe.Pointer(&value))).Should(Equal(2))
		value = 0x15
		Ω(v.LowerBound(2, 4, unsafe.Pointer(&value))).Should(Equal(3))
		Ω(v.UpperBound(2, 4, unsafe.Pointer(&value))).Should(Equal(3))
		value = 0x16
		Ω(v.LowerBound(2, 4, unsafe.Pointer(&value))).Should(Equal(3))
		Ω(v.UpperBound(2, 4, unsafe.Pointer(&value))).Should(Equal(4))
		value = 0x17
		Ω(v.LowerBound(2, 4, unsafe.Pointer(&value))).Should(Equal(4))
		Ω(v.UpperBound(2, 4, unsafe.Pointer(&value))).Should(Equal(4))
		value = 0x17
		Ω(v.LowerBound(3, 4, unsafe.Pointer(&value))).Should(Equal(4))
		Ω(v.UpperBound(3, 4, unsafe.Pointer(&value))).Should(Equal(4))

		v.SafeDestruct()

		var boolVal bool = true
		v2 := NewVector(common.Bool, 5)
		v2.SetBool(0, false)
		v2.SetBool(1, false)
		v2.SetBool(2, false)
		v2.SetBool(3, true)
		v2.SetBool(4, true)
		Ω(v2.LowerBound(0, 5, unsafe.Pointer(&boolVal))).Should(Equal(3))
		Ω(v2.UpperBound(0, 5, unsafe.Pointer(&boolVal))).Should(Equal(5))

		boolVal = false
		Ω(v2.LowerBound(0, 5, unsafe.Pointer(&boolVal))).Should(Equal(0))
		Ω(v2.UpperBound(0, 5, unsafe.Pointer(&boolVal))).Should(Equal(3))

		boolVal = true
		Ω(v2.LowerBound(1, 4, unsafe.Pointer(&boolVal))).Should(Equal(3))
		Ω(v2.UpperBound(1, 4, unsafe.Pointer(&boolVal))).Should(Equal(4))

		boolVal = false
		Ω(v2.LowerBound(1, 4, unsafe.Pointer(&boolVal))).Should(Equal(1))
		Ω(v2.UpperBound(1, 4, unsafe.Pointer(&boolVal))).Should(Equal(3))
	})

	ginkgo.It("GetSliceBytesAligned", func() {
		v := NewVector(common.Bool, 1024)
		buffer, startIndex, bytes := v.GetSliceBytesAligned(1000, 1010)
		Ω(buffer).ShouldNot(Equal(unsafe.Pointer(uintptr(0))))
		Ω(startIndex).Should(Equal(488))
		Ω(bytes).Should(Equal(64))

		buffer, startIndex, bytes = v.GetSliceBytesAligned(1000, 1000)
		Ω(buffer).Should(Equal(unsafe.Pointer(uintptr(0))))
		Ω(startIndex).Should(Equal(0))
		Ω(bytes).Should(Equal(0))
		v.SafeDestruct()

		v = nil
		buffer, startIndex, bytes = v.GetSliceBytesAligned(1000, 1010)
		Ω(buffer).Should(Equal(unsafe.Pointer(uintptr(0))))
		Ω(startIndex).Should(Equal(0))
		Ω(bytes).Should(Equal(0))
	})

	ginkgo.It("CheckAllValid should work", func() {
		vector := NewVector(common.Bool, 8) // 1 bytes
		vector.SetAllValid()
		Ω(vector.CheckAllValid()).Should(BeTrue())
		vector.SetBool(0, false)
		Ω(vector.CheckAllValid()).ShouldNot(BeTrue())

		vector = NewVector(common.Bool, 3) // 1 bytes
		vector.SetBool(0, true)
		vector.SetBool(1, true)
		vector.SetBool(2, true)
		Ω(vector.CheckAllValid()).Should(BeTrue())
		vector.SetBool(0, false)
		Ω(vector.CheckAllValid()).ShouldNot(BeTrue())

		vector = NewVector(common.Bool, 2E7)
		vector.SetAllValid()
		Ω(vector.CheckAllValid()).Should(BeTrue())
		vector.SetBool(1E7, false)
		Ω(vector.CheckAllValid()).ShouldNot(BeTrue())

		vector = NewVector(common.Bool, 2E7+1)
		vector.SetAllValid()
		Ω(vector.CheckAllValid()).Should(BeTrue())
		vector.SetBool(2E7-1, false)
		Ω(vector.CheckAllValid()).ShouldNot(BeTrue())

	})

	ginkgo.It("data value comparison", func() {
		v1, v2 := common.NullDataValue, common.NullDataValue

		Ω(v1.Compare(v2)).Should(BeEquivalentTo(0))

		v1 = common.DataValue{
			Valid: false,
		}
		v2 = common.DataValue{
			Valid: true,
		}
		Ω(v1.Compare(v2)).Should(BeEquivalentTo(-1))

		v1 = common.DataValue{
			Valid: true,
		}
		v2 = common.DataValue{
			Valid: false,
		}
		Ω(v1.Compare(v2)).Should(BeEquivalentTo(1))

		vector1 := NewVector(common.Int32, 1)

		v1 = common.DataValue{
			Valid:   true,
			BoolVal: false,
			IsBool:  true,
		}

		v2 = common.DataValue{
			Valid:   true,
			BoolVal: true,
			IsBool:  true,
		}
		Ω(v1.Compare(v2)).Should(BeEquivalentTo(-1))

		v1 = common.DataValue{
			Valid:   true,
			BoolVal: true,
			IsBool:  true,
		}

		v2 = common.DataValue{
			Valid:   true,
			BoolVal: true,
			IsBool:  true,
		}

		Ω(v1.Compare(v2)).Should(BeEquivalentTo(0))

		v1 = common.DataValue{
			Valid:   true,
			BoolVal: false,
			IsBool:  true,
		}

		v2 = common.DataValue{
			Valid:   true,
			BoolVal: false,
			IsBool:  true,
		}

		Ω(v1.Compare(v2)).Should(BeEquivalentTo(0))

		v1 = common.DataValue{
			Valid:   true,
			BoolVal: true,
			IsBool:  true,
		}

		v2 = common.DataValue{
			Valid:   true,
			BoolVal: false,
			IsBool:  true,
		}

		Ω(v1.Compare(v2)).Should(BeEquivalentTo(1))

		vector2 := NewVector(common.Int32, 1)

		t1, t2 := 0, 0
		v1 = common.DataValue{
			Valid:    true,
			OtherVal: unsafe.Pointer(&t1),
			DataType: vector1.DataType,
			CmpFunc:  vector1.cmpFunc,
		}

		v2 = common.DataValue{
			Valid:    true,
			OtherVal: unsafe.Pointer(&t2),
			DataType: vector2.DataType,
			CmpFunc:  vector2.cmpFunc,
		}
		Ω(v1.Compare(v2)).Should(BeEquivalentTo(0))

		t1, t2 = 0, 1
		v1 = common.DataValue{
			Valid:    true,
			OtherVal: unsafe.Pointer(&t1),
			DataType: vector1.DataType,
			CmpFunc:  vector1.cmpFunc,
		}

		v2 = common.DataValue{
			Valid:    true,
			OtherVal: unsafe.Pointer(&t2),
			DataType: vector2.DataType,
			CmpFunc:  vector2.cmpFunc,
		}
		Ω(v1.Compare(v2)).Should(BeEquivalentTo(-1))

		t1, t2 = 1, 0
		v1 = common.DataValue{
			Valid:    true,
			OtherVal: unsafe.Pointer(&t1),
			DataType: vector1.DataType,
			CmpFunc:  vector1.cmpFunc,
		}

		v2 = common.DataValue{
			Valid:    true,
			OtherVal: unsafe.Pointer(&t2),
			DataType: vector2.DataType,
			CmpFunc:  vector2.cmpFunc,
		}
		Ω(v1.Compare(v2)).Should(BeEquivalentTo(1))
	})
})
