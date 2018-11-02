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
	"bytes"
	"github.com/uber/aresdb/utils"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"unsafe"
)

var _ = ginkgo.Describe("data value", func() {

	ginkgo.It("value comparison int8", func() {
		var v1 int8 = -20
		var v2 int8 = -20
		Ω(CompareInt8(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) == 0).Should(BeTrue())
		v2 = -30
		Ω(CompareInt8(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) > 0).Should(BeTrue())
		v2 = 1
		Ω(CompareInt8(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) < 0).Should(BeTrue())
	})

	ginkgo.It("value comparison uint8", func() {
		var v1 uint8 = 20
		var v2 uint8 = 20
		Ω(CompareUint8(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) == 0).Should(BeTrue())
		v2 = 0
		Ω(CompareUint8(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) > 0).Should(BeTrue())
		v2 = 30
		Ω(CompareUint8(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) < 0).Should(BeTrue())
	})

	ginkgo.It("value comparison int16", func() {
		var v1 int16 = -20
		var v2 int16 = -20
		Ω(CompareInt16(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) == 0).Should(BeTrue())
		v2 = -30
		Ω(CompareInt16(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) > 0).Should(BeTrue())
		v2 = 1
		Ω(CompareInt16(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) < 0).Should(BeTrue())
	})

	ginkgo.It("value comparison uint16", func() {
		var v1 uint16 = 20
		var v2 uint16 = 20
		Ω(CompareUint16(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) == 0).Should(BeTrue())
		v2 = 0
		Ω(CompareUint16(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) > 0).Should(BeTrue())
		v2 = 30
		Ω(CompareUint16(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) < 0).Should(BeTrue())
	})

	ginkgo.It("value comparison int32", func() {
		var v1 int32 = -20
		var v2 int32 = -20
		Ω(CompareInt32(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) == 0).Should(BeTrue())
		v2 = -30
		Ω(CompareInt32(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) > 0).Should(BeTrue())
		v2 = 1
		Ω(CompareInt32(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) < 0).Should(BeTrue())
	})

	ginkgo.It("value comparison uint32", func() {
		var v1 uint32 = 20
		var v2 uint32 = 20
		Ω(CompareUint32(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) == 0).Should(BeTrue())
		v2 = 0
		Ω(CompareUint32(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) > 0).Should(BeTrue())
		v2 = 30
		Ω(CompareUint32(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) < 0).Should(BeTrue())
	})

	ginkgo.It("value comparison float32", func() {
		var v1 float32 = -0.35
		var v2 float32 = -0.35
		Ω(CompareFloat32(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) == 0).Should(BeTrue())
		v2 = -1.3
		Ω(CompareFloat32(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) > 0).Should(BeTrue())
		v2 = 0.1
		Ω(CompareFloat32(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) < 0).Should(BeTrue())
	})

	ginkgo.It("value comparison bool", func() {
		Ω(CompareBool(false, false)).Should(Equal(0))
		Ω(CompareBool(true, true)).Should(Equal(0))
		Ω(CompareBool(false, true)).Should(Equal(-1))
		Ω(CompareBool(true, false)).Should(Equal(1))
	})

	ginkgo.It("test value from string", func() {
		val, err := ValueFromString("null", Bool)
		Ω(err).Should(BeNil())
		Ω(val.Valid).Should(BeFalse())

		// test bool val
		val, err = ValueFromString("invalid bool", Bool)
		Ω(err).ShouldNot(BeNil())
		val, err = ValueFromString("true", Bool)
		Ω(val.Valid).Should(BeTrue())
		Ω(val.BoolVal).Should(BeTrue())

		// int8 out of range
		val, err = ValueFromString("128", Int8)
		Ω(err).ShouldNot(BeNil())
		val, err = ValueFromString("127", Int8)
		Ω(val.Valid).Should(BeTrue())
		Ω(*(*int8)(val.OtherVal)).Should(BeEquivalentTo(127))

		// uint8
		val, err = ValueFromString("256", Uint8)
		Ω(err).ShouldNot(BeNil())
		val, err = ValueFromString("255", Uint8)
		Ω(val.Valid).Should(BeTrue())
		Ω(*(*uint8)(val.OtherVal)).Should(BeEquivalentTo(255))

		// small enum
		val, err = ValueFromString("256", SmallEnum)
		Ω(err).ShouldNot(BeNil())
		val, err = ValueFromString("255", SmallEnum)
		Ω(val.Valid).Should(BeTrue())
		Ω(*(*uint8)(val.OtherVal)).Should(BeEquivalentTo(255))

		// int16
		val, err = ValueFromString("32768", Int16)
		Ω(err).ShouldNot(BeNil())
		val, err = ValueFromString("32767", Int16)
		Ω(val.Valid).Should(BeTrue())
		Ω(*(*int16)(val.OtherVal)).Should(BeEquivalentTo(32767))

		// uint16
		val, err = ValueFromString("65536", Uint16)
		Ω(err).ShouldNot(BeNil())
		val, err = ValueFromString("65535", Uint16)
		Ω(val.Valid).Should(BeTrue())
		Ω(*(*uint16)(val.OtherVal)).Should(BeEquivalentTo(65535))

		// big enum
		val, err = ValueFromString("65536", BigEnum)
		Ω(err).ShouldNot(BeNil())
		val, err = ValueFromString("65535", BigEnum)
		Ω(val.Valid).Should(BeTrue())
		Ω(*(*uint16)(val.OtherVal)).Should(BeEquivalentTo(65535))

		// int32
		val, err = ValueFromString("2147483648", Int32)
		Ω(err).ShouldNot(BeNil())
		val, err = ValueFromString("2147483647", Int32)
		Ω(val.Valid).Should(BeTrue())
		Ω(*(*int32)(val.OtherVal)).Should(BeEquivalentTo(2147483647))

		// uint32
		val, err = ValueFromString("4294967296", Uint32)
		Ω(err).ShouldNot(BeNil())
		val, err = ValueFromString("4294967295", Uint32)
		Ω(val.Valid).Should(BeTrue())
		Ω(*(*uint32)(val.OtherVal)).Should(BeEquivalentTo(4294967295))

		// float32
		val, err = ValueFromString("0.10.1", Float32)
		Ω(err).ShouldNot(BeNil())
		val, err = ValueFromString("0.1", Float32)
		Ω(val.Valid).Should(BeTrue())
		Ω(*(*float32)(val.OtherVal)).Should(BeEquivalentTo(float32(0.1)))

		// uuid
		val, err = ValueFromString("01000000000000000100000000000000", UUID)
		Ω(err).Should(BeNil())
		Ω(val.Valid).Should(BeTrue())
		Ω(*(*[2]uint64)(val.OtherVal)).Should(Equal([2]uint64{1, 1}))

		val, err = ValueFromString("01000000000000000100000000000", UUID)
		Ω(err).ShouldNot(BeNil())

		val, err = ValueFromString("01000000-00000000-01000000-00000000", UUID)
		Ω(err).Should(BeNil())
		Ω(*(*[2]uint64)(val.OtherVal)).Should(Equal([2]uint64{1, 1}))
	})

	ginkgo.It("GetBytes of GeoShapeGo should work", func() {
		shape1 := &GeoShapeGo{
			Polygons: [][]GeoPointGo{
				{
					{
						180.0,
						90.0,
					},
				},
				{
					{
						180.0,
						90.0,
					},
					{
						180.0,
						90.0,
					},
				},
			},
		}
		Ω(shape1.GetBytes()).Should(Equal(24))
	})

	ginkgo.It("Read and Write GeoShapeGo should work", func() {
		buffer := &bytes.Buffer{}
		dataWriter := utils.NewStreamDataWriter(buffer)

		shape1 := &GeoShapeGo{
			Polygons: [][]GeoPointGo{
				{
					{
						180.0,
						90.0,
					},
				},
				{
					{
						180.0,
						90.0,
					},
					{
						180.0,
						90.0,
					},
				},
			},
		}

		shape1.Write(&dataWriter)

		shape2 := &GeoShapeGo{}
		dataReader := utils.NewStreamDataReader(buffer)
		shape2.Read(&dataReader)

		Ω(shape2).Should(Equal(shape1))
	})

	ginkgo.It("ConvertToHumanReadable GeoShape should work", func() {
		shape := &GeoShapeGo{
			Polygons: [][]GeoPointGo{
				{
					{
						90.0,
						180.0,
					},
				},
				{
					{
						90.0,
						180.0,
					},
					{
						90.0,
						180.0,
					},
				},
			},
		}
		dv := DataValue{
			Valid: true,
			GoVal: shape,
		}
		Ω(dv.ConvertToHumanReadable(GeoShape)).Should(Equal("Polygon((180.0000+90.0000),(180.0000+90.0000,180.0000+90.0000))"))
	})
})
