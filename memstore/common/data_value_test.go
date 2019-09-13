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
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber/aresdb/utils"
	"unsafe"
)

var _ = ginkgo.Describe("data value", func() {

	ginkgo.It("value comparison int8", func() {
		var v1 int8 = -20
		var v2 int8 = -20
		Ω(GetCompareFunc(Int8)(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) == 0).Should(BeTrue())
		v2 = -30
		Ω(GetCompareFunc(Int8)(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) > 0).Should(BeTrue())
		v2 = 1
		Ω(GetCompareFunc(Int8)(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) < 0).Should(BeTrue())
	})

	ginkgo.It("value comparison uint8", func() {
		var v1 uint8 = 20
		var v2 uint8 = 20
		Ω(GetCompareFunc(Uint8)(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) == 0).Should(BeTrue())
		v2 = 0
		Ω(GetCompareFunc(Uint8)(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) > 0).Should(BeTrue())
		v2 = 30
		Ω(GetCompareFunc(Uint8)(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) < 0).Should(BeTrue())
	})

	ginkgo.It("value comparison int16", func() {
		var v1 int16 = -20
		var v2 int16 = -20
		Ω(GetCompareFunc(Int16)(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) == 0).Should(BeTrue())
		v2 = -30
		Ω(GetCompareFunc(Int16)(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) > 0).Should(BeTrue())
		v2 = 1
		Ω(GetCompareFunc(Int16)(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) < 0).Should(BeTrue())
	})

	ginkgo.It("value comparison uint16", func() {
		var v1 uint16 = 20
		var v2 uint16 = 20
		Ω(GetCompareFunc(Uint16)(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) == 0).Should(BeTrue())
		v2 = 0
		Ω(GetCompareFunc(Uint16)(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) > 0).Should(BeTrue())
		v2 = 30
		Ω(GetCompareFunc(Uint16)(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) < 0).Should(BeTrue())
	})

	ginkgo.It("value comparison int32", func() {
		var v1 int32 = -20
		var v2 int32 = -20
		Ω(GetCompareFunc(Int32)(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) == 0).Should(BeTrue())
		v2 = -30
		Ω(GetCompareFunc(Int32)(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) > 0).Should(BeTrue())
		v2 = 1
		Ω(GetCompareFunc(Int32)(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) < 0).Should(BeTrue())
	})

	ginkgo.It("value comparison uint32", func() {
		var v1 uint32 = 20
		var v2 uint32 = 20
		Ω(GetCompareFunc(Uint32)(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) == 0).Should(BeTrue())
		v2 = 0
		Ω(GetCompareFunc(Uint32)(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) > 0).Should(BeTrue())
		v2 = 30
		Ω(GetCompareFunc(Uint32)(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) < 0).Should(BeTrue())
	})

	ginkgo.It("value comparison float32", func() {
		var v1 float32 = -0.35
		var v2 float32 = -0.35
		Ω(GetCompareFunc(Float32)(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) == 0).Should(BeTrue())
		v2 = -1.3
		Ω(GetCompareFunc(Float32)(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) > 0).Should(BeTrue())
		v2 = 0.1
		Ω(GetCompareFunc(Float32)(unsafe.Pointer(&v1), unsafe.Pointer(&v2)) < 0).Should(BeTrue())
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

		// int64
		val, err = ValueFromString("4294967296", Int64)
		Ω(val.Valid).Should(BeTrue())
		Ω(*(*int64)(val.OtherVal)).Should(BeEquivalentTo(4294967296))

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

		// geo point
		val, err = ValueFromString("Point", GeoPoint)
		Ω(err).ShouldNot(BeNil())
		Ω(val.Valid).ShouldNot(BeTrue())

		val, err = ValueFromString("Point(1.0 1.0)", GeoPoint)
		Ω(err).Should(BeNil())
		Ω(val.Valid).Should(BeTrue())
		Ω(*(*GeoPointGo)(val.OtherVal)).Should(Equal(GeoPointGo{1.0, 1.0}))
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
		Ω(shape1.GetSerBytes()).Should(Equal(36))
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

	ginkgo.It("ConvertToHumanReadable should work", func() {
		dv := DataValue{DataType: Bool, Valid: true, IsBool: true, BoolVal: true}
		Ω(dv.ConvertToHumanReadable(Bool)).Should(Equal(true))

		dv = DataValue{DataType: Bool, Valid: true, IsBool: true, BoolVal: false}
		Ω(dv.ConvertToHumanReadable(Bool)).Should(Equal(false))

		uint8v := uint8(1)
		dv = DataValue{DataType: Uint8, Valid: true, OtherVal: unsafe.Pointer(&uint8v)}
		Ω(dv.ConvertToHumanReadable(Uint8)).Should(Equal(uint8(1)))

		uint16v := uint16(1)
		dv = DataValue{DataType: Uint16, Valid: true, OtherVal: unsafe.Pointer(&uint16v)}
		Ω(dv.ConvertToHumanReadable(Uint16)).Should(Equal(uint16(1)))

		uint32v := uint32(1)
		dv = DataValue{DataType: Uint32, Valid: true, OtherVal: unsafe.Pointer(&uint32v)}
		Ω(dv.ConvertToHumanReadable(Uint32)).Should(Equal(uint32(1)))

		int32v := int32(1)
		dv = DataValue{DataType: Int32, Valid: true, OtherVal: unsafe.Pointer(&int32v)}
		Ω(dv.ConvertToHumanReadable(Int32)).Should(Equal(int32(1)))

		int16v := int16(1)
		dv = DataValue{DataType: Int16, Valid: true, OtherVal: unsafe.Pointer(&int16v)}
		Ω(dv.ConvertToHumanReadable(Int16)).Should(Equal(int16(1)))

		int64v := int64(1)
		dv = DataValue{DataType: Int64, Valid: true, OtherVal: unsafe.Pointer(&int64v)}
		Ω(dv.ConvertToHumanReadable(Int64)).Should(Equal(int64(1)))

		float32v := float32(1)
		dv = DataValue{DataType: Float32, Valid: true, OtherVal: unsafe.Pointer(&float32v)}
		Ω(dv.ConvertToHumanReadable(Float32)).Should(Equal(float32(1)))

		geoPointV := GeoPointGo{float32(1), float32(1)}
		dv = DataValue{DataType: GeoPoint, Valid: true, OtherVal: unsafe.Pointer(&geoPointV)}
		Ω(dv.ConvertToHumanReadable(GeoPoint)).Should(Equal("Point(1.0000,1.0000)"))

		shapeV := GeoShapeGo{
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
		dv = DataValue{Valid: true, GoVal: &shapeV}
		Ω(dv.ConvertToHumanReadable(GeoShape)).Should(Equal("Polygon((180.0000+90.0000),(180.0000+90.0000,180.0000+90.0000))"))
	})

	ginkgo.It("CalculateListElementBytes should work", func() {
		s := CalculateListElementBytes(Bool, 10)
		Ω(s).Should(Equal(8))

		s = CalculateListElementBytes(Uint8, 10)
		Ω(s).Should(Equal(16))

		s = CalculateListElementBytes(Int16, 10)
		Ω(s).Should(Equal(32))

		s = CalculateListElementBytes(Int16, 0)
		Ω(s).Should(Equal(0))

		s = CalculateListElementBytes(Int32, 10)
		Ω(s).Should(Equal(48))

		s = CalculateListElementBytes(Float32, 10)
		Ω(s).Should(Equal(48))

		s = CalculateListElementBytes(Int64, 10)
		Ω(s).Should(Equal(88))

		s = CalculateListElementBytes(UUID, 10)
		Ω(s).Should(Equal(168))

		s = CalculateListElementBytes(GeoPoint, 10)
		Ω(s).Should(Equal(88))
	})

	ginkgo.It("ArrayValue ConvertToHumanReadable should work", func() {
		// bool
		arrayValue := NewArrayValue(Bool)
		arrayValue.AddItem(true)
		arrayValue.AddItem(false)
		arrayValue.AddItem(true)
		size := arrayValue.GetSerBytes()
		buffer := make([]byte, size)
		writer := utils.NewBufferWriter(buffer)
		arrayValue.Write(&writer)
		dv := DataValue{DataType: ArrayBool, Valid: true, OtherVal: unsafe.Pointer(&buffer[0])}
		Ω(dv.ConvertToHumanReadable(ArrayBool)).Should(Equal("[true,false,true]"))

		// uint8
		arrayValue = NewArrayValue(Uint8)
		arrayValue.AddItem(uint8(11))
		arrayValue.AddItem(nil)
		arrayValue.AddItem(uint8(13))
		size = arrayValue.GetSerBytes()
		buffer = make([]byte, size)
		writer = utils.NewBufferWriter(buffer)
		arrayValue.Write(&writer)
		dv = DataValue{DataType: ArrayUint8, Valid: true, OtherVal: unsafe.Pointer(&buffer[0])}
		Ω(dv.ConvertToHumanReadable(ArrayUint8)).Should(Equal("[11,null,13]"))

		// int8
		arrayValue = NewArrayValue(Int8)
		arrayValue.AddItem(int8(11))
		arrayValue.AddItem(nil)
		arrayValue.AddItem(int8(13))
		size = arrayValue.GetSerBytes()
		buffer = make([]byte, size)
		writer = utils.NewBufferWriter(buffer)
		arrayValue.Write(&writer)
		dv = DataValue{DataType: ArrayInt8, Valid: true, OtherVal: unsafe.Pointer(&buffer[0])}
		Ω(dv.ConvertToHumanReadable(ArrayInt8)).Should(Equal("[11,null,13]"))

		// int16
		arrayValue = NewArrayValue(Int16)
		arrayValue.AddItem(int16(11))
		arrayValue.AddItem(nil)
		arrayValue.AddItem(int16(13))
		size = arrayValue.GetSerBytes()
		buffer = make([]byte, size)
		writer = utils.NewBufferWriter(buffer)
		arrayValue.Write(&writer)
		dv = DataValue{DataType: ArrayInt16, Valid: true, OtherVal: unsafe.Pointer(&buffer[0])}
		Ω(dv.ConvertToHumanReadable(ArrayInt16)).Should(Equal("[11,null,13]"))

		// uint16
		arrayValue = NewArrayValue(Uint16)
		arrayValue.AddItem(uint16(11))
		arrayValue.AddItem(nil)
		arrayValue.AddItem(uint16(13))
		size = arrayValue.GetSerBytes()
		buffer = make([]byte, size)
		writer = utils.NewBufferWriter(buffer)
		arrayValue.Write(&writer)
		dv = DataValue{DataType: ArrayUint16, Valid: true, OtherVal: unsafe.Pointer(&buffer[0])}
		Ω(dv.ConvertToHumanReadable(ArrayUint16)).Should(Equal("[11,null,13]"))

		// int32
		arrayValue = NewArrayValue(Int32)
		arrayValue.AddItem(int32(11))
		arrayValue.AddItem(nil)
		arrayValue.AddItem(int32(13))
		size = arrayValue.GetSerBytes()
		buffer = make([]byte, size)
		writer = utils.NewBufferWriter(buffer)
		arrayValue.Write(&writer)
		dv = DataValue{DataType: ArrayInt32, Valid: true, OtherVal: unsafe.Pointer(&buffer[0])}
		Ω(dv.ConvertToHumanReadable(ArrayInt32)).Should(Equal("[11,null,13]"))

		// int64
		arrayValue = NewArrayValue(Int64)
		arrayValue.AddItem(int64(11))
		arrayValue.AddItem(nil)
		arrayValue.AddItem(int64(13))
		size = arrayValue.GetSerBytes()
		buffer = make([]byte, size)
		writer = utils.NewBufferWriter(buffer)
		arrayValue.Write(&writer)
		dv = DataValue{DataType: ArrayInt64, Valid: true, OtherVal: unsafe.Pointer(&buffer[0])}
		Ω(dv.ConvertToHumanReadable(ArrayInt64)).Should(Equal("[11,null,13]"))

		// float32
		arrayValue = NewArrayValue(Float32)
		arrayValue.AddItem(float32(11.1))
		arrayValue.AddItem(nil)
		arrayValue.AddItem(float32(13.3))
		size = arrayValue.GetSerBytes()
		buffer = make([]byte, size)
		writer = utils.NewBufferWriter(buffer)
		arrayValue.Write(&writer)
		dv = DataValue{DataType: ArrayFloat32, Valid: true, OtherVal: unsafe.Pointer(&buffer[0])}
		Ω(dv.ConvertToHumanReadable(ArrayFloat32)).Should(Equal("[11.1,null,13.3]"))

		// UUID
		arrayValue = NewArrayValue(UUID)
		arrayValue.AddItem([2]uint64{8593473084385232926, 8586401979951933868})
		arrayValue.AddItem(nil)
		arrayValue.AddItem([2]uint64{8593473084385232926, 8730517168027789740})
		size = arrayValue.GetSerBytes()
		buffer = make([]byte, size)
		writer = utils.NewBufferWriter(buffer)
		arrayValue.Write(&writer)
		dv = DataValue{DataType: ArrayUUID, Valid: true, OtherVal: unsafe.Pointer(&buffer[0])}
		Ω(dv.ConvertToHumanReadable(ArrayUUID)).Should(Equal("[\"1e88a975-3d26-4277-ace9-bea91b072977\",null,\"1e88a975-3d26-4277-ace9-bea91b072979\"]"))

		// GeoPoint
		arrayValue = NewArrayValue(GeoPoint)
		arrayValue.AddItem([2]float32{90.0, 180.0})
		arrayValue.AddItem(nil)
		arrayValue.AddItem([2]float32{88.0, 178.0})
		size = arrayValue.GetSerBytes()
		buffer = make([]byte, size)
		writer = utils.NewBufferWriter(buffer)
		arrayValue.Write(&writer)
		dv = DataValue{DataType: ArrayGeoPoint, Valid: true, OtherVal: unsafe.Pointer(&buffer[0])}
		Ω(dv.ConvertToHumanReadable(ArrayGeoPoint)).Should(Equal("[\"Point(180.0000,90.0000)\",null,\"Point(178.0000,88.0000)\"]"))

	})

	ginkgo.It("test GetDataValue", func() {
		// test bool val
		_, err := GetDataValue(true, 1, "Bool")
		Ω(err).Should(BeNil())
		_, err = GetDataValue(1, 1, "Bool")
		Ω(err).ShouldNot(BeNil())

		// int8 out of range
		_, err = GetDataValue(int8(127), 1, "Int8")
		Ω(err).Should(BeNil())
		_, err = GetDataValue(int(127), 1, "Int8")
		Ω(err).Should(BeNil())
		_, err = GetDataValue(true, 1, "Int8")
		Ω(err).ShouldNot(BeNil())

		// uint8
		_, err = GetDataValue(uint8(255), 1, "Uint8")
		Ω(err).Should(BeNil())
		_, err = GetDataValue(int(255), 1, "Uint8")
		Ω(err).Should(BeNil())
		_, err = GetDataValue(true, 1, "Uint8")
		Ω(err).ShouldNot(BeNil())

		// int16
		_, err = GetDataValue(int16(32767), 1, "Int16")
		Ω(err).Should(BeNil())
		_, err = GetDataValue(int(32767), 1, "Int16")
		Ω(err).Should(BeNil())
		_, err = GetDataValue(true, 1, "Int16")
		Ω(err).ShouldNot(BeNil())

		// uint16
		_, err = GetDataValue(uint16(65535), 1, "Uint16")
		Ω(err).Should(BeNil())
		_, err = GetDataValue(int(65535), 1, "Uint16")
		Ω(err).Should(BeNil())
		_, err = GetDataValue(true, 1, "Uint16")
		Ω(err).ShouldNot(BeNil())

		// int32
		_, err = GetDataValue(int32(2147483647), 1, "Int32")
		Ω(err).Should(BeNil())
		_, err = GetDataValue(int(2147483647), 1, "Int32")
		Ω(err).Should(BeNil())
		_, err = GetDataValue(true, 1, "Int32")
		Ω(err).ShouldNot(BeNil())

		// uint32
		_, err = GetDataValue(uint32(4294967295), 1, "Uint32")
		Ω(err).Should(BeNil())
		_, err = GetDataValue(int(4294967295), 1, "Uint32")
		Ω(err).Should(BeNil())
		_, err = GetDataValue(true, 1, "Uint32")
		Ω(err).ShouldNot(BeNil())

		// int64
		_, err = GetDataValue(int64(9223372036854775807), 1, "Int64")
		Ω(err).Should(BeNil())
		_, err = GetDataValue(int(9223372036854775807), 1, "Int64")
		Ω(err).Should(BeNil())
		_, err = GetDataValue(true, 1, "Int64")
		Ω(err).ShouldNot(BeNil())

		// float32
		_, err = GetDataValue(float32(123.1), 1, "Float32")
		Ω(err).Should(BeNil())
		_, err = GetDataValue(float64(132.2), 1, "Float32")
		Ω(err).Should(BeNil())
		_, err = GetDataValue(true, 1, "Float32")
		Ω(err).ShouldNot(BeNil())

		// uuid
		_, err = GetDataValue("3ac6f11d-ba72-4527-970a-da4237145797", 1, "UUID")
		Ω(err).Should(BeNil())
		_, err = GetDataValue(true, 1, "UUID")
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("ArrayValueReader should ok to read null ptr", func() {
		reader := NewArrayValueReader(ArrayInt16, unsafe.Pointer(uintptr(0)))
		Ω(reader.GetLength()).Should(Equal(0))
		reader = NewArrayValueReader(ArrayInt16, nil)
		Ω(reader.GetLength()).Should(Equal(0))
	})
})
