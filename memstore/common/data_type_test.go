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
	"math"
)

var _ = ginkgo.Describe("data_type", func() {

	ginkgo.It("getDimensionDataBytes should work", func() {
		Ω(DataTypeBytes(Bool)).Should(Equal(1))
		Ω(DataTypeBytes(Uint8)).Should(Equal(1))
		Ω(DataTypeBytes(Uint16)).Should(Equal(2))
		Ω(DataTypeBytes(Uint32)).Should(Equal(4))
		Ω(DataTypeBytes(Int64)).Should(Equal(8))
		Ω(DataTypeBytes(UUID)).Should(Equal(16))
	})

	ginkgo.It("calculates data type bits", func() {
		Ω(DataTypeBits(Bool)).Should(Equal(1))
		Ω(DataTypeBits(Int8)).Should(Equal(8))
		Ω(DataTypeBits(Uint8)).Should(Equal(8))
		Ω(DataTypeBits(Int16)).Should(Equal(16))
		Ω(DataTypeBits(Uint16)).Should(Equal(16))
		Ω(DataTypeBits(Int32)).Should(Equal(32))
		Ω(DataTypeBits(Uint32)).Should(Equal(32))
		Ω(DataTypeBits(Int64)).Should(Equal(64))
		Ω(DataTypeBits(Float32)).Should(Equal(32))
		Ω(DataTypeBits(SmallEnum)).Should(Equal(8))
		Ω(DataTypeBits(BigEnum)).Should(Equal(16))
		Ω(DataTypeBits(GeoPoint)).Should(Equal(64))
		Ω(DataTypeBits(UUID)).Should(Equal(128))

		Ω(DataTypeBits(ArrayBool)).Should(Equal(1))
		Ω(DataTypeBits(ArrayInt8)).Should(Equal(8))
		Ω(DataTypeBits(ArrayUint8)).Should(Equal(8))
		Ω(DataTypeBits(ArrayInt16)).Should(Equal(16))
		Ω(DataTypeBits(ArrayUint16)).Should(Equal(16))
		Ω(DataTypeBits(ArrayInt32)).Should(Equal(32))
		Ω(DataTypeBits(ArrayUint32)).Should(Equal(32))
		Ω(DataTypeBits(ArrayInt64)).Should(Equal(64))
		Ω(DataTypeBits(ArrayFloat32)).Should(Equal(32))
		Ω(DataTypeBits(ArraySmallEnum)).Should(Equal(8))
		Ω(DataTypeBits(ArrayBigEnum)).Should(Equal(16))
		Ω(DataTypeBits(ArrayGeoPoint)).Should(Equal(64))
		Ω(DataTypeBits(ArrayUUID)).Should(Equal(128))
	})

	ginkgo.It("maps data type name", func() {
		Ω(DataTypeName[Bool]).Should(Equal("Bool"))
		Ω(DataTypeName[Int8]).Should(Equal("Int8"))
		Ω(DataTypeName[Uint8]).Should(Equal("Uint8"))
		Ω(DataTypeName[Int16]).Should(Equal("Int16"))
		Ω(DataTypeName[Uint16]).Should(Equal("Uint16"))
		Ω(DataTypeName[Int32]).Should(Equal("Int32"))
		Ω(DataTypeName[Uint32]).Should(Equal("Uint32"))
		Ω(DataTypeName[Int64]).Should(Equal("Int64"))
		Ω(DataTypeName[Float32]).Should(Equal("Float32"))
		Ω(DataTypeName[SmallEnum]).Should(Equal("SmallEnum"))
		Ω(DataTypeName[BigEnum]).Should(Equal("BigEnum"))
		Ω(DataTypeName[GeoPoint]).Should(Equal("GeoPoint"))
		Ω(DataTypeName[UUID]).Should(Equal("UUID"))
		Ω(DataTypeName[Unknown]).Should(Equal("Unknown"))

		Ω(DataTypeName[ArrayBool]).Should(Equal("Bool[]"))
		Ω(DataTypeName[ArrayInt8]).Should(Equal("Int8[]"))
		Ω(DataTypeName[ArrayUint8]).Should(Equal("Uint8[]"))
		Ω(DataTypeName[ArrayInt16]).Should(Equal("Int16[]"))
		Ω(DataTypeName[ArrayUint16]).Should(Equal("Uint16[]"))
		Ω(DataTypeName[ArrayInt32]).Should(Equal("Int32[]"))
		Ω(DataTypeName[ArrayUint32]).Should(Equal("Uint32[]"))
		Ω(DataTypeName[ArrayInt64]).Should(Equal("Int64[]"))
		Ω(DataTypeName[ArrayFloat32]).Should(Equal("Float32[]"))
		Ω(DataTypeName[ArraySmallEnum]).Should(Equal("SmallEnum[]"))
		Ω(DataTypeName[ArrayBigEnum]).Should(Equal("BigEnum[]"))
		Ω(DataTypeName[ArrayUUID]).Should(Equal("UUID[]"))
		Ω(DataTypeName[ArrayGeoPoint]).Should(Equal("GeoPoint[]"))
	})

	ginkgo.It("maps string data type name", func() {
		Ω(StringToDataType["Bool"]).Should(Equal(Bool))
		Ω(StringToDataType["Int8"]).Should(Equal(Int8))
		Ω(StringToDataType["Uint8"]).Should(Equal(Uint8))
		Ω(StringToDataType["Int16"]).Should(Equal(Int16))
		Ω(StringToDataType["Uint16"]).Should(Equal(Uint16))
		Ω(StringToDataType["Int32"]).Should(Equal(Int32))
		Ω(StringToDataType["Uint32"]).Should(Equal(Uint32))
		Ω(StringToDataType["Int64"]).Should(Equal(Int64))
		Ω(StringToDataType["Float32"]).Should(Equal(Float32))
		Ω(StringToDataType["SmallEnum"]).Should(Equal(SmallEnum))
		Ω(StringToDataType["BigEnum"]).Should(Equal(BigEnum))
		Ω(StringToDataType["GeoPoint"]).Should(Equal(GeoPoint))
		Ω(StringToDataType["UUID"]).Should(Equal(UUID))

		Ω(StringToDataType["Bool[]"]).Should(Equal(ArrayBool))
		Ω(StringToDataType["Int8[]"]).Should(Equal(ArrayInt8))
		Ω(StringToDataType["Uint8[]"]).Should(Equal(ArrayUint8))
		Ω(StringToDataType["Int16[]"]).Should(Equal(ArrayInt16))
		Ω(StringToDataType["Uint16[]"]).Should(Equal(ArrayUint16))
		Ω(StringToDataType["Int32[]"]).Should(Equal(ArrayInt32))
		Ω(StringToDataType["Uint32[]"]).Should(Equal(ArrayUint32))
		Ω(StringToDataType["Int64[]"]).Should(Equal(ArrayInt64))
		Ω(StringToDataType["Float32[]"]).Should(Equal(ArrayFloat32))
		Ω(StringToDataType["SmallEnum[]"]).Should(Equal(ArraySmallEnum))
		Ω(StringToDataType["BigEnum[]"]).Should(Equal(ArrayBigEnum))
		Ω(StringToDataType["UUID[]"]).Should(Equal(ArrayUUID))
		Ω(StringToDataType["GeoPoint[]"]).Should(Equal(ArrayGeoPoint))
	})

	ginkgo.It("creates new data type", func() {
		dataType, err := NewDataType(uint32(Bool))
		Ω(dataType).Should(Equal(Bool))
		Ω(err).Should(BeNil())

		dataType, err = NewDataType(uint32(Uint32))
		Ω(dataType).Should(Equal(Uint32))
		Ω(err).Should(BeNil())

		dataType, err = NewDataType(0xFFFFFFFF)
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("ConvertToBool", func() {
		v, ok := ConvertToBool("1")
		Ω(ok).Should(BeTrue())
		Ω(v).Should(BeTrue())

		v, ok = ConvertToBool("0")
		Ω(ok).Should(BeTrue())
		Ω(v).Should(BeFalse())

		v, ok = ConvertToBool(0)
		Ω(ok).Should(BeTrue())
		Ω(v).Should(BeFalse())

		v, ok = ConvertToBool(1)
		Ω(ok).Should(BeTrue())
		Ω(v).Should(BeTrue())

		v, ok = ConvertToBool(true)
		Ω(ok).Should(BeTrue())
		Ω(v).Should(BeTrue())

		v, ok = ConvertToBool(false)
		Ω(ok).Should(BeTrue())
		Ω(v).Should(BeFalse())

		v, ok = ConvertToBool("true")
		Ω(ok).Should(BeTrue())
		Ω(v).Should(BeTrue())

		v, ok = ConvertToBool("false")
		Ω(ok).Should(BeTrue())
		Ω(v).Should(BeFalse())

		_, ok = ConvertToBool("unknown")
		Ω(ok).Should(BeFalse())
	})

	ginkgo.It("ConvertToInt8", func() {
		v, ok := ConvertToInt8("1")
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal(int8(1)))

		v, ok = ConvertToInt8(1)
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal(int8(1)))

		v, ok = ConvertToInt8(uint32(1))
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal(int8(1)))

		_, ok = ConvertToInt8(math.MaxInt32)
		Ω(ok).Should(BeFalse())

		_, ok = ConvertToInt8("unknown")
		Ω(ok).Should(BeFalse())
	})

	ginkgo.It("ConvertToUint8", func() {
		v, ok := ConvertToUint8("1")
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal(uint8(1)))

		v, ok = ConvertToUint8(1)
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal(uint8(1)))

		v, ok = ConvertToUint8(uint32(1))
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal(uint8(1)))

		v, ok = ConvertToUint8(float64(1))
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal(uint8(1)))

		_, ok = ConvertToUint8(math.MaxInt32)
		Ω(ok).Should(BeFalse())

		_, ok = ConvertToUint8("unknown")
		Ω(ok).Should(BeFalse())
	})

	ginkgo.It("ConvertToInt16", func() {
		v, ok := ConvertToInt16("1")
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal(int16(1)))

		v, ok = ConvertToInt16(1)
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal(int16(1)))

		v, ok = ConvertToInt16(int64(1))
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal(int16(1)))

		v, ok = ConvertToInt16(float64(1))
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal(int16(1)))

		_, ok = ConvertToUint16(math.MaxInt32)
		Ω(ok).Should(BeFalse())

		_, ok = ConvertToInt16("unknown")
		Ω(ok).Should(BeFalse())
	})

	ginkgo.It("ConvertToUint16", func() {
		v, ok := ConvertToUint16("1")
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal(uint16(1)))

		v, ok = ConvertToUint16(1)
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal(uint16(1)))

		v, ok = ConvertToUint16(uint16(123))
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal(uint16(123)))

		v, ok = ConvertToUint16(int64(1))
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal(uint16(1)))

		v, ok = ConvertToUint16(float64(1))
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal(uint16(1)))

		_, ok = ConvertToUint16(math.MaxInt32)
		Ω(ok).Should(BeFalse())

		_, ok = ConvertToUint16("unknown")
		Ω(ok).Should(BeFalse())
	})

	ginkgo.It("ConvertToInt64", func() {
		v, ok := ConvertToInt64("1")
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal(int64(1)))

		v, ok = ConvertToInt64(1)
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal(int64(1)))

		v, ok = ConvertToInt64(int64(1))
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal(int64(1)))

		v, ok = ConvertToInt64(float64(1))
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal(int64(1)))

		_, ok = ConvertToInt64("unknown")
		Ω(ok).Should(BeFalse())
	})

	ginkgo.It("ConvertToInt32", func() {
		v, ok := ConvertToInt32("1")
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal(int32(1)))

		v, ok = ConvertToInt32(1)
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal(int32(1)))

		v, ok = ConvertToInt32(int32(123))
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal(int32(123)))

		v, ok = ConvertToInt32(int64(1))
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal(int32(1)))

		v, ok = ConvertToInt32(float64(1))
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal(int32(1)))

		_, ok = ConvertToInt32(math.MaxInt64)
		Ω(ok).Should(BeFalse())

		_, ok = ConvertToInt32("unknown")
		Ω(ok).Should(BeFalse())
	})

	ginkgo.It("ConvertToUint32", func() {
		v, ok := ConvertToUint32("1")
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal(uint32(1)))

		v, ok = ConvertToUint32(1)
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal(uint32(1)))

		v, ok = ConvertToUint32(uint32(1))
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal(uint32(1)))

		v, ok = ConvertToUint32(float64(1))
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal(uint32(1)))

		_, ok = ConvertToUint32(math.MaxInt64)
		Ω(ok).Should(BeFalse())

		_, ok = ConvertToUint32("unknown")
		Ω(ok).Should(BeFalse())
	})

	ginkgo.It("ConvertToFloat32", func() {
		v, ok := ConvertToFloat32("1")
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal(float32(1)))

		v, ok = ConvertToFloat32(1)
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal(float32(1)))

		v, ok = ConvertToFloat32(int64(1))
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal(float32(1)))

		v, ok = ConvertToFloat32(float64(1))
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal(float32(1)))

		// this will create +Inf
		_, ok = ConvertToFloat32(math.MaxFloat64)
		Ω(ok).Should(BeFalse())

		_, ok = ConvertToFloat32(math.Inf(0))
		Ω(ok).Should(BeFalse())

		_, ok = ConvertToFloat32(math.NaN())
		Ω(ok).Should(BeFalse())

		_, ok = ConvertToFloat32("unknown")
		Ω(ok).Should(BeFalse())
	})

	ginkgo.It("ConvertToUUID", func() {
		v, ok := ConvertToUUID([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal([2]uint64{506097522914230528, 1084818905618843912}))

		_, ok = ConvertToUUID("unknown")
		Ω(ok).Should(BeFalse())

		v, ok = ConvertToUUID("1e88a975-3d26-4277-ace9-bea91b072977")
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal([2]uint64{8593473084385232926, 8586401979951933868}))

	})

	ginkgo.It("ConvertToGeoPoint", func() {
		expected := [2]float32{90.0, 180.0}
		v, ok := ConvertToGeoPoint("Point(180.0, 90.0)")
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal(expected))

		v, ok = ConvertToGeoPoint([2]float32{90.0, 180.0})
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal(expected))

		v, ok = ConvertToGeoPoint([2]float64{90.0, 180.0})
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal(expected))

		v, ok = ConvertToGeoPoint("point(180.0, 90.0)")
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal(expected))

		v, ok = ConvertToGeoPoint("180.0, 90.0")
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal(expected))

		// extra spaces
		v, ok = ConvertToGeoPoint("    180.0, 90.0 ")
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal(expected))

		v, ok = ConvertToGeoPoint("    Point( 180.0, 90.0 )")
		Ω(ok).Should(BeTrue())
		Ω(v).Should(Equal(expected))

		// lat > 90
		_, ok = ConvertToGeoPoint("Point(123.0, 91.0)")
		Ω(ok).Should(BeFalse())

		// lat < -90
		_, ok = ConvertToGeoPoint("Point(123.0, -91.0)")
		Ω(ok).Should(BeFalse())

		// lng > 180
		_, ok = ConvertToGeoPoint("Point(181.0, 89.0)")
		Ω(ok).Should(BeFalse())

		// lng < -180
		_, ok = ConvertToGeoPoint("Point(-181.0, -89.0)")
		Ω(ok).Should(BeFalse())

		// missing parts
		_, ok = ConvertToGeoPoint("Point(-181.0, )")
		Ω(ok).Should(BeFalse())
	})

	ginkgo.It("ConvertToGeoShape", func() {
		polygonStr := "POLYGON((-180.0 90.0,-180.0 90.0),(-180.0 90.0, -180.0 90.0))"
		expectedShape := &GeoShapeGo{
			Polygons: [][]GeoPointGo{
				{
					{
						90.0,
						-180.0,
					},
					{
						90.0,
						-180.0,
					},
				},
				{
					{
						90.0,
						-180.0,
					},
					{
						90.0,
						-180.0,
					},
				},
			},
		}

		shape, ok := ConvertToGeoShape(polygonStr)
		Ω(ok).Should(BeTrue())
		Ω(shape).Should(Equal(expectedShape))

		buffer := &bytes.Buffer{}
		dataWriter := utils.NewStreamDataWriter(buffer)
		shape.Write(&dataWriter)

		shape, ok = ConvertToGeoShape(buffer.Bytes())
		Ω(ok).Should(BeTrue())
		Ω(shape).Should(Equal(expectedShape))
	})

	ginkgo.It("ConvertToArray", func() {
		//int8
		v, err := ConvertToArrayValue(ArrayInt8, "[\"11\",\"12\",\"13\"]")
		Ω(err).Should(BeNil())
		res := v.(*ArrayValue)
		Ω(res.GetLength()).Should(Equal(3))
		Ω(res.DataType).Should(Equal(Int8))
		Ω(res.Items[0].(int8)).Should(Equal(int8(11)))
		Ω(res.Items[2].(int8)).Should(Equal(int8(13)))

		v, err = ConvertToArrayValue(ArrayInt8, "[11,12,13]")
		Ω(err).Should(BeNil())
		res = v.(*ArrayValue)
		Ω(res.GetLength()).Should(Equal(3))
		Ω(res.DataType).Should(Equal(Int8))
		Ω(res.Items[0].(int8)).Should(Equal(int8(11)))
		Ω(res.Items[2].(int8)).Should(Equal(int8(13)))

		v, err = ConvertToArrayValue(ArrayInt8, "11,12,13")
		Ω(err).Should(BeNil())
		res = v.(*ArrayValue)
		Ω(res.GetLength()).Should(Equal(3))
		Ω(res.DataType).Should(Equal(Int8))
		Ω(res.Items[0].(int8)).Should(Equal(int8(11)))
		Ω(res.Items[2].(int8)).Should(Equal(int8(13)))

		v, err = ConvertToArrayValue(ArrayInt8, "")
		Ω(err).Should(BeNil())
		res = v.(*ArrayValue)
		Ω(res.GetLength()).Should(Equal(1))
		Ω(res.Items[0]).Should(BeNil())

		// uint8
		v, err = ConvertToArrayValue(ArrayUint8, "[11,12,13]")
		Ω(err).Should(BeNil())
		res = v.(*ArrayValue)
		Ω(res.GetLength()).Should(Equal(3))
		Ω(res.DataType).Should(Equal(Uint8))
		Ω(res.Items[0].(uint8)).Should(Equal(uint8(11)))
		Ω(res.Items[2].(uint8)).Should(Equal(uint8(13)))

		// int16
		v, err = ConvertToArrayValue(ArrayInt16, "[11,12,13]")
		Ω(err).Should(BeNil())
		res = v.(*ArrayValue)
		Ω(res.GetLength()).Should(Equal(3))
		Ω(res.DataType).Should(Equal(Int16))
		Ω(res.Items[0].(int16)).Should(Equal(int16(11)))
		Ω(res.Items[2].(int16)).Should(Equal(int16(13)))

		v, err = ConvertToArrayValue(ArrayUint16, "[11,12,13]")
		Ω(err).Should(BeNil())
		res = v.(*ArrayValue)
		Ω(res.GetLength()).Should(Equal(3))
		Ω(res.DataType).Should(Equal(Uint16))
		Ω(res.Items[0].(uint16)).Should(Equal(uint16(11)))
		Ω(res.Items[2].(uint16)).Should(Equal(uint16(13)))

		// int32
		v, err = ConvertToArrayValue(ArrayInt32, "[11,12,13]")
		Ω(err).Should(BeNil())
		res = v.(*ArrayValue)
		Ω(res.GetLength()).Should(Equal(3))
		Ω(res.DataType).Should(Equal(Int32))
		Ω(res.Items[0].(int32)).Should(Equal(int32(11)))
		Ω(res.Items[2].(int32)).Should(Equal(int32(13)))

		// uint32
		v, err = ConvertToArrayValue(ArrayUint32, "[11,12,13]")
		Ω(err).Should(BeNil())
		res = v.(*ArrayValue)
		Ω(res.GetLength()).Should(Equal(3))
		Ω(res.DataType).Should(Equal(Uint32))
		Ω(res.Items[0].(uint32)).Should(Equal(uint32(11)))
		Ω(res.Items[2].(uint32)).Should(Equal(uint32(13)))

		// float32
		v, err = ConvertToArrayValue(ArrayFloat32, "[11.1,12.2,13.3]")
		Ω(err).Should(BeNil())
		res = v.(*ArrayValue)
		Ω(res.GetLength()).Should(Equal(3))
		Ω(res.DataType).Should(Equal(Float32))
		Ω(res.Items[0].(float32)).Should(Equal(float32(11.1)))
		Ω(res.Items[2].(float32)).Should(Equal(float32(13.3)))

		//smallenum
		v, err = ConvertToArrayValue(ArraySmallEnum, "[11,12,13]")
		Ω(err).Should(BeNil())
		res = v.(*ArrayValue)
		Ω(res.GetLength()).Should(Equal(3))
		Ω(res.DataType).Should(Equal(SmallEnum))
		Ω(res.Items[0].(uint8)).Should(Equal(uint8(11)))
		Ω(res.Items[2].(uint8)).Should(Equal(uint8(13)))

		// bigenum
		v, err = ConvertToArrayValue(ArrayBigEnum, "[11,12,13]")
		Ω(err).Should(BeNil())
		res = v.(*ArrayValue)
		Ω(res.GetLength()).Should(Equal(3))
		Ω(res.DataType).Should(Equal(BigEnum))
		Ω(res.Items[0].(uint16)).Should(Equal(uint16(11)))
		Ω(res.Items[2].(uint16)).Should(Equal(uint16(13)))

		// int64
		v, err = ConvertToArrayValue(ArrayInt64, "[11,12,13]")
		Ω(err).Should(BeNil())
		res = v.(*ArrayValue)
		Ω(res.GetLength()).Should(Equal(3))
		Ω(res.DataType).Should(Equal(Int64))
		Ω(res.Items[0].(int64)).Should(Equal(int64(11)))
		Ω(res.Items[2].(int64)).Should(Equal(int64(13)))

		//uuid
		v, err = ConvertToArrayValue(ArrayUUID, "1e88a975-3d26-4277-ace9-bea91b072977,1e88a975-3d26-4277-ace9-bea91b072978,1e88a975-3d26-4277-ace9-bea91b072979")
		Ω(err).Should(BeNil())
		res = v.(*ArrayValue)
		Ω(res.GetLength()).Should(Equal(3))
		Ω(res.DataType).Should(Equal(UUID))
		Ω(res.Items[0].([2]uint64)).Should(Equal([2]uint64{8593473084385232926, 8586401979951933868}))
		Ω(res.Items[2].([2]uint64)).Should(Equal([2]uint64{8593473084385232926, 8730517168027789740}))

		v, err = ConvertToArrayValue(ArrayUUID, "[\"1e88a975-3d26-4277-ace9-bea91b072977\",\"1e88a975-3d26-4277-ace9-bea91b072978\",\"1e88a975-3d26-4277-ace9-bea91b072979\"]")
		Ω(err).Should(BeNil())
		res = v.(*ArrayValue)
		Ω(res.GetLength()).Should(Equal(3))
		Ω(res.DataType).Should(Equal(UUID))
		Ω(res.Items[0].([2]uint64)).Should(Equal([2]uint64{8593473084385232926, 8586401979951933868}))
		Ω(res.Items[2].([2]uint64)).Should(Equal([2]uint64{8593473084385232926, 8730517168027789740}))

		// geopoint
		v, err = ConvertToArrayValue(ArrayGeoPoint, "[\"Point(180.0, 90.0)\",\"Point(179.0, 89.0)\",\"Point(178.0, 88.0)\"]")
		Ω(err).Should(BeNil())
		res = v.(*ArrayValue)
		Ω(res.GetLength()).Should(Equal(3))
		Ω(res.DataType).Should(Equal(GeoPoint))
		Ω(res.Items[0].([2]float32)).Should(Equal([2]float32{90.0, 180.0}))
		Ω(res.Items[2].([2]float32)).Should(Equal([2]float32{88.0, 178.0}))
	})
})
