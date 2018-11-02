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
	"github.com/uber/aresdb/utils"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"unsafe"
)

// NullDataValue is a global data value that stands a null value where the newly added
// columns haven't received any data.
var NullDataValue = DataValue{}

// SizeOfGeoPoint is the size of GeoPointGo in memory
const SizeOfGeoPoint = unsafe.Sizeof(GeoPointGo{})

// CompareFunc represents compare function
type CompareFunc func(a, b unsafe.Pointer) int

// CompareBool compares boolean value
func CompareBool(a, b bool) int {
	if a != b {
		if a {
			return 1
		}
		return -1
	}
	return 0
}

// CompareInt8 compares int8 value
func CompareInt8(a, b unsafe.Pointer) int {
	return int(*(*int8)(a)) - int(*(*int8)(b))
}

// CompareUint8 compares uint8 value
func CompareUint8(a, b unsafe.Pointer) int {
	return int(*(*uint8)(a)) - int(*(*uint8)(b))
}

// CompareInt16 compares int16 value
func CompareInt16(a, b unsafe.Pointer) int {
	return int(*(*int16)(a)) - int(*(*int16)(b))
}

// CompareUint16 compares uint16 value
func CompareUint16(a, b unsafe.Pointer) int {
	return int(*(*uint16)(a)) - int(*(*uint16)(b))
}

// CompareInt32 compares int32 value
func CompareInt32(a, b unsafe.Pointer) int {
	return int(*(*int32)(a)) - int(*(*int32)(b))
}

// CompareUint32 compares uint32 value
func CompareUint32(a, b unsafe.Pointer) int {
	return int(*(*uint32)(a)) - int(*(*uint32)(b))
}

// CompareInt64 compares int64 value
func CompareInt64(a, b unsafe.Pointer) int {
	return int(*(*int64)(a)) - int(*(*int64)(b))
}

// CompareFloat32 compares float32 value
func CompareFloat32(a, b unsafe.Pointer) int {
	fa := *(*float32)(a)
	fb := *(*float32)(b)
	if fa < fb {
		return -1
	} else if fa == fb {
		return 0
	} else {
		return 1
	}
}

// GetCompareFunc get the compare function for specific data type
func GetCompareFunc(dataType DataType) CompareFunc {
	switch dataType {
	case Int8:
		return CompareInt8
	case Uint8, SmallEnum:
		return CompareUint8
	case Int16:
		return CompareInt16
	case Uint16, BigEnum:
		return CompareUint16
	case Int32:
		return CompareInt32
	case Uint32:
		return CompareUint32
	case Int64:
		return CompareInt64
	case Float32:
		return CompareFloat32
	}
	return nil
}

// GoDataValue represents a value backed in golang memory
type GoDataValue interface {
	// GetBytes returns number of bytes copied in golang memory for this value
	GetBytes() int
	// GetSerBytes return the number of bytes required for serialize this value
	GetSerBytes() int
	Write(writer *utils.StreamDataWriter) error
	Read(reader *utils.StreamDataReader) error
}

// DataValue is the wrapper to encapsulate validity, bool value and other value type
// into a single struct to make it easier for value comparison.
type DataValue struct {
	// Used for golang vector party
	GoVal    GoDataValue
	OtherVal unsafe.Pointer
	DataType DataType
	CmpFunc  CompareFunc
	Valid    bool

	IsBool  bool
	BoolVal bool
}

// GeoPointGo represents GeoPoint Golang Type
type GeoPointGo [2]float32

// GeoShapeGo represents GeoShape Golang Type
type GeoShapeGo struct {
	Polygons [][]GeoPointGo
}

// Compare compares two value wrapper.
func (v1 DataValue) Compare(v2 DataValue) int {
	if !v1.Valid || !v2.Valid {
		return CompareBool(v1.Valid, v2.Valid)
	}
	if v1.IsBool {
		return CompareBool(v1.BoolVal, v2.BoolVal)
	}
	if v1.CmpFunc != nil {
		return v1.CmpFunc(v1.OtherVal, v2.OtherVal)
	}
	return 0
}

// ConvertToHumanReadable convert DataValue to meaningful golang data types
func (v1 DataValue) ConvertToHumanReadable(dataType DataType) interface{} {
	if !v1.Valid {
		return nil
	}

	if v1.IsBool {
		return v1.BoolVal
	}

	switch dataType {
	case Int8:
		return *(*int8)(v1.OtherVal)
	case Uint8, SmallEnum:
		return *(*uint8)(v1.OtherVal)
	case Int16:
		return *(*int16)(v1.OtherVal)
	case Uint16, BigEnum:
		return *(*uint16)(v1.OtherVal)
	case Int32:
		return *(*int32)(v1.OtherVal)
	case Uint32:
		return *(*uint32)(v1.OtherVal)
	case Int64:
		return *(*int64)(v1.OtherVal)
	case Float32:
		return *(*float32)(v1.OtherVal)
	case UUID:
		bys := *(*[16]byte)(v1.OtherVal)
		uuidStr := hex.EncodeToString(bys[:])
		if len(uuidStr) == 32 {
			return fmt.Sprintf("%s-%s-%s-%s-%s",
				uuidStr[:8],
				uuidStr[8:12],
				uuidStr[12:16],
				uuidStr[16:20],
				uuidStr[20:])
		}
	case GeoPoint:
		latLngs := *(*[2]float32)(v1.OtherVal)
		// in string format, lng goes first and lat second
		return fmt.Sprintf("Point(%.4f,%.4f)", latLngs[1], latLngs[0])
	case GeoShape:
		shape, ok := (v1.GoVal).(*GeoShapeGo)
		if ok {
			polygons := make([]string, len(shape.Polygons))
			for i, points := range shape.Polygons {
				pointsStrs := make([]string, len(points))
				for j, point := range points {
					// in string format, lng goes first and lat second
					pointsStrs[j] = fmt.Sprintf("%.4f+%.4f", point[1], point[0])
				}
				polygons[i] = fmt.Sprintf("(%s)", strings.Join(pointsStrs, ","))
			}
			return fmt.Sprintf("Polygon(%s)", strings.Join(polygons, ","))
		}
	}
	return nil
}

// ValueFromString converts raw string value to actual value given input data type.
func ValueFromString(str string, dataType DataType) (val DataValue, err error) {
	val.DataType = dataType

	if len(str) == 0 || str == "null" {
		return
	}

	var b bool
	var i int64
	var f float64
	var ui uint64

	switch dataType {
	case Bool:
		val.IsBool = true
		b, err = strconv.ParseBool(str)
		if err != nil {
			err = utils.StackError(err, "")
			return
		}
		val.Valid = true
		val.BoolVal = b
		return
	case Int8:
		i, err = strconv.ParseInt(str, 10, 8)
		if err != nil {
			err = utils.StackError(err, "")
			return
		}

		// We need to convert it from i64 to i8 since strconv.ParseXXX
		// always returns the largest bit size value.
		i8 := int8(i)
		val.Valid = true
		val.OtherVal = unsafe.Pointer(&i8)
		return
	case Uint8, SmallEnum:
		ui, err = strconv.ParseUint(str, 10, 8)
		if err != nil {
			err = utils.StackError(err, "")
			return
		}
		ui8 := uint8(ui)
		val.Valid = true
		val.OtherVal = unsafe.Pointer(&ui8)
		return
	case Int16:
		i, err = strconv.ParseInt(str, 10, 16)
		if err != nil {
			err = utils.StackError(err, "")
			return
		}
		i16 := int16(i)
		val.Valid = true
		val.OtherVal = unsafe.Pointer(&i16)
		return
	case Uint16, BigEnum:
		ui, err = strconv.ParseUint(str, 10, 16)
		if err != nil {
			err = utils.StackError(err, "")
			return
		}
		ui16 := uint16(ui)
		val.Valid = true
		val.OtherVal = unsafe.Pointer(&ui16)
		return
	case Int32:
		i, err = strconv.ParseInt(str, 10, 32)
		if err != nil {
			err = utils.StackError(err, "")
			return
		}
		i32 := int32(i)
		val.Valid = true
		val.OtherVal = unsafe.Pointer(&i32)
		return
	case Uint32:
		ui, err = strconv.ParseUint(str, 10, 32)
		if err != nil {
			err = utils.StackError(err, "")
			return
		}
		ui32 := uint32(ui)
		val.Valid = true
		val.OtherVal = unsafe.Pointer(&ui32)
		return
	case Int64:
		i, err = strconv.ParseInt(str, 10, 64)
		if err != nil {
			err = utils.StackError(err, "")
			return
		}
		val.Valid = true
		val.OtherVal = unsafe.Pointer(&i)
		return
	case Float32:
		f, err = strconv.ParseFloat(str, 32)
		if err != nil {
			err = utils.StackError(err, "")
			return
		}
		f32 := float32(f)
		val.Valid = true
		val.OtherVal = unsafe.Pointer(&f32)
		return
	case UUID:
		var uuidBytes []byte
		if strings.HasPrefix(str, "0x") {
			str = str[2:]
		}
		uuidBytes, err = hex.DecodeString(strings.Replace(str, "-", "", -1))
		if err != nil || len(uuidBytes) != 16 {
			err = utils.StackError(err, "Failed to decode uuid string: %s", str)
			return
		}
		val.Valid = true
		val.OtherVal = unsafe.Pointer(&uuidBytes[0])
		return
	case GeoPoint:
		var point [2]float32
		point, err = GeoPointFromString(str)
		if err != nil {
			err = utils.StackError(err, "Failed to read geopoint string: %s", str)
			return
		}
		val.Valid = true
		val.OtherVal = unsafe.Pointer(&point[0])
		return
	default:
		err = utils.StackError(nil, "Unsupported data type value %#x", dataType)
		return
	}
}

// GetBytes implements GoDataValue interface
func (gs *GeoShapeGo) GetBytes() int {
	numBytes := 0
	for _, polygon := range gs.Polygons {
		numPoints := len(polygon)
		numBytes += numPoints * int(SizeOfGeoPoint)
	}
	return numBytes
}

// GetSerBytes implements GoDataValue interface
func (gs *GeoShapeGo) GetSerBytes() int {
	totalBytes := 0
	// 1. numPolygons (uint32)
	totalBytes += 4
	for _, polygon := range gs.Polygons {
		numPoints := len(polygon)
		// numPoints (uint32)
		totalBytes += 4
		// 8 bytes per point [2]float32
		totalBytes += numPoints * 8
	}
	return totalBytes
}

// Read implements Read interface for GoDataValue
func (gs *GeoShapeGo) Read(dataReader *utils.StreamDataReader) error {
	numPolygons, err := dataReader.ReadUint32()
	if err != nil {
		return err
	}
	gs.Polygons = make([][]GeoPointGo, numPolygons)
	for i := 0; i < int(numPolygons); i++ {
		numPoints, err := dataReader.ReadUint32()
		if err != nil {
			return err
		}
		polygon := make([]GeoPointGo, numPoints)
		allBytes := make([]byte, numPoints*8)
		err = dataReader.Read(allBytes)
		if err != nil {
			return err
		}
		offset := 0
		for j := 0; j < int(numPoints); j++ {
			lat := *(*float32)(unsafe.Pointer(&allBytes[offset]))
			lng := *(*float32)(unsafe.Pointer(&allBytes[offset+4]))
			point := GeoPointGo{lat, lng}
			polygon[j] = point
			offset += 8
		}
		gs.Polygons[i] = polygon
	}
	return dataReader.ReadPadding(int(dataReader.GetBytesRead()), 4)
}

// Write implements Read interface for GoDataValue
func (gs *GeoShapeGo) Write(dataWriter *utils.StreamDataWriter) error {
	numPolygons := len(gs.Polygons)
	err := dataWriter.WriteUint32(uint32(numPolygons))
	if err != nil {
		return err
	}
	for _, polygon := range gs.Polygons {
		numPoints := len(polygon)
		err = dataWriter.WriteUint32(uint32(numPoints))
		if err != nil {
			return err
		}
		for _, point := range polygon {
			err = dataWriter.WriteFloat32(point[0])
			if err != nil {
				return err
			}
			err = dataWriter.WriteFloat32(point[1])
			if err != nil {
				return err
			}
		}
	}
	return dataWriter.WritePadding(int(dataWriter.GetBytesWritten()), 4)
}
