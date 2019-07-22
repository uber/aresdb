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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/uber/aresdb/utils"
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

// CompareUUID compare UUID values
func CompareUUID(a, b unsafe.Pointer) int {
	uuid1 := *(*[2]uint64)(a)
	uuid2 := *(*[2]uint64)(b)
	var res int
	if res = int(uuid1[0] - uuid2[0]); res == 0 {
		res = int(uuid1[1] - uuid2[1])
	}
	return res
}

// CompareGeoPoint compare GeoPoint Values
func CompareGeoPoint(a, b unsafe.Pointer) int {
	point1 := *(*[2]float32)(a)
	point2 := *(*[2]float32)(b)
	var val float32
	if val = point1[0] - point2[0]; val == 0 {
		val = point1[1] - point2[1]
	}
	if val == 0 {
		return 0
	} else if val > 0 {
		return 1
	}
	return -1
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
	case UUID:
		return CompareUUID
	case GeoPoint:
		return CompareGeoPoint
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

// Array value representation in Go for UpsertBatch
type ArrayValue struct {
	// item data type
	DataType DataType
	// item list
	Items []interface{}
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
	default:
		if IsArrayType(dataType) {
			reader := NewArrayValueReader(dataType, v1.OtherVal)
			num := reader.GetLength()
			arrVal := make([]interface{}, num)

			for i := 0; i < int(num); i++ {
				if reader.IsValid(i) {
					var dataValue DataValue
					if reader.itemType == Bool {
						dataValue = DataValue{
							DataType: reader.itemType,
							Valid:    true,
							IsBool:   true,
							BoolVal:  reader.GetBool(i),
						}
					} else {
						dataValue = DataValue{
							DataType: reader.itemType,
							Valid:    true,
							OtherVal: reader.Get(i),
						}
					}
					arrVal[i] = dataValue.ConvertToHumanReadable(reader.itemType)
				} else {
					arrVal[i] = nil
				}
			}
			bytes, _ := json.Marshal(arrVal)
			return string(bytes)
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
		if IsArrayType(dataType) {
			var value interface{}
			value, err = ArrayValueFromString(str, GetItemDataType(dataType))
			if err != nil {
				err = utils.StackError(err, "Failed to read array string: %s", str)
				return
			}
			arrayValue := value.(*ArrayValue)
			bytes := arrayValue.GetSerBytes()
			buffer := make([]byte, bytes)
			valueWriter := utils.NewBufferWriter(buffer)
			err = arrayValue.Write(&valueWriter)
			if err != nil {
				err = utils.StackError(err, "Unable to write array value to buffer: %s", str)
				return
			}

			val.Valid = true
			val.OtherVal = unsafe.Pointer(&buffer[0])
			return
		}
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

// GetLength return item numbers for the array value
func (av *ArrayValue) GetLength() int {
	return len(av.Items)
}

// AddItem add new item into array
func (av *ArrayValue) AddItem(item interface{}) {
	av.Items = append(av.Items, item)
}

// GetSerBytes return the bytes will be used in upsertbatch serialized format
func (av *ArrayValue) GetSerBytes() int {
	return CalculateListElementBytes(av.DataType, av.GetLength())
}

// NewArrayValue create a new ArrayValue instance
func NewArrayValue(dataType DataType) *ArrayValue {
	return &ArrayValue{
		DataType: dataType,
		Items:    make([]interface{}, 0),
	}
}

// Write serialize data into writer
// Serialized Array data format:
// number of items: 4 bytes
// item values: per item bytes * number of items, align to byte
// item validity:  1 bit * number of items
// final align to 8 bytes
func (av *ArrayValue) Write(writer *utils.BufferWriter) error {
	num := av.GetLength()
	err := writer.AppendUint32(uint32(num))
	if err != nil {
		return err
	}
	// add value for each item
	for _, val := range av.Items {
		switch av.DataType {
		case Bool:
			if val == nil {
				err = writer.AppendBool(false)
			} else {
				err = writer.AppendBool(val.(bool))
			}
		case Int8:
			if val == nil {
				err = writer.AppendInt8(0)
			} else {
				err = writer.AppendInt8(val.(int8))
			}
		case Uint8, SmallEnum:
			if val == nil {
				err = writer.AppendUint8(0)
			} else {
				err = writer.AppendUint8(val.(uint8))
			}
		case Int16:
			if val == nil {
				err = writer.AppendInt16(0)
			} else {
				err = writer.AppendInt16(val.(int16))
			}
		case Uint16, BigEnum:
			if val == nil {
				err = writer.AppendUint16(0)
			} else {
				err = writer.AppendUint16(val.(uint16))
			}
		case Int32:
			if val == nil {
				err = writer.AppendInt32(0)
			} else {
				err = writer.AppendInt32(val.(int32))
			}
		case Uint32:
			if val == nil {
				err = writer.AppendUint32(0)
			} else {
				err = writer.AppendUint32(val.(uint32))
			}
		case Float32:
			if val == nil {
				err = writer.AppendFloat32(0)
			} else {
				err = writer.AppendFloat32(val.(float32))
			}
		case Int64:
			if val == nil {
				err = writer.AppendInt64(0)
			} else {
				err = writer.AppendInt64(val.(int64))
			}
		case UUID:
			if val == nil {
				err = writer.AppendUint64(0)
				if err == nil {
					err = writer.AppendUint64(0)
				}
			} else {
				err := writer.AppendUint64(val.([2]uint64)[0])
				if err == nil {
					err = writer.AppendUint64(val.([2]uint64)[1])
				}
			}
		case GeoPoint:
			if val == nil {
				err = writer.AppendFloat32(0)
				if err == nil {
					err = writer.AppendFloat32(0)
				}
			} else {
				err := writer.AppendFloat32(val.([2]float32)[0])
				if err == nil {
					err = writer.AppendFloat32(val.([2]float32)[1])
				}
			}
		}
		if err != nil {
			return err
		}
	}
	writer.AlignBytes(1)

	// add validity bit for each item
	for _, val := range av.Items {
		if val == nil {
			err = writer.AppendBool(false)
		} else {
			err = writer.AppendBool(true)
		}
		if err != nil {
			return err
		}
	}
	writer.AlignBytes(8)

	return nil
}

// ArrayValueReader is an aux class to reader item data from bytes buffer
type ArrayValueReader struct {
	itemType DataType
	value    unsafe.Pointer
	length   int
}

// NewArrayValueReader is to create ArrayValueReader to read from upsertbatch, which includes the item number
func NewArrayValueReader(dataType DataType, value unsafe.Pointer) *ArrayValueReader {
	length := *((*uint32)(value))
	return &ArrayValueReader{
		itemType: GetItemDataType(dataType),
		value:    unsafe.Pointer(uintptr(value) + 4),
		length:   int(length),
	}
}

// GetLength return item numbers inside the array
func (reader *ArrayValueReader) GetLength() int {
	return reader.length
}

// GetBool returns bool value for Bool item type at index
func (reader *ArrayValueReader) GetBool(index int) bool {
	if index < 0 || index >= reader.length {
		return false
	}
	val := *(*byte)(unsafe.Pointer(uintptr(reader.value) + uintptr(index/8)))
	return val&(0x1<<uint8(index%8)) != 0x0
}

// Get returns the buffer pointer for the index-th item
func (reader *ArrayValueReader) Get(index int) unsafe.Pointer {
	if index < 0 || index >= reader.length {
		return nil
	}
	return unsafe.Pointer(uintptr(reader.value) + uintptr(index*DataTypeBytes(reader.itemType)))
}

// IsValid check if the item in index-th place is valid or not
func (reader *ArrayValueReader) IsValid(index int) bool {
	nilOffset := CalculateListNilOffset(reader.itemType, int(reader.length))
	nilByte := *(*byte)(unsafe.Pointer(uintptr(reader.value) + uintptr(nilOffset) + uintptr(index/8)))
	return nilByte&(0x1<<uint8(index%8)) != 0x0
}

// CalculateListElementBytes returns the total size in bytes needs to be allocated for a list type column for a single
// row along with the validity vector start.
func CalculateListElementBytes(dataType DataType, length int) int {
	if length == 0 {
		return 0
	}
	// there is a item number at beginning
	// element_number_bits => 8 * 4 (4 bytes)
	// DataTypeBits(dataType) * length => element_bits, round to byte
	// 1 * length => null bits, round to byte
	// (element_number_bits + element_bits + null_bits + 63) / 64 => round by 64 bits (8 bytes)
	return (4*8 + (DataTypeBits(dataType)*length+7)/8*8 + (length+7)/8*8 + 63) / 64 * 8
}

func CalculateListNilOffset(dataType DataType, length int) int {
	return (DataTypeBits(dataType)*length + 7) / 8
}
