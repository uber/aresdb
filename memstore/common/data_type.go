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
	"fmt"
	"github.com/satori/go.uuid"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"unsafe"

	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/utils"
)

// DataType is the type of value supported in gforcedb.
type DataType uint32

// The list of supported DataTypes.
// DataType & 0x0000FFFF: The width of the data type in bits.
// DataType & 0x00FF0000 >> 16: The base type of the enum.
// DataType & 0xFF000000 >> 24: Reserved for supporting variable length values (array).
// See https://github.com/uber/aresdb/wiki/redologs for more details.
const (
	Unknown   DataType = 0x00000000
	Bool      DataType = 0x00000001
	Int8      DataType = 0x00010008
	Uint8     DataType = 0x00020008
	Int16     DataType = 0x00030010
	Uint16    DataType = 0x00040010
	Int32     DataType = 0x00050020
	Uint32    DataType = 0x00060020
	Float32   DataType = 0x00070020
	SmallEnum DataType = 0x00080008
	BigEnum   DataType = 0x00090010
	UUID      DataType = 0x000a0080
	GeoPoint  DataType = 0x000b0040
	GeoShape  DataType = 0x000c0000
	Int64     DataType = 0x000d0040
)

// DataTypeName returns the literal name of the data type.
var DataTypeName = map[DataType]string{
	Unknown:   "Unknown",
	Bool:      metaCom.Bool,
	Int8:      metaCom.Int8,
	Uint8:     metaCom.Uint8,
	Int16:     metaCom.Int16,
	Uint16:    metaCom.Uint16,
	Int32:     metaCom.Int32,
	Uint32:    metaCom.Uint32,
	Float32:   metaCom.Float32,
	SmallEnum: metaCom.SmallEnum,
	BigEnum:   metaCom.BigEnum,
	UUID:      metaCom.UUID,
	GeoPoint:  metaCom.GeoPoint,
	GeoShape:  metaCom.GeoShape,
	Int64:     metaCom.Int64,
}

// StringToDataType maps string representation to DataType
var StringToDataType = map[string]DataType{
	metaCom.Bool:      Bool,
	metaCom.Int8:      Int8,
	metaCom.Uint8:     Uint8,
	metaCom.Int16:     Int16,
	metaCom.Uint16:    Uint16,
	metaCom.Int32:     Int32,
	metaCom.Uint32:    Uint32,
	metaCom.Float32:   Float32,
	metaCom.SmallEnum: SmallEnum,
	metaCom.BigEnum:   BigEnum,
	metaCom.UUID:      UUID,
	metaCom.GeoPoint:  GeoPoint,
	metaCom.GeoShape:  GeoShape,
	metaCom.Int64:     Int64,
}

// NewDataType converts an uint32 value into a DataType. It returns error if the the data type is
// invalid.
func NewDataType(value uint32) (DataType, error) {
	ret := DataType(value)
	switch ret {
	case Bool:
	case Int8:
	case Uint8:
	case Int16:
	case Uint16:
	case Int32:
	case Uint32:
	case Int64:
	case Float32:
	case SmallEnum:
	case BigEnum:
	case UUID:
	case GeoPoint:
	case GeoShape:
	default:
		return Unknown, utils.StackError(nil, "Invalid data type value %#x", value)
	}
	return ret, nil
}

// IsNumeric determines whether a data type is numeric
func IsNumeric(dataType DataType) bool {
	return (dataType >= Int8 && dataType <= Float32) || dataType == Int64
}

// DataTypeBits returns the number of bits of a data type.
func DataTypeBits(dataType DataType) int {
	return int(0x0000FFFF & dataType)
}

// DataTypeForColumn returns the in memory data type for a column
func DataTypeForColumn(column metaCom.Column) DataType {
	dataType := DataTypeFromString(column.Type)
	if column.HLLConfig.IsHLLColumn {
		return Uint32
	}
	return dataType
}

// DataTypeFromString convert string representation of data type into DataType
func DataTypeFromString(str string) DataType {
	if dataType, exist := StringToDataType[str]; exist {
		return dataType
	}
	return Unknown
}

// DataTypeBytes returns how many bytes a value of the data type occupies.
func DataTypeBytes(dataType DataType) int {
	return (DataTypeBits(dataType) + 7) / 8
}

// ConvertValueForType converts data value based on data type
func ConvertValueForType(dataType DataType, value interface{}) (interface{}, error) {
	ok := false
	var out interface{}
	switch dataType {
	case Bool:
		out, ok = ConvertToBool(value)
	case SmallEnum:
		fallthrough
	case Uint8:
		out, ok = ConvertToUint8(value)
	case Int8:
		out, ok = ConvertToInt8(value)
	case Int16:
		out, ok = ConvertToInt16(value)
	case BigEnum:
		fallthrough
	case Uint16:
		out, ok = ConvertToUint16(value)
	case Uint32:
		out, ok = ConvertToUint32(value)
	case Int32:
		out, ok = ConvertToInt32(value)
	case Int64:
		out, ok = ConvertToInt64(value)
	case Float32:
		out, ok = ConvertToFloat32(value)
	case UUID:
		out, ok = ConvertToUUID(value)
	case GeoPoint:
		out, ok = ConvertToGeoPoint(value)
	case GeoShape:
		out, ok = ConvertToGeoShape(value)
	}
	if !ok {
		return nil, utils.StackError(nil, "Invalid data value %v for data type %s", value, DataTypeName[dataType])
	}
	return out, nil
}

// ConvertToBool convert input into bool at best effort
func ConvertToBool(value interface{}) (bool, bool) {
	if v, ok := value.(bool); ok {
		return v, ok
	}

	// try converting "true" "false"
	if v, ok := value.(string); ok {
		if strings.ToLower(v) == "true" {
			return true, true
		} else if strings.ToLower(v) == "false" {
			return false, true
		}
	}

	// try convert as number
	v, ok := ConvertToInt8(value)
	if ok {
		if v == 1 {
			return true, true
		} else if v == 0 {
			return false, true
		}
	}

	return false, false
}

// ConvertToInt8 convert input into int8 at best effort
func ConvertToInt8(value interface{}) (int8, bool) {
	if v, ok := ConvertToInt64(value); ok {
		if !reflect.ValueOf(int8(0)).OverflowInt(v) {
			return int8(v), ok
		}
	}
	return 0, false
}

// ConvertToUint8 convert input into uint8 at best effort
func ConvertToUint8(value interface{}) (uint8, bool) {
	if v, ok := ConvertToUint64(value); ok {
		if !reflect.ValueOf(uint8(0)).OverflowUint(v) {
			return uint8(v), ok
		}
	}
	return 0, false
}

// ConvertToInt16 convert input into int16 at best effort
func ConvertToInt16(value interface{}) (int16, bool) {
	if v, ok := ConvertToInt64(value); ok {
		if !reflect.ValueOf(int16(0)).OverflowInt(v) {
			return int16(v), ok
		}
	}
	return 0, false
}

// ConvertToUint16 convert input into uint16 at best effort
func ConvertToUint16(value interface{}) (uint16, bool) {
	if v, ok := ConvertToUint64(value); ok {
		if !reflect.ValueOf(uint16(0)).OverflowUint(v) {
			return uint16(v), ok
		}
	}
	return 0, false
}

// ConvertToInt32 convert input into int32 at best effort
func ConvertToInt32(value interface{}) (int32, bool) {
	if v, ok := ConvertToInt64(value); ok {
		if !reflect.ValueOf(int32(0)).OverflowInt(v) {
			return int32(v), ok
		}
	}
	return 0, false
}

// ConvertToUint32 convert input into uint32 at best effort
func ConvertToUint32(value interface{}) (uint32, bool) {
	if v, ok := ConvertToUint64(value); ok {
		if !reflect.ValueOf(uint32(0)).OverflowUint(v) {
			return uint32(v), ok
		}
	}
	return 0, false
}

// ConvertToFloat32 convert input into float32 at best effort
func ConvertToFloat32(value interface{}) (float32, bool) {
	if v, ok := ConvertToFloat64(value); ok {
		if !reflect.ValueOf(float32(0)).OverflowFloat(v) {
			return float32(v), true
		}
	}
	return 0, false
}

// ConvertToUint64 convert input into uint64 at best effort
func ConvertToUint64(value interface{}) (uint64, bool) {
	switch v := value.(type) {
	case int, uint, int8, uint8, int16, uint16, int32, uint32, int64, uint64, float32, float64:
		num := reflect.ValueOf(value).Convert(reflect.TypeOf(uint64(0))).Uint()
		return num, true
	case string:
		num, err := strconv.ParseUint(v, 10, 64)
		if err == nil {
			return num, true
		}
	}
	return 0, false
}

// ConvertToInt64 convert input into int64 at best effort
func ConvertToInt64(value interface{}) (int64, bool) {
	switch v := value.(type) {
	case int, uint, int8, uint8, int16, uint16, int32, uint32, int64, uint64, float32, float64:
		num := reflect.ValueOf(value).Convert(reflect.TypeOf(int64(0))).Int()
		return num, true
	case string:
		num, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			return num, true
		}
	}
	return 0, false
}

// ConvertToFloat64 convert input into float64 at best effort
func ConvertToFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case int, uint, int8, uint8, int16, uint16, int32, uint32, int64, uint64, float32, float64:
		num := reflect.ValueOf(value).Convert(reflect.TypeOf(float64(0))).Float()
		return num, !math.IsInf(num, 0) && !math.IsNaN(num)
	case string:
		num, err := strconv.ParseFloat(v, 64)
		if err == nil {
			return num, true
		}
	}
	return float64(0), false
}

// ConvertToUUID convert input into uuid type ([2]uint64) at best effort
func ConvertToUUID(value interface{}) ([2]uint64, bool) {
	switch v := value.(type) {
	case [2]uint64:
		return v, true
	case []byte:
		if len(v) == 16 {
			return *(*[2]uint64)(unsafe.Pointer(&v[0])), true
		}
		return [2]uint64{}, false
	case string:
		u, err := uuid.FromString(string(v))
		if err != nil {
			return [2]uint64{}, false
		}
		bytes := u.Bytes()
		return *(*[2]uint64)(unsafe.Pointer(&bytes[0])), true
	}

	return [2]uint64{}, false
}

// GeoPointFromString convert string to geopoint
// we support wkt format, eg. Point(lng,lat)
// Inside gforcedb system we store lat,lng format
func GeoPointFromString(str string) (point [2]float32, err error) {
	lngLatStrs := strings.Fields(strings.NewReplacer("p", "", "o", "", "i", "", "n", "", "t", "", "(", "", ")", "", ",", " ").Replace(strings.ToLower(str)))
	if len(lngLatStrs) != 2 {
		err = fmt.Errorf("invalid point, requires format: Point(lng,lat), got %s", str)
		return
	}

	var lng, lat float64
	lng, err = strconv.ParseFloat(lngLatStrs[0], 32)
	if err != nil || lng < -180 || lng > 180 {
		err = utils.StackError(err, "invalid point, longitude should be float number in [-180, 180], got %s", lngLatStrs[0])
		return
	}
	lat, err = strconv.ParseFloat(lngLatStrs[1], 32)
	if err != nil || lat < -90 || lat > 90 {
		err = utils.StackError(err, "invalid point, latitude should be float number in [-90, 90], got %s", lngLatStrs[1])
		return
	}
	return [2]float32{float32(lat), float32(lng)}, nil
}

// ConvertToGeoPoint convert input into uuid type ([2]float32) at best effort
func ConvertToGeoPoint(value interface{}) ([2]float32, bool) {
	switch v := value.(type) {
	case string:
		point, err := GeoPointFromString(v)
		if err == nil {
			return point, true
		}
	case [2]float32:
		return v, true
	case [2]float64:
		return [2]float32{float32(v[0]), float32(v[1])}, true
	}
	return [2]float32{}, false
}

// GeoShapeFromString convert string to geoshape
// Supported format POLYGON ((lng lat, lng lat, lng lat, ...), (...))
func GeoShapeFromString(str string) (GeoShapeGo, error) {
	charsToTrim := "polygon() "
	polygonStrs := regexp.MustCompile(`\),\s*\(`).Split(strings.TrimFunc(strings.ToLower(str), func(r rune) bool {
		return strings.IndexRune(charsToTrim, r) >= 0
	}), -1)

	shape := GeoShapeGo{}
	shape.Polygons = make([][]GeoPointGo, 0, len(polygonStrs))
	for _, polygonStr := range polygonStrs {
		lngLatPairs := strings.Split(polygonStr, ",")
		polygon := make([]GeoPointGo, 0, len(lngLatPairs))
		for _, lngLatPair := range lngLatPairs {
			lngLat := strings.Fields(lngLatPair)
			if len(lngLat) != 2 {
				return GeoShapeGo{}, utils.StackError(nil, "invalid point format %s", lngLatPair)
			}
			lng, err := strconv.ParseFloat(lngLat[0], 32)
			if err != nil || lng < -180 || lng > 180 {
				return GeoShapeGo{}, utils.StackError(err, "invalid longitude, expect float number in [-180, 180], got %s", lngLat[0])
			}
			lat, err := strconv.ParseFloat(lngLat[1], 32)
			if err != nil || lat < -90 || lat > 90 {
				return GeoShapeGo{}, utils.StackError(err, "invalid latitude, expect float number in [-90, 90], got %s", lngLat[1])
			}
			point := GeoPointGo{float32(lat), float32(lng)}
			polygon = append(polygon, point)
		}
		shape.Polygons = append(shape.Polygons, polygon)
	}
	return shape, nil
}

// ConvertToGeoShape converts the arbitrary value to GeoShapeGo
func ConvertToGeoShape(value interface{}) (*GeoShapeGo, bool) {
	switch v := value.(type) {
	case string:
		shape, err := GeoShapeFromString(v)
		if err == nil {
			return &shape, true
		}
	case []byte:
		shape := GeoShapeGo{}
		dataReader := utils.NewStreamDataReader(bytes.NewReader(v))
		err := shape.Read(&dataReader)
		if err == nil {
			return &shape, true
		}
	}
	return nil, false
}

// IsGoType determines whether a data type is golang type
func IsGoType(dataType DataType) bool {
	// for now we only have GeoShape
	return dataType == GeoShape
}

// IsEnumType determines whether a data type is enum type
func IsEnumType(dataType DataType) bool {
	return dataType == SmallEnum || dataType == BigEnum
}

// GetGoDataValue return GoDataValue
func GetGoDataValue(dataType DataType) GoDataValue {
	switch dataType {
	case GeoShape:
		return &GeoShapeGo{}
	}
	return nil
}
