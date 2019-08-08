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

package vectors

// #include <stdlib.h>
// #include <string.h>

import (
	"github.com/uber/aresdb/cgoutils"
	"github.com/uber/aresdb/memstore/common"
	"math"
	"unsafe"

	"fmt"
	"github.com/uber/aresdb/utils"
)

// Vector stores a batch of columnar data (values, nulls, or counts) for a column.
type Vector struct {
	// The data type of the value stored in the vector.
	DataType common.DataType
	CmpFunc  common.CompareFunc

	// Max number of values that can be stored in the vector.
	Size int
	// Allocated size of the vector in bytes.
	Bytes int

	// Number of bits occupied per unit, possible values: 1, 8, 16, 32, 64, 128.
	unitBits int
	// Pointer to the vector buffer.
	buffer uintptr

	// **All following fields only works for live batch's vectors.**

	// Min and Max values seen, only used for time columns of fact tables.
	minValue uint32
	maxValue uint32
	// Number of trues in a bool typed vector.
	numTrues int
}

// NewVector creates a vector with the specified bits per unit and size(capacity).
// The majority of its storage space is managed in C.
func NewVector(dataType common.DataType, size int) *Vector {
	unitBits := common.DataTypeBits(dataType)
	bytes := CalculateVectorBytes(dataType, size)

	buffer := cgoutils.HostAlloc(bytes)

	return &Vector{
		DataType: dataType,
		CmpFunc:  common.GetCompareFunc(dataType),
		unitBits: unitBits,
		Size:     size,
		Bytes:    bytes,
		buffer:   uintptr(buffer),
		minValue: math.MaxUint32,
	}
}

// CalculateVectorBytes calculates bytes the vector will occupy given data type and size without actual allocation.
func CalculateVectorBytes(dataType common.DataType, size int) int {
	unitBits := common.DataTypeBits(dataType)
	bits := unitBits * size
	// Round up to 512 bits (64 bytes).
	remainder := bits % 512
	if remainder > 0 {
		bits += 512 - remainder
	}
	return bits / 8
}

// CalculateVectorPartyBytes calculates bytes the vector party will occupy.
// Note: data type supported in go memory will report memory usage when value is actually set, therefore report 0 here
func CalculateVectorPartyBytes(dataType common.DataType, size int, hasNulls bool, hasCounts bool) int {
	if common.IsGoType(dataType) {
		// batchSize * size of golang pointer
		return size * 8
	}

	if common.IsArrayType(dataType) {
		// this only calculates the offset and caps for list live vector party, value vector is controlled inside vp
		// list archive vector party can not use this either
		offsets := CalculateVectorBytes(common.Uint32, 2*size)
		caps := CalculateVectorBytes(common.Uint32, size)
		return offsets + caps
	}

	bytes := CalculateVectorBytes(dataType, size)

	if hasNulls {
		bytes += CalculateVectorBytes(common.Bool, size)
	}

	if hasCounts {
		bytes += CalculateVectorBytes(common.Uint32, size+1)
	}

	return bytes
}

// SafeDestruct destructs this vector's storage space managed in C.
func (v *Vector) SafeDestruct() {
	if v != nil {
		cgoutils.HostFree(unsafe.Pointer(v.buffer))
	}
}

// Buffer returns the pointer to the underlying buffer.
func (v *Vector) Buffer() unsafe.Pointer {
	return unsafe.Pointer(v.buffer)
}

// SetBool sets the bool value for the specified index.
func (v *Vector) SetBool(index int, value bool) {
	if index >= v.Size {
		panic(fmt.Sprintf("SetBool index %d access out of bound %d", index, v.Size))
	}

	wordOffset := uintptr(index / 32 * 4)
	localBit := uint(index % 32)
	wordAddr := (*uint32)(unsafe.Pointer(v.buffer + wordOffset))

	oldValue := *wordAddr
	if value {
		*wordAddr |= 1 << localBit
	} else {
		*wordAddr &^= 1 << localBit
	}
	if *wordAddr != oldValue {
		if value {
			v.numTrues++
		} else {
			v.numTrues--
		}
	}
}

// SetValue sets the data value for the specified index.
// index bound is not checked!
// data points to a buffer (in UpsertBatch for instance) that contains the value to be set.
func (v *Vector) SetValue(index int, data unsafe.Pointer) {
	if index >= v.Size {
		panic(fmt.Sprintf("SetValue index %d access out of bound %d", index, v.Size))
	}
	idx := uintptr(index)
	switch v.unitBits {
	case 8:
		*(*uint8)(unsafe.Pointer(v.buffer + idx)) = *(*uint8)(data)
	case 16:
		*(*uint16)(unsafe.Pointer(v.buffer + idx*2)) = *(*uint16)(data)
	case 32:
		value := *(*uint32)(data)
		*(*uint32)(unsafe.Pointer(v.buffer + idx*4)) = value
		if value > v.maxValue {
			v.maxValue = value
		}
		if value < v.minValue {
			v.minValue = value
		}
	case 64:
		value := *(*uint64)(data)
		*(*uint64)(unsafe.Pointer(v.buffer + idx*8)) = value
	case 128:
		*(*uint64)(unsafe.Pointer(v.buffer + idx*16)) = *(*uint64)(data)
		*(*uint64)(unsafe.Pointer(v.buffer + idx*16 + 8)) = *(*uint64)(unsafe.Pointer(uintptr(data) + 8))
	}
}

// GetBool returns the bool value for the specified index.
// index bound is not checked!
func (v *Vector) GetBool(index int) bool {
	wordOffset := uintptr(index / 32 * 4)
	localBit := uint(index % 32)
	wordAddr := (*uint32)(unsafe.Pointer(v.buffer + wordOffset))
	return *wordAddr&(1<<localBit) != 0
}

// GetValue returns the data value for the specified index.
// index bound is not checked!
// The return value points to the internal buffer location that stores the value.
func (v *Vector) GetValue(index int) unsafe.Pointer {
	return unsafe.Pointer(v.buffer + uintptr(v.unitBits/8*index))
}

// GetMinValue return the min value of the Vector Party
func (v *Vector) GetMinValue() uint32 {
	return v.minValue
}

// GetMaxValue return the max value of the Vector Party
func (v *Vector) GetMaxValue() uint32 {
	return v.maxValue
}

// LowerBound returns the index of the first element in vector[first, last) that is greater or equal
// to the given value. The result is only valid if vector[first, last) is fully sorted in ascendant
// order. If all values in the given range is less than the given value, LowerBound
// returns last.
// Note that first/last is not checked against vector bound.
func (v *Vector) LowerBound(first int, last int, value unsafe.Pointer) int {
	for first < last {
		mid := (last + first) / 2
		cmpRes := 0
		if v.DataType == common.Bool {
			cmpRes = common.CompareBool(v.GetBool(mid), *(*uint32)(value) != 0)
		} else {
			cmpRes = v.CmpFunc(v.GetValue(mid), value)
		}

		if cmpRes >= 0 {
			last = mid
		} else {
			first = mid + 1
		}
	}
	return first
}

// UpperBound returns the index of the first element in vector[first, last) that is greater than the
// given value. The result is only valid if vector[first, last) is fully sorted in ascendant
// order. If all values in the given range is less than the given value, LowerBound returns last.
// Note that first/last is not checked against vector bound.
func (v *Vector) UpperBound(first int, last int, value unsafe.Pointer) int {
	for first < last {
		mid := (last + first) / 2

		cmpRes := 0
		if v.DataType == common.Bool {
			cmpRes = common.CompareBool(v.GetBool(mid), *(*uint32)(value) != 0)
		} else {
			cmpRes = v.CmpFunc(v.GetValue(mid), value)
		}

		if cmpRes > 0 {
			last = mid
		} else {
			first = mid + 1
		}
	}
	return first
}

// GetSliceBytesAligned calculate the number of bytes of a slice of the vector,
// represented by [lowerBound, upperBound),
// aligned to 64-byte
// return the buffer pointer, new start index (start entry in vector), and length in bytes
func (v *Vector) GetSliceBytesAligned(lowerBound int, upperBound int) (buffer unsafe.Pointer, startIndex int, bytes int) {
	if v == nil || lowerBound == upperBound {
		return
	}
	// find the latest 64-byte aligned boundary
	startByte := v.unitBits * lowerBound / 8
	startByte -= startByte % 64
	startIndex = lowerBound - startByte*8/v.unitBits
	endByte := (v.unitBits*upperBound + 7) / 8
	bytes = utils.AlignOffset(endByte-startByte, 64)
	return utils.MemAccess(v.Buffer(), startByte), startIndex, bytes
}

// SetAllValid set all bits to be 1 in a bool typed vector.
func (v *Vector) SetAllValid() {
	// buffer are 64 bytes aligned so we can assign word by word.
	for i := 0; i < v.Bytes; i += 4 {
		*(*uint32)(utils.MemAccess(unsafe.Pointer(v.buffer), i)) = 0xFFFFFFFF
	}
}

// CheckAllValid checks whether all bits are 1 in a bool typed vector.
func (v *Vector) CheckAllValid() bool {
	i := 0
	totalCompleteBytes := v.Size / 8
	remainingBits := v.Size % 8
	wordBoundary := totalCompleteBytes - totalCompleteBytes%4

	// First check word by word.
	for ; i < wordBoundary; i += 4 {
		if !(*(*uint32)(utils.MemAccess(unsafe.Pointer(v.buffer), i)) == 0xFFFFFFFF) {
			return false
		}
	}

	// then we check byte by byte.
	for ; i < totalCompleteBytes; i++ {
		if !(*(*uint8)(utils.MemAccess(unsafe.Pointer(v.buffer), i)) == 0xFF) {
			return false
		}
	}

	if remainingBits == 0 {
		return true
	}

	// check remaining bits.
	var mask uint8 = (1 << uint(remainingBits)) - 1
	return ((*(*uint8)(utils.MemAccess(unsafe.Pointer(v.buffer), i))) & mask) == mask
}
