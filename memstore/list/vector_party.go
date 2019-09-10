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
	"github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/memstore/vectors"
	"unsafe"
)

// baseVectorParty is the shared struct for live store list vp and archive store
// list vp. Some difference between normal base vp and list base vp:
// 1. There is no mode concept for list vp. therefore nonDefaultValueCount does not make sense for
// 	list vp.
// 2. There is no default value concept for list at least for now, so we will not store default value.
type baseVectorParty struct {
	// offset is a pair of uint32 [offset, length]. therefore its length is 2 * length of vp.
	offsets *vectors.Vector
	// length of vp.
	length int
	// DataType of values. We need it since for mode 0 vector party, we cannot
	// get data type from values vector. Also we store it on disk anyway.
	dataType common.DataType

	reporter HostMemoryChangeReporter
}

// GetOffsetLength returns the <offset, length> pair at ith row.
func (vp *baseVectorParty) GetOffsetLength(row int) (offset uint32, length uint32, valid bool) {
	if vp.offsets == nil || row < 0 || row >= vp.length {
		return
	}
	offset = *(*uint32)(vp.offsets.GetValue(2 * row))
	length = *(*uint32)(vp.offsets.GetValue(2*row + 1))
	if length == 0 && offset == 0 {
		valid = false
	} else {
		valid = true
	}
	return
}

// SetOffsetLength update offset/length for nth fow
func (vp *baseVectorParty) SetOffsetLength(row int, offset, length unsafe.Pointer, valid bool) {
	if valid {
		vp.offsets.SetValue(2*row+1, length)
		if *(*uint32)(length) == 0 {
			zeroValueOffset := common.ZeroLengthArrayFlag
			vp.offsets.SetValue(2*row, unsafe.Pointer(&zeroValueOffset))
		} else {
			vp.offsets.SetValue(2*row, offset)
		}
	} else {
		var zero uint32 = 0
		vp.offsets.SetValue(2*row, unsafe.Pointer(&zero))
		vp.offsets.SetValue(2*row+1, unsafe.Pointer(&zero))
	}
}

// GetElemCount return the number of element for value in n-th row
func (vp *baseVectorParty) GetElemCount(row int) uint32 {
	if vp.offsets == nil || row < 0 || row >= vp.length {
		return 0
	}
	return *(*uint32)(vp.offsets.GetValue(2*row + 1))
}

// GetValidity get validity of given offset.
func (vp *baseVectorParty) GetValidity(row int) bool {
	_, _, valid := vp.GetOffsetLength(row)
	return valid
}

// IsList tells whether this vp is list vp. And user can later on cast it to proper interface.
func (vp *baseVectorParty) IsList() bool {
	return true
}

// GetDataType returns the element date type of this vp.
func (vp *baseVectorParty) GetDataType() common.DataType {
	return vp.dataType
}

// GetLength returns the length of the vp.
func (vp *baseVectorParty) GetLength() int {
	return vp.length
}

// Slice vector party into human readable SlicedVector format. For now just return an
// empty slice.
// TODO(lucafuji): implement slice vector on list vp.
func (vp *baseVectorParty) Slice(startRow, numRows int) common.SlicedVector {
	return common.SlicedVector{}
}

// Check whether two vector parties are equal (used only in unit tests)
// Check common properties like data type and length.
// leftVP is vp underneath
func (vp *baseVectorParty) equals(other common.VectorParty, leftVP common.ListVectorParty) bool {
	if !other.IsList() {
		return false
	}

	if vp.GetDataType() != other.GetDataType() {
		return false
	}

	if vp.GetLength() != other.GetLength() {
		return false
	}

	rightVP := other.AsList()
	for i := 0; i < other.GetLength(); i++ {
		leftValue, leftValid := leftVP.GetListValue(i)
		rightValue, rightValid := rightVP.GetListValue(i)
		if !leftValid && !rightValid {
			continue
		}
		if leftValid != rightValid || !arrayValueCompare(vp.GetDataType(), leftValue, rightValue) {
			return false
		}
	}
	return true
}

func arrayValueCompare(dataType common.DataType, left, right unsafe.Pointer) bool {
	lReader := common.NewArrayValueReader(dataType, left)
	rReader := common.NewArrayValueReader(dataType, right)

	if lReader.GetLength() != rReader.GetLength() {
		return false
	}

	itemType := common.GetElementDataType(dataType)
	cmpFunc := common.GetCompareFunc(itemType)
	for i := 0; i < lReader.GetLength(); i++ {
		if itemType == common.Bool {
			if lReader.GetBool(i) != rReader.GetBool(i) {
				return false
			}
		} else {
			if cmpFunc(lReader.Get(i), rReader.Get(i)) != 0 {
				return false
			}
		}
	}
	return true
}

// GetNonDefaultValueCount get Number of non-default values stored. Since we
// count all list values as valid values, it should be equal to the length of
// the vp. If in future we want to get a count of non default element value
// count, we may need to scan all the old element values when overwriting.
func (vp *baseVectorParty) GetNonDefaultValueCount() int {
	return vp.length
}
