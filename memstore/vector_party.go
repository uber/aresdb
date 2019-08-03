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
	"fmt"
	"github.com/uber/aresdb/cgoutils"
	"github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/memstore/vectors"
	"github.com/uber/aresdb/utils"
	"io"
	"os"
	"unsafe"
)

// TransferableVectorParty is vector party that can be transferred to gpu for processing
type TransferableVectorParty interface {
	// GetHostVectorPartySlice slice vector party between [startIndex, startIndex+length) before transfer to gpu
	GetHostVectorPartySlice(startIndex, length int) common.HostVectorPartySlice
}

// baseVectorParty is the base vector party type
type baseVectorParty struct {
	// DataType of values. We need it since for mode 0 vector party, we cannot
	// get data type from values vector. Also we store it on disk anyway.
	dataType common.DataType
	// Number of non-default values stored, not always the same as Nulls.numTrues.
	nonDefaultValueCount int
	// Length/Size of each vector (not necessarily number of records).
	length int

	// Following fields are initialized during struct initialization.
	// DefaultValue for this column. For convenience.
	defaultValue common.DataValue
}

// cVectorParty combines the value, null, and count vector of a column in a batch.
// Some of the vectors can be nil, following the same modes described at
// https://github.com/uber/aresdb/wiki/VectorStore#vector-party
type cVectorParty struct {
	baseVectorParty

	// Set during archiving/backfill and stored on disk.
	columnMode common.ColumnMode

	values *vectors.Vector
	// Stores the validity bitmap (0 means null) for each value in values.
	nulls *vectors.Vector
	// Stores the accumulative count from the beginning of the vector
	// to the current position. Its length is vp.Length + 1 with first value to
	// be 0 and last value to be vp.Length. We can get a count of current value
	// by Counts[i+1] - Counts[i] for Values[i]
	counts *vectors.Vector
}

// IsList tells whether it's a list vector party or not.
func (vp *baseVectorParty) IsList() bool {
	return false
}

// AsList returns ListVectorParty representation of this vector party.
// Caller should always call IsList before conversion, otherwise panic may happens
// for incompatible vps.
func (vp *baseVectorParty) AsList() common.ListVectorParty {
	utils.GetLogger().Panic("Cannot convert this vp to list vp")
	return nil
}

// GetLength returns the length this vector party
func (vp *baseVectorParty) GetLength() int {
	return vp.length
}

// GetDataType returns the min and max value of this vector party
func (vp *baseVectorParty) GetDataType() common.DataType {
	return vp.dataType
}

// GetNonDefaultValueCount get Number of non-default values stored
func (vp *baseVectorParty) GetNonDefaultValueCount() int {
	return vp.nonDefaultValueCount
}

// GetMode returns the stored column mode of this vector party.
func (vp *cVectorParty) GetMode() common.ColumnMode {
	return vp.columnMode
}

// fillWithDefaultValue fills the values vector and nulls vector with default value
// if it's valid. **It should be only called when both values vector and nulls vector
// presents, otherwise it will panic. Also it requires both value vector and null vector
// is initialized with zeros and have never been touched yet**
func (vp *cVectorParty) fillWithDefaultValue() {
	if vp.values == nil || vp.nulls == nil {
		utils.GetLogger().Panic("Calling FillWithDefaultValue with nil value vector" +
			"or nil null vector")
	}

	if vp.defaultValue.Valid {
		vp.nulls.SetAllValid()
		if vp.dataType == common.Bool && vp.defaultValue.BoolVal {
			vp.values.SetAllValid()
		} else {
			for i := 0; i < vp.values.Size; i++ {
				vp.values.SetValue(i, vp.defaultValue.OtherVal)
			}
		}
	}
}

// SafeDestruct destructs all vectors of this vector party. Corresponding pointer should be set
// as nil after destruction.
func (vp *cVectorParty) SafeDestruct() {
	if vp != nil {
		vp.values.SafeDestruct()
		vp.values = nil
		vp.nulls.SafeDestruct()
		vp.nulls = nil
		vp.counts.SafeDestruct()
		vp.counts = nil
	}
}

// GetBytes returns space occupied by this vector party.
func (vp *cVectorParty) GetBytes() int64 {
	var bytes int64
	if vp.values != nil {
		bytes += int64(vp.values.Bytes)
	}

	if vp.nulls != nil {
		bytes += int64(vp.nulls.Bytes)
	}

	if vp.counts != nil {
		bytes += int64(vp.counts.Bytes)
	}
	return bytes
}

// setValidity set the validity of given offset and update NonDefaultValueCount.
// Third parameter count should only be passed for compressed columns. If
// not passed, the default value is 1.
func (vp *cVectorParty) setValidity(offset int, valid bool) {
	vp.nulls.SetBool(offset, valid)
}

// GetValidity implements GetValidity in cVectorParty
func (vp *cVectorParty) GetValidity(offset int) bool {
	if vp.columnMode == common.AllValuesDefault {
		return vp.defaultValue.Valid
	}
	return vp.nulls == nil || vp.nulls.GetBool(offset)
}

// GetDataValue returns the DataValue for the specified index.
// It first check validity of the value, then it check whether it's a
// boolean column to decide whether to load bool value or other value
// type. Index bound is not checked!
func (vp *cVectorParty) GetDataValue(offset int) common.DataValue {
	if vp.columnMode == common.AllValuesDefault {
		return vp.defaultValue
	}

	val := common.DataValue{
		Valid:    vp.GetValidity(offset),
		DataType: vp.dataType,
	}
	if !val.Valid {
		return val
	}

	if vp.values.DataType == common.Bool {
		val.IsBool = true
		val.BoolVal = vp.values.GetBool(offset)
		return val
	}
	val.OtherVal = vp.values.GetValue(offset)
	val.CmpFunc = vp.values.CmpFunc
	return val
}

// GetDataValueByRow implements GetDataValueByRow in cVectorParty
func (vp *cVectorParty) GetDataValueByRow(row int) common.DataValue {
	offset := row
	if vp.GetMode() == common.HasCountVector {
		offset = vp.counts.UpperBound(0, vp.counts.Size, unsafe.Pointer(&row)) - 1
	}
	return vp.GetDataValue(offset)
}

// SetDataValue implements SetDataValue in cVectorParty
func (vp *cVectorParty) SetDataValue(offset int, value common.DataValue, countsUpdateMode common.ValueCountsUpdateMode, counts ...uint32) {
	vp.setValidity(offset, value.Valid)
	if value.Valid {
		if vp.values.DataType == common.Bool {
			vp.values.SetBool(offset, value.BoolVal)
		} else {
			vp.values.SetValue(offset, value.OtherVal)
		}
	} else {
		if vp.values.DataType == common.Bool {
			vp.values.SetBool(offset, false)
		} else {
			var zero [2]uint64
			vp.values.SetValue(offset, unsafe.Pointer(&zero))
		}
	}

	if countsUpdateMode == common.IgnoreCount {
		return
	}

	isNonDefault := value.Compare(vp.defaultValue) != 0

	count := 1
	if len(counts) > 0 {
		count = int(counts[0])
	}

	if countsUpdateMode == common.IncrementCount {
		if isNonDefault {
			vp.nonDefaultValueCount += count
		}
	} else if countsUpdateMode == common.CheckExistingCount {
		existing := vp.GetDataValue(offset).Compare(vp.defaultValue) != 0
		if !existing && isNonDefault {
			vp.nonDefaultValueCount += count
		} else if existing && !isNonDefault {
			vp.nonDefaultValueCount -= count
		}
	}
}

// JudgeMode judges column mode of current vector party according to value count fields.
func (vp *cVectorParty) JudgeMode() common.ColumnMode {
	if vp.nonDefaultValueCount == 0 {
		// both
		return common.AllValuesDefault
	} else if vp.counts != nil {
		// compressed columns.
		return common.HasCountVector
	} else if vp.nulls == nil || vp.nulls.CheckAllValid() {
		// no null vector.
		return common.AllValuesPresent
	}
	// uncompressed columns
	return common.HasNullVector
}

// Equals checks whether two vector parties are the same. **Only for unit test use.**
func (vp *cVectorParty) Equals(other common.VectorParty) bool {
	if vp == nil || other == nil {
		return vp == nil && other == nil
	}

	var v2 *cVectorParty
	switch v := other.(type) {
	case *archiveVectorParty:
		v2 = &v.cVectorParty
	case *cLiveVectorParty:
		v2 = &v.cVectorParty
	case *cVectorParty:
		v2 = v
	default:
		return false
	}

	if vp.length != v2.length {
		return false
	}

	if vp.GetMode() != v2.GetMode() {
		return false
	}

	// check vector elements
	for i := 0; i < vp.length; i++ {
		if vp.GetDataValue(i).Compare(v2.GetDataValue(i)) != 0 {
			return false
		}
		if vp.counts != nil {
			// compare first count
			// usually this is not needed since first count should always be 0
			if i == 0 {
				if vp.counts.CmpFunc(vp.counts.GetValue(0), v2.counts.GetValue(0)) != 0 {
					return false
				}
			}
			// only compare next count
			if vp.counts.CmpFunc(vp.counts.GetValue(i+1), v2.counts.GetValue(i+1)) != 0 {
				return false
			}
		}
	}
	return true
}

// Slice slice the vector party into the interval of [startRow, startRow+numRows)
func (vp *cVectorParty) Slice(startRow int, numRows int) (vector common.SlicedVector) {
	beginIndex := startRow
	// size is the number of entries in the vector,
	// size != numRows when compressed,
	// although here size is initialized as if vector is uncompressed.
	size := vp.length - beginIndex
	if size < 0 {
		size = 0
	}
	if size > numRows {
		size = numRows
	}

	mode := vp.GetMode()
	if mode == common.AllValuesDefault && size > 0 {
		size = 1
	} else if mode == common.HasCountVector {
		// find the indexes [beginIndex, endIndex) based on [startRow, startRow + numRows)
		lowerCount := uint32(startRow)
		upperCount := uint32(startRow + numRows)
		beginIndex = vp.counts.UpperBound(0, vp.counts.Size, unsafe.Pointer(&lowerCount)) - 1
		endIndex := vp.counts.LowerBound(beginIndex, vp.counts.Size, unsafe.Pointer(&upperCount))
		// subtract endIndex by 1 when endIndex points to vp.length+1
		if endIndex == vp.counts.Size {
			endIndex -= 1
		}
		size = endIndex - beginIndex
	}

	vector = common.SlicedVector{
		Values: make([]interface{}, size),
		Counts: make([]int, size),
	}

	for i := 0; i < size; i++ {
		if mode == common.AllValuesDefault {
			vector.Values[i] = vp.defaultValue.ConvertToHumanReadable(vp.dataType)
			vector.Counts[i] = numRows
			if vector.Counts[i] > vp.length {
				vector.Counts[i] = vp.length
			}
		} else if mode == common.HasCountVector {
			// compressed
			vector.Values[i] = vp.GetDataValue(beginIndex + i).ConvertToHumanReadable(vp.dataType)
			count := int(*(*uint32)(vp.counts.GetValue(beginIndex + i + 1))) - startRow
			if count > numRows {
				count = numRows
			}
			vector.Counts[i] = count
		} else {
			// uncompressed
			vector.Values[i] = vp.GetDataValue(beginIndex + i).ConvertToHumanReadable(vp.dataType)
			vector.Counts[i] = i + 1
		}
	}

	return vector
}

// SliceByValue returns a subrange withing [lowerBoundRow, upperBoundRow) that matches the specified value
func (vp *cVectorParty) SliceByValue(lowerBoundRow, upperBoundRow int, value unsafe.Pointer) (
	startRow int, endRow int, startIndex int, endIndex int) {
	// has counts
	if vp.GetMode() == common.AllValuesDefault {
		// TODO: check whether value itself is null, for IS_NULL
		// return as if the slice is empty [upperBound, upperBound)
		if vp.defaultValue.Valid {
			sliceValue := common.DataValue{
				Valid:    true,
				DataType: vp.dataType,
			}
			if vp.defaultValue.IsBool {
				sliceValue.IsBool = true
				sliceValue.BoolVal = *(*uint32)(value) != 0
			} else {
				sliceValue.OtherVal = value
				sliceValue.CmpFunc = common.GetCompareFunc(vp.dataType)
			}

			// If the default value is equal to the value, we return the whole slice.
			if vp.defaultValue.Compare(sliceValue) == 0 {
				return lowerBoundRow, upperBoundRow, lowerBoundRow, upperBoundRow
			}
		}
		return upperBoundRow, upperBoundRow, upperBoundRow, upperBoundRow
	} else if vp.GetMode() == common.HasCountVector {
		startIndex := vp.counts.UpperBound(0, vp.counts.Size, unsafe.Pointer(&lowerBoundRow)) - 1
		endIndex := vp.counts.LowerBound(startIndex, vp.counts.Size, unsafe.Pointer(&upperBoundRow))
		// subtract endIndex by 1 when endIndex points to vp.length+1
		if endIndex == vp.counts.Size {
			endIndex -= 1
		}

		startIndex = vp.values.LowerBound(startIndex, endIndex, value)
		endIndex = vp.values.UpperBound(startIndex, endIndex, value)

		startRow = int(*(*uint32)(vp.counts.GetValue(startIndex)))
		endRow = int(*(*uint32)(vp.counts.GetValue(endIndex)))
		return startRow, endRow, startIndex, endIndex
	} else {
		startRow = vp.values.LowerBound(lowerBoundRow, upperBoundRow, value)
		endRow = vp.values.UpperBound(lowerBoundRow, upperBoundRow, value)
		return startRow, endRow, startRow, endRow
	}
}

// SliceIndex returns the startIndex and endIndex of the vector party slice given startRow and endRow of original vector
// party.
func (vp *cVectorParty) SliceIndex(lowerBoundRow, upperBoundRow int) (startIndex, endIndex int) {
	if vp.GetMode() == common.HasCountVector {
		startIndex = vp.counts.UpperBound(0, vp.counts.Size, unsafe.Pointer(&lowerBoundRow)) - 1
		endIndex = vp.counts.LowerBound(startIndex, vp.counts.Size, unsafe.Pointer(&upperBoundRow))
		// subtract endIndex by 1 when endIndex points to vp.length+1
		if endIndex == vp.counts.Size {
			endIndex -= 1
		}
		return startIndex, endIndex
	}
	return lowerBoundRow, upperBoundRow
}

// Write writes a vector party to underlying writer. It first writes header and then writes vectors
// based on vector party mode. **This vector party should be from archive batch and already pruned.**
func (vp *cVectorParty) Write(writer io.Writer) error {
	dataWriter := utils.NewStreamDataWriter(writer)
	if err := dataWriter.WriteUint32(common.VectorPartyHeader); err != nil {
		return err
	}

	if err := dataWriter.WriteInt32(int32(vp.length)); err != nil {
		return err
	}

	if err := dataWriter.WriteUint32(uint32(vp.dataType)); err != nil {
		return err
	}

	if err := dataWriter.WriteInt32(int32(vp.nonDefaultValueCount)); err != nil {
		return err
	}

	columnMode := vp.columnMode
	if err := dataWriter.WriteUint16(uint16(columnMode)); err != nil {
		return err
	}

	// Write 6 bytes padding.
	if err := dataWriter.SkipBytes(6); err != nil {
		return err
	}

	// Starting writing vectors.
	// Stop writing since there are no vectors in this vp.
	if columnMode <= common.AllValuesDefault {
		return nil
	}

	// Write value vector.
	// Here we directly move data from c allocated memory into writer.
	if err := dataWriter.Write(
		cgoutils.MakeSliceFromCPtr(uintptr(vp.values.Buffer()), vp.values.Bytes),
	); err != nil {
		return err
	}

	// Stop writing since there are no more vectors in this vp.
	if columnMode <= common.AllValuesPresent {
		return nil
	}

	// Write null vector.
	// Here we directly move data from c allocated memory into writer.
	if err := dataWriter.Write(
		cgoutils.MakeSliceFromCPtr(uintptr(vp.nulls.Buffer()), vp.nulls.Bytes),
	); err != nil {
		return err
	}

	// Stop writing since there are no more vectors in this vp.
	if columnMode <= common.HasNullVector {
		return nil
	}

	// Write count vector.
	// Here we directly move data from c allocated memory into writer.
	if err := dataWriter.Write(
		cgoutils.MakeSliceFromCPtr(uintptr(vp.counts.Buffer()), vp.counts.Bytes),
	); err != nil {
		return err
	}

	return nil
}

// Read reads a vector party from underlying reader. It first reads header from the reader and does
// several sanity checks. Then it reads vectors based on vector party mode.
func (vp *cVectorParty) Read(reader io.Reader, s common.VectorPartySerializer) error {
	dataReader := utils.NewStreamDataReader(reader)
	magicNumber, err := dataReader.ReadUint32()
	if err != nil {
		return err
	}

	if magicNumber != common.VectorPartyHeader {
		return utils.StackError(nil, "Magic number does not match, vector party file may be corrupted")
	}

	rawLength, err := dataReader.ReadInt32()
	if err != nil {
		return err
	}
	length := int(rawLength)

	rawDataType, err := dataReader.ReadUint32()
	if err != nil {
		return err
	}

	dataType, err := common.NewDataType(rawDataType)
	if err != nil {
		return err
	}

	nonDefaultValueCount, err := dataReader.ReadInt32()
	if err != nil {
		return err
	}

	m, err := dataReader.ReadUint16()
	if err != nil {
		return err
	}

	columnMode := common.ColumnMode(m)
	if columnMode >= common.MaxColumnMode {
		return utils.StackError(nil, "Invalid mode %d", columnMode)
	}

	// Read unused bytes
	err = dataReader.SkipBytes(6)
	if err != nil {
		return err
	}

	vp.length = length
	vp.nonDefaultValueCount = int(nonDefaultValueCount)
	vp.dataType = dataType
	vp.columnMode = columnMode

	if err = s.CheckVectorPartySerializable(vp); err != nil {
		return err
	}

	bytes := vectors.CalculateVectorPartyBytes(vp.GetDataType(), vp.GetLength(),
		columnMode == common.HasNullVector || columnMode == common.HasCountVector, columnMode == common.HasCountVector)
	s.ReportVectorPartyMemoryUsage(int64(bytes))

	// Stop reading since there are no vectors in this vp.
	if columnMode <= common.AllValuesDefault {
		return nil
	}

	// Read value vector.
	valueVector := vectors.NewVector(dataType, length)
	// Here we directly read from reader into the c allocated bytes.
	if err = dataReader.Read(
		cgoutils.MakeSliceFromCPtr(uintptr(valueVector.Buffer()), valueVector.Bytes),
	); err != nil {
		valueVector.SafeDestruct()
		return err
	}
	vp.values = valueVector

	// Stop reading since there are no more vectors in this vp.
	if columnMode <= common.AllValuesPresent {
		return nil
	}

	// Read null vector.
	nullVector := vectors.NewVector(common.Bool, length)
	// Here we directly read from reader into the c allocated bytes.
	if err = dataReader.Read(
		cgoutils.MakeSliceFromCPtr(uintptr(nullVector.Buffer()), nullVector.Bytes),
	); err != nil {
		valueVector.SafeDestruct()
		nullVector.SafeDestruct()
		return err
	}
	vp.nulls = nullVector

	// Stop reading since there are no more vectors in this vp.
	if columnMode <= common.HasNullVector {
		return nil
	}

	// Read count vector.
	countVector := vectors.NewVector(common.Uint32, length+1)
	// Here we directly read from reader into the c allocated bytes.
	if err = dataReader.Read(
		cgoutils.MakeSliceFromCPtr(uintptr(countVector.Buffer()), countVector.Bytes),
	); err != nil {
		valueVector.SafeDestruct()
		nullVector.SafeDestruct()
		countVector.SafeDestruct()
		return err
	}
	vp.counts = countVector
	return nil
}

// GetHostVectorPartySlice implements GetHostVectorPartySlice in cVectorParty
func (vp *cVectorParty) GetHostVectorPartySlice(startIndex, length int) common.HostVectorPartySlice {
	endIndex := startIndex + length
	values, valueStartIndex, valueBytes := vp.values.GetSliceBytesAligned(startIndex, endIndex)
	nulls, nullStartIndex, nullBytes := vp.nulls.GetSliceBytesAligned(startIndex, endIndex)
	counts, countStartIndex, countBytes := vp.counts.GetSliceBytesAligned(startIndex, endIndex+1)
	return common.HostVectorPartySlice{
		Values:          values,
		ValueStartIndex: valueStartIndex,
		ValueBytes:      valueBytes,
		ValueType:       vp.dataType,
		DefaultValue:    vp.defaultValue,
		Nulls:           nulls,
		NullStartIndex:  nullStartIndex,
		NullBytes:       nullBytes,
		Counts:          counts,
		CountStartIndex: countStartIndex,
		CountBytes:      countBytes,
		Length:          length,
	}
}

// Allocates implements Allocate in cVectorParty
func (vp *cVectorParty) Allocate(hasCount bool) {
	vp.values = vectors.NewVector(vp.dataType, vp.length)
	vp.nulls = vectors.NewVector(common.Bool, vp.length)
	vp.columnMode = common.HasNullVector
	if hasCount {
		vp.counts = vectors.NewVector(common.Int32, vp.length+1)
		vp.columnMode = common.HasCountVector
	}
}

// Dump is for testing purpose
func (vp *cVectorParty) Dump(file *os.File) {
	fmt.Fprintf(file, "\nVectorParty, type: %s, length: %d, value: \n", common.DataTypeName[vp.dataType], vp.GetLength())
	for i := 0; i < vp.GetLength(); i++ {
		val := vp.GetDataValue(i)
		count := 1
		if vp.counts != nil {
			count = int(*(*uint32)(vp.counts.GetValue(i + 1)) - *(*uint32)(vp.counts.GetValue(i)))
		}
		if val.Valid {
			fmt.Fprintf(file, "\t%v, %d\n", val.ConvertToHumanReadable(vp.dataType), count)
		} else {
			fmt.Fprintf(file, "\tnil\n")
		}
	}
}
