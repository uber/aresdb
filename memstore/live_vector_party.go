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
	"bufio"
	"fmt"
	"github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/memstore/list"
	"github.com/uber/aresdb/utils"
	"io"
	"os"
	"reflect"
	"unsafe"
)

// cLiveVectorParty is the implementation of LiveVectorParty with c allocated memory
// this vector party stores columns with fixed length data type
type cLiveVectorParty struct {
	cVectorParty
}

// SetBool implements SetBool in LiveVectorParty interface
func (vp *cLiveVectorParty) SetBool(offset int, val bool, valid bool) {
	vp.setValidity(offset, valid)
	vp.values.SetBool(offset, val)
	return
}

// SetBool implements SetValue in LiveVectorParty interface
func (vp *cLiveVectorParty) SetValue(offset int, val unsafe.Pointer, valid bool) {
	vp.setValidity(offset, valid)
	if valid {
		vp.values.SetValue(offset, val)
	} else {
		var zero [2]uint64
		vp.values.SetValue(offset, unsafe.Pointer(&zero))
	}
}

// SetGoValue implements SetGoValue in LiveVectorParty interface
func (vp *cLiveVectorParty) SetGoValue(offset int, val common.GoDataValue, valid bool) {
	panic("SetGoValue is not supported in cLiveVectorParty")
}

// GetValue implements GetValue in LiveVectorParty interface
func (vp *cLiveVectorParty) GetValue(offset int) (unsafe.Pointer, bool) {
	return vp.values.GetValue(offset), vp.GetValidity(offset)
}

// goLiveVectorParty is the implementation of LiveVectorParty with go allocated memory
// this vector party stores columns with variable length data type
type goLiveVectorParty struct {
	baseVectorParty

	values            []common.GoDataValue
	hostMemoryManager common.HostMemoryManager

	totalBytes int64
}

// GetMinMaxValue implements GetMinMaxValue in LiveVectorParty interface
func (vp *cLiveVectorParty) GetMinMaxValue() (min uint32, max uint32) {
	return vp.values.GetMinValue(), vp.values.GetMaxValue()
}

// Allocate implements Allocate in VectorParty interface
func (vp *cLiveVectorParty) Allocate(hasCount bool) {
	vp.cVectorParty.Allocate(hasCount)
	vp.fillWithDefaultValue()
}

// Allocate implements Allocate in VectorParty interface
func (vp *goLiveVectorParty) Allocate(hasCount bool) {
	vp.values = make([]common.GoDataValue, vp.length)
}

// SetDataValue implements SetDataValue in VectorParty interface
// liveVectorParty ignores countsUpdateMode or counts
func (vp *goLiveVectorParty) SetDataValue(offset int, value common.DataValue,
	countsUpdateMode common.ValueCountsUpdateMode, counts ...uint32) {
	vp.SetGoValue(offset, value.GoVal, value.Valid)
}

// SetBool implements SetBool in LiveVectorParty interface
func (vp *goLiveVectorParty) SetBool(offset int, val bool, valid bool) {
	panic("SetBool is not supported in goLiveVectorParty")
}

// SetValue implements SetValue in LiveVectorParty interface
func (vp *goLiveVectorParty) SetValue(offset int, val unsafe.Pointer, valid bool) {
	panic("SetValue is not supported in goLiveVectorParty")
}

// GetValue implements GetValue in LiveVectorParty interface
func (vp *goLiveVectorParty) GetValue(offset int) (unsafe.Pointer, bool) {
	panic("GetValue is not supported in goLiveVectorParty")
}

// SetGoValue implements SetGoValue in LiveVectorParty interface
func (vp *goLiveVectorParty) SetGoValue(offset int, val common.GoDataValue, valid bool) {
	oldBytes, newBytes := 0, 0
	if vp.values[offset] != nil {
		oldBytes = vp.values[offset].GetBytes()
	}

	if !valid || val == nil {
		newBytes = 0
		vp.values[offset] = nil
	} else {
		newBytes = val.GetBytes()
		vp.values[offset] = val
	}

	bytesChange := int64(newBytes - oldBytes)
	vp.hostMemoryManager.ReportUnmanagedSpaceUsageChange(bytesChange)
	vp.totalBytes += bytesChange
}

// GetDataValue implements GetDataValue in VectorParty interface
func (vp *goLiveVectorParty) GetDataValue(offset int) common.DataValue {
	val := common.DataValue{
		Valid:    vp.GetValidity(offset),
		DataType: vp.dataType,
	}
	if !val.Valid {
		return val
	}

	val.GoVal = vp.values[offset]
	return val
}

// GetDataValueByRow implements GetDataValueByRow in VectorParty interface
func (vp *goLiveVectorParty) GetDataValueByRow(row int) common.DataValue {
	return vp.GetDataValue(row)
}

// GetValidity implements GetValidity in VectorParty interface
func (vp *goLiveVectorParty) GetValidity(offset int) bool {
	return vp.values[offset] != nil
}

// GetMinMaxValue is **not supported** by goLiveVectorParty
func (vp *goLiveVectorParty) GetMinMaxValue() (min uint32, max uint32) {
	return 0, 0
}

// GetBytes implements GetBytes in VectorParty interface
func (vp *goLiveVectorParty) GetBytes() int64 {
	return vp.totalBytes
}

// Slice implements Slice in VectorParty interface
func (vp *goLiveVectorParty) Slice(startRow, numRows int) common.SlicedVector {
	beginIndex := startRow
	// size is the number of entries in the vector,
	size := vp.length - beginIndex
	if size < 0 {
		size = 0
	}
	if size > numRows {
		size = numRows
	}

	vector := common.SlicedVector{
		Values: make([]interface{}, size),
		Counts: make([]int, size),
	}

	for i := 0; i < size; i++ {
		vector.Values[i] = vp.GetDataValue(beginIndex + i).ConvertToHumanReadable(vp.dataType)
		vector.Counts[i] = i + 1
	}
	return vector
}

// Write implements Write in VectorParty interface
func (vp *goLiveVectorParty) Write(writer io.Writer) error {
	bufferWriter := bufio.NewWriter(writer)
	dataWriter := utils.NewStreamDataWriter(bufferWriter)
	// write total bytes for reporting during loading
	err := dataWriter.WriteUint64(uint64(vp.GetBytes()))
	if err != nil {
		return err
	}

	// write length
	err = dataWriter.WriteUint32(uint32(vp.length))
	if err != nil {
		return err
	}

	// count non nil values
	numValidValues := 0
	for _, value := range vp.values {
		if value != nil {
			numValidValues++
		}
	}
	// write number of valid values
	err = dataWriter.WriteUint32(uint32(numValidValues))
	if err != nil {
		return err
	}

	allValid := numValidValues == vp.length

	// write values
	for i, value := range vp.values {
		if value != nil {
			// only write index if not all valid
			if !allValid {
				err = dataWriter.WriteUint32(uint32(i))
				if err != nil {
					return err
				}
			}
			err = value.Write(&dataWriter)
			if err != nil {
				return err
			}
		}
	}
	return bufferWriter.Flush()
}

// Read implements Read in VectorParty interface
func (vp *goLiveVectorParty) Read(reader io.Reader, serializer common.VectorPartySerializer) error {
	dataReader := utils.NewStreamDataReader(reader)
	// read total bytes for reporting during loading
	totalBytes, err := dataReader.ReadUint64()
	if err != nil {
		return err
	}
	vp.totalBytes = int64(totalBytes)
	serializer.ReportVectorPartyMemoryUsage(int64(totalBytes * utils.GolangMemoryFootprintFactor))

	length, err := dataReader.ReadUint32()
	if err != nil {
		return err
	}
	vp.length = int(length)
	vp.Allocate(false)
	numValidValues, err := dataReader.ReadUint32()
	if err != nil {
		return err
	}
	allValid := numValidValues == uint32(vp.length)
	for i := 0; i < int(numValidValues); i++ {
		var index uint32
		if !allValid {
			index, err = dataReader.ReadUint32()
			if err != nil {
				return err
			}
		} else {
			index = uint32(i)
		}
		goValue := common.GetGoDataValue(vp.dataType)
		err = goValue.Read(&dataReader)
		if err != nil {
			return err
		}
		vp.values[index] = goValue
	}
	return nil
}

// SafeDestruct implements SafeDestruct in VectorParty interface
func (vp *goLiveVectorParty) SafeDestruct() {
	for i := range vp.values {
		vp.values[i] = nil
	}
	vp.values = nil
	vp.length = 0
}

// Equals implements Equals in VectorParty interface
func (vp *goLiveVectorParty) Equals(other common.VectorParty) bool {
	if vp == nil || other == nil {
		return vp == nil && other == nil
	}

	if vp.dataType != other.GetDataType() {
		return false
	}

	if vp.GetLength() != other.GetLength() {
		return false
	}

	otherVP, ok := other.(*goLiveVectorParty)
	if !ok {
		return false
	}

	for i, ptr := range vp.values {
		if ptr == nil {
			if otherVP.values[i] != nil {
				return false
			}
		} else {
			if !reflect.DeepEqual(vp.values[i], otherVP.values[i]) {
				return false
			}
		}
	}
	return true
}

func (vp *goLiveVectorParty) Dump(file *os.File) {
	fmt.Fprintf(file, "\nGO LiveVectorParty, type: %s, length: %d, value: \n", common.DataTypeName[vp.dataType], vp.GetLength())
	for i := 0; i < vp.GetLength(); i++ {
		val := vp.GetDataValue(i)
		if val.Valid {
			fmt.Fprintf(file, "\t%v\n", val.ConvertToHumanReadable(vp.dataType))
		} else {
			fmt.Println(file, "\tnil")
		}
	}
}

// NewLiveVectorParty creates LiveVectorParty
func NewLiveVectorParty(length int, dataType common.DataType, defaultValue common.DataValue, hostMemoryManager common.HostMemoryManager) common.LiveVectorParty {
	isGoType := common.IsGoType(dataType)
	if isGoType {
		return newGoLiveVetorParty(length, dataType, hostMemoryManager)
	}
	if common.IsArrayType(dataType) {
		return list.NewLiveVectorParty(length, dataType, hostMemoryManager)
	}
	return newCLiveVectorParty(length, dataType, defaultValue)
}

// newCLiveVectorParty creates a LiveVectorParty with c allocated memory
func newCLiveVectorParty(length int, dataType common.DataType, defaultValue common.DataValue) *cLiveVectorParty {
	vp := &cLiveVectorParty{
		cVectorParty: cVectorParty{
			baseVectorParty: baseVectorParty{
				length:       length,
				dataType:     dataType,
				defaultValue: defaultValue,
			},
		},
	}
	return vp
}

// newGoLiveVetorParty creates a LiveVectorParty with go allocated memory
func newGoLiveVetorParty(length int, dataType common.DataType, hostMemoryManager common.HostMemoryManager) *goLiveVectorParty {
	vp := &goLiveVectorParty{
		baseVectorParty: baseVectorParty{
			length:       length,
			dataType:     dataType,
			defaultValue: common.NullDataValue,
		},
		hostMemoryManager: hostMemoryManager,
	}
	return vp
}
