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
	"fmt"
	"github.com/uber/aresdb/cgoutils"
	"github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/memstore/vectors"
	"github.com/uber/aresdb/utils"
	"io"
	"os"
	"sync"
	"unsafe"
)

// LiveVectorParty is the representation of list data type vector party in live store.
// It supports random access read and write. However, it does not support serialization into disk.
// It underlying uses a high level memory pool to store the list data. Therefore when this vector
// party is destructed, the underlying memory pool needs to be destroyed as well.
type LiveVectorParty struct {
	baseVectorParty
	// storing the offset to slab footer offset for each row.
	caps       *vectors.Vector
	memoryPool HighLevelMemoryPool
	sync.RWMutex
}

// GetBytes returns the bytes this vp occupies except memory pool
func (vp *LiveVectorParty) GetBytes() int64 {
	vp.RLock()
	defer vp.RUnlock()

	var bytes int64
	if vp.offsets != nil {
		bytes += int64(vp.offsets.Bytes)
	}
	if vp.caps != nil {
		bytes += int64(vp.caps.Bytes)
	}
	return bytes
}

// GetTotalBytes return the bytes this vp occupies including memory pool
func (vp *LiveVectorParty) GetTotalBytes() int64 {
	vp.RLock()
	defer vp.RUnlock()

	var bytes int64
	if vp.offsets != nil {
		bytes += int64(vp.offsets.Bytes)
	}
	if vp.caps != nil {
		bytes += int64(vp.caps.Bytes)
	}

	if vp.memoryPool != nil {
		bytes += vp.memoryPool.GetNativeMemoryAllocator().GetTotalBytes()
	}
	return bytes
}

// SafeDestruct destructs vector party memory.
func (vp *LiveVectorParty) SafeDestruct() {
	vp.Lock()
	defer vp.Unlock()

	if vp != nil {
		if vp.offsets != nil {
			vp.offsets.SafeDestruct()
			vp.offsets = nil
		}
		if vp.caps != nil {
			vp.caps.SafeDestruct()
			vp.caps = nil
		}
		if vp.memoryPool != nil {
			vp.memoryPool.Destroy()
			vp.memoryPool = nil
		}
	}
}

// Write serialize vector party.
func (vp *LiveVectorParty) Write(writer io.Writer) (err error) {
	vp.RLock()
	defer vp.RUnlock()

	dataWriter := utils.NewStreamDataWriter(writer)
	if err = dataWriter.WriteUint32(ListVectorPartyHeader); err != nil {
		return
	}

	// length
	if err = dataWriter.WriteInt32(int32(vp.length)); err != nil {
		return
	}

	// data type
	if err = dataWriter.WriteUint32(uint32(vp.dataType)); err != nil {
		return
	}

	// nonDefaultValue count, 0 for List VectorParty
	if err = dataWriter.WriteInt32(int32(0)); err != nil {
		return
	}

	// columnMode, AllValuesPresent for now
	columnMode := common.AllValuesPresent
	if err = dataWriter.WriteUint16(uint16(columnMode)); err != nil {
		return
	}

	// Write 6 bytes padding.
	if err = dataWriter.SkipBytes(6); err != nil {
		return
	}

	// write offsets
	var valueBytes int
	for i := 0; i < vp.length; i++ {
		_, length, valid := vp.GetOffsetLength(i)
		if !valid {
			dataWriter.WriteUint32(uint32(0))
		} else if length == 0 {
			dataWriter.WriteUint32(uint32(common.ZeroLengthArrayFlag))
		} else {
			dataWriter.WriteUint32(uint32(valueBytes))
		}
		dataWriter.WriteUint32(length)

		valueBytes += common.CalculateListElementBytes(vp.dataType, int(length))
	}
	// to compatible with archive vp, align to 64 bytes alignment
	dataWriter.WritePadding(8*vp.length, 64)

	// value bytes, to compatible with archive vp, align to 64 bytes alignment
	totalValueBytes := utils.AlignOffset(valueBytes, 64)
	if err := dataWriter.WriteUint64(uint64(totalValueBytes)); err != nil {
		return err
	}

	// write values
	baseAddr := vp.memoryPool.GetNativeMemoryAllocator().GetBaseAddr()
	for i := 0; i < vp.length; i++ {
		offset, length, _ := vp.GetOffsetLength(i)
		bytes := common.CalculateListElementBytes(vp.dataType, int(length))

		if bytes > 0 {
			if err = dataWriter.Write(cgoutils.MakeSliceFromCPtr(baseAddr+uintptr(offset), bytes)); err != nil {
				return
			}
		}
	}
	dataWriter.WritePadding(valueBytes, 64)
	return
}

// Read deserialize vector party
func (vp *LiveVectorParty) Read(reader io.Reader, serializer common.VectorPartySerializer) (err error) {
	vp.Lock()
	defer vp.Unlock()

	dataReader := utils.NewStreamDataReader(reader)
	magicNumber, err := dataReader.ReadUint32()
	defer func() {
		if err != nil {
			vp.SafeDestruct()
		}
	}()

	if err != nil {
		return
	}

	if magicNumber != ListVectorPartyHeader {
		return utils.StackError(nil, "Magic number does not match, vector party file may be corrupted")
	}

	rawLength, err := dataReader.ReadInt32()
	if err != nil {
		return
	}
	length := int(rawLength)

	rawDataType, err := dataReader.ReadUint32()
	if err != nil {
		return
	}

	dataType, err := common.NewDataType(rawDataType)
	if err != nil {
		return
	}

	// non default value count
	_, err = dataReader.ReadInt32()
	if err != nil {
		return
	}

	// column mode
	m, err := dataReader.ReadUint16()
	if err != nil {
		return
	}

	columnMode := common.ColumnMode(m)
	if columnMode >= common.MaxColumnMode {
		return utils.StackError(nil, "Invalid mode %d", columnMode)
	}

	// Read unused bytes
	err = dataReader.SkipBytes(6)
	if err != nil {
		return
	}

	vp.length = length
	vp.dataType = dataType

	vp.Allocate(false)
	if err = dataReader.Read(cgoutils.MakeSliceFromCPtr(uintptr(vp.offsets.Buffer()), vp.offsets.Bytes)); err != nil {
		return
	}

	// Read value bytes
	_, err = dataReader.ReadUint64()
	if err != nil {
		return
	}

	var zero uint32 = 0
	for i := 0; i < vp.length; i++ {
		itemLen := *(*uint32)(vp.offsets.GetValue(2*i + 1))
		itemBytes := common.CalculateListElementBytes(vp.dataType, int(itemLen))
		if itemBytes > 0 {
			buf := vp.memoryPool.Allocate(itemBytes)
			// update offset.
			vp.offsets.SetValue(2*i, unsafe.Pointer(&buf[0]))
			// Set footer offset.
			vp.caps.SetValue(i, unsafe.Pointer(&buf[1]))
			addr := vp.memoryPool.Interpret(buf[0])
			if err = dataReader.Read(cgoutils.MakeSliceFromCPtr(addr, itemBytes)); err != nil {
				return
			}
		} else {
			vp.offsets.SetValue(2*i, unsafe.Pointer(&zero))
			vp.caps.SetValue(i, unsafe.Pointer(&zero))
		}
	}
	if serializer != nil {
		serializer.ReportVectorPartyMemoryUsage(int64(vp.GetBytes()))
	}

	return
}

// GetCap returns the cap at ith row. Only used for free a list element in live store.
func (vp *LiveVectorParty) GetCap(row int) uint32 {
	vp.RLock()
	defer vp.RUnlock()

	return *(*uint32)(vp.caps.GetValue(row))
}

// SetBool is not supported by list vector party.
func (vp *LiveVectorParty) SetBool(offset int, val bool, valid bool) {
	utils.GetLogger().Panic("SetBool is not supported by list vector party")
}

// SetValue is the implementation of common.LiveVectorParty
func (vp *LiveVectorParty) SetValue(row int, val unsafe.Pointer, valid bool) {
	vp.Lock()
	defer vp.Unlock()

	var newLen int
	if valid {
		if val != nil {
			newLen = int(*(*uint32)(val))
		}
	}

	oldOffset, oldLen, _ := vp.GetOffsetLength(row)
	oldCap := *(*uint32)(vp.caps.GetValue(row))
	oldBytes := common.CalculateListElementBytes(vp.dataType, int(oldLen))
	newBytes := common.CalculateListElementBytes(vp.dataType, int(newLen))

	buf := vp.memoryPool.Reallocate([2]uintptr{uintptr(oldOffset), uintptr(oldCap)}, oldBytes, newBytes)

	if !valid {
		vp.SetOffsetLength(row, nil, nil)
	} else {
		vp.SetOffsetLength(row, unsafe.Pointer(&buf[0]), unsafe.Pointer(&newLen))
	}
	// Set footer offset.
	vp.caps.SetValue(row, unsafe.Pointer(&buf[1]))

	if valid {
		baseAddr := vp.memoryPool.Interpret(buf[0])
		utils.MemCopy(unsafe.Pointer(baseAddr), val, newBytes)
	}
}

// AsList is the implementation from common.VectorParty
func (vp *LiveVectorParty) AsList() common.ListVectorParty {
	return vp
}

// Equals is the implementation from common.VectorParty
func (vp *LiveVectorParty) Equals(other common.VectorParty) bool {
	return vp.equals(other, vp.AsList())
}

// SetListValue is the implentation of common.ListVecotrParty
func (vp *LiveVectorParty) GetListValue(row int) (unsafe.Pointer, bool) {
	return vp.GetValue(row)
}

// SetListValue is the implentation of common.ListVecotrParty
func (vp *LiveVectorParty) SetListValue(row int, val unsafe.Pointer, valid bool) {
	vp.SetValue(row, val, valid)
}

// SetGoValue is not supported by list vector party.
func (vp *LiveVectorParty) SetGoValue(offset int, val common.GoDataValue, valid bool) {
	utils.GetLogger().Panic("SetGoValue is not supported by list vector party")
}

// GetValue is the implementation from common.VectorParty
func (vp *LiveVectorParty) GetValue(row int) (val unsafe.Pointer, validity bool) {
	vp.RLock()
	defer vp.RUnlock()

	offset, length, valid := vp.GetOffsetLength(row)
	if !valid {
		return nil, false
	} else if length == 0 {
		return nil, true
	}
	baseAddr := vp.memoryPool.GetNativeMemoryAllocator().GetBaseAddr()
	return unsafe.Pointer(baseAddr + uintptr(offset)), true
}

// GetMinMaxValue is not supported by list vector party.
func (vp *LiveVectorParty) GetMinMaxValue() (min, max uint32) {
	utils.GetLogger().Panic("GetMinMaxValue is not supported by list vector party")
	return
}

// GetDataValue is not implemented in baseVectorParty
func (vp *LiveVectorParty) GetDataValue(row int) common.DataValue {
	if row < 0 || row > vp.length {
		return common.NullDataValue
	}
	val, valid := vp.GetValue(row)
	return common.DataValue{
		DataType: vp.dataType,
		Valid:    valid,
		OtherVal: val,
	}
}

// SetDataValue
func (vp *LiveVectorParty) SetDataValue(row int, value common.DataValue,
	countsUpdateMode common.ValueCountsUpdateMode, counts ...uint32) {
	vp.SetValue(row, value.OtherVal, value.Valid)
}

// GetDataValueByRow just call GetDataValue
func (vp *LiveVectorParty) GetDataValueByRow(row int) common.DataValue {
	return vp.GetDataValue(row)
}

// Allocate allocate underlying storage for vector party
func (vp *LiveVectorParty) Allocate(hasCount bool) {
	vp.caps = vectors.NewVector(common.Uint32, vp.length)
	vp.offsets = vectors.NewVector(common.Uint32, vp.length*2)

	vp.memoryPool = NewHighLevelMemoryPool(vp.reporter)
}

// Dump is for testing purpose
func (vp *LiveVectorParty) Dump(file *os.File) {
	fmt.Fprintf(file, "\nArray LiveVectorParty, type: %s, length: %d, value: \n", common.DataTypeName[vp.dataType], vp.GetLength())
	for i := 0; i < vp.GetLength(); i++ {
		val := vp.GetDataValue(i)
		if val.Valid {
			fmt.Fprintf(file, "\t%v\n", val.ConvertToHumanReadable(vp.dataType))
		} else {
			fmt.Fprintln(file, "\tnil")
		}
	}
}

// GetHostVectorPartySlice implements GetHostVectorPartySlice in TransferableVectorParty
func (vp *LiveVectorParty) GetHostVectorPartySlice(startIndex, length int) common.HostVectorPartySlice {
	// LiveVectorParty will always use startIndex = 0, length is whole VP, so startIndex is ignored here
	return common.HostVectorPartySlice{
		Values:     unsafe.Pointer(vp.memoryPool.GetNativeMemoryAllocator().GetBaseAddr()),
		ValueBytes: int(vp.memoryPool.GetNativeMemoryAllocator().GetTotalBytes()),
		Length:     length,
		Offsets:    vp.offsets.Buffer(),
		ValueType:  vp.dataType,
	}
}

// SetLength is only for testing purpose, do NOT use this function in real code
func (vp *LiveVectorParty) SetLength(length int) {
	vp.length = length
}

// NewLiveVectorParty returns a LiveVectorParty pointer which implements ListVectorParty.
// It's safe to pass nil HostMemoryManager.
func NewLiveVectorParty(length int, dataType common.DataType,
	hmm common.HostMemoryManager) common.LiveVectorParty {
	vp := &LiveVectorParty{
		baseVectorParty: baseVectorParty{
			length:   length,
			dataType: dataType,
			reporter: func(bytes int64) {
				if hmm != nil {
					hmm.ReportUnmanagedSpaceUsageChange(bytes)
				}
			},
		},
	}
	vp.baseVectorParty.getDataValueFn = vp.GetDataValue
	return vp
}
