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
	"github.com/uber/aresdb/cgoutils"
	"github.com/uber/aresdb/memstore"
	"github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/utils"
	"io"
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
	caps       *memstore.Vector
	memoryPool HighLevelMemoryPool
	sync.RWMutex
}

// GetBytes returns the bytes this vp occupies.
func (vp *LiveVectorParty) GetBytes() int64 {
	vp.RLock()
	defer vp.RUnlock()

	var bytes int64
	if vp.memoryPool != nil {
		bytes += vp.memoryPool.GetNativeMemoryAllocator().GetTotalBytes()
	}

	if vp.offsets != nil {
		bytes += int64(vp.offsets.Bytes)
	}
	if vp.caps != nil {
		bytes += int64(vp.caps.Bytes)
	}
	return bytes
}

// SafeDestruct destructs vector party memory.
func (vp *LiveVectorParty) SafeDestruct() {
	if vp != nil {
		totalBytes := vp.GetBytes()
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
		if totalBytes > 0 {
			vp.reporter(-totalBytes)
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
		_, length := vp.GetOffsetLength(i)
		if length == 0 {
			dataWriter.WriteUint32(uint32(0))
		} else {
			dataWriter.WriteUint32(uint32(valueBytes))
		}
		dataWriter.WriteUint32(length)

		valueBytes += common.CalculateListElementBytes(vp.dataType, int(length))
	}
	// to compatible with archive vp, align to 64 bytes alignment
	if vp.offsets.Bytes > 8*vp.length {
		dataWriter.SkipBytes(vp.offsets.Bytes - 8*vp.length)
	}

	// value bytes, to compatible with archive vp, align to 64 bytes alignment
	totalValueBytes := (valueBytes*8 + 511) / 512 * 64
	if err := dataWriter.WriteUint64(uint64(totalValueBytes)); err != nil {
		return err
	}

	// write values
	baseAddr := vp.memoryPool.GetNativeMemoryAllocator().GetBaseAddr()
	for i := 0; i < vp.length; i++ {
		offset, length := vp.GetOffsetLength(i)
		bytes := common.CalculateListElementBytes(vp.dataType, int(length))

		if bytes > 0 {
			if err = dataWriter.Write(cgoutils.MakeSliceFromCPtr(baseAddr+uintptr(offset), bytes)); err != nil {
				return
			}
		}
	}
	dataWriter.SkipBytes(totalValueBytes - valueBytes)
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
		newLen = int(*(*uint32)(val))
	}

	oldOffset, oldLen := vp.GetOffsetLength(row)
	oldCap := *(*uint32)(vp.caps.GetValue(row))
	oldBytes := common.CalculateListElementBytes(vp.dataType, int(oldLen))
	newBytes := common.CalculateListElementBytes(vp.dataType, int(newLen))

	buf := vp.memoryPool.Reallocate([2]uintptr{uintptr(oldOffset), uintptr(oldCap)}, oldBytes, newBytes)

	// Set offset.
	vp.offsets.SetValue(2*row, unsafe.Pointer(&buf[0]))
	// Set length.
	vp.offsets.SetValue(2*row+1, unsafe.Pointer(&newLen))
	// Set footer offset.
	vp.caps.SetValue(row, unsafe.Pointer(&buf[1]))

	if valid {
		baseAddr := vp.memoryPool.Interpret(buf[0])

		to := cgoutils.MakeSliceFromCPtr(baseAddr, newBytes)
		from := cgoutils.MakeSliceFromCPtr(uintptr(val), newBytes)
		for i := 0; i < newBytes; i++ {
			to[i] = from[i]
		}
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

	offset, length := vp.GetOffsetLength(row)
	if offset == 0 && length == 0 {
		return unsafe.Pointer(uintptr(0)), false
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
	// Report caps and offsets usage.
	vp.reporter(int64(vp.length * 3 * common.DataTypeBytes(common.Uint32)))
	vp.caps = memstore.NewVector(common.Uint32, vp.length)
	vp.offsets = memstore.NewVector(common.Uint32, vp.length*2)

	// No need to report memory pool usage until allocation happens on memory pool.
	vp.memoryPool = NewHighLevelMemoryPool(vp.reporter)
}

// NewLiveVectorParty returns a LiveVectorParty pointer which implements ListVectorParty.
// It's safe to pass nil HostMemoryManager.
func NewLiveVectorParty(length int, dataType common.DataType,
	hmm common.HostMemoryManager) common.LiveVectorParty {
	return &LiveVectorParty{
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
}
