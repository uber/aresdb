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
	"github.com/uber/aresdb/memstore"
	"github.com/uber/aresdb/diskstore"
	"github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/utils"
	"io"
	"sync"
	"unsafe"
	"github.com/uber/aresdb/cgoutils"
)

const (
	ListVectorPartyHeader uint32 = 0xFADEFACF
)
// ArchiveVectorParty is the representation of list data type vector party in archive store.
// It does not support random access update. Instead updates to archiveListVectorParty can only be done
// via appending to the tail during archiving and backfill.
// It use a single value vector to store the values and validities so that it has the same
// in memory representation except archiving vp does not have cap vector.
type ArchiveVectorParty struct {
	baseVectorParty
	memstore.Pinnable
	values *memstore.Vector
	// bytesWritten should only be used when archiving or backfilling. It's used to record the current position
	// in values vector.
	bytesWritten    int64
	totalValueBytes int64
	// Used in archive batches to allow requesters to wait until the vector party
	// is fully loaded from disk.
	Loader sync.WaitGroup
	// For archive store only. Number of users currently using this vector party.
	// This field is protected by the batch lock.
	pins int
	// For archive store only. The condition for pins to drop down to 0.
	allUsersDone *sync.Cond
}

// NewArchiveVectorParty returns a new ArchiveVectorParty.
// It should only be used during backfill or archiving when constructing a new list
// archiving vp.
// Length is the number of total rows and totalValueBytes is the total bytes used to
// store values and validities.
func NewArchiveVectorParty(length int, dataType common.DataType,
	totalValueBytes int64, locker sync.Locker) common.ArchiveVectorParty {
	return &ArchiveVectorParty{
		baseVectorParty: baseVectorParty{
			length:   length,
			dataType: dataType,
		},
		Pinnable: memstore.Pinnable{
			AllUsersDone: sync.NewCond(locker),
		},
		totalValueBytes: totalValueBytes,
	}
}


// Allocate allocate underlying storage for vector party. Note allocation for
// archive vp does not report host memory change. Memory reporting is done
// after switching to the new version of archive store. Before switching the memory
// managed by this vp is counted as unmanaged memory.
func (vp *ArchiveVectorParty) Allocate(hasCount bool) {
	vp.offsets = memstore.NewVector(common.Uint32, vp.length*2)
	vp.values = memstore.NewVector(common.Uint8, int(vp.totalValueBytes))
}

// GetBytes returns the bytes this vp occupies.
func (vp *ArchiveVectorParty) GetBytes() int64 {
	if vp.values != nil && vp.offsets != nil {
		return int64(vp.values.Bytes + vp.offsets.Bytes)
	}
	return 0
}

// SafeDestruct destructs vector party memory.
func (vp *ArchiveVectorParty) SafeDestruct() {
	if vp != nil {
		vp.offsets.SafeDestruct()
		vp.offsets = nil
		vp.values.SafeDestruct()
		vp.values = nil
	}
}

// AsList is the implementation from common.VectorParty
func (vp *ArchiveVectorParty) AsList() common.ListVectorParty {
	return vp
}

// Equals is the implementation from common.VectorParty
func (vp *ArchiveVectorParty) Equals(other common.VectorParty) bool {
	return vp.equals(other, vp.AsList())
}

// GetValue TODO handling invalid case
func (vp *ArchiveVectorParty) getValue(row int) (val unsafe.Pointer, validity bool) {
	offset, length := vp.GetOffsetLength(row)
	if offset == 0 && length == 0 {
		return unsafe.Pointer(uintptr(0)), false
	}
	baseAddr := uintptr(vp.values.Buffer())
	return unsafe.Pointer(baseAddr + uintptr(offset)), true
}

// GetDataValue is not implemented in baseVectorParty
func (vp *ArchiveVectorParty) GetDataValue(row int) common.DataValue {
	val, valid := vp.getValue(row)
	return common.DataValue{
		DataType: vp.dataType,
		Valid:    valid,
		OtherVal: val,
	}
}

// GetDataValueByRow just call GetDataValue
func (vp *ArchiveVectorParty) GetDataValueByRow(row int) common.DataValue {
	return vp.GetDataValue(row)
}

func (vp *ArchiveVectorParty) setValue(row int, val unsafe.Pointer, valid bool) {
	if !valid {
		var zero uint32
		vp.offsets.SetValue(2*row, unsafe.Pointer(&zero))
		vp.offsets.SetValue(2*row+1, unsafe.Pointer(&zero))
		return
	}
	newLen := int(*(*uint32)(val))
	newBytes := common.CalculateListElementBytes(vp.dataType, newLen)
	// Set offset.
	vp.offsets.SetValue(2*row, unsafe.Pointer(&vp.bytesWritten))
	// Set length.
	vp.offsets.SetValue(2*row+1, unsafe.Pointer(&newLen))

	baseAddr := uintptr(vp.values.Buffer()) + uintptr(vp.bytesWritten)
	to := cgoutils.MakeSliceFromCPtr(baseAddr, newBytes)
	from := cgoutils.MakeSliceFromCPtr(uintptr(val), newBytes)
	for i := 0; i < newBytes; i++ {
		to[i] = from[i]
	}

	vp.bytesWritten += int64(newBytes)
}

// SetDataValue is the implentation of common.VecotrParty
func (vp *ArchiveVectorParty) SetDataValue(row int, value common.DataValue,
	countsUpdateMode common.ValueCountsUpdateMode, counts ...uint32) {
	vp.setValue(row, value.OtherVal, value.Valid)
}

// SetListValue is the implentation of common.ListVecotrParty
func (vp *ArchiveVectorParty) GetListValue(row int) (unsafe.Pointer, bool) {
	return vp.getValue(row)
}

// SetListValue is the implentation of common.ListVecotrParty
func (vp *ArchiveVectorParty) SetListValue(row int, val unsafe.Pointer, valid bool) {
	vp.setValue(row, val, valid)
}

// Write is the implentation of common.VecotrParty
func (vp *ArchiveVectorParty) Write(writer io.Writer) error {
	dataWriter := utils.NewStreamDataWriter(writer)
	if err := dataWriter.WriteUint32(ListVectorPartyHeader); err != nil {
		return err
	}

	// length
	if err := dataWriter.WriteInt32(int32(vp.length)); err != nil {
		return err
	}

	// data type
	if err := dataWriter.WriteUint32(uint32(vp.dataType)); err != nil {
		return err
	}

	// nonDefaultValue count, 0 for List VectorParty
	if err := dataWriter.WriteInt32(int32(0)); err != nil {
		return err
	}

	// columnMode, AllValuesPresent for now
	columnMode := common.AllValuesPresent
	if err := dataWriter.WriteUint16(uint16(columnMode)); err != nil {
		return err
	}

	// Write 6 bytes padding.
	if err := dataWriter.SkipBytes(6); err != nil {
		return err
	}

	// Write offset vector.
	// Here we directly move data from c allocated memory into writer.
	if err := dataWriter.Write(cgoutils.MakeSliceFromCPtr(uintptr(vp.offsets.Buffer()), vp.offsets.Bytes)); err != nil {
		return err
	}

	// value bytes
	if err := dataWriter.WriteUint64(uint64(vp.values.Bytes)); err != nil {
		return err
	}
	// Write value vector.
	if err := dataWriter.Write(
		cgoutils.MakeSliceFromCPtr(uintptr(vp.values.Buffer()), vp.values.Bytes),
	); err != nil {
		return err
	}

	return nil
}

// Read reads a vector party from underlying reader. It first reads header from the reader and does
// several sanity checks. Then it reads vectors based on vector party mode.
func (vp *ArchiveVectorParty) Read(reader io.Reader, s common.VectorPartySerializer) error {
	dataReader := utils.NewStreamDataReader(reader)
	magicNumber, err := dataReader.ReadUint32()
	defer func() {
		if err != nil {
			if vp.offsets != nil {
				vp.offsets.SafeDestruct()
			}
			if vp.values != nil {
				vp.values.SafeDestruct()
			}
		}
	}()

	if err != nil {
		return err
	}

	if magicNumber != ListVectorPartyHeader {
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

	// non default value count
	_, err = dataReader.ReadInt32()
	if err != nil {
		return err
	}

	// column mode
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
	vp.dataType = dataType

	vp.offsets = memstore.NewVector(common.Uint32, vp.length*2)
	if err = dataReader.Read(cgoutils.MakeSliceFromCPtr(uintptr(vp.offsets.Buffer()), vp.offsets.Bytes), ); err != nil {
		return err
	}

	// Read value bytes
	bytes, err := dataReader.ReadUint64()
	if err != nil {
		return err
	}
	vp.totalValueBytes = int64(bytes)
	// Read value vector.
	vp.values = memstore.NewVector(common.Uint8, int(vp.totalValueBytes))
	// Here we directly read from reader into the c allocated bytes.
	if err = dataReader.Read(cgoutils.MakeSliceFromCPtr(uintptr(vp.values.Buffer()), vp.values.Bytes), ); err != nil {
		return err
	}

	s.ReportVectorPartyMemoryUsage(int64(vp.length*4*2) + vp.totalValueBytes)

	return nil
}

// GetCount returns cumulative count on specified offset.
func (vp *ArchiveVectorParty) GetCount(offset int) uint32 {
	// Same as non mode 3 vector.
	return 1
}

// SetCount is not supported by list vector party.
func (vp *ArchiveVectorParty) SetCount(offset int, count uint32) {
	utils.GetLogger().Panic("SetCount is not supported by list vector party")
}

// LoadFromDisk load archive vector party from disk caller should lock archive batch before using
func (vp *ArchiveVectorParty) LoadFromDisk(hostMemManager common.HostMemoryManager, diskStore diskstore.DiskStore,
	table string, shardID int, columnID, batchID int, batchVersion uint32, seqNum uint32) {
	vp.Loader.Add(1)
	go func() {
		serializer := memstore.NewVectorPartyArchiveSerializer(hostMemManager, diskStore, table, shardID, columnID, batchID, batchVersion, seqNum)
		err := serializer.ReadVectorParty(vp)
		if err != nil {
			utils.GetLogger().Panic(err)
		}
		vp.Loader.Done()
	}()
}

// Prune prunes vector party based on column mode to clean memory if possible
func (vp *ArchiveVectorParty) Prune() {
	// Nothing to prune for list vp.
}

// SliceByValue is not supported by list vector party.
func (vp *ArchiveVectorParty) SliceByValue(lowerBoundRow, upperBoundRow int, value unsafe.Pointer) (
	startRow int, endRow int, startIndex int, endIndex int) {
	utils.GetLogger().Panic("SliceByValue is not supported by list vector party")
	return
}

// Slice vector party to get [startIndex, endIndex) based on [lowerBoundRow, upperBoundRow)
func (vp *ArchiveVectorParty) SliceIndex(lowerBoundRow, upperBoundRow int) (
	startIndex, endIndex int) {
	return lowerBoundRow, upperBoundRow
}

// CopyOnWrite is not supported by list vector party
func (vp *ArchiveVectorParty) CopyOnWrite(batchSize int) common.ArchiveVectorParty {
	utils.GetLogger().Panic("CopyOnWrite is not supported by list vector party")
	return nil
}


