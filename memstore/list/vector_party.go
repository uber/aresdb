package list

import "C"
import (
	"github.com/uber/aresdb/diskstore"
	"github.com/uber/aresdb/memstore"
	"github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/utils"
	"io"
	"sync"
	"unsafe"
)

// baseVectorParty is the shared struct for live store list vp and archive store
// list vp. Some difference between normal base vp and list base vp:
// 1. There is no mode concept for list vp. therefore nonDefaultValueCount does not make sense for
// 	list vp.
// 2. There is no default value concept for list at least for now, so we will not store default value.
type baseVectorParty struct {
	// offset is a pair of uint32 [offset, length]. therefore its length is 2 * length of vp.
	offsets *memstore.Vector
	// length of vp.
	length int
	// DataType of values. We need it since for mode 0 vector party, we cannot
	// get data type from values vector. Also we store it on disk anyway.
	dataType common.DataType
	// total number of elements.
	totalElements int64
	reporter      HostMemoryChangeReporter
}

// GetOffsetLength returns the <offset, length> pair at ith row.
func (vp *baseVectorParty) GetOffsetLength(row int) (offset uint32, length uint32) {
	offset = *(*uint32)(vp.offsets.GetValue(2 * row))
	length = *(*uint32)(vp.offsets.GetValue(2*row + 1))
	return
}

// GetValidity get validity of given offset. For list type it should always be true even for
// empty list.
func (vp *baseVectorParty) GetValidity(offset int) bool {
	return true
}

// GetDataValue just return null data value for list type vp. It's questionable to store list element value in
// this common.DataValue since this struct is already too huge.
// TODO(lucafuji): figure out how to read individual element from list vector party.
func (vp *baseVectorParty) GetDataValue(offset int) common.DataValue {
	return common.NullDataValue
}

// SetDataValue is not supported by list vector party.
func (vp *baseVectorParty) SetDataValue(offset int, value common.DataValue,
	countsUpdateMode common.ValueCountsUpdateMode, counts ...uint32) {
	utils.GetLogger().Panic("SetDataValue is not supported by list vector party")
}

// GetDataValueByRow just returns a null data value for now.
// TODO(lucafuji): figure out how to read individual element from list vector party.
func (vp *baseVectorParty) GetDataValueByRow(row int) common.DataValue {
	return common.NullDataValue
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

// GetElementLength returns the length of ith element
func (vp *baseVectorParty) GetElementLength(row int) int {
	_, l := vp.GetOffsetLength(row)
	return int(l)
}

// Slice vector party into human readable SlicedVector format. For now just return an
// empty slice.
// TODO(lucafuji): implement slice vector on list vp.
func (vp *baseVectorParty) Slice(startRow, numRows int) common.SlicedVector {
	return common.SlicedVector{}
}

// Check whether two vector parties are equal (used only in unit tests)
// Check common properties like data type and length.`
func (vp *baseVectorParty) equals(other common.VectorParty, listVP common.ListVectorParty) bool {
	if !other.IsList() {
		return false
	}

	if vp.GetDataType() != other.GetDataType() {
		return false
	}

	if vp.GetLength() != other.GetLength() {
		return false
	}

	rightListVP := other.AsList()
	cmpFunc := common.GetCompareFunc(vp.GetDataType())
	for i := 0; i < other.GetLength(); i++ {
		lLength := vp.GetElementLength(i)
		rLength := rightListVP.GetElementLength(i)
		if lLength != rLength {
			return false
		}

		for j := 0; j < lLength; j++ {
			lValidity := listVP.ReadElementValidity(i, j)
			rValidity := rightListVP.ReadElementValidity(i, j)
			if lValidity != rValidity {
				return false
			}

			if lValidity == true {
				if vp.GetDataType() == common.Bool {
					if listVP.ReadElementBool(i, j) != rightListVP.ReadElementBool(i, j) {
						return false
					}
				} else {
					lValue := listVP.ReadElementValue(i, j)
					rValue := rightListVP.ReadElementValue(i, j)
					if cmpFunc(lValue, rValue) != 0 {
						return false
					}
				}
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

func (vp *baseVectorParty) readElementValue(baseAddr uintptr, row int, i int) unsafe.Pointer {
	offset, length := vp.GetOffsetLength(row)
	if i < 0 || i >= int(length) {
		return nil
	}

	return common.GetValue(baseAddr+uintptr(offset), i, vp.dataType)
}

func (vp *baseVectorParty) readElementBool(baseAddr uintptr, row int, i int) bool {
	offset, length := vp.GetOffsetLength(row)
	if i < 0 || i >= int(length) {
		utils.GetLogger().Panic("runtime error: index out of range")
		return false
	}

	return common.GetBool(baseAddr+uintptr(offset), i)
}

func (vp *baseVectorParty) readElementValidity(baseAddr uintptr, row int, i int) bool {
	offset, length := vp.GetOffsetLength(row)
	if i < 0 || i >= int(length) {
		utils.GetLogger().Panic("runtime error: index out of range")
		return false
	}
	i += common.DataTypeBits(vp.dataType) * int(length)
	return common.GetBool(baseAddr+uintptr(offset), i)
}

// LiveVectorParty is the representation of list data type vector party in live store.
// It supports random access read and write. However, it does not support serialization into disk.
// It underlying uses a high level memory pool to store the list data. Therefore when this vector
// party is destructed, the underlying memory pool needs to be destroyed as well.
type LiveVectorParty struct {
	baseVectorParty
	// storing the offset to slab footer offset for each row.
	caps       *memstore.Vector
	memoryPool HighLevelMemoryPool
}

// GetBytes returns the bytes this vp occupies.
func (vp *LiveVectorParty) GetBytes() int64 {
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
		vp.offsets.SafeDestruct()
		vp.offsets = nil
		vp.caps.SafeDestruct()
		vp.caps = nil
		vp.memoryPool.Destroy()
		vp.memoryPool = nil
		vp.reporter(-totalBytes)
	}
}

// Equals whether two vector parties are equal (used only in unit tests)
func (vp *LiveVectorParty) Equals(other common.VectorParty) bool {
	return vp.equals(other, vp.AsList())
}

// Write serialize vector party.
// TODO(lucafuji): implement live vp serialization (in snapshot).
func (vp *LiveVectorParty) Write(writer io.Writer) error {
	return nil
}

// Read deserialize vector party
// TODO(lucafuji): implement live vp serialization (in snapshot).
func (vp *LiveVectorParty) Read(reader io.Reader, serializer common.VectorPartySerializer) error {
	return nil
}

// GetCap returns the cap at ith row. Only used for free a list element in live store.
func (vp *LiveVectorParty) GetCap(row int) uint32 {
	return *(*uint32)(vp.caps.GetValue(row))
}

// SetBool is not supported by list vector party.
func (vp *LiveVectorParty) SetBool(offset int, val bool, valid bool) {
	utils.GetLogger().Panic("SetBool is not supported by list vector party")
}

// SetValue is not supported by list vector party.
func (vp *LiveVectorParty) SetValue(offset int, val unsafe.Pointer, valid bool) {
	utils.GetLogger().Panic("SetValue is not supported by list vector party")
}

// SetGoValue is not supported by list vector party.
func (vp *LiveVectorParty) SetGoValue(offset int, val common.GoDataValue, valid bool) {
	utils.GetLogger().Panic("SetGoValue is not supported by list vector party")
}

// GetValue is not supported by list vector party.
func (vp *LiveVectorParty) GetValue(offset int) (val unsafe.Pointer, validity bool) {
	utils.GetLogger().Panic("GetValue is not supported by list vector party")
	return
}

// GetMinMaxValue is not supported by list vector party.
func (vp *LiveVectorParty) GetMinMaxValue() (min, max uint32) {
	utils.GetLogger().Panic("GetMinMaxValue is not supported by list vector party")
	return
}

// ReadElementValue implements ReadElementValue of interface common.ListVectorParty.
func (vp *LiveVectorParty) ReadElementValue(row int, i int) unsafe.Pointer {
	return vp.readElementValue(
		vp.memoryPool.GetNativeMemoryAllocator().GetBaseAddr(), row, i)
}

// ReadElementBool implements ReadElementBool of interface common.ListVectorParty.
func (vp *LiveVectorParty) ReadElementBool(row int, i int) bool {
	return vp.readElementBool(
		vp.memoryPool.GetNativeMemoryAllocator().GetBaseAddr(), row, i)
}

// ReadElementValidity implements ReadElementValidity of interface common.ListVectorParty.
func (vp *LiveVectorParty) ReadElementValidity(row int, i int) bool {
	return vp.readElementValidity(
		vp.memoryPool.GetNativeMemoryAllocator().GetBaseAddr(), row, i)
}

// AsList returns ListVectorParty representation of this vector party.
// Caller should always call IsList before conversion, otherwise panic may happens
// for incompatible vps.
func (vp *LiveVectorParty) AsList() common.ListVectorParty {
	return vp
}

// SetListValue sets a list value at ith row. Here we use a DataValueReader as the data source
// to avoid data copy of the list value.
func (vp *LiveVectorParty) SetListValue(row int, reader common.ListDataValueReader) {
	newLen := uint32(reader.GetElementLength(row))
	oldOffset, oldLen := vp.GetOffsetLength(row)
	oldCap := vp.GetCap(row)
	oldBytes := common.CalculateListElementBytes(vp.dataType, int(oldLen))
	newBytes := common.CalculateListElementBytes(vp.dataType, int(newLen))

	buf := vp.memoryPool.Reallocate([2]uintptr{uintptr(oldOffset),
		uintptr(oldCap)}, oldBytes, newBytes)

	// Set offset.
	vp.offsets.SetValue(2*row, unsafe.Pointer(&buf[0]))
	// Set length.
	vp.offsets.SetValue(2*row+1, unsafe.Pointer(&newLen))
	// Set footer offset.
	vp.caps.SetValue(row, unsafe.Pointer(&buf[1]))

	baseAddr := vp.memoryPool.Interpret(buf[0])

	// Set val first.
	for i := 0; i < int(newLen); i++ {
		if vp.dataType == common.Bool {
			vp.setElementBool(baseAddr, i, reader.ReadElementBool(row, i))
		} else {
			valPtr := reader.ReadElementValue(row, i)
			if valPtr != nil {
				vp.setElementValue(baseAddr, i, valPtr)
			}
		}
	}

	// Then set validity.
	for i := 0; i < int(newLen); i++ {
		vp.setElementValidity(baseAddr, i, reader.ReadElementValidity(row, i), int(newLen))
	}

	vp.totalElements += int64(newLen) - int64(oldLen)
}

func (vp *LiveVectorParty) setElementBool(baseAddr uintptr, offset int,
	val bool) {
	common.SetBool(baseAddr, offset, val)
}

func (vp *LiveVectorParty) setElementValidity(baseAddr uintptr, offset int,
	val bool, newLen int) {
	// need to skip newLen of values
	offset += common.DataTypeBits(vp.dataType) * newLen
	common.SetBool(baseAddr, offset, val)
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

func (vp *LiveVectorParty) setElementValue(baseAddr uintptr, offset int,
	val unsafe.Pointer) {
	common.SetValue(baseAddr, offset, val, vp.dataType)
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

// ArchiveVectorParty is the representation of list data type vector party in archive store.
// It does not support random access update. Instead updates to archiveListVectorParty can only be done
// via appending to the tail during archiving and backfill.
// It use a single value vector to store the values and validities so that it has the same
// in memory representation except archiving vp does not have cap vector.
type ArchiveVectorParty struct {
	baseVectorParty
	memstore.PinnableVectorParty
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
		PinnableVectorParty: memstore.PinnableVectorParty{
			AllUsersDone: sync.NewCond(locker),
		},
		totalValueBytes: totalValueBytes,
	}
}

// Equals whether two vector parties are equal (used only in unit tests)
func (vp *ArchiveVectorParty) Equals(other common.VectorParty) bool {
	return vp.equals(other, vp.AsList())
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

// Write serialize vector party.
// TODO(lucafuji): implement live vp serialization (in snapshot).
func (vp *ArchiveVectorParty) Write(writer io.Writer) error {
	return nil
}

// Read deserialize vector party
// TODO(lucafuji): implement live vp deserialization (in snapshot).
func (vp *ArchiveVectorParty) Read(reader io.Reader, serializer common.VectorPartySerializer) error {
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
		// TODO(lucafuji): implement disk loading logic.
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

// SetListValue in ArchiveVectorParty works in an append-only way.
func (vp *ArchiveVectorParty) SetListValue(row int, reader common.ListDataValueReader) {
	newLen := reader.GetElementLength(row)
	// Set offset.
	vp.offsets.SetValue(2*row, unsafe.Pointer(&vp.bytesWritten))
	// Set length.
	vp.offsets.SetValue(2*row+1, unsafe.Pointer(&newLen))

	baseAddr := uintptr(vp.values.Buffer()) + uintptr(vp.bytesWritten)
	// Set value first.
	for i := 0; i < newLen; i++ {
		if vp.dataType == common.Bool {
			common.SetBool(baseAddr, i, reader.ReadElementBool(row, i))
		} else {
			common.SetValue(baseAddr, i, reader.ReadElementValue(row, i), vp.dataType)
		}
	}

	// Then set validity.

	// Nulls starts after values, so we need to adjust the idx by adding
	// common.DataTypeBits(builder.dataType) * newLen.
	validitiesOffset := common.DataTypeBits(vp.dataType) * newLen
	for i := 0; i < newLen; i++ {
		common.SetBool(baseAddr, i+validitiesOffset, reader.ReadElementValidity(row, i))
	}

	vp.totalElements += int64(newLen)
	vp.bytesWritten += int64(common.CalculateListElementBytes(vp.dataType, newLen))
}

// AsList returns ListVectorParty representation of this vector party.
// Caller should always call IsList before conversion, otherwise panic may happens
// for incompatible vps.
func (vp *ArchiveVectorParty) AsList() common.ListVectorParty {
	return vp
}

// ReadElementValue implements ReadElementValue of interface common.ListVectorParty.
func (vp *ArchiveVectorParty) ReadElementValue(row int, i int) unsafe.Pointer {
	return vp.readElementValue(uintptr(vp.values.Buffer()), row, i)
}

// ReadElementBool implements ReadElementBool of interface common.ListVectorParty.
func (vp *ArchiveVectorParty) ReadElementBool(row int, i int) bool {
	return vp.readElementBool(uintptr(vp.values.Buffer()), row, i)
}

// ReadElementValidity implements ReadElementValidity of interface common.ListVectorParty.
func (vp *ArchiveVectorParty) ReadElementValidity(row int, i int) bool {
	return vp.readElementValidity(uintptr(vp.values.Buffer()), row, i)
}
