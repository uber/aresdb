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
	"github.com/uber/aresdb/diskstore"
	"io"
	"unsafe"
)

// ColumnMode represents how many vectors a vector party may have.
// For live batch, it should always be 0,1 or 2.
// For sorted column of archive batch, it will be mode 0 or 3.
// For other columns of archive batch, it can be any of these four modes.
type ColumnMode int

const (
	// AllValuesDefault (mode 0)
	AllValuesDefault ColumnMode = iota
	// AllValuesPresent (mode 1)
	AllValuesPresent
	// HasNullVector (mode 2)
	HasNullVector
	// HasCountVector (mode 3)
	HasCountVector
	// MaxColumnMode represents the upper limit of column modes
	MaxColumnMode
)

// HostVectorPartySlice stores pointers to data for a column in host memory.
// And its start index and Bytes
type HostVectorPartySlice struct {
	Values unsafe.Pointer
	Nulls  unsafe.Pointer
	// The length of the count vector is Length+1
	Counts       unsafe.Pointer
	Length       int
	ValueType    DataType
	DefaultValue DataValue

	ValueStartIndex int
	NullStartIndex  int
	CountStartIndex int

	ValueBytes int
	NullBytes  int
	CountBytes int
}

// ValueCountsUpdateMode represents the way we update value counts when we are writing values to
// vector parties.
type ValueCountsUpdateMode int

// SlicedVector is vector party data represented into human-readable slice format
// consists of a value slice and count slice,
// count slice consists of accumulative counts.
// swagger:model slicedVector
type SlicedVector struct {
	Values []interface{} `json:"values"`
	Counts []int         `json:"counts"`
}

// VectorPartySerializer is the interface to read/write a vector party from/to disk. Refer to
// https://github.com/uber/aresdb/wiki/VectorStore for more details about
// vector party's on disk format.
type VectorPartySerializer interface {
	// ReadVectorParty reads vector party from disk and set fields in passed-in vp.
	ReadVectorParty(vp VectorParty) error
	// WriteSnapshotVectorParty writes vector party to disk
	WriteVectorParty(vp VectorParty) error
	// CheckVectorPartySerializable check if the VectorParty is serializable
	CheckVectorPartySerializable(vp VectorParty) error
	// ReportVectorPartyMemoryUsage report memory usage according to underneath VectorParty property
	ReportVectorPartyMemoryUsage(bytes int64)
}

// VectorParty interface
type VectorParty interface {
	//   allocate underlying storage for vector party
	Allocate(hasCount bool)

	// GetValidity get validity of given offset.
	GetValidity(offset int) bool
	// GetDataValue returns the DataValue for the specified index.
	// It first check validity of the value, then it check whether it's a
	// boolean column to decide whether to load bool value or other value
	// type. Index bound is not checked!
	GetDataValue(offset int) DataValue
	// SetDataValue writes a data value at given offset. Third parameter count should
	// only be passed for compressed columns. checkValueCount is a flag to tell whether
	// need to check value count (NonDefaultValueCount and ValidValueCount) while setting
	// the value. It should be true for archive store and false for live store. **This does
	// not set the count vector as this is not accumulated count.**
	SetDataValue(offset int, value DataValue, countsUpdateMode ValueCountsUpdateMode, counts ...uint32)
	// GetDataValueByRow returns the DataValue for the specified row. It will do binary
	// search on the count vector to find the correct offset if this is a mode 3 vector
	// party. Otherwise it will behave same as GetDataValue.
	// Caller needs to ensure row is within valid range.
	GetDataValueByRow(row int) DataValue

	GetDataType() DataType
	GetLength() int
	GetBytes() int64

	// Slice vector party into human readable SlicedVector format
	Slice(startRow, numRows int) SlicedVector

	// SafeDestruct destructs vector party memory
	SafeDestruct()

	// Write serialize vector party
	Write(writer io.Writer) error
	// Read deserialize vector party
	Read(reader io.Reader, serializer VectorPartySerializer) error
	// Check whether two vector parties are equal (used only in unit tests)
	Equals(other VectorParty) bool
	// GetNonDefaultValueCount get Number of non-default values stored
	GetNonDefaultValueCount() int
	// IsList tells whether it's a list vector party or not
	IsList() bool
	// AsList returns ListVectorParty representation of this vector party.
	// Caller should always call IsList before conversion, otherwise panic may happens
	// for incompatible vps.
	AsList() ListVectorParty
}

// CVectorParty is vector party that is backed by c
type CVectorParty interface {
	//Judge column mode
	JudgeMode() ColumnMode
	// Get column mode
	GetMode() ColumnMode
}

// LiveVectorParty represents vector party in live store
type LiveVectorParty interface {
	VectorParty

	// Note for all following functions, data type are not checked. So callee need to perform the data type check
	// and call the correct SetXXX function.

	// If we already know this is a bool vp, we can set bool directly without constructing a data value struct.
	SetBool(offset int, val bool, valid bool)
	// Set value via a unsafe.Pointer directly.
	SetValue(offset int, val unsafe.Pointer, valid bool)
	// Set go value directly.
	SetGoValue(offset int, val GoDataValue, valid bool)
	// Get value directly
	GetValue(offset int) (unsafe.Pointer, bool)
	// GetMinMaxValue get min and max value,
	// returns uint32 value since only valid for time column
	GetMinMaxValue() (min, max uint32)
}

// ArchiveVectorParty represents vector party in archive store
type ArchiveVectorParty interface {
	VectorParty

	// Get cumulative count on specified offset
	GetCount(offset int) uint32
	// set cumulative count on specified offset
	SetCount(offset int, count uint32)

	// Pin archive vector party for use
	Pin()
	// Release pin
	Release()
	// WaitForUsers Wait/Check whether all users finished
	// batch lock needs to be held before calling if blocking wait
	// eg.
	// 	batch.Lock()
	// 	vp.WaitForUsers(true)
	// 	batch.Unlock()
	WaitForUsers(blocking bool) (usersDone bool)

	// CopyOnWrite copies vector party on write/update
	CopyOnWrite(batchSize int) ArchiveVectorParty
	// LoadFromDisk start loading vector party from disk,
	// this is a non-blocking operation
	LoadFromDisk(hostMemManager HostMemoryManager, diskStore diskstore.DiskStore, table string, shardID int, columnID, batchID int, batchVersion uint32, seqNum uint32)
	// WaitForDiskLoad waits for vector party disk load to finish
	WaitForDiskLoad()
	// Prune prunes vector party based on column mode to clean memory if possible
	Prune()

	// Slice vector party using specified value within [lowerBoundRow, upperBoundRow)
	SliceByValue(lowerBoundRow, upperBoundRow int, value unsafe.Pointer) (startRow int, endRow int, startIndex int, endIndex int)
	// Slice vector party to get [startIndex, endIndex) based on [lowerBoundRow, upperBoundRow)
	SliceIndex(lowerBoundRow, upperBoundRow int) (startIndex, endIndex int)
}

// ListVectorParty is the interface for list vector party to read and write list value.
type ListVectorParty interface {
	GetListValue(row int) (unsafe.Pointer, bool)
	SetListValue(row int, val unsafe.Pointer, valid bool)
}
