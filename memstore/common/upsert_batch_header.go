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
	"github.com/uber/aresdb/utils"
)

// ColumnHeaderSize returns the total size of the column headers.
func ColumnHeaderSize(numCols int) int {
	return (numCols+1)*4 + // offset (4 bytes)
		numCols*4 + // enum dict capacity (4 bytes)
		numCols*4 + // reserved (4 bytes)
		numCols*4 + // data_type (4 bytes)
		numCols*2 + // column_id (2 bytes)
		numCols // column mode (1 byte)
}

// UpsertBatchHeader is a helper class used by upsert batch reader and writer to access the column
// header info.
type UpsertBatchHeader struct {
	offsetVector   []byte
	enumDictLength []byte
	typeVector     []byte
	idVector       []byte
	modeVector     []byte
}

// NewUpsertBatchHeader create upsert batch header from buffer
func NewUpsertBatchHeader(buffer []byte, numCols int) UpsertBatchHeader {
	offset := 0
	// Offset vector is of size numCols + 1.
	offsetVector := buffer[offset : offset+(numCols+1)*4]
	offset += len(offsetVector)

	enumDictLength := buffer[offset : offset+numCols*4]
	offset += len(enumDictLength) +
		numCols*4 // reserved extra space

	typeVector := buffer[offset : offset+numCols*4]
	offset += len(typeVector)

	idVector := buffer[offset : offset+numCols*2]
	offset += len(idVector)

	modeVector := buffer[offset : offset+numCols]

	return UpsertBatchHeader{
		offsetVector:   offsetVector,
		enumDictLength: enumDictLength,
		typeVector:     typeVector,
		idVector:       idVector,
		modeVector:     modeVector,
	}
}

// WriteColumnOffset writes the offset of a column. It can take col index from 0 to numCols + 1.
func (u *UpsertBatchHeader) WriteColumnOffset(value int, col int) error {
	writer := utils.NewBufferWriter(u.offsetVector)
	err := writer.WriteUint32(uint32(value), col*4)
	if err != nil {
		return utils.StackError(err, "Failed to write start offset for column %d", col)
	}
	return nil
}

// WriteEnumDictLength writes the offset of a column. It can take col index from 0 to numCols - 1.
func (u *UpsertBatchHeader) WriteEnumDictLength(value int, col int) error {
	writer := utils.NewBufferWriter(u.enumDictLength)
	err := writer.WriteUint32(uint32(value), col*4)
	if err != nil {
		return utils.StackError(err, "Failed to write enum dict capacity for column %d", col)
	}
	return nil
}

// WriteColumnType writes the type of a column.
func (u *UpsertBatchHeader) WriteColumnType(value DataType, col int) error {
	writer := utils.NewBufferWriter(u.typeVector)
	err := writer.WriteUint32(uint32(value), col*4)
	if err != nil {
		return utils.StackError(err, "Failed to write type for column %d", col)
	}
	return nil
}

// WriteColumnID writes the id of a column.
func (u *UpsertBatchHeader) WriteColumnID(value int, col int) error {
	writer := utils.NewBufferWriter(u.idVector)
	err := writer.WriteUint16(uint16(value), col*2)
	if err != nil {
		return utils.StackError(err, "Failed to write id for column %d", col)
	}
	return nil
}

// WriteColumnFlag writes the mode of a column.
func (u *UpsertBatchHeader) WriteColumnFlag(columnMode ColumnMode, columnUpdateMode ColumnUpdateMode, col int) error {
	value := uint8(columnMode&0x07) | uint8((columnUpdateMode&0x07)<<3)
	writer := utils.NewBufferWriter(u.modeVector)
	err := writer.WriteUint8(value, col)
	if err != nil {
		return utils.StackError(err, "Failed to write mode for column %d", col)
	}
	return nil
}

// ReadColumnOffset takes col index from 0 to numCols + 1 and returns the value stored.
func (u UpsertBatchHeader) ReadColumnOffset(col int) (int, error) {
	result, err := utils.NewBufferReader(u.offsetVector).ReadUint32(col * 4)
	if err != nil {
		return 0, err
	}
	return int(result), err
}

// ReadEnumDictLength takes col index from 0 to numCols - 1 and returns the value stored.
func (u UpsertBatchHeader) ReadEnumDictLength(col int) (int, error) {
	result, err := utils.NewBufferReader(u.enumDictLength).ReadUint32(col * 4)
	if err != nil {
		return 0, err
	}
	return int(result), err
}

// ReadColumnType returns the type for a column.
func (u UpsertBatchHeader) ReadColumnType(col int) (DataType, error) {
	result, err := utils.NewBufferReader(u.typeVector).ReadUint32(col * 4)
	if err != nil {
		return Unknown, err
	}
	columnDataType, err := NewDataType(result)
	if err != nil {
		return Unknown, utils.StackError(err, "Unsupported data type %#x for col %d", result, col)
	}
	return columnDataType, nil
}

// ReadColumnID returns the logical ID for a column.
func (u UpsertBatchHeader) ReadColumnID(col int) (int, error) {
	result, err := utils.NewBufferReader(u.idVector).ReadUint16(col * 2)
	if err != nil {
		return 0, err
	}
	return int(result), err
}

// ReadColumnFlag returns the mode for a column.
func (u UpsertBatchHeader) ReadColumnFlag(col int) (ColumnMode, ColumnUpdateMode, error) {
	result, err := utils.NewBufferReader(u.modeVector).ReadUint8(col)
	if err != nil {
		return 0, 0, err
	}
	columnMode := ColumnMode(result & 0x0007)
	columnUpdateMode := ColumnUpdateMode((result >> 3) & 0x0007)
	if columnMode >= MaxColumnMode {
		return columnMode, columnUpdateMode, utils.StackError(err, "Invalid column mode %d", columnMode)
	} else if columnUpdateMode >= MaxColumnUpdateMode {
		return columnMode, columnUpdateMode, utils.StackError(err, "Invalid column update mode %d", columnUpdateMode)
	}
	return columnMode, columnUpdateMode, nil
}
