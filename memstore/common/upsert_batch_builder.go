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
	"math"
	"unsafe"
)

// ColumnUpdateMode represents how to update data from UpsertBatch
type ColumnUpdateMode int

// UpsertBatchVersion represents the version of upsert batch
type UpsertBatchVersion uint32

const (
	// UpdateOverwriteNotNull (default) will overwrite existing value if new value is NOT null, otherwise just skip
	UpdateOverwriteNotNull ColumnUpdateMode = iota
	// UpdateForceOverwrite will simply overwrite existing value even when new data is null
	UpdateForceOverwrite
	// UpdateWithAddition will add the existing value with new value if new value is not null, existing null value will be treated as 0 in Funculation
	UpdateWithAddition
	// UpdateWithMin will save the minimum of existing and new value if new value is not null, existing null value will be treated as MAX_INT in Funculation
	UpdateWithMin
	// UpdateWithMax will save the maximum of existing and new value if new value is not null, existing null value will be treated as MIN_INT in Funculation
	UpdateWithMax
	// MaxColumnUpdateMode is the current upper limit for column update modes
	MaxColumnUpdateMode
)

const (
	V1 UpsertBatchVersion = 0xFEED0001
)

type columnBuilder struct {
	columnID       int
	dataType       DataType
	values         []interface{}
	numValidValues int
	updateMode     ColumnUpdateMode
}

// SetValue write a value into the column at given row.
func (c *columnBuilder) SetValue(row int, value interface{}) error {
	oldValueNull := c.values[row] == nil

	if value == nil {
		c.values[row] = nil
	} else {
		var err error
		c.values[row], err = ConvertValueForType(c.dataType, value)
		if err != nil {
			return err
		}
	}

	if oldValueNull && c.values[row] != nil {
		c.numValidValues++
	} else if !oldValueNull && c.values[row] == nil {
		c.numValidValues--
	}

	return nil
}

// AddRow grow the value array by 1.
func (c *columnBuilder) AddRow() {
	c.values = append(c.values, nil)
}

// AddRow shrink the value array by 1.
func (c *columnBuilder) RemoveRow() {
	lastValueIndex := len(c.values) - 1
	lastValueNull := c.values[lastValueIndex] == nil
	c.values = c.values[:lastValueIndex]
	if !lastValueNull {
		c.numValidValues--
	}
}

// ResetRows reset the row count to 0.
func (c *columnBuilder) ResetRows() {
	c.values = c.values[0:0]
	c.numValidValues = 0
}

// Calculated BufferSize returns the size of the column data in serialized format.
func (c *columnBuilder) CalculateBufferSize(offset *int) {
	isGoType := IsGoType(c.dataType)
	isArrayType := IsArrayType(c.dataType)

	switch c.GetMode() {
	case AllValuesDefault:
	case HasNullVector:
		if !isGoType {
			*offset += (len(c.values) + 7) / 8
		}
		fallthrough
	case AllValuesPresent:
		// if golang memory or array type, align to 4 bytes for offset vector
		if isGoType || isArrayType {
			*offset = utils.AlignOffset(*offset, 4)
			// 1. uint32 for each offset value, and length = numRows + 1
			// 2. last offset value is the end offset of the offset buffer
			*offset += (len(c.values) + 1) * 4
			// Padding size for value vector
			*offset = utils.AlignOffset(*offset, 8)
			for _, v := range c.values {
				if v != nil {
					if isGoType {
						goVal := v.(GoDataValue)
						*offset += goVal.GetSerBytes()
					} else {
						arrVal := v.(*ArrayValue)
						*offset += arrVal.GetSerBytes()
					}
				}
			}
		} else {
			// Padding size for value vector
			*offset = utils.AlignOffset(*offset, 8)
			// fixed value size
			*offset += (DataTypeBits(c.dataType)*len(c.values) + 7) / 8
		}
	}
}

// AppendToBuffer writes the column data to buffer and advances offset.
func (c *columnBuilder) AppendToBuffer(writer *utils.BufferWriter) error {
	writer.AlignBytes(1)
	// both gotype and array type is variable length
	isVariableLength := IsGoType(c.dataType) || IsArrayType(c.dataType)

	switch c.GetMode() {
	case AllValuesDefault:
		return nil
	case HasNullVector:
		// only non goType needs to write null vector
		if !IsGoType(c.dataType) {
			for row := 0; row < len(c.values); row++ {
				value := c.values[row]
				if err := writer.AppendBool(value != nil); err != nil {
					return utils.StackError(err, "Failed to write null vector at row %d", row)
				}
			}
		}
		fallthrough
	case AllValuesPresent:
		var offsetWriter, valueWriter *utils.BufferWriter
		// variable length data type needs to write offsetVector
		if isVariableLength {
			// Padding to 4 byte alignment for offset vector
			writer.AlignBytes(4)
			writerForked := *writer
			offsetWriter = &writerForked
			// skip offset bytes
			writer.SkipBytes((len(c.values) + 1) * 4)
		}

		// Padding to 8 byte alignment for value vector
		writer.AlignBytes(8)
		valueWriter = writer
		// local byte offset of current value in value vector
		currentValueOffset := uint32(0)
		// write values starting from current value vector offset
		for row := 0; row < len(c.values); row++ {
			// write current offset if offsetWriter is defined
			if offsetWriter != nil {
				err := offsetWriter.AppendUint32(currentValueOffset)
				if err != nil {
					return utils.StackError(err, "Failed to write offset value at row %d", row)
				}
			}

			value := c.values[row]
			// Handle null value.
			if value == nil {
				// only skip bits when there is no offset vector
				if offsetWriter == nil {
					valueWriter.SkipBits(DataTypeBits(c.dataType))
				}
				continue
			}

			switch c.dataType {
			case Bool:
				if err := valueWriter.AppendBool(value.(bool)); err != nil {
					return utils.StackError(err, "Failed to write bool value at row %d", row)
				}
			case Int8:
				if err := valueWriter.AppendInt8(value.(int8)); err != nil {
					return utils.StackError(err, "Failed to write int8 value at row %d", row)
				}
			case Uint8:
				if err := valueWriter.AppendUint8(value.(uint8)); err != nil {
					return utils.StackError(err, "Failed to write uint8 value at row %d", row)
				}
			case Int16:
				if err := valueWriter.AppendInt16(value.(int16)); err != nil {
					return utils.StackError(err, "Failed to write int16 value at row %d", row)
				}
			case Uint16:
				if err := valueWriter.AppendUint16(value.(uint16)); err != nil {
					return utils.StackError(err, "Failed to write uint16 value at row %d", row)
				}
			case Int32:
				if err := valueWriter.AppendInt32(value.(int32)); err != nil {
					return utils.StackError(err, "Failed to write int32 value at row %d", row)
				}
			case Int64:
				if err := valueWriter.AppendInt64(value.(int64)); err != nil {
					return utils.StackError(err, "Failed to write int64 value at row %d", row)
				}
			case Uint32:
				if err := valueWriter.AppendUint32(value.(uint32)); err != nil {
					return utils.StackError(err, "Failed to write uint32 value at row %d", row)
				}
			case Float32:
				if err := valueWriter.AppendFloat32(value.(float32)); err != nil {
					return utils.StackError(err, "Failed to write float32 value at row %d", row)
				}
			case SmallEnum:
				if err := valueWriter.AppendUint8(value.(uint8)); err != nil {
					return utils.StackError(err, "Failed to write small enum value at row %d", row)
				}
			case BigEnum:
				if err := valueWriter.AppendUint16(value.(uint16)); err != nil {
					return utils.StackError(err, "Failed to write big enum value at row %d", row)
				}
			case UUID:
				err := valueWriter.AppendUint64(value.([2]uint64)[0])
				if err == nil {
					err = writer.AppendUint64(value.([2]uint64)[1])
				}
				if err != nil {
					return utils.StackError(err, "Failed to write uuid value at row %d", row)
				}
			case GeoPoint:
				err := valueWriter.AppendFloat32(value.([2]float32)[0])
				if err == nil {
					err = writer.AppendFloat32(value.([2]float32)[1])
				}
				if err != nil {
					return utils.StackError(err, "Failed to write geopoint value at row %d", row)
				}
			case GeoShape:
				goVal := value.(GoDataValue)
				dataWriter := utils.NewStreamDataWriter(valueWriter)
				err := goVal.Write(&dataWriter)
				if err != nil {
					return utils.StackError(err, "Failed to write geoshape value at row %d", row)
				}
				// advance current offset
				currentValueOffset += uint32(goVal.GetSerBytes())
			default:
				if IsArrayType(c.dataType) {
					arrayValue := value.(*ArrayValue)
					bytes := arrayValue.GetSerBytes()
					err := arrayValue.Write(valueWriter)
					if err != nil {
						return utils.StackError(err, "Failed to write array value at row %d", row)
					}
					// advance current offset
					currentValueOffset += uint32(bytes)
				}
			}
		}

		// lastly write the final offset into offsetWriter
		if offsetWriter != nil {
			err := offsetWriter.AppendUint32(currentValueOffset)
			if err != nil {
				return utils.StackError(err, "Failed to write offset value at row %d", len(c.values))
			}
		}
	}
	// Align at byte for bit values.
	writer.AlignBytes(1)
	return nil
}

// GetMode get the mode based on number of valid values.
func (c *columnBuilder) GetMode() ColumnMode {
	if c.numValidValues == 0 {
		return AllValuesDefault
	} else if c.numValidValues == len(c.values) {
		return AllValuesPresent
	} else {
		return HasNullVector
	}
}

// UpsertBatchBuilder is the builder for constructing an UpsertBatch buffer. It allows random value
// write at (row, col).
type UpsertBatchBuilder struct {
	NumRows int
	columns []*columnBuilder
}

// NewUpsertBatchBuilder creates a new builder for constructing an UpersetBatch.
func NewUpsertBatchBuilder() *UpsertBatchBuilder {
	return &UpsertBatchBuilder{}
}

// AddColumn add a new column to the builder. Initially, new columns have all values set to null.
func (u *UpsertBatchBuilder) AddColumn(columnID int, dataType DataType) error {
	if len(u.columns) > math.MaxUint16 {
		return utils.StackError(nil, "Upsert batch cannot hold more than %d columns", math.MaxUint16)
	}
	values := make([]interface{}, u.NumRows)
	column := &columnBuilder{
		columnID:       columnID,
		dataType:       dataType,
		numValidValues: 0,
		values:         values,
	}
	u.columns = append(u.columns, column)
	return nil
}

// AddColumnWithUpdateMode add a new column to the builder with update mode info. Initially, new columns have all values set to null.
func (u *UpsertBatchBuilder) AddColumnWithUpdateMode(columnID int, dataType DataType, updateMode ColumnUpdateMode) error {
	if updateMode >= MaxColumnUpdateMode {
		return utils.StackError(nil, "Invalid update mode %d", updateMode)
	}
	if err := u.AddColumn(columnID, dataType); err != nil {
		return err
	}
	u.columns[len(u.columns)-1].updateMode = updateMode
	return nil
}

// AddRow increases the number of rows in the batch by 1. A new row with all nil values is appended
// to the row array.
func (u *UpsertBatchBuilder) AddRow() {
	for _, column := range u.columns {
		column.AddRow()
	}
	u.NumRows++
}

// RemoveRow decreases the number of rows in the batch by 1. The last row will be removed. It's a
// no-op if the number of rows is 0.
func (u *UpsertBatchBuilder) RemoveRow() {
	if u.NumRows > 0 {
		for _, column := range u.columns {
			column.RemoveRow()
		}
		u.NumRows--
	}
}

// ResetRows reset the row count to 0.
func (u *UpsertBatchBuilder) ResetRows() {
	for _, column := range u.columns {
		column.ResetRows()
	}
	u.NumRows = 0
}

// SetValue set a value to a given (row, col).
func (u *UpsertBatchBuilder) SetValue(row int, col int, value interface{}) error {
	if row >= u.NumRows {
		return utils.StackError(nil, "Row index %d out of range %d", row, u.NumRows)
	}
	if col >= len(u.columns) {
		return utils.StackError(nil, "Col index %d out of range %d", col, len(u.columns))
	}
	return u.columns[col].SetValue(row, value)
}

// ToByteArray produces a serialized UpsertBatch in byte array.
func (u UpsertBatchBuilder) ToByteArray() ([]byte, error) {
	// Create buffer.
	numCols := len(u.columns)
	// initialized size to 4 bytes (version number).
	versionHeaderSize := 4
	// 24 bytes consist of fixed headers:
	// [int32] num_of_rows (4 bytes)
	// [uint16] num_of_columns (2 bytes)
	// <reserve 14 bytes>
	// [uint32] arrival_time (4 bytes)
	fixedHeaderSize := 24
	columnHeaderSize := ColumnHeaderSize(numCols)
	headerSize := versionHeaderSize + fixedHeaderSize + columnHeaderSize
	size := headerSize
	for _, column := range u.columns {
		column.CalculateBufferSize(&size)
	}
	size = utils.AlignOffset(size, 8)
	buffer := make([]byte, size)
	writer := utils.NewBufferWriter(buffer)

	// Write upsert batch version.
	if err := writer.AppendUint32(uint32(V1)); err != nil {
		return nil, utils.StackError(err, "Failed to write version number")
	}
	// Write fixed headers.
	if err := writer.AppendInt32(int32(u.NumRows)); err != nil {
		return nil, utils.StackError(err, "Failed to write number of rows")
	}
	if err := writer.AppendUint16(uint16(len(u.columns))); err != nil {
		return nil, utils.StackError(err, "Failed to write number of columns")
	}
	writer.SkipBytes(14)
	if err := writer.AppendUint32(uint32(utils.Now().Unix())); err != nil {
		return nil, utils.StackError(err, "Failed to write arrival time")
	}
	columnHeader := NewUpsertBatchHeader(buffer[writer.GetOffset():headerSize], numCols)
	// skip to data offset
	writer.SkipBytes(columnHeaderSize)

	// Write per column data their headers.
	for i, column := range u.columns {
		if err := columnHeader.WriteColumnID(column.columnID, i); err != nil {
			return nil, err
		}
		if err := columnHeader.WriteColumnFlag(column.GetMode(), column.updateMode, i); err != nil {
			return nil, err
		}
		if err := columnHeader.WriteColumnType(column.dataType, i); err != nil {
			return nil, err
		}
		if err := columnHeader.WriteColumnOffset(writer.GetOffset(), i); err != nil {
			return nil, err
		}
		if err := column.AppendToBuffer(&writer); err != nil {
			return nil, utils.StackError(err, "Failed to write data for column %d", i)
		}
		if err := columnHeader.WriteColumnOffset(writer.GetOffset(), i+1); err != nil {
			return nil, err
		}
	}

	return buffer, nil
}

func AdditionUpdate(oldValue, newValue unsafe.Pointer, dataType DataType) {
	switch dataType {
	case Int8:
		*(*int8)(oldValue) = *(*int8)(oldValue) + *(*int8)(newValue)
	case Uint8:
		*(*uint8)(oldValue) = *(*uint8)(oldValue) + *(*uint8)(newValue)
	case Int16:
		*(*int16)(oldValue) = *(*int16)(oldValue) + *(*int16)(newValue)
	case Uint16:
		*(*uint16)(oldValue) = *(*uint16)(oldValue) + *(*uint16)(newValue)
	case Int32:
		*(*int32)(oldValue) = *(*int32)(oldValue) + *(*int32)(newValue)
	case Uint32:
		*(*uint32)(oldValue) = *(*uint32)(oldValue) + *(*uint32)(newValue)
	case Int64:
		*(*int64)(oldValue) = *(*int64)(oldValue) + *(*int64)(newValue)
	case Float32:
		*(*float32)(oldValue) = *(*float32)(oldValue) + *(*float32)(newValue)
	}
}

// MinMaxUpdate update the old value if compareRes == expectedRes
func MinMaxUpdate(oldValue, newValue unsafe.Pointer, dataType DataType, cmpFunc CompareFunc, expectedRes int) {
	if compareRes := cmpFunc(oldValue, newValue); compareRes*expectedRes > 0 {
		switch dataType {
		case Int8:
			*(*int8)(oldValue) = *(*int8)(newValue)
		case Uint8:
			*(*uint8)(oldValue) = *(*uint8)(newValue)
		case Int16:
			*(*int16)(oldValue) = *(*int16)(newValue)
		case Uint16:
			*(*uint16)(oldValue) = *(*uint16)(newValue)
		case Int32:
			*(*int32)(oldValue) = *(*int32)(newValue)
		case Uint32:
			*(*uint32)(oldValue) = *(*uint32)(newValue)
		case Int64:
			*(*int64)(oldValue) = *(*int64)(newValue)
		case Float32:
			*(*float32)(oldValue) = *(*float32)(newValue)
		}
	}
}
