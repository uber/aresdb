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
	"unsafe"

	"bytes"
	"github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/memutils"
	"github.com/uber/aresdb/utils"
	"math"
)

// columnReader contains meta data for accessing the data of a column in an UpsertBatch.
type columnReader struct {
	// The logic id of the column.
	columnID int
	// The column mode.
	columnMode common.ColumnMode
	// The column update mode
	columnUpdateMode common.ColumnUpdateMode
	// DataType of the column.
	dataType common.DataType
	// The value vector. can be empty depending on column mode.
	valueVector []byte
	// The null vector. can be empty depending on column mode.
	nullVector []byte
	// The enum dictionary vector
	enumDictVector []byte
	// The offset vector. Only used for variable length values. Not used yet.
	offsetVector []byte
	// Compare function if any.
	cmpFunc common.CompareFunc
}

// ReadGoValue returns the GoDataValue from upsert batch at given row
func (c *columnReader) ReadGoValue(row int) common.GoDataValue {
	offset := c.readOffset(row)
	nextOffset := c.readOffset(row + 1)
	if offset == nextOffset {
		return nil
	}
	goValue := common.GetGoDataValue(c.dataType)
	dataReader := utils.NewStreamDataReader(bytes.NewReader(c.valueVector[offset:]))
	err := goValue.Read(&dataReader)
	if err != nil {
		return nil
	}
	return goValue
}

// ReadValue returns the row data (fixed sized) for a column, including the pointer to the data,
// and the validity of the value.
func (c *columnReader) ReadValue(row int) (unsafe.Pointer, bool) {
	validity := c.readValidity(row)
	if !validity {
		return nil, false
	}
	return unsafe.Pointer(&c.valueVector[row*common.DataTypeBits(c.dataType)/8]), true
}

// ReadValue returns the row data (boolean type) for a column, and its validity.
func (c *columnReader) ReadBool(row int) (bool, bool) {
	validity := c.readValidity(row)
	if !validity {
		return false, false
	}
	return readBool(c.valueVector, row), true
}

func (c *columnReader) readOffset(row int) uint32 {
	return *(*uint32)(unsafe.Pointer(&c.offsetVector[row*4]))
}

// readValidity return the validity value of a row in a column.
func (c *columnReader) readValidity(row int) bool {
	switch c.columnMode {
	case common.AllValuesDefault:
		return false
	case common.AllValuesPresent:
		return true
	}
	return readBool(c.nullVector, row)
}

func readBool(buffer []byte, index int) bool {
	return buffer[index/8]&(0x1<<uint8(index%8)) != 0x0
}

func writeBool(buffer []byte, index int, value bool) {
	if value {
		buffer[index/8] |= 0x1 << uint8(index%8)
	} else {
		buffer[index/8] &^= 0x1 << uint8(index%8)
	}
}

// UpsertBatch stores and indexes a serialized upsert batch of data on a particular table.
// It is used for both client-server data transfer and redo logging.
// In redo logs each batch is prepended by a 4-byte buffer size.
// The serialized buffer of the batch is in the following format:
//	[uint32] magic_number
//	[uint32] buffer_size
//
//	<begin of buffer>
//	[int32]  version_number
//	[int32]  num_of_rows
//	[uint16] num_of_columns
//	<reserve 14 bytes>
//	[uint32] arrival_time
//	[uint32] column_offset_0 ... [uint32] column_offset_x+1
//	[uint32] enum_dict_length_0 ... [uint32] enum_dict_length_x
//	[uint32] column_reserved_field2_0 ... [uint32] column_reserved_field2_x
//	[uint32] column_data_type_0 ... [uint32] column_data_type_x
//	[uint16] column_id_0 ... [uint16] column_id_x
//	[uint8] column_mode_0 ... [uint8] column_mode_x
//
//	(optional) [uint8] null_vector_0
//  (optional) [uint8] enum_dict_vector_0
//	(optional) [padding to 4 byte alignment uint32] offset_vector_0
//	[padding for 8 byte alignment] value_vector_0
//	...
//
//	[padding for 8 byte alignment]
//	<end of buffer>
// Each component in the serialized buffer is byte aligned (not pointer aligned or bit aligned).
// All serialized numbers are written in little-endian.
// The struct is used for both client serialization and server deserialization.
// See https://github.com/uber/aresdb/wiki/redo_logs for more details.
//
// Note: only fixed size values are supported currently.
type UpsertBatch struct {
	// Number of rows in the batch, must be between 0 and 65535.
	NumRows int

	// Number of columns.
	NumColumns int

	// Arrival Time of Upsert Batch
	ArrivalTime uint32

	// Serialized buffer of the batch, starts from NumRows, does not contain the 4-byte
	// buffer size.
	buffer []byte

	// When records are extracted for backfill, buffer is no longer used and we
	// use alternativeBytes to track the memory usage.
	alternativeBytes int

	// Columns to upsert on.
	columns []*columnReader

	// Column id maps the logic column id to local column index.
	columnsByID map[int]int
}

// GetBuffer returns the underline buffer used to construct the upsert batch.
func (u *UpsertBatch) GetBuffer() []byte {
	return u.buffer
}

// GetColumnID returns the logical id of a column.
func (u *UpsertBatch) GetColumnID(col int) (int, error) {
	if col >= len(u.columns) {
		return 0, utils.StackError(nil, "Column index %d out of range %d", col, len(u.columns))
	}
	return u.columns[col].columnID, nil
}

// GetColumnType returns the data type of a column.
func (u *UpsertBatch) GetColumnType(col int) (common.DataType, error) {
	if col >= len(u.columns) {
		return 0, utils.StackError(nil, "Column index %d out of range %d", col, len(u.columns))
	}
	return u.columns[col].dataType, nil
}

// GetColumnIndex returns the local index of a column given a logical index id.
func (u *UpsertBatch) GetColumnIndex(columnID int) (int, error) {
	col, ok := u.columnsByID[columnID]
	if !ok {
		return 0, utils.StackError(nil, "Column %d does not exist", columnID)
	}
	return col, nil
}

// GetValue returns the data (fixed sized) stored at (row, col), including the pointer to the data,
// and the validity of the value.
func (u *UpsertBatch) GetValue(row int, col int) (unsafe.Pointer, bool, error) {
	if col >= len(u.columns) {
		return nil, false, utils.StackError(nil, "Column index %d out of range %d", col, u.columns)
	}
	if row >= u.NumRows {
		return nil, false, utils.StackError(nil, "Row index %d out of range %d", col, u.NumRows)
	}
	data, validity := u.columns[col].ReadValue(row)
	return data, validity, nil
}

// GetBool returns the data (boolean type) stored at (row, col), and the validity of the value.
func (u *UpsertBatch) GetBool(row int, col int) (bool, bool, error) {
	if col >= len(u.columns) {
		return false, false, utils.StackError(nil, "Column index %d out of range %d", col, u.columns)
	}
	if row >= u.NumRows {
		return false, false, utils.StackError(nil, "Row index %d out of range %d", col, u.NumRows)
	}
	data, validity := u.columns[col].ReadBool(row)
	return data, validity, nil
}

// GetDataValue returns the DataValue for the given row and col index.
// It first check validity of the value, then it check whether it's a
// boolean column to decide whether to load bool value or other value
// type.
func (u *UpsertBatch) GetDataValue(row, col int) (common.DataValue, error) {
	val := common.DataValue{}
	if col >= len(u.columns) {
		return val, utils.StackError(nil, "Column index %d out of range %d", col, u.columns)
	}
	if row >= u.NumRows {
		return val, utils.StackError(nil, "Row index %d out of range %d", row, u.NumRows)
	}

	dataType := u.columns[col].dataType

	val.DataType = dataType

	if dataType == common.Bool {
		val.IsBool = true
		val.BoolVal, val.Valid = u.columns[col].ReadBool(row)
		return val, nil
	}

	if common.IsGoType(dataType) {
		val.GoVal = u.columns[col].ReadGoValue(row)
		val.Valid = val.GoVal != nil
		return val, nil
	}

	val.OtherVal, val.Valid = u.columns[col].ReadValue(row)
	val.CmpFunc = common.GetCompareFunc(dataType)
	return val, nil
}

// GetEventColumnIndex returns the column index of event time
func (u *UpsertBatch) GetEventColumnIndex() int {
	// Validate columns in upsert batch are valid.
	for i := 0; i < u.NumColumns; i++ {
		columnID, _ := u.GetColumnID(i)

		if columnID == 0 {
			return i
		}
	}
	return -1
}

// GetPrimaryKeyCols converts primary key columnIDs to cols in this upsert batch.
func (u *UpsertBatch) GetPrimaryKeyCols(primaryKeyColumnIDs []int) ([]int, error) {
	primaryKeyCols := make([]int, len(primaryKeyColumnIDs))

	for i, columnID := range primaryKeyColumnIDs {
		col, err := u.GetColumnIndex(columnID)
		if err != nil {
			return nil, utils.StackError(err, "Primary key column %d is missing from upsert batch", columnID)
		}
		primaryKeyCols[i] = col
	}
	return primaryKeyCols, nil
}

// GetPrimaryKeyBytes returns primary key bytes for a given row. Note primaryKeyCol is not list of primary key
// columnIDs.
func (u *UpsertBatch) GetPrimaryKeyBytes(row int, primaryKeyCols []int, key []byte) error {
	primaryKeyValues := make([]common.DataValue, len(primaryKeyCols))
	var err error
	for i, col := range primaryKeyCols {
		primaryKeyValues[i], err = u.GetDataValue(row, col)
		if err != nil {
			return utils.StackError(err, "Failed to read primary key at row %d, col %d",
				row, col)
		}
	}

	if err := GetPrimaryKeyBytes(primaryKeyValues, key); err != nil {
		return err
	}
	return nil
}

// ExtractBackfillBatch extracts given rows and stores in a new UpsertBatch
// The returned new UpsertBatch is not fully serialized and can only be used for
// structured reads.
func (u *UpsertBatch) ExtractBackfillBatch(backfillRows []int) *UpsertBatch {
	if len(backfillRows) == 0 {
		return nil
	}

	newBatch := *u
	newBatch.NumRows = len(backfillRows)
	newBatch.buffer = nil

	newBatch.columns = make([]*columnReader, 0, len(u.columns))
	for _, oldCol := range u.columns {
		newCol := *oldCol
		if newCol.columnUpdateMode > common.UpdateForceOverwrite {
			// ignore those columns with conditional updates from backfill
			// clean up the column data
			colID := newCol.columnID
			if _, found := newBatch.columnsByID[newCol.columnID]; found {
				delete(newBatch.columnsByID, colID)
			}
			newBatch.NumColumns--
			continue
		}
		newBatch.columnsByID[newCol.columnID] = len(newBatch.columns)
		newBatch.columns = append(newBatch.columns, &newCol)
		newCol.valueVector = nil
		newCol.nullVector = nil
		newCol.offsetVector = nil

		switch newCol.columnMode {
		case common.AllValuesDefault:
		case common.HasNullVector:
			nullVectorLength := utils.AlignOffset(newBatch.NumRows, 8) / 8
			newCol.nullVector = make([]byte, nullVectorLength)
			newBatch.alternativeBytes += nullVectorLength

			for newRow, oldRow := range backfillRows {
				validity := oldCol.readValidity(oldRow)
				writeBool(newCol.nullVector, newRow, validity)
			}
			fallthrough
		case common.AllValuesPresent:
			valueBits := common.DataTypeBits(newCol.dataType)
			valueVectorLength := utils.AlignOffset(newBatch.NumRows*valueBits, 8) / 8
			newCol.valueVector = make([]byte, valueVectorLength)
			newBatch.alternativeBytes += valueVectorLength

			for newRow, oldRow := range backfillRows {
				if valueBits == 1 {
					boolValue := readBool(oldCol.valueVector, oldRow)
					writeBool(newCol.valueVector, newRow, boolValue)
				} else {
					valueBytes := valueBits / 8
					memutils.MemCopy(unsafe.Pointer(&newCol.valueVector[newRow*valueBytes]),
						unsafe.Pointer(&oldCol.valueVector[oldRow*valueBytes]), valueBytes)
				}
			}
		}
	}

	return &newBatch
}

// GetColumnNames reads columnNames in UpsertBatch, user should not lock schema
func (u *UpsertBatch) GetColumnNames(schema *TableSchema) ([]string, error) {
	columnNames := make([]string, u.NumColumns)
	for columnIdx := range u.columns {
		columnID, err := u.GetColumnID(columnIdx)
		if err != nil {
			return nil, err
		}

		schema.RLock()
		if columnID > len(schema.Schema.Columns) {
			schema.RUnlock()
			return nil, utils.StackError(nil, "Column id %d out of range %d",
				columnID, len(schema.Schema.Columns))
		}
		columnNames[columnIdx] = schema.Schema.Columns[columnID].Name
		schema.RUnlock()
	}
	return columnNames, nil
}

// ReadData reads data from upsert batch and convert values to meaningful representations given data type.
func (u *UpsertBatch) ReadData(start int, length int) ([][]interface{}, error) {
	// Only read the column names.
	if length == 0 {
		return nil, nil
	}

	length = int(math.Min(float64(length), float64(u.NumRows-start)))

	if length <= 0 {
		return nil, utils.StackError(nil, "Invalid start or length")
	}

	rows := make([][]interface{}, length)
	for row := start; row < start+length; row++ {
		idx := row - start
		rows[idx] = make([]interface{}, u.NumColumns)
		for col := 0; col < u.NumColumns; col++ {
			val, err := u.GetDataValue(row, col)
			if err != nil {
				return nil, err
			}

			dataType, err := u.GetColumnType(col)
			if err != nil {
				return nil, err
			}

			rows[idx][col] = val.ConvertToHumanReadable(dataType)
		}
	}
	return rows, nil
}

func readUpsertBatch(buffer []byte) (*UpsertBatch, error) {
	batch := &UpsertBatch{
		buffer:      buffer,
		columnsByID: make(map[int]int),
	}

	// numRows.
	reader := utils.NewBufferReader(buffer)

	numRows, err := reader.ReadInt32(4)
	if err != nil {
		return nil, utils.StackError(err, "Failed to read number of rows")
	}
	if numRows < 0 {
		return nil, utils.StackError(err, "Number of rows should be >= 0")
	}

	batch.NumRows = int(numRows)

	// numColumns.
	numColumns, err := reader.ReadUint16(4 + 4)
	if err != nil {
		return nil, utils.StackError(err, "Failed to read number of columns")
	}
	batch.NumColumns = int(numColumns)

	// 2 byte num columns
	arrivalTime, err := reader.ReadUint32(4 + 4 + 2 + 14)
	if err != nil {
		return nil, utils.StackError(err, "Failed to read arrival time")
	}
	batch.ArrivalTime = arrivalTime

	// Header too small, error out.
	if len(buffer) < 28+common.ColumnHeaderSize(batch.NumColumns) {
		return nil, utils.StackError(nil, "Invalid upsert batch data with incomplete header section")
	}

	header := common.NewUpsertBatchHeader(buffer[28:], batch.NumColumns)

	columns := make([]*columnReader, batch.NumColumns)
	for i := range columns {
		columnType, err := header.ReadColumnType(i)
		if err != nil {
			return nil, utils.StackError(err, "Failed to read type for column %d", i)
		}

		columnID, err := header.ReadColumnID(i)
		if err != nil {
			return nil, utils.StackError(err, "Failed to read id for column %d", i)
		}
		batch.columnsByID[columnID] = i

		columnMode, columnUpdateMode, err := header.ReadColumnFlag(i)
		if err != nil {
			return nil, utils.StackError(err, "Failed to read mode for column %d", i)
		}

		columns[i] = &columnReader{columnID: columnID, columnMode: columnMode, columnUpdateMode: columnUpdateMode, dataType: columnType,
			cmpFunc: common.GetCompareFunc(columnType)}

		columnStartOffset, err := header.ReadColumnOffset(i)
		if err != nil {
			return nil, utils.StackError(err, "Failed to read start offset for column %d", i)
		}

		columnEndOffset, err := header.ReadColumnOffset(i + 1)
		if err != nil {
			return nil, utils.StackError(err, "Failed to read end offset for column %d", i)
		}

		enumDictLength := 0
		if isEnumType := common.IsEnumType(columnType); isEnumType {
			enumDictLength, err = header.ReadEnumDictLength(i)
			if err != nil {
				return nil, utils.StackError(err, "Failed to read enum dict length for column %d", i)
			}
		}

		isGoType := common.IsGoType(columnType)
		currentOffset := columnStartOffset
		switch columnMode {
		case common.AllValuesDefault:
		case common.HasNullVector:
			if !isGoType {
				// Null vector points to the beginning of the column data section.
				nullVectorLength := utils.AlignOffset(batch.NumRows, 8) / 8
				columns[i].nullVector = buffer[currentOffset : currentOffset+nullVectorLength]
				currentOffset += nullVectorLength
			}
			fallthrough
		case common.AllValuesPresent:
			// read enum dict vector
			columns[i].enumDictVector = buffer[currentOffset : currentOffset+enumDictLength]
			currentOffset += enumDictLength
			if isGoType {
				currentOffset = utils.AlignOffset(currentOffset, 4)
				offsetVectorLength := (batch.NumRows + 1) * 4
				columns[i].offsetVector = buffer[currentOffset : currentOffset+offsetVectorLength]
				currentOffset += offsetVectorLength
			}
			// Round up to 8 byte padding.
			currentOffset = utils.AlignOffset(currentOffset, 8)
			columns[i].valueVector = buffer[currentOffset:columnEndOffset]
		}
	}
	batch.columns = columns

	return batch, nil

}

// NewUpsertBatch deserializes an upsert batch on the server.
// buffer does not contain the 4-byte buffer size.
func NewUpsertBatch(buffer []byte) (*UpsertBatch, error) {
	reader := utils.NewBufferReader(buffer)
	// read version
	version, err := reader.ReadUint32(0)
	if err != nil {
		return nil, utils.StackError(err, "Failed to read upsert batch version number")
	}

	if version == common.UpsertBatchVersion1 {
		// skip version number bytes for new version
		return readUpsertBatch(buffer)
	}
	return nil, utils.StackError(err, "Invalid upsert batch version")
}
