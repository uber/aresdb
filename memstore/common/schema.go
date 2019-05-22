package common

import (
	"encoding/json"
	"sync"
	"unsafe"

	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/utils"
)

// TableSchema stores metadata of the table such as columns and primary keys.
// It also stores the dictionaries for enum columns.
type TableSchema struct {
	sync.RWMutex `json:"-"`
	// Main schema of the table. Mutable.
	Schema metaCom.Table `json:"schema"`
	// Maps from column names to their IDs. Mutable.
	ColumnIDs map[string]int `json:"columnIDs"`
	// Maps from enum column names to their case dictionaries. Mutable.
	EnumDicts map[string]EnumDict `json:"enumDicts"`
	// DataType for each column ordered by column ID. Mutable.
	ValueTypeByColumn []DataType `json:"valueTypeByColumn"`
	// Number of bytes in the primary key. Immutable.
	PrimaryKeyBytes int `json:"primaryKeyBytes"`
	// Types of each primary key column. Immutable.
	PrimaryKeyColumnTypes []DataType `json:"primaryKeyColumnTypes"`
	// Default values of each column. Mutable. Nil means default value is not set.
	DefaultValues []*DataValue `json:"-"`
}

// EnumDict contains mapping from and to enum strings to numbers.
type EnumDict struct {
	// Either 0x100 for small_enum, or 0x10000 for big_enum.
	Capacity    int            `json:"capacity"`
	Dict        map[string]int `json:"dict"`
	ReverseDict []string       `json:"reverseDict"`
}

// NewTableSchema creates a new table schema object from metaStore table object,
// this does not set enum cases.
func NewTableSchema(table *metaCom.Table) *TableSchema {
	tableSchema := &TableSchema{
		Schema:                *table,
		ColumnIDs:             make(map[string]int),
		EnumDicts:             make(map[string]EnumDict),
		ValueTypeByColumn:     make([]DataType, len(table.Columns)),
		PrimaryKeyColumnTypes: make([]DataType, len(table.PrimaryKeyColumns)),
		DefaultValues:         make([]*DataValue, len(table.Columns)),
	}

	for id, column := range table.Columns {
		if !column.Deleted {
			tableSchema.ColumnIDs[column.Name] = id
		}
		tableSchema.ValueTypeByColumn[id] = DataTypeForColumn(column)
	}

	for i, columnID := range table.PrimaryKeyColumns {
		columnType := tableSchema.ValueTypeByColumn[columnID]
		tableSchema.PrimaryKeyColumnTypes[i] = columnType

		dataBits := DataTypeBits(columnType)
		if dataBits < 8 {
			dataBits = 8
		}
		tableSchema.PrimaryKeyBytes += dataBits / 8
	}
	return tableSchema
}

// MarshalJSON marshals TableSchema into json.
func (t *TableSchema) MarshalJSON() ([]byte, error) {
	// Avoid loop json.Marshal calls.
	type alias TableSchema
	t.RLock()
	defer t.RUnlock()
	return json.Marshal((*alias)(t))
}

// SetTable sets a updated table and update TableSchema,
// should acquire lock before calling.
func (t *TableSchema) SetTable(table *metaCom.Table) {
	t.Schema = *table
	for id, column := range table.Columns {
		if !column.Deleted {
			t.ColumnIDs[column.Name] = id
		} else {
			delete(t.ColumnIDs, column.Name)
		}

		if id >= len(t.ValueTypeByColumn) {
			t.ValueTypeByColumn = append(t.ValueTypeByColumn, DataTypeForColumn(column))
		}

		if id >= len(t.DefaultValues) {
			t.DefaultValues = append(t.DefaultValues, nil)
		}
	}
}

// SetDefaultValue parses the default value string if present and sets to TableSchema.
// Schema lock should be acquired and release by caller and enum dict should already be
// created/update before this function.
func (t *TableSchema) SetDefaultValue(columnID int) {
	// Default values are already set.
	if t.DefaultValues[columnID] != nil {
		return
	}

	column := t.Schema.Columns[columnID]
	defStrVal := column.DefaultValue
	if defStrVal == nil || column.Deleted {
		t.DefaultValues[columnID] = &NullDataValue
		return
	}

	dataType := t.ValueTypeByColumn[columnID]
	dataTypeName := DataTypeName[dataType]
	val := DataValue{
		Valid:    true,
		DataType: dataType,
	}

	if dataType == SmallEnum || dataType == BigEnum {
		enumDict, ok := t.EnumDicts[column.Name]
		if !ok {
			// Should no happen since the enum dict should already be created.
			utils.GetLogger().With(
				"data_type", dataTypeName,
				"default_value", *defStrVal,
				"column", t.Schema.Columns[columnID].Name,
			).Panic("Cannot find EnumDict for column")
		}
		enumVal, ok := enumDict.Dict[*defStrVal]
		if !ok {
			// Should no happen since the enum value should already be created.
			utils.GetLogger().With(
				"data_type", dataTypeName,
				"default_value", *defStrVal,
				"column", t.Schema.Columns[columnID].Name,
			).Panic("Cannot find enum value for column")
		}

		if dataType == SmallEnum {
			enumValUint8 := uint8(enumVal)
			val.OtherVal = unsafe.Pointer(&enumValUint8)
		} else {
			enumValUint16 := uint16(enumVal)
			val.OtherVal = unsafe.Pointer(&enumValUint16)
		}
	} else {
		dataValue, err := ValueFromString(*defStrVal, dataType)
		if err != nil {
			// Should not happen since the string value is already validated by schema handler.
			utils.GetLogger().With(
				"data_type", dataTypeName,
				"default_value", *defStrVal,
				"column", t.Schema.Columns[columnID].Name,
			).Panic("Cannot parse default value")
		}

		if dataType == Bool {
			val.IsBool = true
			val.BoolVal = dataValue.BoolVal
		} else {
			val.OtherVal = dataValue.OtherVal
		}
	}

	val.CmpFunc = GetCompareFunc(dataType)
	t.DefaultValues[columnID] = &val
	return
}

// createEnumDict creates the enum dictionary for the specified column with the
// specified initial cases, and attaches it to TableSchema object.
// Caller should acquire the schema lock before calling this function.
func (t *TableSchema) CreateEnumDict(columnName string, enumCases []string) {
	columnID := t.ColumnIDs[columnName]
	dataType := t.ValueTypeByColumn[columnID]
	enumCapacity := 1 << uint(DataTypeBits(dataType))
	enumDict := map[string]int{}
	for id, enumCase := range enumCases {
		enumDict[enumCase] = id
	}
	t.EnumDicts[columnName] = EnumDict{
		Capacity:    enumCapacity,
		Dict:        enumDict,
		ReverseDict: enumCases,
	}
}

// GetValueTypeByColumn makes a copy of the ValueTypeByColumn so callers don't have to hold a read
// lock to access it.
func (t *TableSchema) GetValueTypeByColumn() []DataType {
	t.RLock()
	defer t.RUnlock()
	return t.ValueTypeByColumn
}

// GetPrimaryKeyColumns makes a copy of the Schema.PrimaryKeyColumns so callers don't have to hold
// a read lock to access it.
func (t *TableSchema) GetPrimaryKeyColumns() []int {
	t.RLock()
	defer t.RUnlock()
	return t.Schema.PrimaryKeyColumns
}

// GetColumnDeletions returns a boolean slice that indicates whether a column has been deleted. Callers
// need to hold a read lock.
func (t *TableSchema) GetColumnDeletions() []bool {
	deletedByColumn := make([]bool, len(t.Schema.Columns))
	for columnID, column := range t.Schema.Columns {
		deletedByColumn[columnID] = column.Deleted
	}
	return deletedByColumn
}

// GetColumnIfNonNilDefault returns a boolean slice that indicates whether a column has non nil default value. Callers
// need to hold a read lock.
func (t *TableSchema) GetColumnIfNonNilDefault() []bool {
	nonNilDefaultByColumn := make([]bool, len(t.Schema.Columns))
	for columnID, column := range t.Schema.Columns {
		nonNilDefaultByColumn[columnID] = column.DefaultValue != nil
	}
	return nonNilDefaultByColumn
}

// GetArchivingSortColumns makes a copy of the Schema.ArchivingSortColumns so
// callers don't have to hold a read lock to access it.
func (t *TableSchema) GetArchivingSortColumns() []int {
	t.RLock()
	defer t.RUnlock()
	return t.Schema.ArchivingSortColumns
}
