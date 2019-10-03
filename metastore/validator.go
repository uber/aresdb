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

package metastore

import (
	"fmt"
	memCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/utils"
	"gopkg.in/validator.v2"
	"reflect"
)

// TableSchemaValidator validates it a new table schema is valid, given existing schema
type TableSchemaValidator interface {
	SetOldTable(table common.Table)
	SetNewTable(table common.Table)
	Validate() error
}

// NewTableSchameValidator returns a new TableSchemaValidator. Pass nil for oldTable if none exists
func NewTableSchameValidator() TableSchemaValidator {
	return &tableSchemaValidatorImpl{}
}

type tableSchemaValidatorImpl struct {
	newTable *common.Table
	oldTable *common.Table
}

func (v *tableSchemaValidatorImpl) SetOldTable(table common.Table) {
	v.oldTable = &table
}

func (v *tableSchemaValidatorImpl) SetNewTable(table common.Table) {
	v.newTable = &table
}

func (v tableSchemaValidatorImpl) Validate() (err error) {
	if v.oldTable == nil {
		return v.validateIndividualSchema(v.newTable, true)
	}
	return v.validateSchemaUpdate(v.newTable, v.oldTable)
}

// ValidateHLLConfig validates hll config
func validateColumnHLLConfig(c common.Column) error {
	if c.HLLConfig.IsHLLColumn {
		if c.Type != common.Uint32 && c.Type != common.Int32 && c.Type != common.Int64 && c.Type != common.UUID {
			return fmt.Errorf("data Type %s not allowed for fast hll aggregation, valid options: [%s|%s|%s|%s]",
				c.Type, common.Uint32, common.Int32, common.Int64, common.UUID)
		}
	}
	return nil
}

// checks performed:
//	table has at least 1 valid column
//	table has at least 1 valid primary key column
//  fact table must have a time column as first column
//	fact table must have sort columns that are valid
//	each column have valid data type and default value
//	sort columns cannot have duplicate columnID
//	primary key columns cannot have duplicate columnID
//	column name cannot duplicate
//  check hll cannot be enabled on time column
//  check column configs
func (v tableSchemaValidatorImpl) validateIndividualSchema(table *common.Table, creation bool) (err error) {
	var colIdDedup []bool

	nonDeletedColumnsCount := 0
	colNameDedup := make(map[string]bool)
	for columnID, column := range table.Columns {
		if !column.Deleted {
			nonDeletedColumnsCount++
		}
		if colNameDedup[column.Name] {
			return common.ErrDuplicatedColumnName
		}
		colNameDedup[column.Name] = true

		// validate data type
		if dataType := memCom.DataTypeFromString(column.Type); dataType == memCom.Unknown {
			return common.ErrInvalidDataType
		} else if table.IsFactTable && columnID == 0 && dataType != memCom.Uint32 {
			return common.ErrMissingTimeColumn
		}

		// validate hll config
		if err := validateColumnHLLConfig(column); err != nil {
			return err
		}

		// time column does not allow hll config
		if table.IsFactTable && columnID == 0 && column.HLLConfig.IsHLLColumn {
			return common.ErrTimeColumnDoesNotAllowHLLConfig
		}

		if column.DefaultValue != nil {
			if table.IsFactTable && columnID == 0 {
				return common.ErrTimeColumnDoesNotAllowDefault
			}

			if column.HLLConfig.IsHLLColumn {
				return common.ErrHLLColumnDoesNotAllowDefaultValue
			}

			err = ValidateDefaultValue(*column.DefaultValue, column.Type)
			if err != nil {
				return err
			}
		}
	}
	if nonDeletedColumnsCount == 0 {
		return common.ErrAllColumnsInvalid
	}

	if len(table.PrimaryKeyColumns) == 0 {
		return common.ErrMissingPrimaryKey
	}

	colIdDedup = make([]bool, len(table.Columns))
	for _, colId := range table.PrimaryKeyColumns {
		if colId >= len(table.Columns) {
			return common.ErrColumnNonExist
		}
		if table.Columns[colId].Deleted {
			return common.ErrColumnDeleted
		}
		if colIdDedup[colId] {
			return common.ErrDuplicatedColumn
		}
		colDataType := memCom.DataTypeFromString(table.Columns[colId].Type)
		if memCom.IsArrayType(colDataType) {
			return common.ErrInvalidPrimaryKeyDataType
		}
		colIdDedup[colId] = true
	}

	if err := validator.Validate(table.Config); err != nil {
		return utils.StackError(err, "invalid table config")
	}

	if table.IsFactTable {
		colIdDedup = make([]bool, len(table.Columns))
		for _, sortColumnId := range table.ArchivingSortColumns {
			if sortColumnId >= len(table.Columns) {
				return common.ErrColumnNonExist
			}
			if table.Columns[sortColumnId].Deleted {
				return common.ErrColumnDeleted
			}
			if colIdDedup[sortColumnId] {
				return common.ErrDuplicatedColumn
			}
			colDataType := memCom.DataTypeFromString(table.Columns[sortColumnId].Type)
			if colDataType >= memCom.ArrayBool && colDataType <= memCom.ArrayInt64 {
				return common.ErrInvalidSortColumnDataType
			}
			colIdDedup[sortColumnId] = true
		}
	}

	return
}

// checks performed
//	check that new table is valid table
//	check new table has larger version number
//	check no changes on immutable fields (table name, type, pk)
//	check updates on columns and sort columns are valid
//  check allowMissingEventTime cannot be changed from true to false
//  check hllConfig cannot be changed
func (v tableSchemaValidatorImpl) validateSchemaUpdate(newTable, oldTable *common.Table) (err error) {
	if err := v.validateIndividualSchema(newTable, false); err != nil {
		return err
	}

	if newTable.Name != oldTable.Name {
		return common.ErrSchemaUpdateNotAllowed
	}

	if newTable.IsFactTable != oldTable.IsFactTable {
		return common.ErrSchemaUpdateNotAllowed
	}

	// validate columns
	if len(newTable.Columns) < len(oldTable.Columns) {
		// even with column deletion, or recreation, column id are not reused
		return common.ErrInsufficientColumnCount
	}

	if oldTable.IsFactTable && oldTable.Config.AllowMissingEventTime && !newTable.Config.AllowMissingEventTime {
		return common.ErrDisallowMissingEventTime
	}

	var i int

	for i = 0; i < len(oldTable.Columns); i++ {
		oldCol := oldTable.Columns[i]
		newCol := newTable.Columns[i]
		if oldCol.Deleted {
			if !newCol.Deleted {
				return common.ErrReusingColumnIDNotAllowed
			}
		}
		// check that no column configs are modified, even for deleted columns
		if oldCol.Name != newCol.Name ||
			oldCol.Type != newCol.Type ||
			!reflect.DeepEqual(oldCol.DefaultValue, newCol.DefaultValue) ||
			oldCol.CaseInsensitive != newCol.CaseInsensitive ||
			oldCol.DisableAutoExpand != newCol.DisableAutoExpand ||
			oldCol.HLLConfig != newCol.HLLConfig {
			return common.ErrSchemaUpdateNotAllowed
		}
	}
	// end validate columns

	// primary key columns
	if !reflect.DeepEqual(newTable.PrimaryKeyColumns, oldTable.PrimaryKeyColumns) {
		return common.ErrChangePrimaryKeyColumn
	}

	// sort columns
	if len(newTable.ArchivingSortColumns) < len(oldTable.ArchivingSortColumns) {
		return common.ErrIllegalChangeSortColumn
	}
	for i, sortColumnId := range newTable.ArchivingSortColumns {
		if i < len(oldTable.ArchivingSortColumns) {
			if oldTable.ArchivingSortColumns[i] != sortColumnId {
				return common.ErrIllegalChangeSortColumn
			}
		}
		if sortColumnId >= len(newTable.Columns) {
			return common.ErrColumnNonExist
		}
		if newTable.Columns[sortColumnId].Deleted {
			return common.ErrColumnDeleted
		}
	}

	return
}

// ValidateDefaultValue validates default value against data type
func ValidateDefaultValue(valueStr, dataTypeStr string) (err error) {
	dataType := memCom.DataTypeFromString(dataTypeStr)
	switch dataType {
	// BigEnum or Small Enum ares string values, no need to validate
	case memCom.BigEnum, memCom.SmallEnum:
		return nil
	default:
		value, err := memCom.ValueFromString(valueStr, dataType)
		if err != nil || !value.Valid {
			return utils.StackError(err, "invalid value %s for type %s", valueStr, dataTypeStr)
		}
	}
	return err
}
