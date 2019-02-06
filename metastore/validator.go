package metastore

import (
	"fmt"
	"reflect"

	memCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/utils"
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
func (v tableSchemaValidatorImpl) validateIndividualSchema(table *common.Table, creation bool) (err error) {
	var colIDDedup []bool

	nonDeletedColumnsCount := 0
	colNameDedup := make(map[string]bool)
	for columnID, column := range table.Columns {
		if !column.Deleted {
			nonDeletedColumnsCount++
		} else if creation {
			return ErrNewColumnWithDeletion
		}
		if colNameDedup[column.Name] {
			return ErrDuplicatedColumnName
		}
		colNameDedup[column.Name] = true

		// validate data type
		if dataType := memCom.DataTypeFromString(column.Type); dataType == memCom.Unknown {
			return ErrInvalidDataType
		} else if table.IsFactTable && columnID == 0 && dataType != memCom.Uint32 {
			return ErrMissingTimeColumn
		}

		// validate hll config
		if err = validateColumnHLLConfig(column); err != nil {
			return
		}

		// time column does not allow hll config
		if table.IsFactTable && columnID == 0 && column.HLLConfig.IsHLLColumn {
			return ErrTimeColumnDoesNotAllowHLLConfig
		}

		if column.DefaultValue != nil {
			if table.IsFactTable && columnID == 0 {
				return ErrTimeColumnDoesNotAllowDefault
			}

			if column.HLLConfig.IsHLLColumn {
				return ErrHLLColumnDoesNotAllowDefaultValue
			}

			err = ValidateDefaultValue(*column.DefaultValue, column.Type)
			if err != nil {
				return
			}
		}
	}
	if nonDeletedColumnsCount == 0 {
		return ErrAllColumnsInvalid
	}

	if len(table.PrimaryKeyColumns) == 0 {
		return ErrMissingPrimaryKey
	}

	colIDDedup = make([]bool, len(table.Columns))
	for _, colID := range table.PrimaryKeyColumns {
		if colID >= len(table.Columns) {
			return ErrColumnNonExist
		}
		if table.Columns[colID].Deleted {
			return ErrColumnDeleted
		}
		if colIDDedup[colID] {
			return ErrDuplicatedColumn
		}
		colIDDedup[colID] = true
	}

	// TODO: checks for config?

	if table.IsFactTable {
		colIDDedup = make([]bool, len(table.Columns))
		for _, sortColumnID := range table.ArchivingSortColumns {
			if sortColumnID >= len(table.Columns) {
				return ErrColumnNonExist
			}
			if table.Columns[sortColumnID].Deleted {
				return ErrColumnDeleted
			}
			if colIDDedup[sortColumnID] {
				return ErrDuplicatedColumn
			}
			colIDDedup[sortColumnID] = true
		}
	}

	return
}

// checks performed
//	check that new table is valid table
//	check new table has larger version number
//	check no changes on immutable fields (table name, type, pk)
//	check updates on columns and sort columns are valid
func (v tableSchemaValidatorImpl) validateSchemaUpdate(newTable, oldTable *common.Table) (err error) {
	if err = v.validateIndividualSchema(newTable, false); err != nil {
		return
	}

	if newTable.Name != oldTable.Name {
		return ErrSchemaUpdateNotAllowed
	}

	if newTable.IsFactTable != oldTable.IsFactTable {
		return ErrSchemaUpdateNotAllowed
	}

	// validate columns
	if len(newTable.Columns) < len(oldTable.Columns) {
		// even with column deletion, or recreation, column id are not reused
		return ErrInsufficientColumnCount
	}

	if oldTable.IsFactTable && oldTable.Config.AllowMissingEventTime && !newTable.Config.AllowMissingEventTime {
		return ErrDisallowMissingEventTime
	}

	var i int

	for i = 0; i < len(oldTable.Columns); i++ {
		oldCol := oldTable.Columns[i]
		newCol := newTable.Columns[i]
		if oldCol.Deleted {
			if !newCol.Deleted {
				return ErrReusingColumnIDNotAllowed
			}
		}
		// check that no column configs are modified, even for deleted columns
		if oldCol.Name != newCol.Name ||
			oldCol.Type != newCol.Type ||
			!reflect.DeepEqual(oldCol.DefaultValue, newCol.DefaultValue) ||
			oldCol.CaseInsensitive != newCol.CaseInsensitive ||
			oldCol.DisableAutoExpand != newCol.DisableAutoExpand ||
			oldCol.HLLConfig != newCol.HLLConfig {
			return ErrSchemaUpdateNotAllowed
		}
	}

	for ; i < len(newTable.Columns); i++ {
		newCol := newTable.Columns[i]
		if newCol.Deleted {
			return ErrNewColumnWithDeletion
		}
	}
	// end validate columns

	// primary key columns
	if !reflect.DeepEqual(newTable.PrimaryKeyColumns, oldTable.PrimaryKeyColumns) {
		return ErrChangePrimaryKeyColumn
	}

	// sort columns
	if len(newTable.ArchivingSortColumns) < len(oldTable.ArchivingSortColumns) {
		return ErrIllegalChangeSortColumn
	}
	for i, sortColumnID := range newTable.ArchivingSortColumns {
		if i < len(oldTable.ArchivingSortColumns) {
			if oldTable.ArchivingSortColumns[i] != sortColumnID {
				return ErrIllegalChangeSortColumn
			}
		}
		if sortColumnID >= len(newTable.Columns) {
			return ErrColumnNonExist
		}
		if newTable.Columns[sortColumnID].Deleted {
			return ErrColumnDeleted
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
		return
	default:
		value, err := memCom.ValueFromString(valueStr, dataType)
		if err != nil || !value.Valid {
			return utils.StackError(err, "invalid value %s for type %s", valueStr, dataTypeStr)
		}
	}
	return
}
