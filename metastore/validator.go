package metastore

import (
	"github.com/uber/aresdb/metastore/common"
	memCom "github.com/uber/aresdb/memstore/common"
	"reflect"
	"github.com/uber/aresdb/utils"
)

// TableSchemaValidator validates it a new table schema is valid, given existing schema
type TableSchemaValidator interface {
	Validate() error
}

// NewTableSchameValidator returns a new TableSchemaValidator. Pass nil for oldTable if none exists
func NewTableSchameValidator(newTable *common.Table, oldTable *common.Table) TableSchemaValidator {
	return tableSchemaValidatorImpl{
		newTable: newTable,
		oldTable: oldTable,
	}
}

type tableSchemaValidatorImpl struct {
	newTable *common.Table
	oldTable *common.Table
}

func (v tableSchemaValidatorImpl) Validate() (err error) {
	if v.oldTable == nil {
		return v.validateIndividualSchema(v.newTable, true)
	}
	return v.validateSchemaUpdate(v.newTable, v.oldTable)
}

// checks performed:
//	table has at least 1 valid column
//	table has at least 1 valid primary key column
//	fact table must have sort columns that are valid
//	each column have valid data type and default value
func (v tableSchemaValidatorImpl) validateIndividualSchema(table *common.Table, creation bool) (err error) {
	nonDeletedColumnsCount := 0
	for _, column := range table.Columns {
		if !column.Deleted {
			nonDeletedColumnsCount++
		} else {
			if creation {
				return ErrNewColumnWithDeletion
			}
		}

	}
	if nonDeletedColumnsCount == 0 {
		return ErrAllColumnsInvalid
	}

	if len(table.PrimaryKeyColumns) == 0 {
		return ErrMissingPrimaryKey
	}
	for _, colId := range table.PrimaryKeyColumns {
		if colId >= len(table.Columns) {
			return ErrColumnNonExist
		}
		if table.Columns[colId].Deleted {
			return ErrColumnDeleted
		}
	}

	// TODO: checks for config?

	if table.IsFactTable {
		for _, sortColumnId := range table.ArchivingSortColumns {
			if sortColumnId >= len(table.Columns) {
				return ErrColumnNonExist
			}
			if table.Columns[sortColumnId].Deleted {
				return ErrColumnDeleted
			}
		}
	}

	// validate data type
	for _, column := range table.Columns {
		if dataType := memCom.DataTypeFromString(column.Type); dataType == memCom.Unknown {
			return ErrInvalidDataType
		}
		if column.DefaultValue != nil {
			err = ValidateDefaultValue(*column.DefaultValue, column.Type)
			if err != nil {
				return err
			}
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
	if err := v.validateIndividualSchema(newTable, false); err != nil {
		return err
	}

	if newTable.Version <= oldTable.Version {
		return ErrIllegalSchemaVersion
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
			oldCol.DefaultValue != newCol.DefaultValue ||
			oldCol.CaseInsensitive != newCol.CaseInsensitive ||
			oldCol.DisableAutoExpand != newCol.DisableAutoExpand {
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
	if !reflect.DeepEqual(newTable.PrimaryKeyColumns, oldTable.PrimaryKeyColumns ){
		return ErrChangePrimaryKeyColumn
	}

	// sort columns
	if len(newTable.ArchivingSortColumns) < len(oldTable.ArchivingSortColumns) {
		return ErrIllegalChangeSortColumn
	}
	for i, sortColumnId := range newTable.ArchivingSortColumns {
		if i < len(oldTable.ArchivingSortColumns) {
			if oldTable.ArchivingSortColumns[i] != sortColumnId {
				return ErrIllegalChangeSortColumn
			}
		}
		if sortColumnId >= len(newTable.Columns) {
			return ErrColumnNonExist
		}
		if newTable.Columns[sortColumnId].Deleted {
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
		return nil
	default:
		value, err := memCom.ValueFromString(valueStr, dataType)
		if err != nil || !value.Valid {
			return utils.StackError(err, "invalid value %s for type %s", valueStr, dataTypeStr)
		}
	}
	return err
}
