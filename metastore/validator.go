package metastore

import (
	"github.com/uber/aresdb/metastore/common"
	memCom "github.com/uber/aresdb/memstore/common"
	"reflect"
	"github.com/uber/aresdb/utils"
	"fmt"
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
		return v.validateIndividualSchema(v.newTable)
	}
	return v.validateSchemaUpdate(v.newTable, v.oldTable)
}


func (v tableSchemaValidatorImpl) validateIndividualSchema(table *common.Table) (err error) {
	existingColumns := make(map[int]bool)
	for i, column := range table.Columns {
		if !column.Deleted {
			existingColumns[i] = true
		}
	}
	if len(existingColumns) == 0 {
		return fmt.Errorf("all columns are invalid")
	}

	if len(table.PrimaryKeyColumns) == 0 {
		return fmt.Errorf("primary key should have at least 1 column")
	}
	for _, idx := range table.PrimaryKeyColumns {
		if !existingColumns[idx] {
			return fmt.Errorf("primariky columns must exist")
		}
	}

	// TODO: checks for config?

	if table.IsFactTable {
		if len(table.ArchivingSortColumns) == 0 {
			return fmt.Errorf("must specify sort columns for fact tables")
		}
		for _, sortColumn := range table.ArchivingSortColumns {
			if !existingColumns[sortColumn] {
				return fmt.Errorf("sort columns must exist")
			}
		}
	}

	// validate data type
	for _, column := range table.Columns {
		if dataType := memCom.DataTypeFromString(column.Type); dataType == memCom.Unknown {
			return fmt.Errorf("unknown data type")
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

func (v tableSchemaValidatorImpl) validateSchemaUpdate(newTable, oldTable *common.Table) (err error) {
	if newTable.Version <= oldTable.Version {
		return fmt.Errorf("schema updates must bump version")
	}

	if newTable.Name != oldTable.Name {
		return fmt.Errorf("can not change table name")
	}

	if newTable.IsFactTable != oldTable.IsFactTable {
		return fmt.Errorf("can not change table type")
	}

	// validate columns
	if len(newTable.Columns) < len(oldTable.Columns) {
		// even with column deletion, or recreation, column id are not reused
		return fmt.Errorf("insufficient column count")
	}

	validColumns := make(map[int]bool)
	var i int

	for i = 0; i < len(oldTable.Columns); i++ {
		oldCol := oldTable.Columns[i]
		newCol := newTable.Columns[i]
		if oldCol.Deleted {
			if !newCol.Deleted {
				return fmt.Errorf("reusing column id not allowed")
			}
			continue
		}
		if newCol.Deleted {
			// delete allowed, skip other checks
			continue
		}

		if oldCol.Name != newCol.Name ||
			oldCol.Type != newCol.Type ||
			oldCol.DefaultValue != newCol.DefaultValue ||
			oldCol.CaseInsensitive != newCol.CaseInsensitive ||
			oldCol.DisableAutoExpand != newCol.DisableAutoExpand {
				return fmt.Errorf("only column config change allowed")
		}
		validColumns[i] = true
	}

	for ; i < len(newTable.Columns); i++ {
		newCol := newTable.Columns[i]
		if newCol.Deleted {
			return fmt.Errorf("can not add column with deleted flag on")
		}
		validColumns[i] = true
	}
	// end validate columns

	// primary key columns
	if !reflect.DeepEqual(newTable.PrimaryKeyColumns, oldTable.PrimaryKeyColumns ){
		return fmt.Errorf("primary key columns can not be changed")
	}

	// sort columns
	if len(newTable.ArchivingSortColumns) < len(oldTable.ArchivingSortColumns) {
		return fmt.Errorf("sort columns are append only")
	}
	for i, idx := range newTable.ArchivingSortColumns {
		if i < len(oldTable.ArchivingSortColumns) {
			if oldTable.ArchivingSortColumns[i] != idx {
				return fmt.Errorf("sort columns are append only")
			}
			continue
		}
		if !validColumns[idx] {
			return fmt.Errorf("sort columns must be a valid column")
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
