package metastore

import (
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber/aresdb/metastore/common"
)

var _ = ginkgo.Describe("Validator", func() {
	ginkgo.It("should be happy with valid single schema", func() {
		table := common.Table{
			Name: "testTable",
			Columns: []common.Column{
				{
					Name: "col1",
					Type: "Uint32",
				},
			},
			PrimaryKeyColumns: []int{0},
		}
		validator := NewTableSchameValidator()
		validator.SetNewTable(table)
		err := validator.Validate()
		Ω(err).Should(BeNil())
	})

	ginkgo.It("should return err for missing time column", func() {
		table := common.Table{
			Name: "testTable",
			Columns: []common.Column{
				{
					Name: "col1",
					Type: "SmallEnum",
				},
			},
			IsFactTable: true,
		}
		validator := NewTableSchameValidator()
		validator.SetNewTable(table)
		err := validator.Validate()
		Ω(err).Should(Equal(ErrMissingTimeColumn))
	})

	ginkgo.It("should return err for missing pk", func() {
		table := common.Table{
			Name: "testTable",
			Columns: []common.Column{
				{
					Name: "col1",
					Type: "Uint32",
				},
			},
		}
		validator := NewTableSchameValidator()
		validator.SetNewTable(table)
		err := validator.Validate()
		Ω(err).Should(Equal(ErrMissingPrimaryKey))
	})

	ginkgo.It("should return err for dup column name", func() {
		table := common.Table{
			Name: "testTable",
			Columns: []common.Column{
				{
					Name: "col1",
					Type: "Uint32",
				},
				{
					Name: "col1",
					Type: "Uint32",
				},
			},
		}
		validator := NewTableSchameValidator()
		validator.SetNewTable(table)
		err := validator.Validate()
		Ω(err).Should(Equal(ErrDuplicatedColumnName))
	})

	ginkgo.It("should return err for dup column", func() {
		table := common.Table{
			Name: "testTable",
			Columns: []common.Column{
				{
					Name: "col1",
					Type: "Uint32",
				},
			},
			PrimaryKeyColumns: []int{0, 0},
		}
		validator := NewTableSchameValidator()
		validator.SetNewTable(table)
		err := validator.Validate()
		Ω(err).Should(Equal(ErrDuplicatedColumn))
	})

	ginkgo.It("should return err for dup column", func() {
		table := common.Table{
			Name: "testTable",
			Columns: []common.Column{
				{
					Name: "col1",
					Type: "Uint32",
				},
			},
			PrimaryKeyColumns:    []int{0},
			IsFactTable:          true,
			ArchivingSortColumns: []int{0, 0},
		}
		validator := NewTableSchameValidator()
		validator.SetNewTable(table)
		err := validator.Validate()
		Ω(err).Should(Equal(ErrDuplicatedColumn))
	})

	ginkgo.It("should return err for too few columns", func() {
		table := common.Table{
			Name:    "testTable",
			Columns: []common.Column{},
		}
		validator := NewTableSchameValidator()
		validator.SetNewTable(table)
		err := validator.Validate()
		Ω(err).Should(Equal(ErrAllColumnsInvalid))
	})

	ginkgo.It("should be happy with valid updates", func() {
		dv1 := "foo"
		dv2 := "foo"

		oldTable := common.Table{
			Name: "testTable",
			Columns: []common.Column{
				{
					Name: "col1",
					Type: "Uint32",
				},
				{
					Name:         "col2",
					Type:         "SmallEnum",
					DefaultValue: &dv1,
				},
			},
			PrimaryKeyColumns:    []int{0},
			IsFactTable:          true,
			ArchivingSortColumns: []int{1},
			Version:              0,
		}
		newTable := common.Table{
			Name: "testTable",
			Columns: []common.Column{
				{
					Name: "col1",
					Type: "Uint32",
				},
				{
					Name:         "col2",
					Type:         "SmallEnum",
					DefaultValue: &dv2,
				},
				{
					Name: "col3",
					Type: "Uint32",
				},
			},
			PrimaryKeyColumns:    []int{0},
			IsFactTable:          true,
			ArchivingSortColumns: []int{1},
			Version:              1,
		}
		validator := NewTableSchameValidator()
		validator.SetNewTable(newTable)
		validator.SetOldTable(oldTable)
		err := validator.Validate()
		Ω(err).Should(BeNil())
	})

	ginkgo.It("should fail for name change", func() {
		oldTable := common.Table{
			Name: "testTable",
			Columns: []common.Column{
				{
					Name: "col1",
					Type: "Uint32",
				},
			},
			PrimaryKeyColumns:    []int{0},
			IsFactTable:          true,
			ArchivingSortColumns: []int{0},
			Version:              0,
		}
		newTable := common.Table{
			Name: "testTable123",
			Columns: []common.Column{
				{
					Name: "col1",
					Type: "Uint32",
				},
			},
			PrimaryKeyColumns:    []int{0},
			IsFactTable:          true,
			ArchivingSortColumns: []int{0},
			Version:              1,
		}
		validator := NewTableSchameValidator()
		validator.SetNewTable(newTable)
		validator.SetOldTable(oldTable)
		err := validator.Validate()
		Ω(err).Should(Equal(ErrSchemaUpdateNotAllowed))
	})

	ginkgo.It("should fail for table type change", func() {
		oldTable := common.Table{
			Name: "testTable",
			Columns: []common.Column{
				{
					Name: "col1",
					Type: "Uint32",
				},
			},
			PrimaryKeyColumns:    []int{0},
			IsFactTable:          true,
			ArchivingSortColumns: []int{1},
			Version:              0,
		}
		newTable := common.Table{
			Name: "testTable",
			Columns: []common.Column{
				{
					Name: "col1",
					Type: "Uint32",
				},
			},
			PrimaryKeyColumns:    []int{0},
			ArchivingSortColumns: []int{1},
			Version:              1,
		}
		validator := NewTableSchameValidator()
		validator.SetNewTable(newTable)
		validator.SetOldTable(oldTable)
		err := validator.Validate()
		Ω(err).Should(Equal(ErrSchemaUpdateNotAllowed))
	})

	ginkgo.It("should fail for number of columns reduction", func() {
		oldTable := common.Table{
			Name: "testTable",
			Columns: []common.Column{
				{
					Name: "col1",
					Type: "Uint32",
				},
				{
					Name: "col2",
					Type: "Uint32",
				},
			},
			PrimaryKeyColumns: []int{0},
			Version:           0,
		}
		newTable := common.Table{
			Name: "testTable",
			Columns: []common.Column{
				{
					Name: "col1",
					Type: "Uint32",
				},
			},
			PrimaryKeyColumns: []int{0},
			Version:           1,
		}
		validator := NewTableSchameValidator()
		validator.SetNewTable(newTable)
		validator.SetOldTable(oldTable)
		err := validator.Validate()
		Ω(err).Should(Equal(ErrInsufficientColumnCount))
	})

	ginkgo.It("should fail for modification on deleted column", func() {
		oldTable := common.Table{
			Name: "testTable",
			Columns: []common.Column{
				{
					Name: "col1",
					Type: "Uint32",
				},
				{
					Name:    "col2",
					Type:    "Uint32",
					Deleted: true,
				},
			},
			PrimaryKeyColumns: []int{0},
			Version:           0,
		}
		newTable := common.Table{
			Name: "testTable",
			Columns: []common.Column{
				{
					Name: "col1",
					Type: "Uint32",
				},
				{
					Name: "col2",
					Type: "Uint32",
				},
			},
			PrimaryKeyColumns: []int{0},
			Version:           1,
		}
		validator := NewTableSchameValidator()
		validator.SetNewTable(newTable)
		validator.SetOldTable(oldTable)
		err := validator.Validate()
		Ω(err).Should(Equal(ErrReusingColumnIDNotAllowed))
	})

	ginkgo.It("should fail for modification on immutable column fields", func() {
		oldTable := common.Table{
			Name: "testTable",
			Columns: []common.Column{
				{
					Name: "col1",
					Type: "Uint32",
				},
				{
					Name: "col2",
					Type: "Uint32",
				},
			},
			PrimaryKeyColumns: []int{0},
			Version:           0,
		}
		newTable := common.Table{
			Name: "testTable",
			Columns: []common.Column{
				{
					Name: "col1",
					Type: "Uint32",
				},
				{
					Name: "col_mod",
					Type: "Uint32",
				},
			},
			PrimaryKeyColumns: []int{0},
			Version:           1,
		}
		validator := NewTableSchameValidator()
		validator.SetNewTable(newTable)
		validator.SetOldTable(oldTable)
		err := validator.Validate()
		Ω(err).Should(Equal(ErrSchemaUpdateNotAllowed))
	})

	ginkgo.It("should fail for adding deleted columns", func() {
		oldTable := common.Table{
			Name: "testTable",
			Columns: []common.Column{
				{
					Name: "col1",
					Type: "Uint32",
				},
			},
			PrimaryKeyColumns: []int{0},
			Version:           0,
		}
		newTable := common.Table{
			Name: "testTable",
			Columns: []common.Column{
				{
					Name: "col1",
					Type: "Uint32",
				},
				{
					Name:    "col2",
					Type:    "Uint32",
					Deleted: true,
				},
			},
			PrimaryKeyColumns: []int{0},
			Version:           1,
		}
		validator := NewTableSchameValidator()
		validator.SetNewTable(newTable)
		validator.SetOldTable(oldTable)
		err := validator.Validate()
		Ω(err).Should(Equal(ErrNewColumnWithDeletion))
	})

	ginkgo.It("should fail for changing pk cloumns", func() {
		oldTable := common.Table{
			Name: "testTable",
			Columns: []common.Column{
				{
					Name: "col1",
					Type: "Uint32",
				},
			},
			PrimaryKeyColumns: []int{0},
			Version:           0,
		}
		newTable := common.Table{
			Name: "testTable",
			Columns: []common.Column{
				{
					Name: "col1",
					Type: "Uint32",
				},
				{
					Name: "col2",
					Type: "Uint32",
				},
			},
			PrimaryKeyColumns: []int{0, 1},
			Version:           1,
		}
		validator := NewTableSchameValidator()
		validator.SetNewTable(newTable)
		validator.SetOldTable(oldTable)
		err := validator.Validate()
		Ω(err).Should(Equal(ErrChangePrimaryKeyColumn))
	})

	ginkgo.It("should fail for changing sort columns", func() {
		oldTable := common.Table{
			Name: "testTable",
			Columns: []common.Column{
				{
					Name: "col1",
					Type: "Uint32",
				},
				{
					Name: "col2",
					Type: "Uint32",
				},
				{
					Name: "col3",
					Type: "Uint32",
				},
			},
			IsFactTable:          true,
			PrimaryKeyColumns:    []int{0},
			ArchivingSortColumns: []int{1, 2},
			Version:              0,
		}
		newTable := common.Table{
			Name: "testTable",
			Columns: []common.Column{
				{
					Name: "col1",
					Type: "Uint32",
				},
				{
					Name: "col2",
					Type: "Uint32",
				},
				{
					Name: "col3",
					Type: "Uint32",
				},
			},
			IsFactTable:          true,
			PrimaryKeyColumns:    []int{0},
			ArchivingSortColumns: []int{1},
			Version:              1,
		}
		// removing sort columns is not allowed
		validator := NewTableSchameValidator()
		validator.SetNewTable(newTable)
		validator.SetOldTable(oldTable)
		err := validator.Validate()
		Ω(err).Should(Equal(ErrIllegalChangeSortColumn))

		// changing existing sort columns is not allowed
		oldTable.ArchivingSortColumns = []int{1}
		newTable.ArchivingSortColumns = []int{2}
		validator.SetNewTable(newTable)
		validator.SetOldTable(oldTable)
		err = validator.Validate()
		Ω(err).Should(Equal(ErrIllegalChangeSortColumn))
	})

	ginkgo.It("ValidateDefaultValue should work", func() {
		Ω(ValidateDefaultValue("trues", common.Bool)).ShouldNot(BeNil())
		Ω(ValidateDefaultValue("true", common.Bool)).Should(BeNil())

		Ω(ValidateDefaultValue("1000", common.Uint8)).ShouldNot(BeNil())
		Ω(ValidateDefaultValue("0", common.Uint8)).Should(BeNil())

		Ω(ValidateDefaultValue("100000", common.Uint16)).ShouldNot(BeNil())
		Ω(ValidateDefaultValue("0", common.Uint16)).Should(BeNil())

		Ω(ValidateDefaultValue("100000000000000", common.Uint32)).ShouldNot(BeNil())
		Ω(ValidateDefaultValue("0", common.Uint32)).Should(BeNil())

		Ω(ValidateDefaultValue("1000", common.Int8)).ShouldNot(BeNil())
		Ω(ValidateDefaultValue("0", common.Int8)).Should(BeNil())

		Ω(ValidateDefaultValue("100000000", common.Int16)).ShouldNot(BeNil())
		Ω(ValidateDefaultValue("0", common.Int16)).Should(BeNil())

		Ω(ValidateDefaultValue("100000000000000", common.Uint32)).ShouldNot(BeNil())
		Ω(ValidateDefaultValue("0", common.Uint32)).Should(BeNil())

		Ω(ValidateDefaultValue("1.s", common.Float32)).ShouldNot(BeNil())
		Ω(ValidateDefaultValue("0.0", common.Float32)).Should(BeNil())

		Ω(ValidateDefaultValue("00000000000000000000000000000000000000", common.UUID)).ShouldNot(BeNil())
		Ω(ValidateDefaultValue("2cdc434e-4752-11e8-842f-0ed5f89f718b", common.UUID)).Should(BeNil())

		Ω(ValidateDefaultValue("-122.44231998 37.77901703", common.GeoPoint)).Should(BeNil())
		Ω(ValidateDefaultValue("-122.44231998,37.77901703", common.GeoPoint)).Should(BeNil())
		Ω(ValidateDefaultValue("Point(-122.44231998,37.77901703)", common.GeoPoint)).Should(BeNil())
		Ω(ValidateDefaultValue("  Point( -122.44231998 37.77901703  ) ", common.GeoPoint)).Should(BeNil())
	})

	ginkgo.It("should fail for setting default value for time column column", func() {
		zero := "0"
		newTable := common.Table{
			Name: "testTable",
			Columns: []common.Column{
				{
					Name:         "col1",
					Type:         "Uint32",
					DefaultValue: &zero,
				},
				{
					Name: "col2",
					Type: "Uint32",
				},
				{
					Name: "col3",
					Type: "Uint32",
				},
			},
			IsFactTable: true,
		}
		// removing sort columns is not allowed
		validator := NewTableSchameValidator()
		validator.SetNewTable(newTable)
		err := validator.Validate()
		Ω(err).Should(Equal(ErrTimeColumnDoesNotAllowDefault))
	})

	ginkgo.It("should fail when disallow missing event time", func() {
		oldTable := common.Table{
			Name: "testTable",
			Columns: []common.Column{
				{
					Name: "col1",
					Type: "Uint32",
				},
				{
					Name: "col2",
					Type: "Uint32",
				},
				{
					Name: "col3",
					Type: "Uint32",
				},
			},
			PrimaryKeyColumns: []int{1},
			IsFactTable:       true,
			Version:           0,
			Config: common.TableConfig{
				AllowMissingEventTime: true,
			},
		}

		newTable := common.Table{
			Name: "testTable",
			Columns: []common.Column{
				{
					Name: "col1",
					Type: "Uint32",
				},
				{
					Name: "col2",
					Type: "Uint32",
				},
				{
					Name: "col3",
					Type: "Uint32",
				},
			},
			IsFactTable:       true,
			PrimaryKeyColumns: []int{1},
			Version:           1,
			Config: common.TableConfig{
				AllowMissingEventTime: false,
			},
		}

		validator := NewTableSchameValidator()
		validator.SetNewTable(newTable)
		validator.SetOldTable(oldTable)
		err := validator.Validate()
		Ω(err).Should(Equal(ErrDisallowMissingEventTime))

		oldTable = common.Table{
			Name: "testTable",
			Columns: []common.Column{
				{
					Name: "col1",
					Type: "Uint32",
				},
				{
					Name: "col2",
					Type: "Uint32",
				},
				{
					Name: "col3",
					Type: "Uint32",
				},
			},
			PrimaryKeyColumns: []int{1},
			IsFactTable:       true,
			Version:           0,
			Config: common.TableConfig{
				AllowMissingEventTime: false,
			},
		}

		newTable = common.Table{
			Name: "testTable",
			Columns: []common.Column{
				{
					Name: "col1",
					Type: "Uint32",
				},
				{
					Name: "col2",
					Type: "Uint32",
				},
				{
					Name: "col3",
					Type: "Uint32",
				},
			},
			IsFactTable:       true,
			PrimaryKeyColumns: []int{1},
			Version:           1,
			Config: common.TableConfig{
				AllowMissingEventTime: true,
			},
		}

		validator.SetNewTable(newTable)
		validator.SetOldTable(oldTable)
		err = validator.Validate()
		Ω(err).Should(BeNil())
	})
})
