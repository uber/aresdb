package common

import (
	"github.com/uber/aresdb/utils"
)

type TableSchemaReader interface {
	// GetSchema returns schema for a table.
	GetSchema(table string) (*TableSchema, error)
	// GetSchemas returns all table schemas.
	GetSchemas() map[string]*TableSchema

	// Provide exclusive access to read/write data protected by MemStore.
	utils.RWLocker
}
