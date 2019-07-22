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

type EnumUpdater interface {
	// UpdateEnum can update enum for one column
	UpdateEnum(table, column string, enumList []string) error
}

// BootStrapToken used to Acqure/Release token during data purge operations
type BootStrapToken interface {
	// Call AcquireToken to reserve usage token before any data purge operation
	// when return result is true, then you can proceed to the purge operation and later call ReleaseToken to release the token
	// when return result is false, then some bootstrap work is going on, no purge operation is permitted
	AcquireToken(table string, shard uint32) bool
	// Call ReleaseToken wheneven you call AcquireToken with true return value to release token
	ReleaseToken(table string, shard uint32)
}
