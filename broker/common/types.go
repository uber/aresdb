package common

import (
	queryCom "github.com/uber/aresdb/query/common"
	memCom "github.com/uber/aresdb/memstore/common"
	"io"
	"net/http"
)

// SchemaManager keeps table schema from all namespaces in current ENV up to date
type SchemaManager interface {
	// Run initializes all schema and starts jobs to sync from controller
	Run()
	// GetTable gets schema by namespace and table name
	GetTableSchemaReader(namespace string) (memCom.TableSchemaReader, error)
}

// QueryExecutor defines query executor
type QueryExecutor interface {
	// Execute executes query and flush result to connection
	Execute(namespace, sqlQuery string, w http.ResponseWriter) (err error)
}

// BlockingPlanNode defines query plan nodes that waits for children to finish
type BlockingPlanNode interface {
	Run() (queryCom.AQLQueryResult, error)
	Children() []BlockingPlanNode
	Add(...BlockingPlanNode)
}

// StreamingPlanNode defines query plan nodes that eager flushes to output
type StreamingPlanNode interface {
	Run(writer io.Writer) error
}