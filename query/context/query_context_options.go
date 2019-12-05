package context

import (
	memCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/query/common"
)

type QueryContextOptions interface {
	GetSchema(tableID int) *memCom.TableSchema
	GetTableID(alias string) (int, bool)
	GetQuery() *common.AQLQuery
	SetError(err error)
	IsDataOnly() bool
}
