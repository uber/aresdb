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

package context

import (
	memCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/query/common"
)

// QueryContextOptions interface is to abstract some common methods
// which can be used by the QueryContextHelper
type QueryContextOptions interface {
	// GetSchema get table schema using tableID
	GetSchema(tableID int) *memCom.TableSchema
	// GetTableID get tableID using table alias name
	GetTableID(alias string) (int, bool)
	// GetQuery get AQLQuery instance
	GetQuery() *common.AQLQuery
	// SetError set Error back to QueryContext
	SetError(err error)
	// IsDataOnly whether it's a DataOnly request from broker
	IsDataOnly() bool
}
