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

package broker

import (
	"github.com/uber/aresdb/cluster/topology"
	"github.com/uber/aresdb/memstore"
	"github.com/uber/aresdb/query"
	"github.com/uber/aresdb/utils"
	"net/http"
)

// QueryExecutor defines query executor
type QueryExecutor interface {
	// Execute executes query and flush result to connection
	Execute(namespace, sqlQuery string, w http.ResponseWriter) (err error)
}

// NewQueryExecutor creates a new QueryExecutor
func NewQueryExecutor(sm SchemaManager, topo topology.Topology) QueryExecutor {
	return &queryExecutorImpl{
		schemaManager: sm,
		topo:          topo,
	}
}

// queryExecutorImpl will be reused across all queries
type queryExecutorImpl struct {
	schemaManager SchemaManager
	topo          topology.Topology
}

func (qe *queryExecutorImpl) Execute(namespace, sqlQuery string, w http.ResponseWriter) (err error) {
	// parse
	var aqlQuery *query.AQLQuery
	aqlQuery, err = query.Parse(sqlQuery, utils.GetLogger())

	// compile
	var schemaReader memstore.TableSchemaReader
	schemaReader, err = qe.schemaManager.GetTableSchemaReader(namespace)
	if err != nil {
		return
	}

	// TODO: add timeout
	qc := aqlQuery.Compile(schemaReader, false)
	if qc.IsNonAggregationQuery {
		return qe.executeNonAggQuery(qc, w)
	}
	return qe.executeAggQuery(qc, w)
}

func (qe *queryExecutorImpl) executeNonAggQuery(qc *query.AQLQueryContext, w http.ResponseWriter) (err error) {
	// TODO impplement non agg query executor
	//1. write headers
	//2. calculate fan out plan based on topology
	//3. fan out requests, upon data from data nodes, flush to w
	//4. close, clean up, logging, metrics
	return
}
func (qe *queryExecutorImpl) executeAggQuery(qc *query.AQLQueryContext, w http.ResponseWriter) (err error) {
	// TODO implement agg query executor
	//1. calculate fan out plan based on topology and aggregate functions
	//2. fan out request, wait for all response (retry on failures)
	//3. merge, then flush to w, logging, metrics
	return
}
