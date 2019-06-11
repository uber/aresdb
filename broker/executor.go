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
	"context"
	"fmt"
	"github.com/uber/aresdb/broker/common"
	"github.com/uber/aresdb/cluster/topology"
	dataCli "github.com/uber/aresdb/datanode/client"
	memCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/query"
	queryCom "github.com/uber/aresdb/query/common"
	"github.com/uber/aresdb/utils"
	"net/http"
)

// NewQueryExecutor creates a new QueryExecutor
func NewQueryExecutor(sm common.SchemaManager, topo topology.Topology, client dataCli.DataNodeQueryClient) common.QueryExecutor {
	return &queryExecutorImpl{
		schemaManager:  sm,
		topo:           topo,
		dataNodeClient: client,
	}
}

// queryExecutorImpl will be reused across all queries
type queryExecutorImpl struct {
	schemaManager  common.SchemaManager
	topo           topology.Topology
	dataNodeClient dataCli.DataNodeQueryClient
}

func (qe *queryExecutorImpl) Execute(ctx context.Context, namespace, sqlQuery string, w http.ResponseWriter) (err error) {
	// parse
	var aqlQuery *queryCom.AQLQuery
	aqlQuery, err = query.Parse(sqlQuery, utils.GetLogger())

	// compile
	var schemaReader memCom.TableSchemaReader
	schemaReader, err = qe.schemaManager.GetTableSchemaReader(namespace)
	if err != nil {
		return
	}

	// TODO: add timeout; hll
	qc := &query.AQLQueryContext{
		Query: aqlQuery,
	}
	qc.Compile(schemaReader)

	// execute
	if qc.IsNonAggregationQuery {
		return qe.executeNonAggQuery(ctx, qc, w)
	}
	return qe.executeAggQuery(ctx, qc, w)
}

func (qe *queryExecutorImpl) executeNonAggQuery(ctx context.Context, qc *query.AQLQueryContext, w http.ResponseWriter) (err error) {
	// TODO impplement non agg query executor
	//1. write headers
	//2. calculate fan out plan based on topology
	//3. fan out requests, upon data from data nodes, flush to w
	//4. close, clean up, logging, metrics
	return
}

func (qe *queryExecutorImpl) executeAggQuery(ctx context.Context, qc *query.AQLQueryContext, w http.ResponseWriter) (err error) {
	plan := NewAggQueryPlan(qc, qe.topo, qe.dataNodeClient)
	var result queryCom.AQLQueryResult
	result, err = plan.Execute(ctx)
	if err != nil {
		return
	}
	fmt.Println(result)
	w.Write([]byte("todo!"))
	return
}
