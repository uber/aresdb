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
	"encoding/json"
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
func NewQueryExecutor(tsr memCom.TableSchemaReader, topo topology.Topology, client dataCli.DataNodeQueryClient) common.QueryExecutor {
	return &queryExecutorImpl{
		tableSchemaReader: tsr,
		topo:              topo,
		dataNodeClient:    client,
	}
}

// queryExecutorImpl will be reused across all queries
type queryExecutorImpl struct {
	tableSchemaReader memCom.TableSchemaReader
	topo              topology.Topology
	dataNodeClient    dataCli.DataNodeQueryClient
}

func (qe *queryExecutorImpl) Execute(ctx context.Context, sqlQuery string, w http.ResponseWriter) (err error) {
	// parse
	var aqlQuery *queryCom.AQLQuery
	aqlQuery, err = query.Parse(sqlQuery, utils.GetLogger())
	if err != nil {
		return
	}

	// compile
	// TODO: add timeout; hll
	qc := &query.AQLQueryContext{
		Query: aqlQuery,
	}
	qc.Compile(qe.tableSchemaReader)
	if qc.Error != nil {
		err = qc.Error
		return
	}

	// execute
	if qc.IsNonAggregationQuery {
		return qe.executeNonAggQuery(ctx, qc, w)
	}
	return qe.executeAggQuery(ctx, qc, w)
}

func (qe *queryExecutorImpl) executeNonAggQuery(ctx context.Context, qc *query.AQLQueryContext, w http.ResponseWriter) (err error) {
	plan := NewNonAggQueryPlan(qc, qe.topo, qe.dataNodeClient, w)
	err = plan.Execute(ctx)
	return
}

func (qe *queryExecutorImpl) executeAggQuery(ctx context.Context, qc *query.AQLQueryContext, w http.ResponseWriter) (err error) {
	plan := NewAggQueryPlan(qc, qe.topo, qe.dataNodeClient)
	var result queryCom.AQLQueryResult
	result, err = plan.Execute(ctx)
	if err != nil {
		return
	}
	var bs []byte
	bs, err = json.Marshal(result)
	w.Write([]byte(bs))
	return
}
