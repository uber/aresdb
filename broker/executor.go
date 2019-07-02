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
	metaCom "github.com/uber/aresdb/metastore/common"
	queryCom "github.com/uber/aresdb/query/common"
	"net/http"
)

// NewQueryExecutor creates a new QueryExecutor
func NewQueryExecutor(tsr metaCom.TableSchemaReader, topo topology.Topology, client dataCli.DataNodeQueryClient) common.QueryExecutor {
	return &queryExecutorImpl{
		tableSchemaReader: tsr,
		topo:              topo,
		dataNodeClient:    client,
	}
}

// queryExecutorImpl will be reused across all queries
type queryExecutorImpl struct {
	tableSchemaReader metaCom.TableSchemaReader
	topo              topology.Topology
	dataNodeClient    dataCli.DataNodeQueryClient
}

func (qe *queryExecutorImpl) Execute(ctx context.Context, aql *queryCom.AQLQuery, w http.ResponseWriter) (err error) {
	// TODO: add timeout

	// compile
	qc := NewQueryContext(aql, w)
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

func (qe *queryExecutorImpl) executeNonAggQuery(ctx context.Context, qc *QueryContext, w http.ResponseWriter) (err error) {
	var plan NonAggQueryPlan
	plan, err = NewNonAggQueryPlan(qc, qe.topo, qe.dataNodeClient, w)
	if err != nil {
		return
	}
	return plan.Execute(ctx)
}

func (qe *queryExecutorImpl) executeAggQuery(ctx context.Context, qc *QueryContext, w http.ResponseWriter) (err error) {
	var plan AggQueryPlan
	plan, err = NewAggQueryPlan(qc, qe.topo, qe.dataNodeClient)
	if err != nil {
		return
	}
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
