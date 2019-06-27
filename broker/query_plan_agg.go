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
	queryCom "github.com/uber/aresdb/query/common"
	"github.com/uber/aresdb/query/expr"
	"github.com/uber/aresdb/utils"
	"math/rand"
	"strings"
	"sync"
)

const (
	rpcRetries = 2
)

type blockingPlanNodeImpl struct {
	children []common.BlockingPlanNode
}

func (bpn *blockingPlanNodeImpl) Children() []common.BlockingPlanNode {
	return bpn.children
}

func (bpn *blockingPlanNodeImpl) Add(nodes ...common.BlockingPlanNode) {
	bpn.children = append(bpn.children, nodes...)
}

func NewMergeNode(agg common.AggType) common.MergeNode {
	return &mergeNodeImpl{
		aggType: agg,
	}
}

type mergeNodeImpl struct {
	blockingPlanNodeImpl
	// MeasureType decides merge behaviour
	aggType common.AggType
}

func (mn *mergeNodeImpl) AggType() common.AggType {
	return mn.aggType
}

func (mn *mergeNodeImpl) Execute(ctx context.Context) (result queryCom.AQLQueryResult, err error) {
	nChildren := len(mn.children)
	// checks before fan out
	if common.Avg == mn.aggType {
		if nChildren != 2 {
			err = utils.StackError(nil, "Avg MergeNode should have 2 children")
			return
		}
		lhs, ok := mn.children[0].(common.MergeNode)
		if !ok {
			err = utils.StackError(nil, "LHS of avg node must be sum node")
			return
		}
		if common.Sum != lhs.AggType() {
			err = utils.StackError(nil, "LHS of avg node must be sum node")
			return
		}
		rhs, ok := mn.children[1].(common.MergeNode)
		if !ok {
			err = utils.StackError(nil, "RHS of avg node must be count node")
			return
		}
		if common.Count != rhs.AggType() {
			err = utils.StackError(nil, "RHS of avg node must be count node")
			return
		}
	}

	childrenResult := make([]queryCom.AQLQueryResult, nChildren)
	nerrs := 0
	wg := &sync.WaitGroup{}
	for i, c := range mn.children {
		wg.Add(1)
		go func(i int, n common.BlockingPlanNode) {
			defer wg.Done()
			var res queryCom.AQLQueryResult
			res, err = n.Execute(ctx)
			if err != nil {
				// err means downstream retry failed
				utils.GetLogger().With(
					"error", err,
				).Error("child node failed")
				nerrs++
				return
			}
			childrenResult[i] = res
		}(i, c)
	}

	// TODO early merge before all results come back
	wg.Wait()

	if nerrs > 0 {
		err = utils.StackError(nil, fmt.Sprintf("%d errors happened executing merge node", nerrs))
		return
	}

	result = childrenResult[0]
	for i := 1; i < nChildren; i++ {
		mergeCtx := newResultMergeContext(mn.aggType)
		result = mergeCtx.run(result, childrenResult[i])
		if mergeCtx.err != nil {
			err = mergeCtx.err
			return
		}
	}
	return
}

// BlockingScanNode is a BlockingPlanNode that handles rpc calls to fetch data from datanode
type BlockingScanNode struct {
	blockingPlanNodeImpl

	query          queryCom.AQLQuery
	shardID        uint32
	topo           topology.Topology
	dataNodeClient dataCli.DataNodeQueryClient
}

func (sn *BlockingScanNode) Execute(ctx context.Context) (result queryCom.AQLQueryResult, err error) {
	isHll := common.CallNameToAggType[sn.query.Measures[0].ExprParsed.(*expr.Call).Name] == common.Hll

	trial := 0
	for trial < rpcRetries {
		trial++

		hosts, routeErr := sn.topo.Get().RouteShard(sn.shardID)
		if routeErr != nil {
			utils.GetLogger().With(
				"error", routeErr,
				"shard", sn.shardID,
				"trial", trial).Error("route shard failed")
			err = utils.StackError(routeErr, "route shard failed")
			continue
		}
		// pick random routable host
		idx := rand.Intn(len(hosts))
		host := hosts[idx]

		var fetchErr error
		utils.GetLogger().With("host", host, "query", sn.query).Debug("sending query to datanode")
		result, fetchErr = sn.dataNodeClient.Query(ctx, host, sn.query, isHll)
		if fetchErr != nil {
			utils.GetLogger().With(
				"error", fetchErr,
				"shard", sn.shardID,
				"host", host,
				"query", sn.query,
				"trial", trial).Error("fetch from datanode failed")
			err = utils.StackError(fetchErr, "fetch from datanode failed")
			continue
		}
		utils.GetLogger().With(
			"trial", trial,
			"shard", sn.shardID,
			"host", host).Info("fetch from datanode succeeded")
		break
	}
	if result != nil {
		err = nil
	}
	return
}

// AggQueryPlan is the plan for aggregate queries
type AggQueryPlan struct {
	root common.BlockingPlanNode
}

// NewAggQueryPlan creates a new agg query plan
func NewAggQueryPlan(qc *QueryContext, topo topology.Topology, client dataCli.DataNodeQueryClient) (plan AggQueryPlan) {
	var root common.MergeNode

	shards := topo.Get().ShardSet().AllIDs()

	// compiler already checked that only 1 measure exists, which is a expr.Call
	measure := qc.AQLQuery.Measures[0].ExprParsed.(*expr.Call)
	agg := common.CallNameToAggType[measure.Name]
	// TODO revisit how to implement AVG. maybe add rollingAvg to datanode so only 1 call per shard needed
	switch agg {
	case common.Avg:
		root = NewMergeNode(common.Avg)
		sumQuery, countQuery := splitAvgQuery(*qc.AQLQuery)
		root.Add(
			buildSubPlan(common.Sum, sumQuery, shards, topo, client),
			buildSubPlan(common.Count, countQuery, shards, topo, client))
	default:
		root = buildSubPlan(agg, *qc.AQLQuery, shards, topo, client)
	}

	plan = AggQueryPlan{
		root: root,
	}
	return
}

func (ap *AggQueryPlan) Execute(ctx context.Context) (results queryCom.AQLQueryResult, err error) {
	return ap.root.Execute(ctx)
}

// splitAvgQuery to sum and count queries
func splitAvgQuery(q queryCom.AQLQuery) (sumq queryCom.AQLQuery, countq queryCom.AQLQuery) {
	measure := q.Measures[0]

	sumq = q
	sumq.Measures = []queryCom.Measure{
		{
			Alias:   measure.Alias,
			Expr:    measure.Expr,
			Filters: measure.Filters,
		},
	}
	sumq.Measures[0].Expr = strings.Replace(strings.ToLower(sumq.Measures[0].Expr), "avg", "sum", 1)
	sumq.Measures[0].ExprParsed, _ = expr.ParseExpr(sumq.Measures[0].Expr)

	countq = q
	countq.Measures = []queryCom.Measure{
		{
			Alias:   measure.Alias,
			Expr:    measure.Expr,
			Filters: measure.Filters,
		},
	}
	countq.Measures[0].Expr = "count(*)"
	countq.Measures[0].ExprParsed, _ = expr.ParseExpr(countq.Measures[0].Expr)
	return
}

func buildSubPlan(agg common.AggType, q queryCom.AQLQuery, shardIDs []uint32, topo topology.Topology, client dataCli.DataNodeQueryClient) common.MergeNode {
	root := NewMergeNode(agg)
	for _, shardID := range shardIDs {
		root.Add(&BlockingScanNode{
			query:          q,
			shardID:        shardID,
			topo:           topo,
			dataNodeClient: client,
		})
	}
	return root
}
