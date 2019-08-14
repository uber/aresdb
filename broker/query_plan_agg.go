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
	"fmt"
	"github.com/uber/aresdb/broker/common"
	"github.com/uber/aresdb/broker/util"
	"github.com/uber/aresdb/cluster/topology"
	dataCli "github.com/uber/aresdb/datanode/client"
	memCom "github.com/uber/aresdb/memstore/common"
	queryCom "github.com/uber/aresdb/query/common"
	"github.com/uber/aresdb/query/expr"
	"github.com/uber/aresdb/utils"
	"net/http"
	"strconv"
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

	dataNodeWaitStart := utils.Now()
	// TODO early merge before all results come back
	wg.Wait()
	utils.GetRootReporter().GetTimer(utils.TimeWaitedForDataNode).Record(utils.Now().Sub(dataNodeWaitStart))

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

	qc             QueryContext
	host           topology.Host
	dataNodeClient dataCli.DataNodeQueryClient
	topo           topology.HealthTrackingDynamicTopoloy
}

func (sn *BlockingScanNode) Execute(ctx context.Context) (result queryCom.AQLQueryResult, err error) {
	isHll := common.CallNameToAggType[sn.qc.AQLQuery.Measures[0].ExprParsed.(*expr.Call).Name] == common.Hll

	trial := 0
	hostHealthy := true
	defer func() {
		var markErr error
		if hostHealthy {
			markErr = sn.topo.MarkHostHealthy(sn.host)
		} else {
			markErr = sn.topo.MarkHostUnhealthy(sn.host)
		}
		if markErr != nil {
			utils.GetLogger().With("host", sn.host, "healthy", hostHealthy).Error("failed to mark host healthiness")
		}
	}()

	for trial < rpcRetries {
		trial++

		var fetchErr error
		utils.GetLogger().With("host", sn.host, "query", sn.qc.AQLQuery).Debug("sending query to datanode")
		result, fetchErr = sn.dataNodeClient.Query(ctx, sn.qc.RequestID, sn.host, *sn.qc.AQLQuery, isHll)
		if fetchErr != nil {
			utils.GetRootReporter().GetCounter(utils.DataNodeQueryFailures).Inc(1)
			utils.GetLogger().With(
				"error", fetchErr,
				"host", sn.host,
				"query", sn.qc.AQLQuery,
				"requestID", sn.qc.RequestID,
				"trial", trial).Error("fetch from datanode failed")
			if fetchErr == dataCli.ErrFailedToConnect {
				hostHealthy = false
			}
			err = utils.StackError(fetchErr, "fetch from datanode failed")
			continue
		}
		utils.GetLogger().With(
			"trial", trial,
			"host", sn.host).Info("fetch from datanode succeeded")
		break
	}
	if result != nil {
		hostHealthy = true
		err = nil
	}
	return
}

// AggQueryPlan is the plan for aggregate queries
type AggQueryPlan struct {
	aggType common.AggType
	qc      *QueryContext
	root    common.BlockingPlanNode
}

// NewAggQueryPlan creates a new agg query plan
func NewAggQueryPlan(qc *QueryContext, topo topology.HealthTrackingDynamicTopoloy, client dataCli.DataNodeQueryClient) (plan *AggQueryPlan, err error) {
	var root common.MergeNode

	var assignments map[topology.Host][]uint32
	assignments, err = util.CalculateShardAssignment(topo)
	if err != nil {
		return
	}

	// compiler already checked that only 1 measure exists, which is a expr.Call
	measure := qc.AQLQuery.Measures[0].ExprParsed.(*expr.Call)
	agg := common.CallNameToAggType[measure.Name]
	// TODO revisit how to implement AVG. maybe add rollingAvg to datanode so only 1 call per shard needed
	switch agg {
	case common.Avg:
		root = NewMergeNode(common.Avg)
		sumQuery, countQuery := splitAvgQuery(*qc)
		root.Add(
			buildSubPlan(common.Sum, sumQuery, assignments, topo, client),
			buildSubPlan(common.Count, countQuery, assignments, topo, client))
	default:
		root = buildSubPlan(agg, *qc, assignments, topo, client)
	}

	plan = &AggQueryPlan{
		aggType: agg,
		qc:      qc,
		root:    root,
	}
	return
}

func (ap *AggQueryPlan) postProcess(results queryCom.AQLQueryResult, execErr error, w http.ResponseWriter) (err error) {
	var data []byte
	if ap.qc.ReturnHLLBinary {
		w.Header().Set(utils.HTTPContentTypeHeaderKey, utils.HTTPContentTypeHyperLogLog)
		data, err = ap.postProcessHLLBinary(results, execErr)
	} else {
		if execErr != nil {
			err = execErr
			return
		}
		var rewritten interface{}
		rewritten, err = ap.translateEnum(results)
		if err != nil {
			return
		}
		data, err = json.Marshal(rewritten)
	}

	if err != nil {
		return
	}
	_, err = w.Write([]byte(data))
	return
}

func (ap *AggQueryPlan) translateEnum(results queryCom.AQLQueryResult) (rewritten interface{}, err error) {
	return traverseRecursive(0, map[string]interface{}(results), ap.qc.DimensionEnumReverseDicts)
}

// helper function that traverse AQLQueryResult, translate enum rank to value
func traverseRecursive(dimIndex int, curr interface{}, dimReverseDict map[int][]string) (rewritten interface{}, err error) {
	if len(dimReverseDict) == 0 {
		return curr, nil
	}

	if v, ok := curr.(map[string]interface{}); ok {
		for ck, child := range v {
			rc, err := traverseRecursive(dimIndex+1, child, dimReverseDict)
			if err != nil {
				return nil, err
			}
			v[ck] = rc
		}
	}

	// visit curr and translate
	if reverseDict, exists := dimReverseDict[dimIndex]; exists {
		v, ok := curr.(map[string]interface{})
		if !ok {
			return nil, utils.StackError(nil, "failed to translate enum at %dth dimension", dimIndex)
		}

		newRes := make(map[string]interface{})
		for k, val := range v {
			if queryCom.NULLString == k {
				newRes[k] = val
				continue
			}
			enumRank, err := strconv.Atoi(k)
			if err != nil {
				return nil, err
			}
			if enumRank >= len(reverseDict) || enumRank < 0 {
				return nil, utils.StackError(nil, "invalid enum rank %d, enums were %s", enumRank, reverseDict)
			}
			enumVal := reverseDict[enumRank]
			newRes[enumVal] = val
		}
		return newRes, nil
	}

	return curr, nil
}

// convert HLL to binary format
func (ap *AggQueryPlan) postProcessHLLBinary(res queryCom.AQLQueryResult, execErr error) (data []byte, err error) {
	hllQueryResults := queryCom.NewHLLQueryResults()
	if execErr != nil {
		hllQueryResults.WriteError(execErr)
		data = hllQueryResults.GetBytes()
		return
	}

	var (
		qc           = ap.qc
		dimDataTypes = make([]memCom.DataType, len(qc.AQLQuery.Dimensions))
		reverseDicts = make(map[int][]string)
		enumDicts    = make(map[int]map[string]int)
		resultSize   = len(res)
		hllVector    []byte
		dimVector    []byte
		countVector  []byte
	)

	// build dataTypes and enumDicts
	for dimIdx, dim := range qc.AQLQuery.Dimensions {
		dimDataTypes[dimIdx] = queryCom.GetDimensionDataType(dim.ExprParsed)
		if memCom.IsEnumType(dimDataTypes[dimIdx]) {
			var (
				tableID  int
				columnID int
			)
			tableID, columnID, err = qc.resolveColumn(dim.Expr)
			if err != nil {
				return
			}
			table := qc.Tables[tableID]
			reverseDicts[dimIdx] = table.EnumDicts[table.Schema.Columns[columnID].Name].ReverseDict
			enumDicts[dimIdx] = table.EnumDicts[table.Schema.Columns[columnID].Name].Dict
		}
	}

	// build HLL binary
	hllVector, dimVector, countVector, err = queryCom.BuildVectorsFromHLLResult(res, dimDataTypes, enumDicts, qc.DimensionVectorIndex)
	if err != nil {
		return
	}
	paddedRawDimValuesVectorLength := (uint32(queryCom.DimValResVectorSize(resultSize, qc.NumDimsPerDimWidth)) + 7) / 8 * 8
	paddedHLLVectorLength := (int64(len(hllVector)) + 7) / 8 * 8

	builder := queryCom.HLLDataWriter{
		HLLData: queryCom.HLLData{
			ResultSize:                     uint32(resultSize),
			NumDimsPerDimWidth:             qc.NumDimsPerDimWidth,
			DimIndexes:                     qc.DimensionVectorIndex,
			DataTypes:                      dimDataTypes,
			EnumDicts:                      reverseDicts,
			PaddedRawDimValuesVectorLength: paddedRawDimValuesVectorLength,
			PaddedHLLVectorLength:          paddedHLLVectorLength,
		},
	}

	headerSize, totalSize := builder.CalculateSizes()
	builder.Buffer = make([]byte, totalSize)
	err = builder.SerializeHeader()
	if err != nil {
		return
	}

	writer := utils.NewBufferWriter(builder.Buffer)
	writer.SkipBytes(int(headerSize))
	_, err = writer.Write(dimVector)
	if err != nil {
		return
	}
	writer.AlignBytes(8)
	_, err = writer.Write(countVector)
	if err != nil {
		return
	}
	writer.AlignBytes(8)
	_, err = writer.Write(hllVector)
	if err != nil {
		return
	}

	// write hll binary to hll result, and return
	hllQueryResults.WriteResult(builder.Buffer)
	return hllQueryResults.GetBytes(), nil
}

func (ap *AggQueryPlan) Execute(ctx context.Context, w http.ResponseWriter) (err error) {
	var results queryCom.AQLQueryResult
	results, err = ap.root.Execute(ctx)
	return ap.postProcess(results, err, w)
}

// splitAvgQuery to sum and count queries
func splitAvgQuery(qc QueryContext) (sumqc QueryContext, countqc QueryContext) {
	q := qc.AQLQuery
	measure := q.Measures[0]

	sumq := *q
	sumq.Measures = []queryCom.Measure{
		{
			Alias:   measure.Alias,
			Expr:    measure.Expr,
			Filters: measure.Filters,
		},
	}
	sumq.Measures[0].Expr = strings.Replace(strings.ToLower(sumq.Measures[0].Expr), "avg", "sum", 1)
	sumq.Measures[0].ExprParsed, _ = expr.ParseExpr(sumq.Measures[0].Expr)

	countq := *q
	countq.Measures = []queryCom.Measure{
		{
			Alias:   measure.Alias,
			Expr:    measure.Expr,
			Filters: measure.Filters,
		},
	}
	countq.Measures[0].Expr = "count(*)"
	countq.Measures[0].ExprParsed, _ = expr.ParseExpr(countq.Measures[0].Expr)

	sumqc = qc
	sumqc.AQLQuery = &sumq
	countqc = qc
	countqc.AQLQuery = &countq
	return
}

func buildSubPlan(agg common.AggType, qc QueryContext, assignments map[topology.Host][]uint32, topo topology.HealthTrackingDynamicTopoloy, client dataCli.DataNodeQueryClient) common.MergeNode {
	root := NewMergeNode(agg)
	for host, shardIDs := range assignments {
		// make deep copy
		currQ := *qc.AQLQuery
		for _, shard := range shardIDs {
			currQ.Shards = append(currQ.Shards, int(shard))
		}
		currQc := qc
		currQc.AQLQuery = &currQ
		root.Add(&BlockingScanNode{
			qc:             currQc,
			host:           host,
			dataNodeClient: client,
			topo:           topo,
		})
	}
	return root
}
