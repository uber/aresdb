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
	"github.com/uber/aresdb/broker/util"
	"github.com/uber/aresdb/cluster/topology"
	dataCli "github.com/uber/aresdb/datanode/client"
	"github.com/uber/aresdb/utils"
	"net/http"
)

// StreamingScanNode implements StreamingPlanNode
type StreamingScanNode struct {
	qc             QueryContext
	host           topology.Host
	dataNodeClient dataCli.DataNodeQueryClient
	topo           topology.HealthTrackingDynamicTopoloy
}

func (ssn *StreamingScanNode) Execute(ctx context.Context) (bs []byte, err error) {

	hostHealthy := true
	defer func() {
		var markErr error
		if hostHealthy {
			markErr = ssn.topo.MarkHostHealthy(ssn.host)
		} else {
			markErr = ssn.topo.MarkHostUnhealthy(ssn.host)
		}
		if markErr != nil {
			utils.GetLogger().With("host", ssn.host, "healthy", hostHealthy).Error("failed to mark host healthiness")
		}
	}()
	for trial := 1; trial <= rpcRetries; trial++ {

		var fetchErr error

		utils.GetLogger().With("host", ssn.host, "query", ssn.qc.AQLQuery).Debug("sending query to datanode")
		bs, fetchErr = ssn.dataNodeClient.QueryRaw(ctx, ssn.qc.RequestID, ssn.host, *ssn.qc.AQLQuery)
		if fetchErr != nil {
			utils.GetRootReporter().GetCounter(utils.DataNodeQueryFailures).Inc(1)
			utils.GetLogger().With(
				"error", fetchErr,
				"host", ssn.host,
				"query", ssn.qc.AQLQuery,
				"requestID", ssn.qc.RequestID,
				"trial", trial).Error("fetch from datanode failed")
			if fetchErr == dataCli.ErrFailedToConnect {
				hostHealthy = false
			}
			err = utils.StackError(fetchErr, "fetch from datanode failed")
			continue
		}
		utils.GetLogger().With(
			"trial", trial,
			"host", ssn.host).Info("fetch from datanode succeeded")
		break
	}
	if bs != nil {
		hostHealthy = true
		err = nil
	}
	return
}

func NewNonAggQueryPlan(qc *QueryContext, topo topology.HealthTrackingDynamicTopoloy, client dataCli.DataNodeQueryClient) (plan *NonAggQueryPlan, err error) {
	headers := make([]string, len(qc.AQLQuery.Dimensions))
	for i, dim := range qc.AQLQuery.Dimensions {
		headers[i] = dim.Expr
	}

	plan = &NonAggQueryPlan{
		headers:    headers,
		resultChan: make(chan streamingScanNoderesult),
		limit:      qc.AQLQuery.Limit,
	}

	var assignment map[topology.Host][]uint32
	assignment, err = util.CalculateShardAssignment(topo)
	if err != nil {
		return
	}

	plan.nodes = make([]*StreamingScanNode, len(assignment))
	i := 0
	for host, shards := range assignment {
		// make deep copy
		currQ := *qc.AQLQuery
		for _, shard := range shards {
			currQ.Shards = append(currQ.Shards, int(shard))
		}
		currQc := *qc
		currQc.AQLQuery = &currQ
		plan.nodes[i] = &StreamingScanNode{
			qc:             currQc,
			host:           host,
			dataNodeClient: client,
			topo:           topo,
		}
		i++
	}

	return
}

type streamingScanNoderesult struct {
	data []byte
	err  error
}

// NonAggQueryPlan implements QueryPlan
type NonAggQueryPlan struct {
	resultChan chan streamingScanNoderesult
	headers    []string
	nodes      []*StreamingScanNode
	// number of rows needed
	limit int
	// number of rows flushed
	flushed int
}

func (nqp *NonAggQueryPlan) Execute(ctx context.Context, w http.ResponseWriter) (err error) {
	var headersBytes []byte
	headersBytes, err = json.Marshal(nqp.headers)
	if err != nil {
		return
	}
	_, err = w.Write([]byte(`{"headers":`))
	if err != nil {
		return
	}
	_, err = w.Write(headersBytes)
	if err != nil {
		return
	}
	_, err = w.Write([]byte(`,"matrixData":[`))
	if err != nil {
		return
	}

	for _, node := range nqp.nodes {
		go func(n *StreamingScanNode) {
			var bs []byte
			bs, err = n.Execute(ctx)
			utils.GetLogger().With("dataSize", len(bs), "error", err).Debug("sending result to result channel")
			nqp.resultChan <- streamingScanNoderesult{
				data: bs,
				err:  err,
			}
		}(node)
	}

	dataNodeWaitStart := utils.Now()

	// the first result
	processedFirtBatch := false
	for i := 0; i < len(nqp.nodes); i++ {
		if nqp.getRowsWanted() == 0 {
			utils.GetLogger().Debug("got enough rows, exiting")
			break
		}
		res := <-nqp.resultChan

		if i == 0 {
			// only log time waited for the fastest datanode for now
			utils.GetRootReporter().GetTimer(utils.TimeWaitedForDataNode).Record(utils.Now().Sub(dataNodeWaitStart))
		}

		if res.err != nil {
			err = res.err
			return
		}

		if len(res.data) == 0 {
			continue
		}

		// write rows
		if nqp.limit < 0 {
			// when no limit, flush data directly
			if processedFirtBatch {
				w.Write([]byte(`,`))
			}
			w.Write(res.data)
		} else {
			// with limit, we have to deserialize
			serDeStart := utils.Now()

			res.data = append([]byte("["), res.data[:]...)
			res.data = append(res.data, byte(']'))
			var resultData [][]interface{}
			err = json.Unmarshal(res.data, &resultData)

			if err != nil {
				return
			}

			if len(resultData) <= nqp.getRowsWanted() {
				if processedFirtBatch {
					w.Write([]byte(`,`))
				}
				w.Write(res.data[1 : len(res.data)-1])
				nqp.flushed += len(resultData)
				utils.GetLogger().With("nrows", len(resultData)).Debug("flushed batch")
			} else {
				rowsToFlush := nqp.getRowsWanted()
				dataToFlush := resultData[:rowsToFlush]
				var bs []byte
				bs, err = json.Marshal(dataToFlush)
				if err != nil {
					return
				}
				if processedFirtBatch {
					w.Write([]byte(`,`))
				}
				// strip brackets
				w.Write(bs[1 : len(bs)-1])
				nqp.flushed += rowsToFlush
				utils.GetLogger().With("nrows", rowsToFlush).Debug("flushed rows")
			}
			utils.GetRootReporter().GetTimer(utils.TimeSerDeDataNodeResponse).Record(utils.Now().Sub(serDeStart))
		}
		processedFirtBatch = true
	}

	_, err = w.Write([]byte(`]}`))
	return
}

func (nqp *NonAggQueryPlan) getRowsWanted() int {
	return nqp.limit - nqp.flushed
}
