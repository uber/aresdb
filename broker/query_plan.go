package broker

import (
	"github.com/uber/aresdb/cluster/topology"
	"github.com/uber/aresdb/query"
	queryCom "github.com/uber/aresdb/query/common"
	"github.com/uber/aresdb/broker/common"
	"github.com/uber/aresdb/utils"
	"sync"
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

// MergeNode is a blocking plan node that merges results from sub nodes
type MergeNode struct {
	blockingPlanNodeImpl
	// MeasureType decides merge behaviour
	AggType common.AggType
}

func (mn *MergeNode) Run() (result queryCom.AQLQueryResult, err error){
	nChildren := len(mn.children)
	// checks before fan out
	if common.Avg == mn.AggType {
		if nChildren != 2 {
			err = utils.StackError(nil, "Avg MergeNode should have 2 children")
			return
		}
		lhs, ok := mn.children[0].(*MergeNode)
		if !ok {
			err = utils.StackError(nil, "LHS of avg node must be sum node")
			return
		}
		if common.Sum != lhs.AggType {
			err = utils.StackError(nil, "LHS of avg node must be sum node")
			return
		}
		rhs, ok := mn.children[0].(*MergeNode)
		if !ok {
			err = utils.StackError(nil, "RHS of avg node must be count node")
			return
		}
		if common.Count != rhs.AggType {
			err = utils.StackError(nil, "RHS of avg node must be count node")
			return
		}
	}

	childrenResult := make([]queryCom.AQLQueryResult, nChildren)
	errs := make([]error, nChildren)
	wg := &sync.WaitGroup{}
	for i, c := range mn.children {
		wg.Add(1)
		bpn, ok := c.(common.BlockingPlanNode)
		if !ok {
			err = utils.StackError(nil, "expecting BlockingPlanNode")
			return
		}
		go func(i int, n common.BlockingPlanNode) {
			defer wg.Done()
			var res queryCom.AQLQueryResult
			res, err = n.Run()
			if err != nil {
				// err means downstream retry failed
				errs[i ] = utils.StackError(err, "child node failed")
				return
			}
			childrenResult[i] = res
		}(i, bpn)
	}
	wg.Wait()

	result = childrenResult[0]
	for i := 1; i < nChildren; i++ {
		ctx := newResultMergeContext(mn.AggType)
		result = ctx.run(result, childrenResult[i])
		if ctx.err != nil {
			err = ctx.err
			return
		}
	}

	return
}

// AggQueryPlan is the plan for aggregate queries
type AggQueryPlan struct {
	root common.BlockingPlanNode

}

func NewAggQueryPlan(qc *query.AQLQueryContext, topo topology.Topology) (plan AggQueryPlan, err error) {
	// TODO support multiple measures
	if len(qc.Query.Measures) != 0 {
		err = utils.StackError(nil, "aggregate query has to have exactly 1 measure")
		return
	}

	// TODO build query plan
	//measure := qc.Query.Measures[0]
	//switch measure.ExprParsed.
		//shards := topo.Get().ShardSet().All()
	//for
	return

}

func (ap *AggQueryPlan) Run() (results queryCom.AQLQueryResult, err error) {
	return
}

// NonAggQueryPlan implements QueryPlan
//1. write headers
//2. calculate fan out plan based on topology
//3. fan out requests, upon data from data nodes, flush to w
//4. close, clean up, logging, metrics
// TODO
type NonAggQueryPlan struct {

}

