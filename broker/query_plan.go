package broker

import (
	"github.com/uber/aresdb/cluster/topology"
	"github.com/uber/aresdb/query"
	"github.com/uber/aresdb/query/common"
	"github.com/uber/aresdb/utils"
	"io"
	"sync"
)

type AggType int
const (
	Count AggType = iota
	Sum
	Max
	Min
	Avg
	Hll //todo
)

// BlockingPlanNode defines query plan nodes that waits for children to finish
type BlockingPlanNode interface {
	Run() (common.AQLQueryResult, error)
	Children() []BlockingPlanNode
	Add(...BlockingPlanNode)
}

// StreamingPlanNode defines query plan nodes that eager flushes to output
type StreamingPlanNode interface {
	Run(writer io.Writer) error
}

type blockingPlanNodeImpl struct {
	children []BlockingPlanNode
}

func (bpn *blockingPlanNodeImpl) Children() []BlockingPlanNode {
	return bpn.children
}

func (bpn *blockingPlanNodeImpl) Add(nodes ...BlockingPlanNode) {
	bpn.children = append(bpn.children, nodes...)
}

// MergeNode is a blocking plan node that merges results from sub nodes
type MergeNode struct {
	blockingPlanNodeImpl
	// MeasureType decides merge behaviour
	AggType AggType
}

func (mn *MergeNode) Run() (result common.AQLQueryResult, err error){
	nChildren := len(mn.children)
	// checks before fan out
	if Avg == mn.AggType {
		if nChildren != 2 {
			err = utils.StackError(nil, "Avg MergeNode should have 2 children")
			return
		}
		lhs, ok := mn.children[0].(*MergeNode)
		if !ok {
			err = utils.StackError(nil, "LHS of avg node must be sum node")
			return
		}
		if Sum != lhs.AggType {
			err = utils.StackError(nil, "LHS of avg node must be sum node")
			return
		}
		rhs, ok := mn.children[0].(*MergeNode)
		if !ok {
			err = utils.StackError(nil, "RHS of avg node must be count node")
			return
		}
		if Count != rhs.AggType {
			err = utils.StackError(nil, "RHS of avg node must be count node")
			return
		}
	}

	childrenResult := make([]common.AQLQueryResult, nChildren)
	errs := make([]error, nChildren)
	wg := &sync.WaitGroup{}
	for i, c := range mn.children {
		wg.Add(1)
		bpn, ok := c.(BlockingPlanNode)
		if !ok {
			err = utils.StackError(nil, "expecting BlockingPlanNode")
			return
		}
		go func(i int, n BlockingPlanNode) {
			defer wg.Done()
			var res common.AQLQueryResult
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
		//mergeResults(&result, childrenResult[i], mn.AggType)
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
	root BlockingPlanNode

}

func NewAggQueryPlan(qc *query.AQLQueryContext, topo topology.Topology) (plan AggQueryPlan, err error) {
	// TODO support multiple measures
	if len(qc.Query.Measures) != 0 {
		err = utils.StackError(nil, "aggregate query has to have exactly 1 measure")
		return
	}
	//measure := qc.Query.Measures[0]
	//switch measure.ExprParsed.
		//shards := topo.Get().ShardSet().All()
	//for
	return

}

func (ap *AggQueryPlan) Run() (results common.AQLQueryResult, err error) {
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

