package broker

import (
	"fmt"
	queryCom "github.com/uber/aresdb/query/common"
	"github.com/uber/aresdb/broker/common"
	"github.com/uber/aresdb/utils"
	"reflect"
)

func newResultMergeContext(aggType common.AggType) resultMergeContext {
	return resultMergeContext{
		agg: aggType,
		path: []string{},
	}
}

// resultMergeContext is the context for merging results
// caller should check for err after calling
type resultMergeContext struct {
	agg common.AggType
	parent queryCom.AQLQueryResult
	path []string
	err error
}

// run merges results from rhs to lhs in place
func (c *resultMergeContext) run(lhs, rhs queryCom.AQLQueryResult) queryCom.AQLQueryResult {
	c.mergeResultsRecursive(map[string]interface{}(lhs), map[string]interface{}(rhs))
	return lhs
}

func (c *resultMergeContext) mergeResultsRecursive(lhs, rhs interface{}) {
	if lhs == nil && rhs == nil {
		return
	}

	if lhs == nil {
		if c.agg == common.Avg {
			c.err = utils.StackError(nil, "error calculating avg: some dimension has only sum. path: %v", c.path)
		}
		c.parent[c.path[len(c.path)-1]] = rhs
		lhs = rhs
		return
	}

	if rhs == nil {
		if c.agg == common.Avg {
			c.err = utils.StackError(nil, "error calculating avg: some dimension has only count. path: %v", c.path)
		}
		c.parent[c.path[len(c.path)-1]] = lhs
		return
	}

	lhsType := reflect.TypeOf(lhs)
	rhsType := reflect.TypeOf(rhs)

	if lhsType != rhsType {
		c.err = utils.StackError(nil, fmt.Sprintf("error merging: different type lhs: %s vs. rhs: %s", lhsType, rhsType))
		return
	}

	switch l := lhs.(type) {
	case float64:
		r := rhs.(float64)
		switch c.agg {
		case common.Count, common.Sum:
			l = l + r
		case common.Max:
			if r > l {
				l = r
			}
		case common.Min:
			if r < l {
				l = r
			}
		case common.Avg:
			l = l / r
		}
		c.parent[c.path[len(c.path)-1]] = l
	case map[string]interface{}:
		r := rhs.(map[string]interface{})
		for k, lv := range l {
			prevPath := c.path
			c.path = append(c.path, k)
			prevParent := c.parent
			c.parent = l

			c.mergeResultsRecursive(lv, r[k])
			if c.err != nil {
				c.err = utils.StackError(c.err, "failed to merge results, path: %v", c.path)
				return
			}
			c.path = prevPath
			c.parent = prevParent
		}
		for k, rv := range r {
			if _, exists := l[k]; !exists {
				prevPath := c.path
				c.path = append(c.path, k)
				prevParent := c.parent
				c.parent = l

				c.mergeResultsRecursive(l[k], rv)
				if c.err != nil {
					c.err = utils.StackError(c.err, "failed to merge results, path: %v", c.path)
					return
				}
				c.path = prevPath
				c.parent = prevParent
			}
		}
		if c.parent != nil {
			c.parent[c.path[len(c.path)-1]] = l
		}
	default:
		// should not happen
		utils.GetLogger().Panic("unknown type", reflect.TypeOf(lhs))
	}
}