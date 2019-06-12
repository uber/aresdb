package broker

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
	"github.com/uber/aresdb/broker/common"
	"github.com/uber/aresdb/broker/common/mocks"
	shardMock "github.com/uber/aresdb/cluster/shard/mocks"
	"github.com/uber/aresdb/cluster/topology"
	topoMock "github.com/uber/aresdb/cluster/topology/mocks"
	dataCliMock "github.com/uber/aresdb/datanode/client/mocks"
	"github.com/uber/aresdb/query"
	common2 "github.com/uber/aresdb/query/common"
	"github.com/uber/aresdb/query/expr"
)

var _ = ginkgo.Describe("query plan", func() {
	ginkgo.It("splitAvgQuery should work", func() {
		q := common2.AQLQuery{
			Table: "foo",
			Measures: []common2.Measure{
				{Expr: "avg(fare)"},
			},
		}

		q1, q2 := splitAvgQuery(q)
		Ω(q1).Should(Equal(common2.AQLQuery{
			Table: "foo",
			Measures: []common2.Measure{
				{Expr: "sum(fare)"},
			},
		}))
		Ω(q2).Should(Equal(common2.AQLQuery{
			Table: "foo",
			Measures: []common2.Measure{
				{Expr: "count(*)"},
			},
		}))
		Ω(q).Should(Equal(common2.AQLQuery{
			Table: "foo",
			Measures: []common2.Measure{
				{Expr: "avg(fare)"},
			},
		}))
	})

	ginkgo.It("MergeNode should work", func() {
		mockSumNode := mocks.MergeNode{}
		mockCountNode := mocks.MergeNode{}

		mockSumNode.On("Execute", mock.Anything).Return(common2.AQLQueryResult{
			"1": map[string]interface{}{
				"dim1": float64(2),
			},
		}, nil)
		mockSumNode.On("AggType").Return(common.Sum)

		mockCountNode.On("Execute", mock.Anything).Return(common2.AQLQueryResult{
			"1": map[string]interface{}{
				"dim1": float64(1),
			},
		}, nil)
		mockCountNode.On("AggType").Return(common.Count)

		node := NewMergeNode(common.Avg, )
		node.Add(&mockSumNode, &mockCountNode)

		res, err := node.Execute(context.TODO())
		Ω(err).Should(BeNil())
		bs, err := json.Marshal(res)
		Ω(err).Should(BeNil())
		Ω(bs).Should(MatchJSON(`{
			"1": {
				"dim1": 2
			}
		}`))
	})

	ginkgo.It("MergeNode Execute should error", func() {
		mockSumNode := mocks.MergeNode{}
		mockCountNode := mocks.MergeNode{}

		avgNode := NewMergeNode(common.Avg)
		avgNode.Add(&mockSumNode)

		_, err := avgNode.Execute(context.TODO())
		Ω(err.Error()).Should(ContainSubstring("Avg MergeNode should have 2 children"))

		mockSumNode.On("AggType").Return(common.Avg).Once()
		avgNode.Add(&mockCountNode)
		_, err = avgNode.Execute(context.TODO())
		Ω(err.Error()).Should(ContainSubstring("LHS of avg node must be sum node"))

		mockSumNode.On("AggType").Return(common.Sum).Once()
		mockCountNode.On("AggType").Return(common.Sum).Once()
		_, err = avgNode.Execute(context.TODO())
		Ω(err.Error()).Should(ContainSubstring("RHS of avg node must be count node"))

		mockBlockingNode := mocks.BlockingPlanNode{}
		mockBlockingNode.On("Execute", mock.Anything).Return(nil, errors.New("some error"))
		sumNode := NewMergeNode(common.Sum)
		sumNode.Add(&mockBlockingNode)
		_, err = sumNode.Execute(context.TODO())
		Ω(err.Error()).Should(ContainSubstring("errors happened executing merge node"))
	})

	ginkgo.It("NewAggQueryPlan should work", func() {
		q := common2.AQLQuery{
			Table: "table1",
			Measures: []common2.Measure{
				{Expr: "count(*)", ExprParsed: &expr.Call{Name: "count"}},
			},
		}
		qc := query.AQLQueryContext{
			Query: &q,
		}
		mockTopo := topoMock.Topology{}
		mockMap := topoMock.Map{}
		mockShardSet := shardMock.ShardSet{}
		mockTopo.On("Get").Return(&mockMap)
		mockMap.On("ShardSet").Return(&mockShardSet)
		mockShardIds := []uint32{0, 1, 2}
		mockShardSet.On("AllIDs").Return(mockShardIds)

		mockDatanodeCli := dataCliMock.DataNodeQueryClient{}

		plan := NewAggQueryPlan(&qc, &mockTopo, &mockDatanodeCli)
		mn, ok := plan.root.(*mergeNodeImpl)
		Ω(ok).Should(BeTrue())
		Ω(mn.aggType).Should(Equal(common.Count))
		Ω(mn.children).Should(HaveLen(len(mockShardIds)))
		sn, ok := mn.children[0].(*ScanNode)
		Ω(ok).Should(BeTrue())
		Ω(sn.shardID).Should(Equal(uint32(0)))
	})

	ginkgo.It("NewAggQueryPlan should work for avg query", func() {
		q := common2.AQLQuery{
			Table: "table1",
			Measures: []common2.Measure{
				{Expr: "avg(*)", ExprParsed: &expr.Call{Name: "avg"}},
			},
		}
		qc := query.AQLQueryContext{
			Query: &q,
		}
		mockTopo := topoMock.Topology{}
		mockMap := topoMock.Map{}
		mockShardSet := shardMock.ShardSet{}
		mockTopo.On("Get").Return(&mockMap)
		mockMap.On("ShardSet").Return(&mockShardSet)
		mockShardIds := []uint32{0, 1, 2}
		mockShardSet.On("AllIDs").Return(mockShardIds)

		mockDatanodeCli := dataCliMock.DataNodeQueryClient{}

		plan := NewAggQueryPlan(&qc, &mockTopo, &mockDatanodeCli)
		mn, ok := plan.root.(*mergeNodeImpl)
		Ω(ok).Should(BeTrue())
		Ω(mn.aggType).Should(Equal(common.Avg))
		Ω(mn.children).Should(HaveLen(2))
		sumn, ok := mn.children[0].(*mergeNodeImpl)
		Ω(ok).Should(BeTrue())
		Ω(sumn.aggType).Should(Equal(common.Sum))
		Ω(sumn.children).Should(HaveLen(len(mockShardIds)))
		countn, ok := mn.children[1].(*mergeNodeImpl)
		Ω(ok).Should(BeTrue())
		Ω(countn.aggType).Should(Equal(common.Count))
		Ω(countn.children).Should(HaveLen(len(mockShardIds)))
	})

	ginkgo.It("ScanNode Execute should work happy path", func() {
		q := common2.AQLQuery{}

		mockTopo := topoMock.Topology{}
		mockMap := topoMock.Map{}
		mockTopo.On("Get").Return(&mockMap)
		mockHost1 := topoMock.Host{}
		mockHost2 := topoMock.Host{}
		mockMap.On("RouteShard", uint32(0)).Return([]topology.Host{&mockHost1, &mockHost2}, nil)

		mockDatanodeCli := dataCliMock.DataNodeQueryClient{}

		myResult := common2.AQLQueryResult{"foo": 1}
		mockDatanodeCli.On("Query", mock.Anything, mock.Anything, mock.Anything).Return(myResult, nil)

		sn := ScanNode{
			query:          q,
			shardID:        0,
			topo:           &mockTopo,
			dataNodeClient: &mockDatanodeCli,
		}

		res, err := sn.Execute(context.TODO())
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal(myResult))
	})

	ginkgo.It("ScanNode Execute should fail routing error", func() {
		q := common2.AQLQuery{}

		mockTopo := topoMock.Topology{}
		mockMap := topoMock.Map{}
		mockTopo.On("Get").Return(&mockMap)
		mockMap.On("RouteShard", uint32(0)).Return(nil, errors.New("routing error")).Times(rpcRetries)

		mockDatanodeCli := dataCliMock.DataNodeQueryClient{}

		sn := ScanNode{
			query:          q,
			shardID:        0,
			topo:           &mockTopo,
			dataNodeClient: &mockDatanodeCli,
		}

		_, err := sn.Execute(context.TODO())
		Ω(err.Error()).Should(ContainSubstring("route shard failed"))
	})

	ginkgo.It("ScanNode Execute should fail datanode error", func() {
		q := common2.AQLQuery{}

		mockTopo := topoMock.Topology{}
		mockMap := topoMock.Map{}
		mockTopo.On("Get").Return(&mockMap)
		mockHost1 := topoMock.Host{}
		mockHost2 := topoMock.Host{}
		mockMap.On("RouteShard", uint32(0)).Return([]topology.Host{&mockHost1, &mockHost2}, nil).Times(rpcRetries)

		mockDatanodeCli := dataCliMock.DataNodeQueryClient{}

		mockDatanodeCli.On("Query", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("rpc error")).Times(rpcRetries)

		sn := ScanNode{
			query:          q,
			shardID:        0,
			topo:           &mockTopo,
			dataNodeClient: &mockDatanodeCli,
		}

		_, err := sn.Execute(context.TODO())
		Ω(err.Error()).Should(ContainSubstring("fetch from datanode failed"))
	})

	ginkgo.It("ScanNode Execute should work after retry", func() {
		q := common2.AQLQuery{}

		mockTopo := topoMock.Topology{}
		mockMap := topoMock.Map{}
		mockTopo.On("Get").Return(&mockMap)
		mockHost1 := topoMock.Host{}
		mockHost2 := topoMock.Host{}
		mockMap.On("RouteShard", uint32(0)).Return([]topology.Host{&mockHost1, &mockHost2}, nil).Times(rpcRetries)

		mockDatanodeCli := dataCliMock.DataNodeQueryClient{}

		mockDatanodeCli.On("Query", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("rpc error")).Once()
		myResult := common2.AQLQueryResult{"foo": 1}
		mockDatanodeCli.On("Query", mock.Anything, mock.Anything, mock.Anything).Return(myResult, nil).Once()

		sn := ScanNode{
			query:          q,
			shardID:        0,
			topo:           &mockTopo,
			dataNodeClient: &mockDatanodeCli,
		}

		res, err := sn.Execute(context.TODO())
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal(myResult))
	})
})
