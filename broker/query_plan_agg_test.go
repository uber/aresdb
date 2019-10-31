package broker

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
	"github.com/uber-go/tally"
	brokerCom "github.com/uber/aresdb/broker/common"
	"github.com/uber/aresdb/broker/common/mocks"
	shardMock "github.com/uber/aresdb/cluster/shard/mocks"
	"github.com/uber/aresdb/cluster/topology"
	topoMock "github.com/uber/aresdb/cluster/topology/mocks"
	"github.com/uber/aresdb/common"
	"github.com/uber/aresdb/datanode/client"
	dataCliMock "github.com/uber/aresdb/datanode/client/mocks"
	memCom "github.com/uber/aresdb/memstore/common"
	memComMocks "github.com/uber/aresdb/memstore/common/mocks"
	metaCom "github.com/uber/aresdb/metastore/common"
	queryCom "github.com/uber/aresdb/query/common"
	"github.com/uber/aresdb/query/expr"
	"github.com/uber/aresdb/utils"
	"io/ioutil"
	"net/http/httptest"
	"reflect"
)

var _ = ginkgo.Describe("agg query plan", func() {
	utils.Init(common.AresServerConfig{}, common.NewLoggerFactory().GetDefaultLogger(), common.NewLoggerFactory().GetDefaultLogger(), tally.NewTestScope("test", nil))

	ginkgo.It("splitAvgQuery should work", func() {
		q := queryCom.AQLQuery{
			Table: "foo",
			Measures: []queryCom.Measure{
				{Expr: "avg(fare)"},
			},
		}

		qc := QueryContext{AQLQuery: &q}
		q1, q2 := splitAvgQuery(qc)
		Ω(q1).Should(Equal(QueryContext{AQLQuery: &queryCom.AQLQuery{
			Table: "foo",
			Measures: []queryCom.Measure{
				{Expr: "sum(fare)", ExprParsed: &expr.Call{Name: "sum", Args: []expr.Expr{&expr.VarRef{Val: "fare"}}}},
			},
		}}))
		Ω(q2).Should(Equal(QueryContext{AQLQuery: &queryCom.AQLQuery{
			Table: "foo",
			Measures: []queryCom.Measure{
				{Expr: "count(*)", ExprParsed: &expr.Call{Name: "count", Args: []expr.Expr{&expr.Wildcard{}}}},
			},
		}}))

		// original qc should not be changed
		Ω(qc).Should(Equal(QueryContext{AQLQuery: &queryCom.AQLQuery{
			Table: "foo",
			Measures: []queryCom.Measure{
				{Expr: "avg(fare)"},
			},
		}}))
	})

	ginkgo.It("MergeNode should work", func() {
		mockSumNode := mocks.MergeNode{}
		mockCountNode := mocks.MergeNode{}

		mockSumNode.On("Execute", mock.Anything).Return(queryCom.AQLQueryResult{
			"1": map[string]interface{}{
				"dim1": float64(2),
			},
		}, nil)
		mockSumNode.On("AggType").Return(brokerCom.Sum)

		mockCountNode.On("Execute", mock.Anything).Return(queryCom.AQLQueryResult{
			"1": map[string]interface{}{
				"dim1": float64(1),
			},
		}, nil)
		mockCountNode.On("AggType").Return(brokerCom.Count)

		node := NewMergeNode(brokerCom.Avg)
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

		avgNode := NewMergeNode(brokerCom.Avg)
		avgNode.Add(&mockSumNode)

		_, err := avgNode.Execute(context.TODO())
		Ω(err.Error()).Should(ContainSubstring("Avg MergeNode should have 2 children"))

		mockSumNode.On("AggType").Return(brokerCom.Avg).Once()
		avgNode.Add(&mockCountNode)
		_, err = avgNode.Execute(context.TODO())
		Ω(err.Error()).Should(ContainSubstring("LHS of avg node must be sum node"))

		mockSumNode.On("AggType").Return(brokerCom.Sum).Once()
		mockCountNode.On("AggType").Return(brokerCom.Sum).Once()
		_, err = avgNode.Execute(context.TODO())
		Ω(err.Error()).Should(ContainSubstring("RHS of avg node must be count node"))

		mockBlockingNode := mocks.BlockingPlanNode{}
		mockBlockingNode.On("Execute", mock.Anything).Return(nil, errors.New("some error"))
		sumNode := NewMergeNode(brokerCom.Sum)
		sumNode.Add(&mockBlockingNode)
		_, err = sumNode.Execute(context.TODO())
		Ω(err.Error()).Should(ContainSubstring("errors happened executing merge node"))
	})

	ginkgo.It("NewAggQueryPlan should work", func() {
		q := queryCom.AQLQuery{
			Table: "table1",
			Measures: []queryCom.Measure{
				{Expr: "count(*)", ExprParsed: &expr.Call{Name: "count"}},
			},
		}
		qc := QueryContext{
			AQLQuery: &q,
			Tables: []*memCom.TableSchema{
				{
					Schema: metaCom.Table{
						IsFactTable: true,
					},
				},
			},
		}
		mockTopo := topoMock.HealthTrackingDynamicTopoloy{}
		mockMap := topoMock.Map{}
		mockShardSet := shardMock.ShardSet{}
		mockTopo.On("Get").Return(&mockMap).Once()
		mockMap.On("ShardSet").Return(&mockShardSet)
		mockShardIds := []uint32{0, 1, 2, 3, 4, 5}
		mockShardSet.On("AllIDs").Return(mockShardIds)
		mockHost1 := &topoMock.Host{}
		mockHost2 := &topoMock.Host{}
		mockHost3 := &topoMock.Host{}
		mockHosts := []topology.Host{
			mockHost1,
			mockHost2,
			mockHost3,
		}
		mockMap.On("Hosts").Return(mockHosts)
		//host1: 0,1,2,3
		//host2: 4,5,0,1
		//host3: 2,3,4,5
		mockMap.On("RouteShard", uint32(0)).Return([]topology.Host{mockHost1, mockHost2}, nil)
		mockMap.On("RouteShard", uint32(1)).Return([]topology.Host{mockHost1, mockHost2}, nil)
		mockMap.On("RouteShard", uint32(2)).Return([]topology.Host{mockHost1, mockHost3}, nil)
		mockMap.On("RouteShard", uint32(3)).Return([]topology.Host{mockHost1, mockHost3}, nil)
		mockMap.On("RouteShard", uint32(4)).Return([]topology.Host{mockHost2, mockHost3}, nil)
		mockMap.On("RouteShard", uint32(5)).Return([]topology.Host{mockHost2, mockHost3}, nil)

		mockDatanodeCli := dataCliMock.DataNodeQueryClient{}

		plan, err := NewAggQueryPlan(&qc, &mockTopo, &mockDatanodeCli)
		Ω(err).Should(BeNil())
		mn, ok := plan.root.(*mergeNodeImpl)
		Ω(ok).Should(BeTrue())
		Ω(mn.aggType).Should(Equal(brokerCom.Count))
		Ω(mn.children).Should(HaveLen(len(mockHosts)))
		sn1, ok := mn.children[0].(*BlockingScanNode)
		Ω(ok).Should(BeTrue())
		Ω(sn1.qc.AQLQuery.Shards).Should(HaveLen(2))
		sn2, ok := mn.children[1].(*BlockingScanNode)
		Ω(ok).Should(BeTrue())
		Ω(sn2.qc.AQLQuery.Shards).Should(HaveLen(2))
	})

	ginkgo.It("NewAggQueryPlan should work for avg query", func() {
		q := queryCom.AQLQuery{
			Table: "table1",
			Measures: []queryCom.Measure{
				{Expr: "avg(*)", ExprParsed: &expr.Call{Name: "avg"}},
			},
		}
		qc := QueryContext{
			AQLQuery: &q,
			Tables: []*memCom.TableSchema{
				{
					Schema: metaCom.Table{
						IsFactTable: true,
					},
				},
			},
		}
		mockTopo := topoMock.HealthTrackingDynamicTopoloy{}
		mockMap := topoMock.Map{}
		mockShardSet := shardMock.ShardSet{}
		mockTopo.On("Get").Return(&mockMap)
		mockMap.On("ShardSet").Return(&mockShardSet)
		mockShardIds := []uint32{0, 1, 2, 3, 4, 5}
		mockShardSet.On("AllIDs").Return(mockShardIds)
		mockHost1 := &topoMock.Host{}
		mockHost2 := &topoMock.Host{}
		mockHost3 := &topoMock.Host{}
		mockHosts := []topology.Host{
			mockHost1,
			mockHost2,
			mockHost3,
		}
		mockMap.On("Hosts").Return(mockHosts)
		//host1: 0,1,2,3
		//host2: 4,5,0,1
		//host3: 2,3,4,5
		mockMap.On("RouteShard", uint32(0)).Return([]topology.Host{mockHost1, mockHost2}, nil)
		mockMap.On("RouteShard", uint32(1)).Return([]topology.Host{mockHost1, mockHost2}, nil)
		mockMap.On("RouteShard", uint32(2)).Return([]topology.Host{mockHost1, mockHost3}, nil)
		mockMap.On("RouteShard", uint32(3)).Return([]topology.Host{mockHost1, mockHost3}, nil)
		mockMap.On("RouteShard", uint32(4)).Return([]topology.Host{mockHost2, mockHost3}, nil)
		mockMap.On("RouteShard", uint32(5)).Return([]topology.Host{mockHost2, mockHost3}, nil)

		mockDatanodeCli := dataCliMock.DataNodeQueryClient{}

		plan, err := NewAggQueryPlan(&qc, &mockTopo, &mockDatanodeCli)
		Ω(err).Should(BeNil())
		mn, ok := plan.root.(*mergeNodeImpl)
		Ω(ok).Should(BeTrue())
		Ω(mn.aggType).Should(Equal(brokerCom.Avg))
		Ω(mn.children).Should(HaveLen(2))
		sumn, ok := mn.children[0].(*mergeNodeImpl)
		Ω(ok).Should(BeTrue())
		Ω(sumn.aggType).Should(Equal(brokerCom.Sum))
		Ω(sumn.children).Should(HaveLen(len(mockHosts)))
		countn, ok := mn.children[1].(*mergeNodeImpl)
		Ω(ok).Should(BeTrue())
		Ω(countn.aggType).Should(Equal(brokerCom.Count))
		Ω(countn.children).Should(HaveLen(len(mockHosts)))
	})

	ginkgo.It("NewAggQueryPlan should work for dimension table", func() {
		q := queryCom.AQLQuery{
			Table: "table1",
			Measures: []queryCom.Measure{
				{Expr: "count(*)", ExprParsed: &expr.Call{Name: "count"}},
			},
		}
		qc := QueryContext{
			AQLQuery: &q,
			Tables: []*memCom.TableSchema{
				{
					Schema: metaCom.Table{
						IsFactTable: false,
					},
				},
			},
		}
		mockTopo := topoMock.HealthTrackingDynamicTopoloy{}
		mockMap := topoMock.Map{}
		mockTopo.On("Get").Return(&mockMap)
		mockHost1 := &topoMock.Host{}
		mockHost2 := &topoMock.Host{}
		mockHost3 := &topoMock.Host{}
		mockHosts := []topology.Host{
			mockHost1,
			mockHost2,
			mockHost3,
		}
		mockMap.On("Hosts").Return(mockHosts)
		//host1: 0,1,2,3
		//host2: 4,5,0,1
		//host3: 2,3,4,5
		mockMap.On("RouteShard", uint32(0)).Return([]topology.Host{mockHost1, mockHost2}, nil)
		mockMap.On("RouteShard", uint32(1)).Return([]topology.Host{mockHost1, mockHost2}, nil)
		mockMap.On("RouteShard", uint32(2)).Return([]topology.Host{mockHost1, mockHost3}, nil)
		mockMap.On("RouteShard", uint32(3)).Return([]topology.Host{mockHost1, mockHost3}, nil)
		mockMap.On("RouteShard", uint32(4)).Return([]topology.Host{mockHost2, mockHost3}, nil)
		mockMap.On("RouteShard", uint32(5)).Return([]topology.Host{mockHost2, mockHost3}, nil)

		mockDatanodeCli := dataCliMock.DataNodeQueryClient{}

		plan, err := NewAggQueryPlan(&qc, &mockTopo, &mockDatanodeCli)
		Ω(err).Should(BeNil())
		mn, ok := plan.root.(*mergeNodeImpl)
		Ω(ok).Should(BeTrue())
		Ω(mn.children).Should(HaveLen(1))
		sn, ok := mn.children[0].(*BlockingScanNode)
		Ω(ok).Should(BeTrue())
		Ω(sn.qc.AQLQuery.Shards).Should(Equal([]int{0}))
	})

	ginkgo.It("BlockingScanNode Execute should work happy path", func() {
		q := queryCom.AQLQuery{
			Measures: []queryCom.Measure{{ExprParsed: &expr.Call{Name: "count"}}},
		}

		mockTopo := topoMock.HealthTrackingDynamicTopoloy{}
		mockHost1 := topoMock.Host{}
		mockTopo.On("MarkHostHealthy", &mockHost1).Return(nil).Once()
		mockDatanodeCli := dataCliMock.DataNodeQueryClient{}
		myResult := queryCom.AQLQueryResult{"foo": 1}
		mockDatanodeCli.On("Query", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(myResult, nil)

		sn := BlockingScanNode{
			qc:             QueryContext{AQLQuery: &q},
			dataNodeClient: &mockDatanodeCli,
			host:           &mockHost1,
			topo:           &mockTopo,
		}

		res, err := sn.Execute(context.TODO())
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal(myResult))
	})

	ginkgo.It("BlockingScanNode Execute hll should work happy path", func() {
		q := queryCom.AQLQuery{
			Measures: []queryCom.Measure{{ExprParsed: &expr.Call{Name: "hll"}}},
		}

		mockTopo := topoMock.HealthTrackingDynamicTopoloy{}
		mockTopo.On("MarkHostHealthy").Return(nil).Once()
		mockHost1 := topoMock.Host{}
		mockTopo.On("MarkHostHealthy", &mockHost1).Return(nil).Once()
		mockDatanodeCli := dataCliMock.DataNodeQueryClient{}

		myResult := queryCom.AQLQueryResult{"foo": 1}
		mockDatanodeCli.On("Query", mock.Anything, mock.Anything, mock.Anything, mock.Anything, true).Return(myResult, nil)

		sn := BlockingScanNode{
			qc:             QueryContext{AQLQuery: &q},
			dataNodeClient: &mockDatanodeCli,
			host:           &mockHost1,
			topo:           &mockTopo,
		}

		res, err := sn.Execute(context.TODO())
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal(myResult))
	})

	ginkgo.It("BlockingScanNode Execute should fail datanode error", func() {
		q := queryCom.AQLQuery{
			Measures: []queryCom.Measure{{ExprParsed: &expr.Call{Name: "count"}}},
		}

		mockTopo := topoMock.HealthTrackingDynamicTopoloy{}
		mockHost1 := topoMock.Host{}
		mockTopo.On("MarkHostHealthy", &mockHost1).Return(nil).Once()

		mockDatanodeCli := dataCliMock.DataNodeQueryClient{}

		mockDatanodeCli.On("Query", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("rpc error")).Times(rpcRetries)

		sn := BlockingScanNode{
			qc:             QueryContext{AQLQuery: &q},
			host:           &mockHost1,
			dataNodeClient: &mockDatanodeCli,
			topo:           &mockTopo,
		}

		_, err := sn.Execute(context.TODO())
		Ω(err).ShouldNot(BeNil())
		Ω(err.Error()).Should(ContainSubstring("fetch from datanode failed"))
	})

	ginkgo.It("BlockingScanNode Execute should mark host unhealthy on datanode connection error", func() {
		q := queryCom.AQLQuery{
			Measures: []queryCom.Measure{{ExprParsed: &expr.Call{Name: "count"}}},
		}

		mockTopo := topoMock.HealthTrackingDynamicTopoloy{}
		mockHost1 := topoMock.Host{}
		mockTopo.On("MarkHostUnhealthy", &mockHost1).Return(nil).Once()

		mockDatanodeCli := dataCliMock.DataNodeQueryClient{}

		mockDatanodeCli.On("Query", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, client.ErrFailedToConnect).Times(rpcRetries)

		sn := BlockingScanNode{
			qc:             QueryContext{AQLQuery: &q},
			host:           &mockHost1,
			dataNodeClient: &mockDatanodeCli,
			topo:           &mockTopo,
		}

		_, err := sn.Execute(context.TODO())
		Ω(err).ShouldNot(BeNil())
		Ω(err.Error()).Should(ContainSubstring("fetch from datanode failed"))
	})

	ginkgo.It("BlockingScanNode Execute should work after retry", func() {
		q := queryCom.AQLQuery{
			Measures: []queryCom.Measure{{ExprParsed: &expr.Call{Name: "count"}}},
		}

		mockTopo := topoMock.HealthTrackingDynamicTopoloy{}
		mockHost1 := topoMock.Host{}
		mockTopo.On("MarkHostHealthy", &mockHost1).Return(nil).Once()

		mockDatanodeCli := dataCliMock.DataNodeQueryClient{}

		mockDatanodeCli.On("Query", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("rpc error")).Once()
		myResult := queryCom.AQLQueryResult{"foo": 1}
		mockDatanodeCli.On("Query", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(myResult, nil).Once()

		sn := BlockingScanNode{
			qc:             QueryContext{AQLQuery: &q},
			dataNodeClient: &mockDatanodeCli,
			host:           &mockHost1,
			topo:           &mockTopo,
		}

		res, err := sn.Execute(context.TODO())
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal(myResult))
	})

	ginkgo.It("post process hll should work", func() {
		table1 := &metaCom.Table{
			Name: "table1",
			Columns: []metaCom.Column{
				{Name: "field1", Type: "Uint32"},
				{Name: "field2", Type: "SmallEnum"},
				{Name: "field3", Type: "Uint16"},
				{Name: "field4", Type: "Uint32"},
			},
		}
		tableSchema1 := memCom.NewTableSchema(table1)
		tableSchema1.CreateEnumDict("field2", []string{"c", "d"})

		hllResult := queryCom.AQLQueryResult{
			"NULL": map[string]interface{}{
				"NULL": map[string]interface{}{
					"514": queryCom.HLL{NonZeroRegisters: 4, SparseData: []queryCom.HLLRegister{{Index: 255, Rho: 1}, {Index: 254, Rho: 2}, {Index: 253, Rho: 3}, {Index: 252, Rho: 4}}},
					"2": queryCom.HLL{NonZeroRegisters: 3,
						SparseData: []queryCom.HLLRegister{{Index: 1, Rho: 255}, {Index: 2, Rho: 254}, {Index: 3, Rho: 253}},
					},
					"NULL": queryCom.HLL{NonZeroRegisters: 3,
						SparseData: []queryCom.HLLRegister{{Index: 1, Rho: 255}, {Index: 2, Rho: 254}, {Index: 3, Rho: 253}},
					},
				}},
			"1": map[string]interface{}{
				"c": map[string]interface{}{
					"514": queryCom.HLL{NonZeroRegisters: 4, SparseData: []queryCom.HLLRegister{{Index: 255, Rho: 1}, {Index: 254, Rho: 2}, {Index: 253, Rho: 3}, {Index: 252, Rho: 4}}},
					"2": queryCom.HLL{NonZeroRegisters: 3,
						SparseData: []queryCom.HLLRegister{{Index: 1, Rho: 255}, {Index: 2, Rho: 254}, {Index: 3, Rho: 253}},
					},
					"NULL": queryCom.HLL{NonZeroRegisters: 3,
						SparseData: []queryCom.HLLRegister{{Index: 1, Rho: 255}, {Index: 2, Rho: 254}, {Index: 3, Rho: 253}},
					},
				},
			},
			"4294967295": map[string]interface{}{
				"d": map[string]interface{}{
					"514": queryCom.HLL{NonZeroRegisters: 4, SparseData: []queryCom.HLLRegister{{Index: 255, Rho: 1}, {Index: 254, Rho: 2}, {Index: 253, Rho: 3}, {Index: 252, Rho: 4}}},
					"2": queryCom.HLL{NonZeroRegisters: 3,
						SparseData: []queryCom.HLLRegister{{Index: 1, Rho: 255}, {Index: 2, Rho: 254}, {Index: 3, Rho: 253}},
					},
					"NULL": queryCom.HLL{NonZeroRegisters: 3,
						SparseData: []queryCom.HLLRegister{{Index: 1, Rho: 255}, {Index: 2, Rho: 254}, {Index: 3, Rho: 253}},
					},
				},
			}}

		mockTableSchemaReader := memComMocks.TableSchemaReader{}
		mockTableSchemaReader.On("RLock").Return(nil)
		mockTableSchemaReader.On("RUnlock").Return(nil)
		mockTableSchemaReader.On("GetSchema", "table1").Return(tableSchema1, nil)

		mockBlockingPlanNode := mocks.BlockingPlanNode{}
		mockBlockingPlanNode.On("Execute", mock.Anything).Return(hllResult, nil).Once()

		q := queryCom.AQLQuery{
			Table: "table1",
			Dimensions: []queryCom.Dimension{
				{Expr: "(field1)"},
				{Expr: "(field2)"},
				{Expr: "(field3)"},
			},
			Measures: []queryCom.Measure{
				{Expr: "(hll(field4))"},
			},
		}
		w := httptest.NewRecorder()
		qc := NewQueryContext(&q, true, w)
		qc.Compile(&mockTableSchemaReader)
		Ω(qc.Error).Should(BeNil())

		plan := AggQueryPlan{
			aggType: brokerCom.Hll,
			qc:      qc,
			root:    &mockBlockingPlanNode,
		}

		err := plan.Execute(context.TODO(), w)
		Ω(err).Should(BeNil())
		Ω(w.Header().Get(utils.HTTPContentTypeHeaderKey)).Should(Equal(utils.HTTPContentTypeHyperLogLog))
		var bs []byte
		bs, err = ioutil.ReadAll(w.Result().Body)
		Ω(err).Should(BeNil())

		var (
			qResults []queryCom.AQLQueryResult
			qErrors  []error
		)
		qResults, qErrors, err = queryCom.ParseHLLQueryResults(bs, false)
		Ω(err).Should(BeNil())
		Ω(qErrors).Should(HaveLen(1))
		Ω(qErrors[0]).Should(BeNil())
		Ω(qResults).Should(HaveLen(1))
		Ω(qResults[0]).Should(Equal(hllResult))
		Ω(reflect.DeepEqual(qResults[0], hllResult)).Should(BeTrue())

		qResults, qErrors, err = queryCom.ParseHLLQueryResults(bs, true)
		Ω(err).Should(BeNil())
		Ω(qErrors).Should(HaveLen(1))
		Ω(qErrors[0]).Should(BeNil())
		Ω(qResults).Should(HaveLen(1))
		Ω(qResults[0]).Should(Equal(queryCom.AQLQueryResult{
			"NULL": map[string]interface{}{
				"NULL": map[string]interface{}{
					"NULL": queryCom.HLL{NonZeroRegisters: 3,
						SparseData: []queryCom.HLLRegister{{Index: 1, Rho: 255}, {Index: 2, Rho: 254}, {Index: 3, Rho: 253}},
					},
					"2": queryCom.HLL{NonZeroRegisters: 3,
						SparseData: []queryCom.HLLRegister{{Index: 1, Rho: 255}, {Index: 2, Rho: 254}, {Index: 3, Rho: 253}},
					},
					"514": queryCom.HLL{NonZeroRegisters: 4, SparseData: []queryCom.HLLRegister{{Index: 255, Rho: 1}, {Index: 254, Rho: 2}, {Index: 253, Rho: 3}, {Index: 252, Rho: 4}}},
				}},
			"1": map[string]interface{}{
				"0": map[string]interface{}{
					"NULL": queryCom.HLL{NonZeroRegisters: 3,
						SparseData: []queryCom.HLLRegister{{Index: 1, Rho: 255}, {Index: 2, Rho: 254}, {Index: 3, Rho: 253}},
					},
					"2": queryCom.HLL{NonZeroRegisters: 3,
						SparseData: []queryCom.HLLRegister{{Index: 1, Rho: 255}, {Index: 2, Rho: 254}, {Index: 3, Rho: 253}},
					},
					"514": queryCom.HLL{NonZeroRegisters: 4, SparseData: []queryCom.HLLRegister{{Index: 255, Rho: 1}, {Index: 254, Rho: 2}, {Index: 253, Rho: 3}, {Index: 252, Rho: 4}}},
				},
			},
			"4294967295": map[string]interface{}{
				"1": map[string]interface{}{
					"NULL": queryCom.HLL{NonZeroRegisters: 3,
						SparseData: []queryCom.HLLRegister{{Index: 1, Rho: 255}, {Index: 2, Rho: 254}, {Index: 3, Rho: 253}},
					},
					"2": queryCom.HLL{NonZeroRegisters: 3,
						SparseData: []queryCom.HLLRegister{{Index: 1, Rho: 255}, {Index: 2, Rho: 254}, {Index: 3, Rho: 253}},
					},
					"514": queryCom.HLL{NonZeroRegisters: 4, SparseData: []queryCom.HLLRegister{{Index: 255, Rho: 1}, {Index: 254, Rho: 2}, {Index: 253, Rho: 3}, {Index: 252, Rho: 4}}},
				},
			}}))

		// empty results
		mockBlockingPlanNode.On("Execute", mock.Anything).Return(queryCom.AQLQueryResult{}, nil).Once()
		w = httptest.NewRecorder()
		err = plan.Execute(context.TODO(), w)
		Ω(err).Should(BeNil())
		Ω(w.Header().Get(utils.HTTPContentTypeHeaderKey)).Should(Equal(utils.HTTPContentTypeHyperLogLog))
		bs, err = ioutil.ReadAll(w.Result().Body)
		Ω(err).Should(BeNil())

		qResults, qErrors, err = queryCom.ParseHLLQueryResults(bs, false)
		Ω(err).Should(BeNil())
		Ω(qErrors).Should(HaveLen(1))
		Ω(qErrors[0]).Should(BeNil())
		Ω(qResults).Should(HaveLen(1))
		Ω(qResults[0]).Should(Equal(queryCom.AQLQueryResult{}))
	})

	ginkgo.It("translate enums should work", func() {
		plan := AggQueryPlan{
			qc: &QueryContext{
				DimensionEnumReverseDicts: map[int][]string{
					0: {"foo", "bar"},
					3: {"baz", "boy"},
				},
			},
		}

		result := queryCom.AQLQueryResult{
			"0": map[string]interface{}{
				"d1.0": map[string]interface{}{
					"d2.0": map[string]interface{}{
						"1":    "1.2",
						"NULL": "2.1",
					},
				},
			},
		}
		res, err := plan.translateEnum(result)
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal(map[string]interface{}{
			"foo": map[string]interface{}{
				"d1.0": map[string]interface{}{
					"d2.0": map[string]interface{}{
						"boy":  "1.2",
						"NULL": "2.1",
					},
				},
			},
		}))
	})

	ginkgo.It("cancel query on context cancel", func() {
		ctx, cf := context.WithCancel(context.Background())
		cf()

		q := queryCom.AQLQuery{
			Measures: []queryCom.Measure{{ExprParsed: &expr.Call{Name: "count"}}},
		}

		mockTopo := topoMock.HealthTrackingDynamicTopoloy{}
		mockHost1 := topoMock.Host{}
		mockTopo.On("MarkHostHealthy", &mockHost1).Return(nil).Once()
		mockDatanodeCli := dataCliMock.DataNodeQueryClient{}
		myResult := queryCom.AQLQueryResult{"foo": 1}
		mockDatanodeCli.On("Query", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(myResult, nil)

		sn := BlockingScanNode{
			qc:             QueryContext{AQLQuery: &q},
			dataNodeClient: &mockDatanodeCli,
			host:           &mockHost1,
			topo:           &mockTopo,
		}

		_, err := sn.Execute(ctx)
		Ω(err.Error()).Should(ContainSubstring("BlockingScanNode execution canceled"))
	})
})
