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
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
	"github.com/uber-go/tally"
	shardMock "github.com/uber/aresdb/cluster/shard/mocks"
	"github.com/uber/aresdb/cluster/topology"
	topoMock "github.com/uber/aresdb/cluster/topology/mocks"
	common2 "github.com/uber/aresdb/common"
	dataCliMock "github.com/uber/aresdb/datanode/client/mocks"
	"github.com/uber/aresdb/query/common"
	"github.com/uber/aresdb/utils"
	"net/http/httptest"
)

var _ = ginkgo.Describe("non agg query plan", func() {
	utils.Init(common2.AresServerConfig{}, common2.NewLoggerFactory().GetDefaultLogger(), common2.NewLoggerFactory().GetDefaultLogger(), tally.NewTestScope("test", nil))

	ginkgo.It("should work happy path", func() {

		q := common.AQLQuery{
			Table: "table1",
			Measures: []common.Measure{
				{Expr: "1"},
			},
			Dimensions: []common.Dimension{
				{Expr: "field1"},
				{Expr: "field2"},
			},
			Limit: -1,
		}
		qc := QueryContext{
			AQLQuery:              &q,
			IsNonAggregationQuery: true,
		}
		mockTopo := topoMock.Topology{}
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

		// test negative limit (no limit)
		w := httptest.NewRecorder()
		plan, err := NewNonAggQueryPlan(&qc, &mockTopo, &mockDatanodeCli)
		Ω(err).Should(BeNil())

		Ω(plan.nodes).Should(HaveLen(len(mockHosts)))
		Ω(plan.headers).Should(Equal([]string{"field1", "field2"}))

		bs := []byte(`["foo","1"],["bar","2"]`)
		mockDatanodeCli.On("QueryRaw", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(bs, nil).Times(len(mockShardIds))

		Ω(plan.nodes[0].qc.AQLQuery.Shards).Should(HaveLen(2))
		Ω(plan.nodes[1].qc.AQLQuery.Shards).Should(HaveLen(2))

		err = plan.Execute(context.TODO(), w)
		Ω(err).Should(BeNil())

		Ω(w.Body.String()).Should(Equal(`{"headers":["field1","field2"],"matrixData":[["foo","1"],["bar","2"],["foo","1"],["bar","2"],["foo","1"],["bar","2"]]}`))

		// test limit no enough data
		qc.AQLQuery.Limit = 3
		w = httptest.NewRecorder()
		plan, err = NewNonAggQueryPlan(&qc, &mockTopo, &mockDatanodeCli)
		Ω(err).Should(BeNil())
		mockDatanodeCli.On("QueryRaw", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(bs, nil).Times(len(mockShardIds))
		err = plan.Execute(context.TODO())

		bsEmpty := []byte(``)
		mockDatanodeCli.On("QueryRaw", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(bs, nil).Once()
		mockDatanodeCli.On("QueryRaw", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(bsEmpty, nil).Times(len(mockHosts) - 1)
		err = plan.Execute(context.TODO(), w)
		Ω(err).Should(BeNil())
		Ω(w.Body.String()).Should(Equal(`{"headers":["field1","field2"],"matrixData":[["foo","1"],["bar","2"]]}`))

		// test limit with enough data 1
		qc.AQLQuery.Limit = 3
		w = httptest.NewRecorder()
		plan, err = NewNonAggQueryPlan(&qc, &mockTopo, &mockDatanodeCli)
		Ω(err).Should(BeNil())

		mockDatanodeCli.On("QueryRaw", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(bsEmpty, nil).Times(len(mockHosts) - 2)
		mockDatanodeCli.On("QueryRaw", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(bs, nil).Times(2)
		err = plan.Execute(context.TODO(), w)
		Ω(err).Should(BeNil())
		Ω(w.Body.String()).Should(Equal(`{"headers":["field1","field2"],"matrixData":[["foo","1"],["bar","2"],["foo","1"]]}`))

		// test limit with enough data 2
		w = httptest.NewRecorder()
		plan, err = NewNonAggQueryPlan(&qc, &mockTopo, &mockDatanodeCli)
		Ω(err).Should(BeNil())

		mockDatanodeCli.On("QueryRaw", mock.Anything, mock.Anything, mock.Anything).Return(bs, nil).Times(2)
		mockDatanodeCli.On("QueryRaw", mock.Anything, mock.Anything, mock.Anything).Return(bsEmpty, nil).Times(len(mockHosts) - 2)
		err = plan.Execute(context.TODO(), w)
		Ω(err).Should(BeNil())
		Ω(w.Body.String()).Should(Equal(`{"headers":["field1","field2"],"matrixData":[["foo","1"],["bar","2"],["foo","1"]]}`))
	})
})
