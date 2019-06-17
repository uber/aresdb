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
	shardMock "github.com/uber/aresdb/cluster/shard/mocks"
	"github.com/uber/aresdb/cluster/topology"
	topoMock "github.com/uber/aresdb/cluster/topology/mocks"
	dataCliMock "github.com/uber/aresdb/datanode/client/mocks"
	"github.com/uber/aresdb/query"
	"github.com/uber/aresdb/query/common"
	"net/http/httptest"
)

var _ = ginkgo.Describe("non agg query plan", func() {
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
		mockHost1 := topoMock.Host{}
		mockHost2 := topoMock.Host{}
		mockMap.On("RouteShard", uint32(0)).Return([]topology.Host{&mockHost1, &mockHost2}, nil)
		mockMap.On("RouteShard", uint32(1)).Return([]topology.Host{&mockHost1, &mockHost2}, nil)
		mockMap.On("RouteShard", uint32(2)).Return([]topology.Host{&mockHost1, &mockHost2}, nil)

		mockDatanodeCli := dataCliMock.DataNodeQueryClient{}

		w := httptest.NewRecorder()
		plan := NewNonAggQueryPlan(&qc, &mockTopo, &mockDatanodeCli, w)

		Ω(plan.nodes).Should(HaveLen(len(mockShardIds)))
		Ω(plan.headers).Should(Equal([]string{"field1", "field2"}))

		bs := []byte(`["foo", "1"],["bar", "2"']`)
		mockDatanodeCli.On("QueryRaw", mock.Anything, mock.Anything, mock.Anything).Return(bs, nil).Times(len(mockShardIds))

		err := plan.Execute(context.TODO())
		Ω(err).Should(BeNil())

		Ω(w.Body.String()).Should(Equal(`{"headers":["field1","field2"],"matrixData":[["foo", "1"],["bar", "2"'],["foo", "1"],["bar", "2"'],["foo", "1"],["bar", "2"']]}`))
	})
})
