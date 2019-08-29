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

package util

import (
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
	shardMock "github.com/uber/aresdb/cluster/shard/mocks"
	"github.com/uber/aresdb/cluster/topology"
	topoMock "github.com/uber/aresdb/cluster/topology/mocks"
)

var _ = ginkgo.Describe("broker util", func() {
	ginkgo.It("should work happy path", func() {
		mockTopo := topoMock.Topology{}
		mockMap := topoMock.Map{}
		mockShardSet := shardMock.ShardSet{}
		mockTopo.On("Get").Return(&mockMap)
		mockMap.On("ShardSet").Return(&mockShardSet)
		mockShardIds := []uint32{0, 1, 2, 3, 4, 5, 6, 7}
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
		//host1: 0,1,2,3,6
		//host2: 4,5,0,1,7
		//host3: 2,3,4,5,6
		mockMap.On("RouteShard", uint32(0)).Return([]topology.Host{mockHost1, mockHost2}, nil)
		mockMap.On("RouteShard", uint32(1)).Return([]topology.Host{mockHost1, mockHost2}, nil)
		mockMap.On("RouteShard", uint32(2)).Return([]topology.Host{mockHost1, mockHost3}, nil)
		mockMap.On("RouteShard", uint32(3)).Return([]topology.Host{mockHost1, mockHost3}, nil)
		mockMap.On("RouteShard", uint32(4)).Return([]topology.Host{mockHost2, mockHost3}, nil)
		mockMap.On("RouteShard", uint32(5)).Return([]topology.Host{mockHost2, mockHost3}, nil)
		mockMap.On("RouteShard", uint32(6)).Return([]topology.Host{mockHost1, mockHost3}, nil)
		mockMap.On("RouteShard", uint32(7)).Return([]topology.Host{mockHost2}, nil)

		res, err := CalculateShardAssignment(&mockTopo)
		Ω(err).Should(BeNil())
		Ω(res[mockHost1]).Should(HaveLen(3))
		Ω(res[mockHost2]).Should(HaveLen(3))
		Ω(res[mockHost3]).Should(HaveLen(2))
	})

	ginkgo.It("should work no available host", func() {
		mockTopo := topoMock.Topology{}
		mockMap := topoMock.Map{}
		mockShardSet := shardMock.ShardSet{}
		mockTopo.On("Get").Return(&mockMap)
		mockMap.On("ShardSet").Return(&mockShardSet)
		mockShardIds := []uint32{0, 1, 2, 3, 4, 5, 6, 7}
		mockShardSet.On("AllIDs").Return(mockShardIds)
		mockMap.On("Hosts").Return([]topology.Host{})
		mockMap.On("RouteShard", mock.Anything).Return([]topology.Host{}, nil)

		_, err := CalculateShardAssignment(&mockTopo)
		Ω(err.Error()).Should(ContainSubstring("failed to assign host for shard"))
	})
})
