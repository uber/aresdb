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

package memstore

import (
	"github.com/onsi/ginkgo"
	"github.com/stretchr/testify/mock"
	"github.com/uber/aresdb/cluster/topology"
	"github.com/uber/aresdb/memstore/common"

	. "github.com/onsi/gomega"
	metaMocks "github.com/uber/aresdb/metastore/mocks"
	"time"
)

var _ = ginkgo.Describe("batch stats should work", func() {
	metaStore := &metaMocks.MetaStore{}
	memStore := createMemStore("abc", 0, []common.DataType{common.Uint32, common.Uint8}, []int{0}, 10, true, false, metaStore, CreateMockDiskStore())

	batchStatsReporter := NewBatchStatsReporter(1, memStore, topology.NewStaticShardOwner([]int{0}))

	ginkgo.It("batch stats report should work", func() {
		metaStore.On("GetOwnedShards", mock.Anything).Return([]int{0}, nil)
		metaStore.On("GetArchiveBatchVersion", mock.Anything, 0, mock.Anything, mock.Anything).Return(uint32(0), uint32(0), 0, nil)

		builder := common.NewUpsertBatchBuilder()
		// Put event time to the 2nd column.
		builder.AddColumn(1, common.Uint8)
		builder.AddColumn(0, common.Uint32)
		builder.AddRow()
		builder.SetValue(0, 1, uint32(23456))
		builder.SetValue(0, 0, uint8(123))
		buffer, _ := builder.ToByteArray()
		upsertBatch, _ := common.NewUpsertBatch(buffer)
		shard, err := memStore.GetTableShard("abc", 0)

		err = memStore.HandleIngestion("abc", 0, upsertBatch)
		Ω(err).Should(BeNil())
		Ω(shard.LiveStore.lastModifiedTimePerColumn).Should(Equal([]uint32{23456, 23456}))

		Ω(shard.LiveStore.LastReadRecord.BatchID).Should(Equal(BaseBatchID))

		go batchStatsReporter.Run()
		time.Sleep(time.Second * 2)
		batchStatsReporter.Stop()
	})
})
