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
	"sync"

	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber/aresdb/diskstore/mocks"
	"github.com/uber/aresdb/memstore/common"
	metaCom "github.com/uber/aresdb/metastore/common"
)

var _ = ginkgo.Describe("table Shard", func() {
	ginkgo.It("deletes data for a column upon request", func() {
		// Prepare schema and Shard
		diskStore := &mocks.DiskStore{}
		schema := NewTableSchema(&metaCom.Table{
			Name:                 "trips",
			IsFactTable:          true,
			PrimaryKeyColumns:    []int{1},
			ArchivingSortColumns: []int{3},
			Columns: []metaCom.Column{
				{
					Name: "request_at",
					Type: "Uint32",
				},
				{
					Name: "uuid",
					Type: "UUID",
				},
				{
					Name:    "ubercabid",
					Type:    "int8",
					Deleted: true,
				},
				{
					Name: "city_id",
					Type: "uint16",
				},
			},
		})

		for columnID := range schema.Schema.Columns {
			schema.SetDefaultValue(columnID)
		}

		shard := NewTableShard(schema, nil, diskStore,
			NewHostMemoryManager(getFactory().NewMockMemStore(), 1<<32), 0)

		// Prepare live store
		shard.LiveStore.AdvanceNextWriteRecord()
		var batch *LiveBatch
		for _, b := range shard.LiveStore.Batches {
			batch = b
			break
		}
		batch.GetOrCreateVectorParty(2, false)
		Ω(batch.Columns[2]).ShouldNot(BeNil())
		shard.LiveStore.AdvanceLastReadRecord()

		// Prepare archive store
		aBatch := &ArchiveBatch{
			Batch: Batch{RWMutex: &sync.RWMutex{}},
			Version: 1600,
			BatchID: 100,
			Shard:   shard,
		}
		aBatch.Columns = []common.VectorParty{nil, nil, &archiveVectorParty{allUsersDone: sync.NewCond(aBatch)}}
		shard.ArchiveStore.CurrentVersion.Batches[100] = aBatch

		// Test!
		diskStore.On("DeleteColumn", "trips", 2, 0).Return(nil)
		err := shard.DeleteColumn(2)
		Ω(err).Should(BeNil())

		Ω(batch.Columns[2]).Should(BeNil())
		Ω(aBatch.Columns[2]).Should(BeNil())

		err = shard.DeleteColumn(3)
		Ω(err).Should(BeNil())
	})
})
