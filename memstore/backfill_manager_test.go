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
	"fmt"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
	memCom "github.com/uber/aresdb/memstore/common"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/metastore/mocks"
)

var _ = ginkgo.Describe("backfill manager", func(){

	table := "test"
	tableSchema := &memCom.TableSchema{
		Schema: metaCom.Table{
			Name: table,
			Config: metaCom.TableConfig{
				ArchivingDelayMinutes:    500,
				ArchivingIntervalMinutes: 300,
				BackfillStoreBatchSize:   20000,
				BackfillMaxBufferSize: 1,
			},
			IsFactTable:          true,
			ArchivingSortColumns: []int{1, 5},
			PrimaryKeyColumns:    []int{1, 2},
			Columns: []metaCom.Column{
				{Name: "c1", Deleted: false},
				{Name: "c2", Deleted: false}, // sort col, pk 1
				{Name: "c3", Deleted: false}, // pk 2
				{Name: "c4", Deleted: true},  // should skip this column.
				{Name: "c5", Deleted: false}, // unsort col
				{Name: "c6", Deleted: false}, // sort col, non pk
			},
		},
		PrimaryKeyBytes:   8,
		ValueTypeByColumn: []memCom.DataType{memCom.Uint32, memCom.Uint32, memCom.Uint32, memCom.Uint32, memCom.Uint32, memCom.Uint32},
		DefaultValues: []*memCom.DataValue{&memCom.NullDataValue, &memCom.NullDataValue,
			&memCom.NullDataValue, &memCom.NullDataValue, &memCom.NullDataValue, &memCom.NullDataValue},
	}
	upsertBatch, _ := getFactory().ReadUpsertBatch("backfill/upsertBatch0")
	numRows := upsertBatch.NumRows

	metaStoreMock := &mocks.MetaStore{}
	metaStoreMock.On("UpdateBackfillProgress", table, 0, mock.Anything, mock.Anything).Return(nil)
	ginkgo.It("backfill manager should work", func() {
		bm := NewBackfillManager(table, 0, tableSchema.Schema.Config)
		shouldLock := bm.Append(upsertBatch, 1, 10)
		// MaxBufferSize is one byte, so we will block client in such case
		Ω(shouldLock).Should(BeTrue())
		done := make(chan bool)
		// simulate waiting client
		go func() {
			bm.WaitForBackfillBufferAvailability()
			done <- true
			close(done)
		}()

		// simulate backfill start
		_, fileID, offset := bm.StartBackfill()
		Ω(fileID).Should(Equal(int64(1)))
		Ω(offset).Should(Equal(uint32(10)))
		// simulate backfill done
		err := bm.Done(fileID, offset, metaStoreMock)
		Ω(err).Should(BeNil())

		// client should be onblocked and done waiting after backfill advance
		clientDone := <-done
		Ω(clientDone).Should(BeTrue())
		bm.Destruct()
	})

	ginkgo.It("ReadUpsertBatch should work ", func() {
		bm := NewBackfillManager(table, 0, tableSchema.Schema.Config)
		_ = bm.Append(upsertBatch, 1, 10)
		data, columnNames, err := bm.ReadUpsertBatch(0, 0, numRows, tableSchema)
		Ω(err).Should(BeNil())
		Ω(fmt.Sprintf("%v", data)).Should(Equal("[[0 0 0 0] [1 0 1 11] [11 11 0 11]]"))
		Ω(columnNames).Should(Equal([]string{"c1", "c2", "c3", "c6"}))
		bm.Destruct()
	})

})
