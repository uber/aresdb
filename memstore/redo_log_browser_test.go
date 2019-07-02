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
	"path/filepath"

	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber/aresdb/diskstore"
	"github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/metastore"
)

var _ = ginkgo.Describe("redo_log_browser", func() {
	rootPath := "../testing/data/integration/sample-ares-root"
	diskStore := diskstore.NewLocalDiskStore(rootPath)

	metaStorePath := filepath.Join(rootPath, "metastore")
	metaStore, _ := metastore.NewDiskMetaStore(metaStorePath)
	table := "abc"
	shardID := 0
	t, _ := metaStore.GetTable(table)
	m := GetFactory().NewMockMemStore()
	schema := common.NewTableSchema(t)

	shard := NewTableShard(schema, metaStore, diskStore, NewHostMemoryManager(m, 1<<32), shardID, m.options)

	var rb *redoLogBrowser

	var redoLogFile int64 = 1501869573

	ginkgo.BeforeEach(func() {
		rb = shard.NewRedoLogBrowser().(*redoLogBrowser)
	})

	ginkgo.It("NewRedoLogBrowser should work", func() {
		Ω(*rb).Should(Equal(redoLogBrowser{
			tableName: table,
			shardID:   shardID,
			schema:    schema,
			diskStore: diskStore}))
	})

	ginkgo.It("ListLogFiles should work", func() {
		redoLogs, err := rb.ListLogFiles()
		Ω(err).Should(BeNil())
		Ω(redoLogs).Should(ConsistOf(redoLogFile))
	})

	ginkgo.It("ListUpsertBatch should work", func() {
		offsets, err := rb.ListUpsertBatch(redoLogFile)
		Ω(err).Should(BeNil())
		Ω(offsets).Should(Equal([]int64{4}))
	})

	ginkgo.It("ReadData should work", func() {
		rows, columnNames, numRows, err := rb.ReadData(redoLogFile, 4, 0, 5)
		Ω(err).Should(BeNil())
		Ω(rows).Should(HaveLen(2))
		Ω(rows[0]).Should(ConsistOf(BeEquivalentTo(123), BeEquivalentTo(0)))
		Ω(rows[1]).Should(ConsistOf(BeEquivalentTo(234), BeEquivalentTo(1)))
		Ω(numRows).Should(Equal(2))
		Ω(columnNames).Should(ConsistOf("c1", "c2"))

		rows, columnNames, numRows, err = rb.ReadData(redoLogFile, 4, 1, 5)
		Ω(err).Should(BeNil())
		Ω(rows).Should(HaveLen(1))
		Ω(rows[0]).Should(ConsistOf(BeEquivalentTo(234), BeEquivalentTo(1)))
		Ω(numRows).Should(Equal(2))
		Ω(columnNames).Should(ConsistOf("c1", "c2"))

		rows, _, numRows, err = rb.ReadData(redoLogFile, 4, 2, 5)
		Ω(err).ShouldNot(BeNil())
		Ω(err.Error()).Should(ContainSubstring("Invalid start or length"))
		Ω(rows).Should(BeNil())
		Ω(numRows).Should(Equal(0))

		rows, _, numRows, err = rb.ReadData(redoLogFile, 12, 0, 1)
		Ω(err).ShouldNot(BeNil())
		Ω(err.Error()).Should(ContainSubstring("Failed to read upsert batch version number"))
		Ω(rows).Should(BeNil())
		Ω(numRows).Should(Equal(0))
	})
})
