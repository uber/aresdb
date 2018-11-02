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

package diskstore

import (
	"fmt"

	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = ginkgo.Describe("DiskStoreUtils", func() {
	var path string
	ginkgo.It("Test General Utils", func() {
		path = getPathForTableShard("/path/to/store", "myTable", 1)
		Ω(path).Should(Equal("/path/to/store/data/myTable_1"))
		path = getPathForTableShard("/path/to/store/", "myTable", 1)
		Ω(path).Should(Equal("/path/to/store/data/myTable_1"))
		path = getPathForTableShard("", "myTable", 1)
		Ω(path).Should(Equal("data/myTable_1"))
	})

	ginkgo.It("Test Redolog Utils", func() {
		path = GetPathForTableRedologs("/path/to/store/", "myTable", 1)
		Ω(path).Should(Equal(fmt.Sprintf("/path/to/store/data/myTable_1/redologs")))
		path = GetPathForRedologFile("/path/to/store/", "myTable", 1, 1500496811)
		Ω(path).Should(Equal(fmt.Sprintf("/path/to/store/data/myTable_1/redologs/1500496811.redolog")))
	})

	ginkgo.It("Test Snapshot Utils", func() {
		path = GetPathForTableSnapshotDir("/path/to/store/", "myTable", 1)
		Ω(path).Should(Equal(fmt.Sprintf("/path/to/store/data/myTable_1/snapshots")))
		path = GetPathForTableSnapshotDirPath("/path/to/store/", "myTable", 1, 12345, 123)
		Ω(path).Should(Equal(fmt.Sprintf("/path/to/store/data/myTable_1/snapshots/12345_123")))
	})

	ginkgo.It("Test Archive batch Utils", func() {
		path = GetPathForTableArchiveBatchRootDir("/path/to/store/", "myTable", 1)
		Ω(path).Should(Equal(fmt.Sprintf("/path/to/store/data/myTable_1/archiving_batches")))
		path = GetPathForTableArchiveBatchDir("/path/to/store/", "myTable", 1, "2017-07-19", 1499970253, 0)
		Ω(path).Should(Equal(fmt.Sprintf("/path/to/store/data/myTable_1/archiving_batches/2017-07-19_1499970253")))
		path = GetPathForTableArchiveBatchColumnFile("/path/to/store/", "myTable", 1, "2017-07-19", 1499970253, 0, 100)
		Ω(path).Should(Equal(fmt.Sprintf("/path/to/store/data/myTable_1/archiving_batches/2017-07-19_1499970253/100.data")))
	})

	ginkgo.It("Test ParseBatchIDAndVersionName Utils", func() {
		// make sure it's backward compatible: seq number does not exist
		batchID, batchVersion, seqNum, err := ParseBatchIDAndVersionName("2017-07-19_1499970253")
		Ω(batchID).Should(Equal("2017-07-19"))
		Ω(batchVersion).Should(Equal(uint32(1499970253)))
		Ω(seqNum).Should(Equal(uint32(0)))
		Ω(err).Should(BeNil())
		// seq num exists
		batchID, batchVersion, seqNum, err = ParseBatchIDAndVersionName("2017-07-19_1499970253-3")
		Ω(batchID).Should(Equal("2017-07-19"))
		Ω(batchVersion).Should(Equal(uint32(1499970253)))
		Ω(seqNum).Should(Equal(uint32(3)))
		Ω(err).Should(BeNil())
		batchID, batchVersion, seqNum, err = ParseBatchIDAndVersionName("2017-07-191499970253")
		Ω(err).ShouldNot(BeNil())
		batchID, batchVersion, seqNum, err = ParseBatchIDAndVersionName("2017-07-19_xxx")
		Ω(err).ShouldNot(BeNil())
	})

})
