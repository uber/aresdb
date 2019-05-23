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

package common

import (
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"math/rand"
	"testing"
)

var _ = ginkgo.Describe("CuckooIndex", func() {
	ginkgo.It("Should Contains the New Key, RecordID On Insert A New Key RecordID with TTL in the future", func() {
		hashIndex := newCuckooIndex(4, true, 10, manager)
		hashIndex.rand = rand.New(rand.NewSource(int64(0)))
		hashIndex.UpdateEventTimeCutoff(0)
		key := Key{'a', 'b', 'c', 'd'}
		recordID := RecordID{
			BatchID: 1,
			Index:   1,
		}

		hashIndex.FindOrInsert(key, recordID, 1)
		found, v, err := hashIndex.FindOrInsert(key, recordID, 1)
		Ω(found).Should(BeTrue())
		Ω(err).Should(BeNil())
		Ω(v).Should(Equal(recordID))
		hashIndex.Destruct()
	})

	ginkgo.It("Should Not Contains the New Key, RecordID On Insert A New Key RecordID with TTL in the past", func() {
		hashIndex := newCuckooIndex(4, true, 10, manager)
		hashIndex.rand = rand.New(rand.NewSource(int64(0)))
		hashIndex.UpdateEventTimeCutoff(1)
		key := Key{'a', 'b', 'c', 'd'}
		value := RecordID{
			BatchID: 1,
			Index:   1,
		}

		_, _, err := hashIndex.FindOrInsert(key, value, 0)
		Ω(err).ShouldNot(BeNil())
		hashIndex.Destruct()
	})

	ginkgo.It("Should Return Existing RecordID On Insert A Existing Key", func() {
		hashIndex := newCuckooIndex(4, true, 10, manager)
		hashIndex.rand = rand.New(rand.NewSource(int64(0)))
		key := Key{'a', 'b', 'c', 'd'}
		value1 := RecordID{
			BatchID: 1,
			Index:   1,
		}
		value2 := RecordID{
			BatchID: 1,
			Index:   2,
		}
		hashIndex.FindOrInsert(key, value1, 1)
		found, v, err := hashIndex.FindOrInsert(key, value2, 1)
		Ω(found).Should(BeTrue())
		Ω(v).Should(Equal(value1))
		Ω(err).Should(BeNil())
		hashIndex.Destruct()
	})

	ginkgo.It("Should make key deleted On deleting existing key", func() {
		hashIndex := newCuckooIndex(4, true, 10, manager)
		hashIndex.rand = rand.New(rand.NewSource(int64(0)))
		key := Key{'a', 'b', 'c', 'd'}
		value := RecordID{
			BatchID: 1,
			Index:   1,
		}

		hashIndex.FindOrInsert(key, value, 1)
		found, _, err := hashIndex.FindOrInsert(key, value, 1)
		Ω(found).Should(BeTrue())
		Ω(err).Should(BeNil())

		hashIndex.Delete(key)
		found, _, err = hashIndex.FindOrInsert(key, value, 1)
		Ω(found).Should(BeFalse())
		Ω(err).Should(BeNil())
		hashIndex.Destruct()
	})

	ginkgo.It("Should Insert all keys On Inserting more than the initial capacity", func() {
		hashIndex := newCuckooIndex(4, true, 10, manager)
		hashIndex.rand = rand.New(rand.NewSource(int64(0)))
		numberOfInsersion := 1000

		for i := 0; i < numberOfInsersion; i++ {
			key := make(Key, 4, 4)
			key[0], key[1], key[2], key[3] = byte(i), byte(i>>8), byte(i>>16), byte(i>>24)
			value := RecordID{
				BatchID: 0,
				Index:   uint32(i),
			}
			found, v, err := hashIndex.FindOrInsert(key, value, 1)
			Ω(found).Should(BeFalse())
			Ω(v).Should(Equal(value))
			Ω(err).Should(BeNil())
		}
		Ω(hashIndex.Size()).Should(Equal(uint(numberOfInsersion)))

		for i := 0; i < numberOfInsersion; i++ {
			key := make(Key, 4, 4)
			key[0], key[1], key[2], key[3] = byte(i), byte(i>>8), byte(i>>16), byte(i>>24)
			value := RecordID{
				BatchID: 0,
				Index:   uint32(i),
			}

			found, v, err := hashIndex.FindOrInsert(key, value, 1)
			Ω(found).Should(BeTrue())
			Ω(v).Should(Equal(value))
			Ω(err).Should(BeNil())
		}

		Ω(hashIndex.Size()).Should(Equal(uint(numberOfInsersion)))
		hashIndex.Destruct()
	})

	ginkgo.It("Should insert all keys on inserting more than the initial capacity with no event time", func() {
		hashIndex := newCuckooIndex(4, false, 10, manager)
		hashIndex.rand = rand.New(rand.NewSource(int64(0)))
		numberOfInsersion := 1000

		for i := 0; i < numberOfInsersion; i++ {
			key := make(Key, 4, 4)
			key[0], key[1], key[2], key[3] = byte(i), byte(i>>8), byte(i>>16), byte(i>>24)
			value := RecordID{
				BatchID: 0,
				Index:   uint32(i),
			}
			found, v, err := hashIndex.FindOrInsert(key, value, 1)
			Ω(found).Should(BeFalse())
			Ω(v).Should(Equal(value))
			Ω(err).Should(BeNil())
		}
		Ω(hashIndex.Size()).Should(Equal(uint(numberOfInsersion)))

		for i := 0; i < numberOfInsersion; i++ {
			key := make(Key, 4, 4)
			key[0], key[1], key[2], key[3] = byte(i), byte(i>>8), byte(i>>16), byte(i>>24)
			value := RecordID{
				BatchID: 0,
				Index:   uint32(i),
			}

			found, v, err := hashIndex.FindOrInsert(key, value, 1)
			Ω(found).Should(BeTrue())
			Ω(v).Should(Equal(value))
			Ω(err).Should(BeNil())
		}

		Ω(hashIndex.Size()).Should(Equal(uint(numberOfInsersion)))
		hashIndex.Destruct()
	})

	ginkgo.It("Should Insert, Expire, Insert, Delete, Find", func() {
		hashIndex := newCuckooIndex(4, true, 10, manager)
		hashIndex.rand = rand.New(rand.NewSource(int64(0)))
		numberOfInsersion := 2000

		// insert the first half
		for i := 0; i < numberOfInsersion/2; i++ {
			key := make(Key, 4, 4)
			key[0], key[1], key[2], key[3] = byte(i), byte(i>>8), byte(i>>16), byte(i>>24)
			value := RecordID{
				BatchID: 0,
				Index:   uint32(i),
			}
			found, _, err := hashIndex.FindOrInsert(key, value, 1)
			Ω(found).Should(BeFalse())
			Ω(err).Should(BeNil())
		}
		Ω(hashIndex.Size()).Should(Equal(uint(numberOfInsersion / 2)))

		// expire all old inserts
		hashIndex.UpdateEventTimeCutoff(2)

		// insert the second half
		for i := numberOfInsersion / 2; i < numberOfInsersion; i++ {
			key := make(Key, 4, 4)
			key[0], key[1], key[2], key[3] = byte(i), byte(i>>8), byte(i>>16), byte(i>>24)
			value := RecordID{
				BatchID: 0,
				Index:   uint32(i),
			}
			found, _, err := hashIndex.FindOrInsert(key, value, 2)
			Ω(found).Should(BeFalse())
			Ω(err).Should(BeNil())
		}

		// the first half should be expired and not found
		for i := 0; i < numberOfInsersion/2; i++ {
			key := make(Key, 4, 4)
			key[0], key[1], key[2], key[3] = byte(i), byte(i>>8), byte(i>>16), byte(i>>24)
			value := RecordID{
				BatchID: 0,
				Index:   uint32(i),
			}

			found, _, err := hashIndex.FindOrInsert(key, value, 2)
			Ω(found).Should(BeFalse())
			Ω(err).Should(BeNil())
		}

		// the second half should be found
		for i := numberOfInsersion / 2; i < numberOfInsersion; i++ {
			key := make(Key, 4, 4)
			key[0], key[1], key[2], key[3] = byte(i), byte(i>>8), byte(i>>16), byte(i>>24)
			value := RecordID{
				BatchID: 0,
				Index:   uint32(i),
			}
			found, v, err := hashIndex.FindOrInsert(key, value, 2)
			Ω(found).Should(BeTrue())
			Ω(v).Should(Equal(value))
			Ω(err).Should(BeNil())
		}

		// delete all keys
		for i := 0; i < numberOfInsersion; i++ {
			key := make(Key, 4, 4)
			key[0], key[1], key[2], key[3] = byte(i), byte(i>>8), byte(i>>16), byte(i>>24)
			hashIndex.Delete(key)
		}

		for i := 0; i < numberOfInsersion; i++ {
			key := make(Key, 4, 4)
			key[0], key[1], key[2], key[3] = byte(i), byte(i>>8), byte(i>>16), byte(i>>24)
			found, _, err := hashIndex.FindOrInsert(key, RecordID{}, 2)
			Ω(found).Should(BeFalse())
			Ω(err).Should(BeNil())
		}

		hashIndex.Destruct()
	})

	ginkgo.It("Should Insert, Expire, Insert, Delete, Find with initial bucket size of 1", func() {
		hashIndex := newCuckooIndex(4, true, 1, manager)
		hashIndex.rand = rand.New(rand.NewSource(int64(1)))
		numberOfInsersion := 20

		// insert the first half
		for i := 0; i < numberOfInsersion; i++ {
			key := make(Key, 4, 4)
			key[0], key[1], key[2], key[3] = byte(i), byte(i>>8), byte(i>>16), byte(i>>24)
			value := RecordID{
				BatchID: 0,
				Index:   uint32(i),
			}
			found, _, err := hashIndex.FindOrInsert(key, value, 1)
			Ω(found).Should(BeFalse())
			Ω(err).Should(BeNil())
		}
		Ω(hashIndex.Size()).Should(Equal(uint(numberOfInsersion)))

		//all items should be found
		for i := 0; i < numberOfInsersion; i++ {
			key := make(Key, 4, 4)
			key[0], key[1], key[2], key[3] = byte(i), byte(i>>8), byte(i>>16), byte(i>>24)
			value := RecordID{
				BatchID: 0,
				Index:   uint32(i),
			}
			found, v, err := hashIndex.FindOrInsert(key, value, 2)
			Ω(found).Should(BeTrue())
			Ω(v).Should(Equal(value))
			Ω(err).Should(BeNil())
		}

		hashIndex.Destruct()
	})

	ginkgo.It("Should Insert, Expire, Insert, Delete, Find with initial bucket size of 1 with no event time", func() {
		hashIndex := newCuckooIndex(4, false, 1, manager)
		hashIndex.rand = rand.New(rand.NewSource(int64(1)))
		numberOfInsersion := 20

		// insert the first half
		for i := 0; i < numberOfInsersion; i++ {
			key := make(Key, 4, 4)
			key[0], key[1], key[2], key[3] = byte(i), byte(i>>8), byte(i>>16), byte(i>>24)
			value := RecordID{
				BatchID: 0,
				Index:   uint32(i),
			}
			found, _, err := hashIndex.FindOrInsert(key, value, 1)
			Ω(found).Should(BeFalse())
			Ω(err).Should(BeNil())
		}
		Ω(hashIndex.Size()).Should(Equal(uint(numberOfInsersion)))

		//all items should be found
		for i := 0; i < numberOfInsersion; i++ {
			key := make(Key, 4, 4)
			key[0], key[1], key[2], key[3] = byte(i), byte(i>>8), byte(i>>16), byte(i>>24)
			value := RecordID{
				BatchID: 0,
				Index:   uint32(i),
			}
			found, v, err := hashIndex.FindOrInsert(key, value, 2)
			Ω(found).Should(BeTrue())
			Ω(v).Should(Equal(value))
			Ω(err).Should(BeNil())
		}

		hashIndex.Destruct()
	})

	ginkgo.It("Should find the existing record with key", func() {
		hashIndex := newCuckooIndex(4, true, 10, manager)
		hashIndex.rand = rand.New(rand.NewSource(int64(0)))
		hashIndex.UpdateEventTimeCutoff(0)
		key := Key{'a', 'b', 'c', 'd'}
		recordID := RecordID{
			BatchID: 1,
			Index:   1,
		}

		r, f := hashIndex.Find(key)
		Ω(f).Should(BeFalse())

		hashIndex.FindOrInsert(key, recordID, 1)

		r, f = hashIndex.Find(key)
		Ω(f).Should(BeTrue())
		Ω(r).Should(Equal(recordID))

		hashIndex.UpdateEventTimeCutoff(2)

		r, f = hashIndex.Find(key)
		Ω(f).Should(BeFalse())

		hashIndex.Destruct()
	})

	ginkgo.It("Update should work on existing key and return false for missing key", func() {
		hashIndex := newCuckooIndex(4, false, 10, manager)
		hashIndex.rand = rand.New(rand.NewSource(int64(0)))
		key := Key{'a', 'b', 'c', 'd'}
		value := RecordID{
			BatchID: 1,
			Index:   1,
		}
		newValue := RecordID{
			BatchID: 1,
			Index:   2,
		}

		hashIndex.FindOrInsert(key, value, 1)
		Ω(hashIndex.Update(key, newValue)).Should(BeTrue())
		Ω(hashIndex.Update(Key{'a', 'b', 'c', 'c'}, newValue)).Should(BeFalse())
		v, found := hashIndex.Find(key)
		Ω(found).Should(BeTrue())
		Ω(v).Should(Equal(newValue))
		hashIndex.Destruct()
	})

	ginkgo.It("Should work on UUID as primary key", func() {
		hashIndex := newCuckooIndex(16, false, 2, manager)
		hashIndex.rand = rand.New(rand.NewSource(int64(0)))
		hashIndex.UpdateEventTimeCutoff(0)

		nUUID := 3
		for i := 0; i < nUUID; i++ {
			var keyBytes [16]byte
			keyBytes[0] = byte(i)
			key := Key(keyBytes[:])
			recordID := RecordID{
				BatchID: 0,
				Index:   uint32(i),
			}

			hashIndex.FindOrInsert(key, recordID, 1)
			found, v, err := hashIndex.FindOrInsert(key, recordID, 1)
			Ω(found).Should(BeTrue())
			Ω(err).Should(BeNil())
			Ω(v).Should(Equal(recordID))
		}

		hashIndex.Destruct()
	})
})

// test purpose, need to move HostMemoryManager to some other place for mock purpose in future
type TestHostMemoryManager struct {
}

func (*TestHostMemoryManager) ReportUnmanagedSpaceUsageChange(bytes int64) {
}
func (*TestHostMemoryManager) ReportManagedObject(table string, shard, batchID, columnID int, bytes int64) {
}
func (*TestHostMemoryManager) GetArchiveMemoryUsageByTableShard() (map[string]map[string]*ColumnMemoryUsage, error) {
	return nil, nil
}
func (*TestHostMemoryManager) TriggerEviction() {
}
func (*TestHostMemoryManager) TriggerPreload(tableName string, columnID int, oldPreloadingDays int, newPreloadingDays int) {
}
func (*TestHostMemoryManager) Start() {
}
func (*TestHostMemoryManager) Stop() {
}

var (
	manager        = &TestHostMemoryManager{}
	benchIndex     = newCuckooIndex(4, true, 0, manager)
	benchTestValue = RecordID{
		BatchID: 0,
		Index:   1,
	}
	benchTestKey = make(Key, 4, 4)
)

func BenchmarkCuckooIndex_Insert(b *testing.B) {

	for i := 0; i < b.N; i++ {
		benchTestKey[0], benchTestKey[1], benchTestKey[2], benchTestKey[3] = byte(i), byte(i>>8), byte(i>>16), byte(i>>24)
		benchIndex.FindOrInsert(benchTestKey, benchTestValue, 1)
	}
}
