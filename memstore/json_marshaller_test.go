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
	"encoding/json"

	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber/aresdb/memstore/common"
	metaCom "github.com/uber/aresdb/metastore/common"
	"sync"
	"time"
)

var _ = ginkgo.Describe("json marshaller", func() {

	m := getFactory().NewMockMemStore()
	hostMemoryManager := NewHostMemoryManager(m, 1<<32)

	liveBatch := LiveBatch{Batch: Batch{
		RWMutex: &sync.RWMutex{},
		Columns: make([]common.VectorParty, 10),
	}}

	liveStore := LiveStore{
		Batches: map[int32]*LiveBatch{
			int32(1): &liveBatch,
		},
		RedoLogManager: NewRedoLogManager(1, 1<<30, nil, "test", 1),
		BackfillManager: NewBackfillManager("ares_trips", 0, metaCom.TableConfig{
			BackfillMaxBufferSize:    1 << 32,
			BackfillThresholdInBytes: 1 << 21,
		}),
		PrimaryKey: NewPrimaryKey(4, true, 10, hostMemoryManager),
		BatchSize:  10,
		lastModifiedTimePerColumn: []uint32{
			1,
		},
	}

	archiveBatch := ArchiveBatch{
		Batch: Batch{
			RWMutex: &sync.RWMutex{},
			Columns: make([]common.VectorParty, 10),
		}}

	archiveStore := ArchiveStore{
		CurrentVersion: &ArchiveStoreVersion{
			Batches: map[int32]*ArchiveBatch{
				int32(1): &archiveBatch,
			},
		},
	}

	tableShard := TableShard{
		ArchiveStore: &ArchiveStore{
			CurrentVersion: &ArchiveStoreVersion{
				Batches: map[int32]*ArchiveBatch{
					int32(1): &archiveBatch,
				},
			},
		},
		LiveStore: &LiveStore{
			Batches: map[int32]*LiveBatch{
				int32(1): &liveBatch,
			},
			RedoLogManager: NewRedoLogManager(1, 1<<30, nil, "test", 1),
			BackfillManager: NewBackfillManager("ares_trips", 0, metaCom.TableConfig{
				BackfillMaxBufferSize:    1 << 32,
				BackfillThresholdInBytes: 1 << 21,
			}),
			PrimaryKey: NewPrimaryKey(4, true, 10, hostMemoryManager),
			lastModifiedTimePerColumn: []uint32{
				1,
			},
		},
	}

	snapshotManager := &SnapshotManager{
		shard:             &tableShard,
		SnapshotThreshold: 100,
		SnapshotInterval:  time.Duration(5) * time.Minute,
	}

	ginkgo.It("UnsortedBatch should work", func() {
		jsonStr, err := json.Marshal(&liveBatch)
		Ω(err).Should(BeNil())
		Ω(jsonStr).Should(MatchJSON(`{
			"numColumns": 10,
			"capacity": 0
		  }`,
		))
	})

	ginkgo.It("SnapshotManager should work", func() {
		jsonStr, err := json.Marshal(&snapshotManager)
		Ω(err).Should(BeNil())
		Ω(jsonStr).Should(MatchJSON(`{
			"numMutations": 0,
			"LastSnapshotTime": "0001-01-01T00:00:00Z",
			"lastRedoFile": 0,
			"lastBatchOffset": 0,
        	"LastRecord": {
          	  "batchID": 0,
          	  "index": 0
        	},
			"currentRedoFile": 0,
			"currentBatchOffset": 0,
        	"CurrentRecord": {
         	  "batchID": 0,
         	  "index": 0
        	},
			"snapshotInterval": 300000000000,
			"snapshotThreshold": 100
		  }`,
		))
	})

	ginkgo.It("LiveStore should work", func() {
		jsonStr, err := json.Marshal(&liveStore)
		Ω(err).Should(BeNil())
		Ω(jsonStr).Should(MatchJSON(`{
			"batchSize": 10,
			"batches": {
			  "1": {
				"capacity": 0,
				"numColumns": 10
			  }
			},
			"lastReadRecord": {
			  "batchID": 0,
			  "index": 0
			},
			 "lastModifiedTimePerColumn": [
					  1
					],
			"nextWriteRecord": {
			  "batchID": 0,
			  "index": 0
			},
			"primaryKey": {
			  "capacity": 80,
			  "eventTimeCutoff": 0,
			  "size": 0,
			  "allocatedBytes": 1360
			},
			"redoLogManager": {
			  "rotationInterval": 1,
			  "maxRedoLogSize": 1073741824,
			  "currentRedoLogSize": 0,
			  "maxEventTimePerFile": {},
			  "sizePerFile": {},
			  "totalRedologSize": 0,
			  "batchCountPerFile": {},
			  "currentFileCreationTime": 0
			},
			"backfillManager": {
              "currentBufferSize": 0,
              "backfillingBufferSize": 0,
              "maxBufferSize": 4294967296,
              "numUpsertBatches": 0,
              "numRecords": 0,
              "backfillThresholdInBytes": 2097152,
              "lastRedoFile": 0,
              "lastBatchOffset": 0,
              "currentRedoFile": 0,
              "currentBatchOffset": 0
            },
            "snapshotManager": null
		  }`,
		))
	})

	ginkgo.It("SortedBatch should work", func() {
		jsonStr, err := json.Marshal(&archiveBatch)
		Ω(err).Should(BeNil())
		Ω(jsonStr).Should(MatchJSON(`{
			"numColumns": 10,
			"size": 0,
			"version": 0
		  }
		`))
	})

	ginkgo.It("UnsortedBatch should work", func() {
		jsonStr, err := json.Marshal(&liveBatch)
		Ω(err).Should(BeNil())
		Ω(jsonStr).Should(MatchJSON(`{
			"numColumns": 10,
			"capacity": 0
		  }`,
		))
	})

	ginkgo.It("SortedVectorStore should work", func() {
		jsonStr, err := json.Marshal(&archiveStore)
		Ω(err).Should(BeNil())
		Ω(jsonStr).Should(MatchJSON(`{
			"currentVersion": {
			  "batches": {
				"1": {
				  "numColumns": 10,
				  "size": 0,
				  "version": 0
				}
			  },
			  "archivingCutoff": 0
			}
		  }
		`))
	})

	ginkgo.It("RedoLogManager should work", func() {
		jsonStr, err := json.Marshal(&liveStore.RedoLogManager)
		Ω(err).Should(BeNil())
		Ω(jsonStr).Should(MatchJSON(`{
			"rotationInterval": 1,
			"maxRedoLogSize": 1073741824,
			"totalRedologSize": 0,
			"sizePerFile": {},
			"currentRedoLogSize": 0,
			"maxEventTimePerFile": {},
			"batchCountPerFile": {},
			"currentFileCreationTime": 0
		  }`))
	})

	ginkgo.It("TableSchema should work", func() {
		jsonStr, err := json.Marshal(&TableSchema{})
		Ω(err).Should(BeNil())
		Ω(jsonStr).Should(MatchJSON(`{
			"schema": {
			  "name": "",
			  "columns": null,
			  "primaryKeyColumns": null,
			  "isFactTable": false,
			  "config": {},
			  "version": 0
			},
			"columnIDs": null,
			"enumDicts": null,
			"valueTypeByColumn": null,
			"primaryKeyBytes": 0,
			"primaryKeyColumnTypes": null
		  }`,
		))
	})

	ginkgo.It("TableShard should work", func() {
		jsonStr, err := json.Marshal(&tableShard)
		Ω(err).Should(BeNil())
		Ω(jsonStr).Should(MatchJSON(`{
        "schema": null,
        "liveStore": {
          "batchSize": 0,
          "batches": {
            "1": {
              "capacity": 0,
              "numColumns": 10
            }
          },
          "lastReadRecord": {
            "batchID": 0,
            "index": 0
          },
          "nextWriteRecord": {
            "batchID": 0,
            "index": 0
          },
          "primaryKey": {
            "capacity": 80,
            "eventTimeCutoff": 0,
            "size": 0,
            "allocatedBytes": 1360
          },
          "redoLogManager": {
            "rotationInterval": 1,
			"maxRedoLogSize": 1073741824,
			"totalRedologSize": 0,
			"currentRedoLogSize": 0,
			"maxEventTimePerFile": {},
			"sizePerFile": {},
            "batchCountPerFile": {},
            "currentFileCreationTime": 0
          },
          "backfillManager": {
            "currentBufferSize": 0,
            "backfillingBufferSize": 0,
            "maxBufferSize": 4294967296,
            "backfillThresholdInBytes": 2097152,
            "numUpsertBatches": 0,
            "numRecords": 0,
            "lastRedoFile": 0,
            "lastBatchOffset": 0,
            "currentRedoFile": 0,
            "currentBatchOffset": 0
          },
          "snapshotManager": null,
	       "lastModifiedTimePerColumn": [
             1
           ]
        },
        "archiveStore": {
          "currentVersion": {
            "batches": {
              "1": {
                "numColumns": 10,
                "size": 0,
                "version": 0
              }
            },
            "archivingCutoff": 0
          }
        }
      }`,
		))
	})
})
