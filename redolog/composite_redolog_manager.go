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

package redolog

import (
	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/uber/aresdb/diskstore"
	"github.com/uber/aresdb/memstore/common"
	metaCom "github.com/uber/aresdb/metastore/common"
	"sync"
)

// compositeRedologManager is the class to take data ingestion from all data source (kafka, http, etc.), write to local redolog when necessary,
// and then call storeFunc to store data
type compositeRedoLogManager struct {
	sync.RWMutex `json:"-"`
	// table name
	Table string `json:"table"`
	// table shard id
	Shard int `json:"shard"`
	// Local file redolog manager
	fileRedoLogManager *FileRedoLogManager
	// Kafka consumer if kafka import is supported
	kafkaRedoLogManager *kafkaRedoLogManager
}

// NewCompositeRedoLogManager create compositeRedoLogManager oibject
func newCompositeRedoLogManager(namespace, table string, shard int, tableConfig *metaCom.TableConfig,
	consumer sarama.Consumer, diskStore diskstore.DiskStore,
	commitFunc func(string, int, int64) error,
	checkPointFunc func(string, int, int64) error,
	getCommitOffsetFunc func(string, int) (int64, error),
	getCheckpointOffsetFunc func(string, int) (int64, error)) *compositeRedoLogManager {

	fileRedoLogManager := newFileRedoLogManager(int64(tableConfig.RedoLogRotationInterval), int64(tableConfig.MaxRedoLogFileSize), diskStore, table, shard)

	kafkaReader := newKafkaRedoLogManager(namespace, table, shard, consumer, false, commitFunc, checkPointFunc, getCommitOffsetFunc, getCheckpointOffsetFunc)

	manager := &compositeRedoLogManager{
		Table:               table,
		Shard:               shard,
		fileRedoLogManager:  fileRedoLogManager,
		kafkaRedoLogManager: kafkaReader,
	}

	return manager
}

// Iterator walk through redolog batch from both file and kafka
func (s *compositeRedoLogManager) Iterator() (NextUpsertFunc, error) {
	fileNext, err := s.fileRedoLogManager.Iterator()
	if err != nil {
		return nil, err
	}
	kafkaNext, _ := s.kafkaRedoLogManager.Iterator()
	if err != nil {
		return nil, err
	}

	return func() *NextUpsertBatchInfo {
		var res *NextUpsertBatchInfo
		if !s.fileRedoLogManager.recoveryDone {
			res = fileNext()
		}
		if res == nil {
			res = kafkaNext()
		}
		return res
	}, nil
}

// WaitForRecoveryDone block call to wait for recovery finish
func (s *compositeRedoLogManager) WaitForRecoveryDone() {
	s.fileRedoLogManager.WaitForRecoveryDone()
}

// AppendToRedoLog append upsert batch into redolog file or commit offset
func (s *compositeRedoLogManager) AppendToRedoLog(upsertBatch *common.UpsertBatch) (int64, uint32) {
	return s.fileRedoLogManager.AppendToRedoLog(upsertBatch)
}

// UpdateMaxEventTime update max event time for related redolog file
func (s *compositeRedoLogManager) UpdateMaxEventTime(eventTime uint32, redoFile int64) {
	s.fileRedoLogManager.UpdateMaxEventTime(eventTime, redoFile)
}

// CheckpointRedolog clean up obsolete redolog files and save checkpoint offset
func (s *compositeRedoLogManager) CheckpointRedolog(cutoff uint32, redoFileCheckpointed int64, batchOffset uint32) error {
	return s.fileRedoLogManager.CheckpointRedolog(cutoff, redoFileCheckpointed, batchOffset)
}

func (s *compositeRedoLogManager) GetTotalSize() int {
	return s.fileRedoLogManager.GetTotalSize()
}

func (s *compositeRedoLogManager) GetNumFiles() int {
	return s.fileRedoLogManager.GetNumFiles()
}

func (s *compositeRedoLogManager) GetBatchReceived() int {
	return s.kafkaRedoLogManager.batchReceived
}

func (s *compositeRedoLogManager) GetBatchRecovered() int {
	return s.fileRedoLogManager.batchRecovered
}

func (s *compositeRedoLogManager) Close() {
	s.Lock()
	defer s.Unlock()

	if s.kafkaRedoLogManager != nil {
		s.kafkaRedoLogManager.Close()
		s.kafkaRedoLogManager = nil
	}

	if s.fileRedoLogManager != nil {
		s.fileRedoLogManager.Close()
		s.fileRedoLogManager = nil
	}
}

// MarshalJSON marshals a fileRedologManager into json.
func (s *compositeRedoLogManager) MarshalJSON() ([]byte, error) {
	// Avoid json.Marshal loop calls.
	type alias compositeRedoLogManager
	s.RLock()
	defer s.RUnlock()
	return json.Marshal((*alias)(s))
}
