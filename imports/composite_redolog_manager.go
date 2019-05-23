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

package imports

import (
	"encoding/json"
	"errors"
	"github.com/Shopify/sarama"
	"github.com/uber/aresdb/memstore/common"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/utils"
	"sync"
)

// CompositeRedologManager is the class to take data ingestion from all data source (kafka, http, etc.), write to local redolog when necessary,
// and then call storeFunc to store data
type CompositeRedologManager struct {
	sync.RWMutex `json:"-"`
	// table name
	Table string `json:"table"`
	// table shard id
	Shard int `json:"shard"`
	// pointer back to the factory
	factory *RedologManagerFactory
	// Local file redolog manager
	fileRedoLogManager *FileRedologManager
	// Kafka consumer if kafka import is supported
	kafkaReader *kafkaPartitionReader
	// ready channel is used to indicate if recovery is finished
	readyChan chan bool
	// function to store received upsertbatch
	storeFunc BatchStoreFunc

	// convenient variables
	recoveryDone       bool
	redoFilePersisited int64
	offsetPersisted    uint32
	// flag to indicate the ingestion from kafka is dead
	done bool
	// message counts
	msgRecovered int64
	msgReceived  int64
	msgError     int64
}

// NewCompositeRedologManager create CompositeRedologManager oibject
func NewCompositeRedologManager(table string, shard int, factory *RedologManagerFactory, tableConfig *metaCom.TableConfig, storeFunc BatchStoreFunc) *CompositeRedologManager {
	var fileRedoLogManager *FileRedologManager
	if factory == nil || factory.enableLocalRedoLog {
		fileRedoLogManager = NewFileRedoLogManager(int64(tableConfig.RedoLogRotationInterval), int64(tableConfig.MaxRedoLogFileSize), factory.diskStore, table, shard)
	}
	var kafkaReader *kafkaPartitionReader

	if factory != nil && factory.consumer != nil {
		commitFunc := func(offset int64) error {
			return factory.metaStore.UpdateIngestionCommitOffset(table, shard, offset)
		}
		checkPointFunc := func(offset int64) error {
			return factory.metaStore.UpdateIngestionCheckpointOffset(table, shard, offset)
		}
		kafkaReader = NewKafkaPartitionReader(factory.namespace, table, shard, factory.consumer, commitFunc, checkPointFunc)
	}
	manager := &CompositeRedologManager{
		Table:              table,
		Shard:              shard,
		factory:            factory,
		fileRedoLogManager: fileRedoLogManager,
		kafkaReader:        kafkaReader,
		readyChan:          make(chan bool, 1),
		storeFunc:          storeFunc,
	}

	return manager
}

// convenient function for backward test
func (s *CompositeRedologManager) GetLocalFileRedologManager() *FileRedologManager {
	return s.fileRedoLogManager
}

// convenient function for backward test
func (s *CompositeRedologManager) GetKafkaReader() *kafkaPartitionReader {
	return s.kafkaReader
}

// Start start the recovery and data ingestion routine
func (s *CompositeRedologManager) Start(redoLogFilePersisted int64, offsetPersisted uint32) error {
	s.redoFilePersisited = redoLogFilePersisted
	s.offsetPersisted = offsetPersisted

	go func() {
		// first read from local redolog file for recovery if it is enabled
		if s.fileRedoLogManager != nil {
			utils.GetLogger().With("action", "ingestion", "table", s.Table, "shard", s.Shard).
				Info("Start recovery from local redolog")
			fn := s.fileRedoLogManager.NextUpsertBatch()
			for {
				batch, redoLogFile, batchOffset := fn()
				if batch == nil {
					break
				} else {
					if err := s.applyUpsertBatchOnly(batch, redoLogFile, batchOffset); err != nil {
						utils.GetLogger().With("err", err).Panic("Failed to apply upsert batch during recovery")
					}
				}
			}
			s.setRecoveryDone()
			utils.GetLogger().With("action", "ingestion", "table", s.Table, "shard", s.Shard,
				"batchRecovered", s.msgRecovered).Info("Finished recovery from local redolog")
		}
		// next read from kafka if kafka input is enabled
		if s.kafkaReader != nil {
			var offsetFrom, offsetTo int64
			var err error
			if s.fileRedoLogManager != nil {
				offsetFrom, err = s.factory.metaStore.GetIngestionCommitOffset(s.Table, s.Shard)
				offsetTo = offsetFrom
			} else {
				offsetFrom, err = s.factory.metaStore.GetIngestionCheckpointOffset(s.Table, s.Shard)
				if err != nil {
					utils.GetLogger().Fatal(err)
				}
				offsetTo, err = s.factory.metaStore.GetIngestionCommitOffset(s.Table, s.Shard)
			}

			if err != nil {
				utils.GetLogger().Fatal(err)
			}
			if offsetFrom == 0 {
				offsetFrom = sarama.OffsetNewest
			}
			if offsetTo < offsetFrom {
				offsetTo = offsetFrom
			}

			utils.GetLogger().With("action", "ingestion", "table", s.Table, "shard", s.Shard,
				"offset", offsetFrom).Info("Start read from kafka")

			err = s.kafkaReader.ConsumeFrom(offsetFrom)
			if err != nil {
				utils.GetLogger().Fatal(err)
			}

			fn := s.kafkaReader.NextUpsertBatch()
			for {
				batch, kafkaOffset := fn()
				if batch == nil {
					break
				}

				if !s.recoveryDone && kafkaOffset >= offsetTo {
					s.setRecoveryDone()
					utils.GetLogger().With("action", "ingestion", "table", s.Table, "shard", s.Shard,
						"offset", offsetTo, "batchRecovered", s.msgRecovered).Info("Recovery from kafka done, start normal ingestion")
				}
				if s.recoveryDone {
					err = s.applyUpsertBatchWithLogging(batch, kafkaOffset)
				} else {
					redoLogFile, batchOffset := s.kafkaReader.getFileOffset(kafkaOffset)
					err = s.applyUpsertBatchOnly(batch, redoLogFile, batchOffset)
				}

				if err != nil {
					s.msgError++
					utils.GetLogger().With("action", "ingestion", "table", s.Table, "shard", s.Shard,
						"offset", kafkaOffset).Error("Error save upsertbatch to store")
					utils.GetReporter(s.Table, s.Shard).GetCounter(utils.IngestedErrorBatches).Inc(1)
				}
			}
			utils.GetLogger().With("action", "ingestion", "table", s.Table, "shard", s.Shard).Warn("Kafka ingestion stopped")
		}
		s.done = true
	}()

	return nil
}

func (s *CompositeRedologManager) setRecoveryDone() {
	// no locking here as this will always be sequential read/write
	s.recoveryDone = true
	s.readyChan <- true
}

// applyUpsertBatchOnly is used for replay redolog, so no redolog write necessary
func (s *CompositeRedologManager) applyUpsertBatchOnly(batch *common.UpsertBatch, redoLogFile int64, batchOffset uint32) error {

	utils.GetReporter(s.Table, s.Shard).GetCounter(utils.IngestedUpsertBatches).Inc(1)
	utils.GetReporter(s.Table, s.Shard).GetGauge(utils.UpsertBatchSize).Update(float64(len(batch.GetBuffer())))

	s.Lock()
	defer s.Unlock()

	s.msgRecovered++
	skipBackfillRows := redoLogFile < s.redoFilePersisited ||
		(redoLogFile == s.redoFilePersisited && batchOffset <= s.offsetPersisted)

	s.UpdateMaxEventTime(0, redoLogFile)
	if s.storeFunc != nil {
		return s.storeFunc(batch, redoLogFile, batchOffset, skipBackfillRows)
	}
	return nil
}

// applyUpsertBatchWithLogging is used for normal ingestion (kafka or http)
func (s *CompositeRedologManager) applyUpsertBatchWithLogging(batch *common.UpsertBatch, offsetInSource int64) error {

	utils.GetReporter(s.Table, s.Shard).GetCounter(utils.IngestedUpsertBatches).Inc(1)
	utils.GetReporter(s.Table, s.Shard).GetGauge(utils.UpsertBatchSize).Update(float64(len(batch.GetBuffer())))

	s.Lock()
	defer s.Unlock()

	s.msgReceived++
	redoLogFile, batchOffset := s.AppendToRedoLog(batch, offsetInSource)
	if s.storeFunc != nil {
		return s.storeFunc(batch, redoLogFile, batchOffset, false)
	}
	return nil
}

// WaitForRecoveryDone block call to wait for recovery finish
func (s *CompositeRedologManager) WaitForRecoveryDone() {
	select {
	case <-s.readyChan:
		break
	}

	// report redolog size after replay
	utils.GetReporter(s.Table, s.Shard).GetGauge(utils.NumberOfRedologs).Update(float64(s.GetNumFiles()))
	utils.GetReporter(s.Table, s.Shard).GetGauge(utils.SizeOfRedologs).Update(float64(s.GetTotalSize()))
}

// WriteUpsertBatch append upsertbatch to redolog and save to storage
func (s *CompositeRedologManager) WriteUpsertBatch(batch *common.UpsertBatch) error {
	if s.fileRedoLogManager != nil {
		return s.applyUpsertBatchWithLogging(batch, -1)
	} else {
		return errors.New("Local file redolog not configured")
	}
}

// AppendToRedoLog append upsert batch into redolog file or commit offset
func (s *CompositeRedologManager) AppendToRedoLog(upsertBatch *common.UpsertBatch, offsetInSource int64) (int64, uint32) {
	if s.fileRedoLogManager != nil {
		if s.kafkaReader != nil && offsetInSource >= 0 {
			// when save redolog in file and ingestion is from kafka, need to remember kafka offset separately
			s.kafkaReader.Commit(offsetInSource)
		}
		return s.fileRedoLogManager.AppendToRedoLog(upsertBatch, offsetInSource)
	} else {
		return s.kafkaReader.AppendToRedoLog(upsertBatch, offsetInSource)
	}
}

// UpdateMaxEventTime update max event time for related redolog file
func (s *CompositeRedologManager) UpdateMaxEventTime(eventTime uint32, redoFile int64) {
	if s.fileRedoLogManager != nil {
		s.fileRedoLogManager.UpdateMaxEventTime(eventTime, redoFile)
	} else {
		s.kafkaReader.UpdateMaxEventTime(eventTime, redoFile)
	}
}

// CheckpointRedolog clean up obsolete redolog files and save checkpoint offset
func (s *CompositeRedologManager) CheckpointRedolog(cutoff uint32, redoFileCheckpointed int64, batchOffset uint32) error {
	if s.fileRedoLogManager != nil {
		return s.fileRedoLogManager.CheckpointRedolog(cutoff, redoFileCheckpointed, batchOffset)
	} else {
		return s.kafkaReader.CheckpointRedolog(cutoff, redoFileCheckpointed, batchOffset)
	}
}

func (s *CompositeRedologManager) GetTotalSize() int {
	if s.fileRedoLogManager != nil {
		return s.fileRedoLogManager.GetTotalSize()
	} else {
		return s.kafkaReader.GetTotalSize()
	}
}

func (s *CompositeRedologManager) IsDone() bool {
	return s.done
}

func (s *CompositeRedologManager) GetNumFiles() int {
	if s.fileRedoLogManager != nil {
		return s.fileRedoLogManager.GetNumFiles()
	} else {
		return s.kafkaReader.GetNumFiles()
	}
}

func (s *CompositeRedologManager) Close() {
	s.Lock()
	defer s.Unlock()

	if s.kafkaReader != nil {
		s.kafkaReader.Close()
		s.kafkaReader = nil
	}

	if s.fileRedoLogManager != nil {
		s.fileRedoLogManager.Close()
		s.fileRedoLogManager = nil
	}

	// remove from factory
	s.factory.Close(s.Table, s.Shard)
}

// MarshalJSON marshals a fileRedologManager into json.
func (s *CompositeRedologManager) MarshalJSON() ([]byte, error) {
	// Avoid json.Marshal loop calls.
	type alias CompositeRedologManager
	s.RLock()
	defer s.RUnlock()
	return json.Marshal((*alias)(s))
}
