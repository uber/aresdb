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
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/uber/aresdb/common"
	"github.com/uber/aresdb/diskstore"
	"github.com/uber/aresdb/metastore"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/utils"
	"sync"
)

// Master class to create shard level redolog manager
type RedoLogManagerMaster struct {
	sync.Mutex
	// redolog config
	RedoLogConfig *common.RedoLogConfig
	// kafka consuer if kafka consumer is configured
	consumer sarama.Consumer
	// DiskStore
	diskStore diskstore.DiskStore
	// Metastore
	metaStore metastore.MetaStore
	// save all table partition redolog managers
	managers map[string]map[int]RedologManager
}

// NewRedoLogManagerMaster create RedoLogManagerMaster instance
func NewRedoLogManagerMaster(c *common.RedoLogConfig, diskStore diskstore.DiskStore, metaStore metastore.MetaStore) (*RedoLogManagerMaster, error) {
	return NewKafkaRedoLogManagerMaster(c, diskStore, metaStore, nil)
}

// NewKafkaRedoLogManagerMaster convenient function if the kafka consumer can be passed in from outside
func NewKafkaRedoLogManagerMaster(cfg *common.RedoLogConfig, diskStore diskstore.DiskStore, metaStore metastore.MetaStore, consumer sarama.Consumer) (*RedoLogManagerMaster, error) {

	if cfg == nil {
		cfg = &common.RedoLogConfig{}
	}
	if cfg.KafkaConfig.Enabled {
		if consumer == nil {
			if len(cfg.KafkaConfig.Brokers) == 0 {
				return nil, fmt.Errorf("No kafka broker info configured")
			}
			var err error
			if consumer, err = sarama.NewConsumer(cfg.KafkaConfig.Brokers, sarama.NewConfig()); err != nil {
				return nil, err
			}
		}
	} else {
		consumer = nil
	}

	return &RedoLogManagerMaster{
		RedoLogConfig:      cfg,
		diskStore:          diskStore,
		managers:           make(map[string]map[int]RedologManager),
		metaStore:          metaStore,
		consumer:           consumer,
	}, nil
}

// NewRedologManager create compositeRedoLogManager on specified table/shard
// each table/shard should only have one compositeRedoLogManager
func (m *RedoLogManagerMaster) NewRedologManager(table string, shard int, tableConfig *metaCom.TableConfig) (RedologManager, error) {
	utils.GetLogger().With("action", "ingestion", "table", table, "shard", shard).Info("Create Redolog Manager")
	m.Lock()
	defer m.Unlock()

	var tableManager map[int]RedologManager
	var ok bool
	if tableManager, ok = m.managers[table]; !ok || tableManager == nil {
		tableManager = make(map[int]RedologManager)
		m.managers[table] = tableManager
	}
	manager, ok := tableManager[shard]
	if ok && manager != nil {
		return nil, fmt.Errorf("NewRedologManager for table: %s, shard: %d is already running", table, shard)
	}

	if m.RedoLogConfig.KafkaConfig.Enabled {
		commitFunc := m.metaStore.UpdateRedoLogCommitOffset
		checkPointFunc := m.metaStore.UpdateRedoLogCheckpointOffset
		getCommitOffsetFunc := m.metaStore.GetRedoLogCommitOffset
		getCheckpointOffsetFunc := m.metaStore.GetRedoLogCheckpointOffset

		if m.RedoLogConfig.DiskConfig.Disabled {
			manager = newKafkaRedoLogManager(m.RedoLogConfig.Namespace, table, shard, m.consumer, true, commitFunc, checkPointFunc, getCommitOffsetFunc, getCheckpointOffsetFunc)
		} else {
			manager = newCompositeRedoLogManager(m.RedoLogConfig.Namespace, table, shard, tableConfig, m.consumer, m.diskStore, commitFunc, checkPointFunc, getCommitOffsetFunc, getCheckpointOffsetFunc)
		}
	} else {
		manager = newFileRedoLogManager(int64(tableConfig.RedoLogRotationInterval), int64(tableConfig.MaxRedoLogFileSize), m.diskStore, table, shard)
	}

	tableManager[shard] = manager

	return manager, nil
}

// Close one table shard Redolog manager
func (m *RedoLogManagerMaster) Close(table string, shard int) {
	m.Lock()
	defer m.Unlock()

	if tableManager, ok := m.managers[table]; ok {
		if tableManager != nil {
			if manager, ok := tableManager[shard]; ok {
				manager.Close()
				delete(tableManager, shard)
				if len(tableManager) == 0 {
					delete(m.managers, table)
				}
			}
		}
	}
}

// Stop close all shard redolog manager and kafka consumer
func (m *RedoLogManagerMaster) Stop() {
	m.Lock()
	defer m.Unlock()

	for table, tableManager := range m.managers {
		for shard, manager := range tableManager {
			manager.Close()
			delete(tableManager, shard)
		}
		delete(m.managers, table)
	}
	if m.consumer != nil {
		m.consumer.Close()
		m.consumer = nil
	}
}
