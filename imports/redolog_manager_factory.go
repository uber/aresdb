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
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/uber/aresdb/common"
	"github.com/uber/aresdb/diskstore"
	memCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/metastore"
	metaCom "github.com/uber/aresdb/metastore/common"
	"sync"
	"github.com/uber/aresdb/utils"
)

// Factory class to create shard level redolog manager
type RedologManagerFactory struct {
	sync.Mutex
	namespace string
	// flag to enable/disable local redolog
	enableLocalRedoLog bool
	// kafka consuer if kafka consumer is configured
	consumer sarama.Consumer
	// DiskStore
	diskStore diskstore.DiskStore
	// Metastore
	metaStore metastore.MetaStore
	// save all table partition redolog managers
	managers map[string]map[int]RedologManager
}

// Function definitio to store upsertbatch
type BatchStoreFunc func(batch *memCom.UpsertBatch, redoFile int64, offset uint32, skipBackFillRows bool) error

// NewRedologManagerFactory create ImportsFactory instance
func NewRedologManagerFactory(c *common.ImportsConfig, diskStore diskstore.DiskStore, metaStore metastore.MetaStore) (*RedologManagerFactory, error) {
	if c != nil && c.Source == common.KafkaSoureOnly && len(c.KafkaConfig.Brokers) == 0 {
		return nil, fmt.Errorf("No kafka broker info configured")
	}

	enableLocalRedoLog := true
	if c != nil && c.Source == common.KafkaSoureOnly {
		// we will turn on local redolog if import source is not Kafka only
		enableLocalRedoLog = !c.RedoLog.Disabled
	}

	factory := &RedologManagerFactory{
		namespace:          c.Namespace,
		enableLocalRedoLog: enableLocalRedoLog,
		diskStore:          diskStore,
		managers:           make(map[string]map[int]RedologManager),
		metaStore:          metaStore,
	}

	consumer, err := factory.newKafkaConsumer(c.KafkaConfig.Brokers)
	if err != nil {
		return nil, err
	}

	factory.consumer = consumer

	return factory, nil
}

// NewKafkaConsumer create kafka consumer
func (c *RedologManagerFactory) newKafkaConsumer(brokers []string) (sarama.Consumer, error) {
	if len(brokers) > 0 {
		return sarama.NewConsumer(brokers, sarama.NewConfig())
	}
	return nil, nil
}

// NewRedologManager create CompositeRedologManager on specified table/shard
// each table/shard should only have one CompositeRedologManager
func (f *RedologManagerFactory) NewRedologManager(table string, shard int, tableConfig *metaCom.TableConfig, storeFunc BatchStoreFunc) (RedologManager, error) {
	utils.GetLogger().With("action", "ingestion", "table", table, "shard", shard).Info("Create Redolog Manager")
	f.Lock()
	defer f.Unlock()

	var tableManager map[int]RedologManager
	var ok bool
	if tableManager, ok = f.managers[table]; !ok || tableManager == nil {
		tableManager = make(map[int]RedologManager)
		f.managers[table] = tableManager
	}
	manager, ok := tableManager[shard]
	if ok && manager != nil {
		fmt.Printf("error: %v\n", utils.StackError(nil, "error"))
		return nil, fmt.Errorf("NewRedologManager for table: %s, shard: %d is already running", table, shard)
	}

	manager = NewCompositeRedologManager(table, shard, f, tableConfig, storeFunc)
	tableManager[shard] = manager

	return manager, nil
}

// Close one table shard Redolog manager
func (f *RedologManagerFactory) Close(table string, shard int) {
	f.Lock()
	defer f.Unlock()

	if tableManager, ok := f.managers[table]; ok {
		if tableManager != nil {
			if _, ok := tableManager[shard]; ok {
				delete(tableManager, shard)
				if len(tableManager) == 0 {
					delete(f.managers, table)
				}
			}

		}
	}
}

// Stop close all shard redolog manager and kafka consumer
func (f *RedologManagerFactory) Stop() {
	for table, tableManager := range f.managers {
		for shard, manager := range tableManager {
			manager.Close()
			delete(tableManager, shard)
		}
		delete(f.managers, table)
	}
	if f.consumer != nil {
		f.consumer.Close()
		f.consumer = nil
	}
}
