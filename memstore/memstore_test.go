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
	"unsafe"

	"github.com/stretchr/testify/mock"
	"github.com/uber/aresdb/diskstore"
	"github.com/uber/aresdb/diskstore/mocks"
	memCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/metastore"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/testing"
	"github.com/uber/aresdb/utils"
	config "github.com/uber/aresdb/common"
)

func CreateMockDiskStore() *mocks.DiskStore {
	diskStore := &mocks.DiskStore{}
	diskStore.On("OpenLogFileForAppend", mock.Anything, mock.Anything, mock.Anything).Return(&testing.TestReadWriteCloser{}, nil)
	return diskStore
}

// createMemStore creates a memStoreImpl instance for testing.
func createMemStore(tableName string, shardID int, columnTypes []memCom.DataType,
	primaryKeyColumns []int, batchSize int, isFactTable bool, allowMissingEventTime bool,
	metaStore metastore.MetaStore, diskStore diskstore.DiskStore) *memStoreImpl {
	// Create schemas.
	mainSchema := metaCom.Table{
		Name:              tableName,
		Columns:           make([]metaCom.Column, len(columnTypes)),
		PrimaryKeyColumns: primaryKeyColumns,
		Config: metaCom.TableConfig{
			BatchSize:                batchSize,
			BackfillMaxBufferSize:    1 << 32,
			BackfillThresholdInBytes: 1 << 21,
			AllowMissingEventTime:    allowMissingEventTime,
		},
		IsFactTable: isFactTable,
	}
	for i, dataType := range columnTypes {
		mainSchema.Columns[i].Type = memCom.DataTypeName[dataType]
	}
	schema := NewTableSchema(&mainSchema)

	for i := range columnTypes {
		schema.SetDefaultValue(i)
	}
	redoLogManagerFactory, _ := NewRedoLogManagerFactory("test", config.IngestionConfig{})
	memStore := NewMemStore(metaStore, diskStore, nil, redoLogManagerFactory).(*memStoreImpl)
	// Create shards.
	shards := map[int]*TableShard{
		shardID: NewTableShard(schema, metaStore, diskStore, NewHostMemoryManager(memStore, 1<<32), shardID, nil, redoLogManagerFactory),
	}
	memStore.TableShards[tableName] = shards
	memStore.TableSchemas[tableName] = schema

	return memStore
}

// ReadShardValue reads a value from a shard at given position.
func ReadShardValue(shard *TableShard, columnID int, primaryKey []byte) (unsafe.Pointer, bool) {
	vp, index := getVectorParty(shard, columnID, primaryKey)
	if vp == nil {
		return nil, false
	}

	validity := vp.GetValidity(index)
	if validity {
		return vp.GetDataValue(index).OtherVal, true
	}
	return nil, false
}

// ReadShardBool reads a bool value from a shard at given position.
func ReadShardBool(shard *TableShard, columnID int, primaryKey []byte) (bool, bool) {
	vp, index := getVectorParty(shard, columnID, primaryKey)
	if vp == nil {
		return false, false
	}

	validity := vp.GetValidity(index)
	if validity {
		return vp.GetDataValue(index).BoolVal, true
	}
	return false, false
}

// Read the vector party and record index.
func getVectorParty(shard *TableShard, columnID int, primaryKey []byte) (memCom.VectorParty, int) {
	existing, record, err := shard.LiveStore.PrimaryKey.FindOrInsert(primaryKey, RecordID{}, uint32(utils.Now().Unix()/1000))
	if err != nil || !existing {
		return nil, 0
	}

	batch := shard.LiveStore.GetBatchForRead(record.BatchID)
	defer batch.RUnlock()
	vp := batch.GetVectorParty(columnID)
	return vp, int(record.Index)
}
