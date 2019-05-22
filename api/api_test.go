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

package api

import (
	"bytes"
	"encoding/json"

	"io"

	"fmt"
	"github.com/stretchr/testify/mock"
	"github.com/uber/aresdb/diskstore"
	diskMocks "github.com/uber/aresdb/diskstore/mocks"
	"github.com/uber/aresdb/memstore"
	memCom "github.com/uber/aresdb/memstore/common"
	memComMocks "github.com/uber/aresdb/memstore/common/mocks"
	memMocks "github.com/uber/aresdb/memstore/mocks"
	"github.com/uber/aresdb/metastore"
	metaMocks "github.com/uber/aresdb/metastore/mocks"
	"github.com/uber/aresdb/testing"
	"github.com/uber/aresdb/imports"
	"github.com/uber/aresdb/common"
)

// CreateMockDiskStore creates a mocked DiskStore for testing.
func CreateMockDiskStore() *diskMocks.DiskStore {
	diskStore := &diskMocks.DiskStore{}
	diskStore.On("OpenLogFileForAppend", mock.Anything, mock.Anything, mock.Anything).Return(&testing.TestReadWriteCloser{}, nil)
	return diskStore
}

// CreateMockMetaStore creates a mocked MetaStore for testing.
func CreateMockMetaStore() *metaMocks.MetaStore {
	metaStore := &metaMocks.MetaStore{}
	return metaStore
}

// CreateMemStore creates a mocked hostMemoryManager for testing.
func CreateMockHostMemoryManger() *memComMocks.HostMemoryManager {
	hostMemoryManager := &memComMocks.HostMemoryManager{}
	hostMemoryManager.On("ReportUnmanagedSpaceUsageChange", mock.Anything).Return()
	hostMemoryManager.On("ReportManagedObject", mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).Return()
	return hostMemoryManager
}

// CreateMemStore creates a mocked MemStore for testing.
func CreateMemStore(schema *memCom.TableSchema, shardID int, metaStore metastore.MetaStore,
	diskStore diskstore.DiskStore) *memMocks.MemStore {
	redoManagerFactory, _ := imports.NewRedologManagerFactory(&common.ImportsConfig{}, diskStore, metaStore)
	shard := memstore.NewTableShard(schema, metaStore, diskStore, CreateMockHostMemoryManger(), shardID, redoManagerFactory)

	memStore := new(memMocks.MemStore)
	memStore.On("GetTableShard", schema.Schema.Name, shardID).Return(shard, nil).
		Run(func(arguments mock.Arguments) {
			shard.Users.Add(1)
		})
	memStore.On("GetSchema", schema.Schema.Name).Return(schema, nil)
	memStore.On("GetSchema", mock.Anything).Return(nil, fmt.Errorf("some error"))
	memStore.On("GetSchemas").Return(map[string]*memCom.TableSchema{schema.Schema.Name: schema})
	memStore.On("RLock").Return()
	memStore.On("RUnlock").Return()
	memStore.On("Lock").Return()
	memStore.On("Unlock").Return()
	return memStore
}

// RequestToBody marshals a request struct into json and provide a reader of it.
func RequestToBody(request interface{}) io.Reader {
	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(request)
	return b
}
