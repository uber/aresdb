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
package etcd

import (
	"testing"

	"github.com/stretchr/testify/assert"
	pb "github.com/uber/aresdb/controller/generated/proto"
	"github.com/uber/aresdb/metastore"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/utils"
	"go.uber.org/zap"
)

func TestSchemaMutator(t *testing.T) {

	defaultConfig := metaCom.TableConfig{
		BatchSize:                metastore.DefaultBatchSize,
		ArchivingIntervalMinutes: metastore.DefaultArchivingIntervalMinutes,
		ArchivingDelayMinutes:    metastore.DefaultArchivingDelayMinutes,
		BackfillMaxBufferSize:    metastore.DefaultBackfillMaxBufferSize,
		BackfillIntervalMinutes:  metastore.DefaultBackfillIntervalMinutes,
		BackfillThresholdInBytes: metastore.DefaultBackfillThresholdInBytes,
		BackfillStoreBatchSize:   metastore.DefaultBackfillStoreBatchSize,
		RecordRetentionInDays:    metastore.DefaultRecordRetentionInDays,
		SnapshotIntervalMinutes:  metastore.DefaultSnapshotIntervalMinutes,
		SnapshotThreshold:        metastore.DefaultSnapshotThreshold,
		RedoLogRotationInterval:  metastore.DefaultRedologRotationInterval,
		MaxRedoLogFileSize:       metastore.DefaultMaxRedoLogSize,
	}

	testTable := metaCom.Table{
		Version: 0,
		Name:    "test1",
		Columns: []metaCom.Column{
			{
				Name: "col1",
				Type: "Int32",
			},
		},
		PrimaryKeyColumns: []int{0},
		Config:            metastore.DefaultTableConfig,
	}
	testTable2 := metaCom.Table{
		Version: 0,
		Name:    "test1",
		Columns: []metaCom.Column{
			{
				Name: "col1",
				Type: "Int32",
			},
			{
				Name: "col2",
				Type: "Int32",
			},
		},
		Config:            metastore.DefaultTableConfig,
		PrimaryKeyColumns: []int{0},
	}
	testTable2.Config.BatchSize = 100

	testTableInvalid := metaCom.Table{
		Version: 0,
		Name:    "test1",
		Columns: []metaCom.Column{
			{
				Name: "col1",
				Type: "Int32",
			},
		},
	}

	t.Run("create, read, list, delete should work", func(t *testing.T) {
		cleanUp, port := utils.SetUpEtcdTestServer(t)
		defer cleanUp()
		clusterClient := utils.SetUpEtcdTestClient(t, port)
		store, err := clusterClient.Txn()
		assert.NoError(t, err)

		_, err = store.Set(utils.SchemaListKey("ns1"), &pb.EntityList{})
		assert.NoError(t, err)

		schemaMutator := tableSchemaMutator{
			txnStore: store,
			logger:   zap.NewExample().Sugar(),
		}

		res, err := schemaMutator.ListTables("ns1")
		assert.NoError(t, err)
		assert.Empty(t, res)

		err = schemaMutator.CreateTable("ns1", &testTable, false)
		assert.NoError(t, err)

		hash1, err := schemaMutator.GetHash("ns1")
		assert.NoError(t, err)

		tbs, err := schemaMutator.ListTables("ns1")
		assert.NoError(t, err)
		assert.Equal(t, []string{"test1"}, tbs)

		table, err := schemaMutator.GetTable("ns1", "test1")
		assert.NoError(t, err)
		expectedTable := testTable
		expectedTable.Config = defaultConfig
		assert.Equal(t, expectedTable, *table)

		err = schemaMutator.UpdateTable("ns1", testTable2, false)
		assert.NoError(t, err)

		hash2, err := schemaMutator.GetHash("ns1")
		assert.NoError(t, err)
		assert.NotEqual(t, hash1, hash2)

		table2, err := schemaMutator.GetTable("ns1", "test1")
		assert.NoError(t, err)
		expectedTable2 := testTable2
		expectedTable2.Config = defaultConfig
		// default should not overwrite explicit config
		expectedTable2.Config.BatchSize = 100
		assert.Equal(t, expectedTable2, *table2)

		err = schemaMutator.DeleteTable("ns1", "test1")
		assert.NoError(t, err)

		tbs, err = schemaMutator.ListTables("ns1")
		assert.NoError(t, err)
		assert.Empty(t, tbs)
	})

	t.Run("reuse table should success", func(t *testing.T) {
		// test setup
		cleanUp, port := utils.SetUpEtcdTestServer(t)
		defer cleanUp()

		clusterClient := utils.SetUpEtcdTestClient(t, port)
		txnStore, err := clusterClient.Txn()
		assert.NoError(t, err)

		expectedTable := testTable
		expectedTable.Config = defaultConfig

		// test
		// fail if table doesn't exist
		schemaMutator := tableSchemaMutator{
			txnStore: txnStore,
			logger:   zap.NewExample().Sugar(),
		}

		_, err = txnStore.Set(utils.SchemaListKey("ns1"), &pb.EntityList{})
		assert.NoError(t, err)

		err = schemaMutator.CreateTable("ns1", &testTable, false)
		assert.NoError(t, err)

		table, err := schemaMutator.GetTable("ns1", testTable.Name)
		assert.NoError(t, err)
		assert.Equal(t, expectedTable, *table)

		err = schemaMutator.DeleteTable("ns1", testTable.Name)
		assert.NoError(t, err)

		err = schemaMutator.CreateTable("ns1", &testTable, false)
		assert.NoError(t, err)

		table, err = schemaMutator.GetTable("ns1", testTable.Name)
		assert.NoError(t, err)
		expectedTable.Incarnation++
		assert.Equal(t, expectedTable, *table)
	})

	t.Run("create should fail", func(t *testing.T) {
		cleanUp, port := utils.SetUpEtcdTestServer(t)
		defer cleanUp()

		clusterClient := utils.SetUpEtcdTestClient(t, port)
		txnStore, err := clusterClient.Txn()
		assert.NoError(t, err)

		schemaMutator := tableSchemaMutator{
			txnStore: txnStore,
			logger:   zap.NewExample().Sugar(),
		}
		err = schemaMutator.CreateTable("ns2", &testTable, false)
		assert.EqualError(t, err, "Namespace does not exist")

		_, err = txnStore.Set(utils.SchemaListKey("ns"), &pb.EntityList{
			Entities: []*pb.EntityName{
				{
					Name:      "test1",
					Tomstoned: false,
				},
			},
		})
		assert.NoError(t, err)
		_, err = txnStore.Set(utils.SchemaKey("ns", "test1"), &pb.EntityConfig{})
		assert.NoError(t, err)

		// fail if table schema already exists
		err = schemaMutator.CreateTable("ns", &testTable, true)
		assert.EqualError(t, err, "Table already exists")

		// fail if schema invalid
		err = schemaMutator.CreateTable("ns2", &testTableInvalid, false)
		assert.EqualError(t, err, "Primary key columns not specified")
	})

	t.Run("force flag should work", func(t *testing.T) {
		// test setup
		cleanUp, port := utils.SetUpEtcdTestServer(t)
		defer cleanUp()

		clusterClient := utils.SetUpEtcdTestClient(t, port)
		txnStore, err := clusterClient.Txn()
		assert.NoError(t, err)
		_, err = txnStore.Set(utils.SchemaListKey("ns1"), &pb.EntityList{})
		assert.NoError(t, err)

		// test
		schemaMutator := tableSchemaMutator{
			txnStore: txnStore,
			logger:   zap.NewExample().Sugar(),
		}
		err = schemaMutator.CreateTable("ns1", &testTable, true)
		assert.NoError(t, err)

		err = schemaMutator.UpdateTable("ns1", testTable2, true)
		assert.NoError(t, err)
	})

	t.Run("delete should fail", func(t *testing.T) {
		// test setup
		cleanUp, port := utils.SetUpEtcdTestServer(t)
		defer cleanUp()

		clusterClient := utils.SetUpEtcdTestClient(t, port)
		txnStore, err := clusterClient.Txn()
		assert.NoError(t, err)

		// test
		// fail if table doesn't exist
		schemaMutator := tableSchemaMutator{
			txnStore: txnStore,
			logger:   zap.NewExample().Sugar(),
		}

		_, err = txnStore.Set(utils.SchemaListKey("ns2"), &pb.EntityList{})
		assert.NoError(t, err)

		err = schemaMutator.DeleteTable("ns2", "non-exist-table")
		assert.EqualError(t, err, "Table does not exist")
	})
}
