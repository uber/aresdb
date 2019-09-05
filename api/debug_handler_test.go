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
	"encoding/json"
	"fmt"
	"github.com/uber/aresdb/cluster/topology"
	"io/ioutil"
	"net/http"
	"net/http/httptest"

	"time"

	"github.com/gorilla/mux"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber/aresdb/diskstore"
	"github.com/uber/aresdb/memstore"
	memCom "github.com/uber/aresdb/memstore/common"
	memComMocks "github.com/uber/aresdb/memstore/common/mocks"
	memMocks "github.com/uber/aresdb/memstore/mocks"

	"github.com/uber/aresdb/metastore"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/utils"
	utilsMocks "github.com/uber/aresdb/utils/mocks"

	"path/filepath"

	"strconv"

	"bytes"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/mock"
	"github.com/uber/aresdb/common"
	"github.com/uber/aresdb/query"
	"github.com/uber/aresdb/redolog"
	"sync"
	"unsafe"
)

// convertToAPIError wraps up an error into APIError
func convertToAPIError(err error) error {
	if _, ok := err.(utils.APIError); ok {
		return err
	}

	return utils.APIError{
		Message: err.Error(),
	}
}

var _ = ginkgo.Describe("DebugHandler", func() {

	testFactory := memstore.GetFactory()

	testTableName := "test"
	testTableShardID := 1
	var batchID int32 = 1
	testTable := metaCom.Table{
		Name:        testTableName,
		IsFactTable: true,
		Columns: []metaCom.Column{
			{
				Name: "c0",
			},
			{
				Name: "c1",
			},
			{
				Name: "c2",
			},
			{
				Name: "c3",
			},
			{
				Name: "c4",
			},
			{
				Name: "c5",
				Type: metaCom.Bool,
			},
			{
				Name: "c6",
				Type: metaCom.SmallEnum,
			},
		},
		Config: metaCom.TableConfig{
			BatchSize:                10,
			BackfillMaxBufferSize:    1 << 32,
			BackfillThresholdInBytes: 1 << 21,
		},
	}
	var testSchema *memCom.TableSchema

	redoLogTableName := "abc"
	redoLogShardID := 0
	var redoLogFile int64 = 1501869573

	var memStore *memMocks.MemStore
	var testServer *httptest.Server
	var debugHandler *DebugHandler
	var scheduler *memMocks.Scheduler

	ginkgo.BeforeEach(func() {
		testBatch, _ := testFactory.ReadArchiveBatch("archiveBatch")
		testArchiveBatch := memstore.ArchiveBatch{
			Batch: memCom.Batch{
				RWMutex: &sync.RWMutex{},
				Columns: testBatch.Columns,
			},
			Size:    5,
			Version: 0,
		}
		mockMetaStore := CreateMockMetaStore()
		mockDiskStore := CreateMockDiskStore()
		testRootPath := "../testing/data/integration/sample-ares-root"
		testDiskStore := diskstore.NewLocalDiskStore(testRootPath)
		testMetaStore, err := metastore.NewDiskMetaStore(filepath.Join(testRootPath, "metastore"))
		Ω(err).Should(BeNil())

		// test table
		testSchema = memCom.NewTableSchema(&testTable)
		for col := range testSchema.Schema.Columns {
			testSchema.SetDefaultValue(col)
		}

		testSchema.EnumDicts["c6"] = memCom.EnumDict{}
		enumDict, ok := testSchema.EnumDicts["c6"]
		Ω(ok).Should(BeTrue())
		enumDict.ReverseDict = append(enumDict.ReverseDict, "enum case1")
		testSchema.EnumDicts["c6"] = enumDict
		memStore = CreateMemStore(testSchema, testTableShardID, mockMetaStore, mockDiskStore)
		testShard, _ := memStore.GetTableShard(testTableName, testTableShardID)
		memstore.NewArchiveStoreVersion(100, testShard)
		testShard.Schema = testSchema
		testShard.ArchiveStore = &memstore.ArchiveStore{
			CurrentVersion: memstore.NewArchiveStoreVersion(100, testShard),
		}

		testShard.ArchiveStore.CurrentVersion.Batches = map[int32]*memstore.ArchiveBatch{
			batchID: &testArchiveBatch,
		}

		testArchiveBatch.Shard = testShard

		key := []byte{1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1}
		recordID := memCom.RecordID{
			BatchID: 1,
			Index:   1,
		}

		testShard.LiveStore.PrimaryKey.FindOrInsert(key, recordID, 1)

		// Create first batch.
		testShard.LiveStore.AdvanceNextWriteRecord()
		testShard.LiveStore.AdvanceLastReadRecord()
		liveBatch := testShard.LiveStore.GetBatchForWrite(memstore.BaseBatchID)
		liveBatch.Unlock()
		vp := liveBatch.GetOrCreateVectorParty(5, false)
		vp.SetDataValue(0, memCom.DataValue{Valid: true}, memCom.IgnoreCount)

		var val uint8 = 0
		vp = liveBatch.GetOrCreateVectorParty(6, false)
		vp.SetDataValue(0, memCom.DataValue{Valid: true, OtherVal: unsafe.Pointer(&val)}, memCom.IgnoreCount)

		// redolog table.
		redoLogTable, err := testMetaStore.GetTable(redoLogTableName)
		Ω(err).Should(BeNil())
		redoLogTableSchema := &memCom.TableSchema{
			Schema: *redoLogTable,
		}
		redoManagerFactory, _ := redolog.NewRedoLogManagerMaster("", &common.RedoLogConfig{}, mockDiskStore, mockMetaStore)

		options := memstore.NewOptions(new(memComMocks.BootStrapToken), redoManagerFactory)
		redoLogShard := memstore.NewTableShard(redoLogTableSchema, mockMetaStore, testDiskStore, CreateMockHostMemoryManger(), redoLogShardID, options)

		mockShardNotExistErr := convertToAPIError(errors.New("Failed to get shard"))
		memStore.On("GetTableShard", redoLogTableName, redoLogShardID).Return(redoLogShard, nil).
			Run(func(arguments mock.Arguments) {
				redoLogShard.Users.Add(1)
			})
		memStore.On("GetTableShard", redoLogTableName, testTableShardID).Return(nil, mockShardNotExistErr)
		memStore.On("GetTableShard", testTableName, 2).Return(nil, mockShardNotExistErr)
		memStore.On("GetSchema", redoLogTableName).Return(redoLogTableSchema, nil)

		scheduler = new(memMocks.Scheduler)
		memStore.On("GetScheduler").Return(scheduler)
		utils.SetClockImplementation(func() time.Time {
			return time.Unix(100, 0)
		})

		mockMetaStore.On(
			"AddArchiveBatchVersion",
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockMetaStore.On(
			"UpdateArchivingCutoff", mock.Anything, mock.Anything,
			mock.Anything).Return(nil)
		mockDiskStore.On(
			"DeleteBatchVersions", mock.Anything, mock.Anything,
			mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockDiskStore.On(
			"DeleteLogFile", mock.Anything, mock.Anything,
			mock.Anything).Return(nil)

		writer := new(utilsMocks.WriteCloser)
		writer.On("Write", mock.Anything).Return(0, nil)
		writer.On("Close").Return(nil)
		mockDiskStore.On(
			"OpenVectorPartyFileForWrite", mock.Anything,
			mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(writer, nil)

		queryHandler := NewQueryHandler(
			memStore,
			topology.NewStaticShardOwner([]int{0}),
			common.QueryConfig{
				DeviceMemoryUtilization: 0.9,
				DeviceChoosingTimeout:   5,
			})

		healthCheckHandler := NewHealthCheckHandler()
		debugHandler = NewDebugHandler("", memStore, mockMetaStore, queryHandler, healthCheckHandler, topology.NewStaticShardOwner([]int{0}), nil)
		testRouter := mux.NewRouter()
		debugHandler.Register(testRouter.PathPrefix("/debug").Subrouter())
		testServer = httptest.NewUnstartedServer(testRouter)
		testServer.Start()
	})

	ginkgo.AfterEach(func() {
		utils.ResetClockImplementation()
		testServer.Close()
	})

	ginkgo.It("Health", func() {
		Ω(debugHandler.healthCheckHandler.disable).Should(BeFalse())
		hostPort := testServer.Listener.Addr().String()

		resp, err := http.Get(fmt.Sprintf("http://%s/debug/health", hostPort))
		Ω(err).Should(BeNil())
		bs, err := ioutil.ReadAll(resp.Body)
		Ω(resp.StatusCode).Should(Equal(200))
		Ω(string(bs)).Should(Equal("on"))

		debugHandler.healthCheckHandler.disable = true

		resp, err = http.Get(fmt.Sprintf("http://%s/debug/health", hostPort))
		Ω(err).Should(BeNil())
		bs, err = ioutil.ReadAll(resp.Body)
		Ω(resp.StatusCode).Should(Equal(200))
		Ω(string(bs)).Should(Equal("off"))
	})

	ginkgo.It("HealthSwitch", func() {
		Ω(debugHandler.healthCheckHandler.disable).Should(BeFalse())
		hostPort := testServer.Listener.Addr().String()

		resp, err := http.Post(fmt.Sprintf("http://%s/debug/health/off", hostPort), "", nil)
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(200))
		Ω(debugHandler.healthCheckHandler.disable).Should(BeTrue())

		resp, err = http.Post(fmt.Sprintf("http://%s/debug/health/on", hostPort), "", nil)
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(200))

		Ω(debugHandler.healthCheckHandler.disable).Should(BeFalse())

		resp, err = http.Post(fmt.Sprintf("http://%s/debug/health/os", hostPort), "", nil)
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(400))
		Ω(debugHandler.healthCheckHandler.disable).Should(BeFalse())
	})
	ginkgo.It("ShowBatch", func() {
		hostPort := testServer.Listener.Addr().String()
		resp, err := http.Get(fmt.Sprintf("http://%s/debug/%s/%d/batches/%d?startRow=0&numRows=10", hostPort, testTableName, testTableShardID, batchID))
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(200))
		bs, err := ioutil.ReadAll(resp.Body)
		var body ShowBatchResponse
		json.Unmarshal(bs, &body.Body)
		Ω(body.Body.Columns).Should(Equal([]string{"c0", "c1", "c2", "c3", "c4", "c5", "c6"}))
		Ω(body.Body.NumRows).Should(Equal(5))
		Ω(body.Body.Vectors[0].Counts).Should(Equal([]int{1, 2, 3, 4, 5}))
		Ω(body.Body.Vectors[1].Counts).Should(Equal([]int{3, 4, 5}))
		Ω(body.Body.Vectors[2].Counts).Should(Equal([]int{1, 2, 3, 4, 5}))
		Ω(body.Body.Vectors[3].Counts).Should(Equal([]int{5}))
		Ω(body.Body.Vectors[4].Counts).Should(Equal([]int{1, 2, 3, 4, 5}))
		Ω(body.Body.Vectors[5].Counts).Should(Equal([]int{5}))

		// startRow and numRows should be optional
		resp, _ = http.Get(fmt.Sprintf("http://%s/debug/%s/%d/batches/%d", hostPort, testTableName, testTableShardID, batchID))
		bs, _ = ioutil.ReadAll(resp.Body)
		json.Unmarshal(bs, &body.Body)
		Ω(resp.StatusCode).Should(Equal(200))
		Ω(body.Body.Columns).Should(Equal([]string{"c0", "c1", "c2", "c3", "c4", "c5", "c6"}))
		Ω(body.Body.NumRows).Should(Equal(5))
	})

	ginkgo.It("show live batch should work", func() {
		hostPort := testServer.Listener.Addr().String()
		resp, err := http.Get(fmt.Sprintf("http://%s/debug/%s/%d/batches/%d?startRow=0&numRows=5",
			hostPort, testTableName, testTableShardID, memstore.BaseBatchID))
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(200))
		bs, err := ioutil.ReadAll(resp.Body)
		var body ShowBatchResponse
		json.Unmarshal(bs, &body.Body)
		Ω(body.Body.Columns).Should(Equal([]string{"c0", "c1", "c2", "c3", "c4", "c5", "c6"}))
		Ω(body.Body.NumRows).Should(Equal(1))
		Ω(body.Body.Vectors[0].Values).Should(Equal([]interface{}{nil}))
		Ω(body.Body.Vectors[1].Values).Should(Equal([]interface{}{nil}))
		Ω(body.Body.Vectors[2].Values).Should(Equal([]interface{}{nil}))
		Ω(body.Body.Vectors[3].Values).Should(Equal([]interface{}{nil}))
		Ω(body.Body.Vectors[4].Values).Should(Equal([]interface{}{nil}))
		Ω(body.Body.Vectors[5].Values).Should(Equal([]interface{}{false}))
		Ω(body.Body.Vectors[6].Values).Should(Equal([]interface{}{"enum case1"}))

		// startRow and numRows should be optional
		resp, _ = http.Get(fmt.Sprintf("http://%s/debug/%s/%d/batches/%d", hostPort, testTableName, testTableShardID, memstore.BaseBatchID))
		bs, _ = ioutil.ReadAll(resp.Body)
		json.Unmarshal(bs, &body.Body)
		Ω(resp.StatusCode).Should(Equal(200))
		Ω(body.Body.Columns).Should(Equal([]string{"c0", "c1", "c2", "c3", "c4", "c5", "c6"}))
		Ω(body.Body.NumRows).Should(Equal(1))
	})

	ginkgo.It("LookupPrimaryKey", func() {
		testSchema.PrimaryKeyBytes = 21
		testSchema.PrimaryKeyColumnTypes = []memCom.DataType{memCom.UUID, memCom.Uint32, memCom.Bool}
		hostPort := testServer.Listener.Addr().String()
		resp, err := http.Get(fmt.Sprintf("http://%s/debug/%s/%d/primary-keys?key=01000000000000000100000000000000,1,true", hostPort, testTableName, testTableShardID))
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusOK))
		bs, _ := ioutil.ReadAll(resp.Body)
		var r memCom.RecordID
		json.Unmarshal(bs, &r)
		Ω(r).Should(Equal(memCom.RecordID{
			BatchID: 1,
			Index:   1,
		}))
	})

	ginkgo.It("Archiving request should work", func() {
		hostPort := testServer.Listener.Addr().String()
		request := &ArchiveRequest{}
		request.Body.Cutoff = 200
		job := new(memMocks.Job)
		scheduler.On("NewArchivingJob", mock.Anything, mock.Anything, mock.Anything).Return(job)
		scheduler.On("SubmitJob", job).Return(nil, nil)
		job.On("Run", mock.Anything).Return(nil)
		correctURL := fmt.Sprintf("http://%s/debug/%s/%d/archive", hostPort, testTableName, testTableShardID)
		contentType := "application/json"
		resp, err := http.Post(correctURL, contentType, RequestToBody(&request.Body))
		Ω(err).Should(BeNil())
		bs, err := ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusOK))
		Ω(string(bs)).Should(ContainSubstring("Archiving job submitted"))

		// shard does not exist.
		resp, err = http.Post(
			fmt.Sprintf("http://%s/debug/%s/%d/archive", hostPort, testTableName, 2), contentType,
			RequestToBody(&request.Body))
		Ω(err).Should(BeNil())
		bs, err = ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusBadRequest))
		Ω(string(bs)).Should(ContainSubstring("Failed to get shard"))
	})

	ginkgo.It("ListRedoLogs should work", func() {
		hostPort := testServer.Listener.Addr().String()
		resp, err := http.Get(
			fmt.Sprintf("http://%s/debug/%s/%d/redologs", hostPort, redoLogTableName, redoLogShardID))
		Ω(err).Should(BeNil())
		bs, err := ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusOK))

		var redoLogs []string
		Ω(json.Unmarshal(bs, &redoLogs)).Should(BeNil())
		Ω(redoLogs).Should(ConsistOf(strconv.FormatInt(redoLogFile, 10)))

		// Fail to get shard.
		resp, err = http.Get(
			fmt.Sprintf("http://%s/debug/%s/%d/redologs", hostPort, redoLogTableName, testTableShardID))
		Ω(err).Should(BeNil())
		bs, err = ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusBadRequest))
		Ω(string(bs)).Should(ContainSubstring("Failed to get shard"))
	})

	ginkgo.It("ListUpsertBatches should work", func() {
		hostPort := testServer.Listener.Addr().String()
		resp, err := http.Get(
			fmt.Sprintf("http://%s/debug/%s/%d/redologs/%d/upsertbatches", hostPort, redoLogTableName,
				redoLogShardID, redoLogFile))
		Ω(err).Should(BeNil())
		bs, err := ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusOK))

		var respBody ListUpsertBatchesResponse
		Ω(json.Unmarshal(bs, &respBody)).Should(BeNil())
		Ω(respBody).Should(ConsistOf(int64(4)))

		// Fail to get shard.
		resp, err = http.Get(
			fmt.Sprintf("http://%s/debug/%s/%d/redologs/%d/upsertbatches", hostPort, redoLogTableName,
				testTableShardID, redoLogFile))
		Ω(err).Should(BeNil())
		bs, err = ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusBadRequest))
		Ω(string(bs)).Should(ContainSubstring("Failed to get shard"))

		// Redo log file does not exist.
		resp, err = http.Get(
			fmt.Sprintf("http://%s/debug/%s/%d/redologs/%d/upsertbatches", hostPort, redoLogTableName,
				redoLogShardID, 1))
		Ω(err).Should(BeNil())
		bs, err = ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusInternalServerError))
		Ω(string(bs)).Should(ContainSubstring("Failed to open redolog file"))
	})

	ginkgo.It("ReadUpsertBatch should work", func() {
		expectedRows := [][]interface{}{{123, 0}, {234, 1}}
		expectedColumnNames := []string{"c1", "c2"}
		hostPort := testServer.Listener.Addr().String()
		resp, err := http.Get(
			fmt.Sprintf("http://%s/debug/%s/%d/redologs/%d/upsertbatches/%d"+
				"?draw=%d&start=%d&length=%d",
				hostPort, redoLogTableName, redoLogShardID, redoLogFile, 4, 0, 0, 2))
		Ω(err).Should(BeNil())
		bs, err := ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusOK))

		respBody := ReadUpsertBatchResponse{
			Draw:            0,
			Data:            expectedRows,
			ColumnNames:     expectedColumnNames,
			RecordsTotal:    2,
			RecordsFiltered: 2,
		}

		jsonBytes, _ := json.Marshal(respBody)
		Ω(string(bs)).Should(MatchJSON(string(jsonBytes)))

		// shard does not exist
		resp, err = http.Get(
			fmt.Sprintf("http://%s/debug/%s/%d/redologs/%d/upsertbatches/%d"+
				"?draw=%d&start=%d&length=%d",
				hostPort, redoLogTableName, testTableShardID, redoLogFile, 4, 0, 0, 2))
		Ω(err).Should(BeNil())
		bs, err = ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusBadRequest))
		Ω(string(bs)).Should(ContainSubstring("Failed to get shard"))

		// Invalid offset.
		resp, err = http.Get(
			fmt.Sprintf("http://%s/debug/%s/%d/redologs/%d/upsertbatches/%d"+
				"?draw=%d&start=%d&length=%d",
				hostPort, redoLogTableName, redoLogShardID, redoLogFile, 5, 0, 0, 2))
		Ω(err).Should(BeNil())
		bs, err = ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusInternalServerError))

		// Upsert batch row out of bound.
		resp, err = http.Get(
			fmt.Sprintf("http://%s/debug/%s/%d/redologs/%d/upsertbatches/%d"+
				"?draw=%d&start=%d&length=%d",
				hostPort, redoLogTableName, redoLogShardID, redoLogFile, 4, 0, 2, 2))
		Ω(err).Should(BeNil())
		bs, err = ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusInternalServerError))
		Ω(string(bs)).Should(ContainSubstring("Invalid start or length"))
	})

	ginkgo.It("ShowShardMeta request should work", func() {
		hostPort := testServer.Listener.Addr().String()
		resp, err := http.Get(
			fmt.Sprintf("http://%s/debug/%s/%d", hostPort, testTableName, testTableShardID))
		Ω(err).Should(BeNil())
		bs, err := ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusOK))
		Ω(string(bs)).Should(MatchJSON(`
{
  "schema": {
    "schema": {
      "name": "test",
      "columns": [
        {
          "name": "c0",
          "type": "",
          "config": {},
          "hllConfig": {}
        },
        {
          "name": "c1",
          "type": "",
          "config": {},
          "hllConfig": {}
        },
        {
          "name": "c2",
          "type": "",
          "config": {},
          "hllConfig": {}
        },
        {
          "name": "c3",
          "type": "",
          "config": {},
          "hllConfig": {}
        },
        {
          "name": "c4",
          "type": "",
          "config": {},
          "hllConfig": {}
        },
        {
          "name": "c5",
          "type": "Bool",
          "config": {},
          "hllConfig": {}
        },
        {
          "name": "c6",
          "type": "SmallEnum",
          "config": {},
          "hllConfig": {}
        }
      ],
      "primaryKeyColumns": null,
      "isFactTable": true,
      "config": {
        "batchSize": 10,
        "backfillMaxBufferSize": 4294967296,
        "backfillThresholdInBytes": 2097152
      },
      "incarnation": 0,
      "version": 0
    },
    "columnIDs": {
      "c0": 0,
      "c1": 1,
      "c2": 2,
      "c3": 3,
      "c4": 4,
      "c5": 5,
      "c6": 6
    },
    "enumDicts": {
      "c6": {
        "capacity": 0,
        "dict": null,
        "reverseDict": [
          "enum case1"
        ]
      }
    },
    "valueTypeByColumn": [
      0,
      0,
      0,
      0,
      0,
      1,
      524296
    ],
    "primaryKeyBytes": 0,
    "primaryKeyColumnTypes": []
  },
  "BootstrapState": 0,
  "bootstrapDetails": {
	"source": "",
	"startedAt": 0,
    "stage": "waiting",
    "numColumns": 0,
    "batches": {}
  },
  "liveStore": {
    "backfillManager": {
      "numRecords": 0,
      "currentBufferSize": 0,
      "backfillingBufferSize": 0,
      "maxBufferSize": 4294967296,
      "backfillThresholdInBytes": 2097152,
      "lastRedoFile": 0,
      "lastBatchOffset": 0,
      "currentRedoFile": 0,
      "currentBatchOffset": 0,
      "numUpsertBatches": 0
    },
    "batchSize": 10,
    "batches": {
      "-2147483648": {
        "capacity": 10,
        "numColumns": 7
      }
    },
    "lastModifiedTimePerColumn": null,
    "lastReadRecord": {
      "batchID": -2147483648,
      "index": 1
    },
    "nextWriteRecord": {
      "batchID": -2147483648,
      "index": 1
    },
    "primaryKey": {
      "allocatedBytes": 104000,
      "capacity": 8000,
      "eventTimeCutoff": 0,
      "size": 1
    },
    "redoLogManager": {
      "rotationInterval": 0,
       "maxRedoLogSize": 0,
       "currentRedoLogSize": 0,
       "totalRedologSize": 0,
       "maxEventTimePerFile": {},
       "batchCountPerFile": {},
       "sizePerFile": {},
       "currentFileCreationTime": 0
    },
    "snapshotManager": null
  },
  "archiveStore": {
    "currentVersion": {
      "batches": {
        "1": {
          "numColumns": 6,
          "size": 5,
          "version": 0
        }
      },
      "archivingCutoff": 100
    }
  }
}
		`))

		// shard does not exist.
		resp, err = http.Get(
			fmt.Sprintf("http://%s/debug/%s/%d", hostPort, testTableName, 2))
		Ω(err).Should(BeNil())
		bs, err = ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusBadRequest))
	})

	ginkgo.It("LoadVectorParty should work", func() {
		hostPort := testServer.Listener.Addr().String()
		columnName := "c0"

		resp, err := http.Get(fmt.Sprintf("http://%s/debug/%s/%d/batches/%d/vector-parties/%s", hostPort, testTableName, testTableShardID, -1, columnName))
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusBadRequest))

		resp, err = http.Get(fmt.Sprintf("http://%s/debug/%s/%d/batches/%d/vector-parties/%s", hostPort, testTableName, testTableShardID, batchID, columnName))
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusOK))

	})

	ginkgo.It("EvictVectorParty should work", func() {
		hostPort := testServer.Listener.Addr().String()
		columnName := "c0"

		request, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("http://%s/debug/%s/%d/batches/%d/vector-parties/%s", hostPort, testTableName, testTableShardID, -1, columnName), nil)
		resp, err := http.DefaultClient.Do(request)
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusBadRequest))

		request, err = http.NewRequest(http.MethodDelete, fmt.Sprintf("http://%s/debug/%s/%d/batches/%d/vector-parties/%s", hostPort, testTableName, testTableShardID, batchID, columnName), nil)
		resp, err = http.DefaultClient.Do(request)
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusOK))
	})

	ginkgo.It("ShowJobStatus for archiving job should work", func() {
		expectedStatus := string(`
			{
				"test|1|archiving": {
				  "currentCutoff": 200,
				  "status": "succeeded",
				  "stage": "complete",
				  "runningCutoff": 200,
				  "nextRun": "0001-01-01T00:00:00Z",
				  "lastCutoff": 0,
				  "lastStartTime": "1970-01-01T00:01:40Z",
				  "lastRun": "0001-01-01T00:00:00Z"
				}
			 }
      	`)

		scheduler.On("RLock").Return()
		scheduler.On("RUnlock").Return()
		scheduler.On("GetJobDetails", memCom.ArchivingJobType).Return(map[string]*memstore.ArchiveJobDetail{
			"test|1|archiving": {
				JobDetail: memstore.JobDetail{
					Status:        memstore.JobSucceeded,
					LastStartTime: time.Unix(100, 0).UTC(),
				},
				CurrentCutoff: 200,
				Stage:         memstore.ArchivingComplete,
				RunningCutoff: 200,
				LastCutoff:    0,
			}})
		hostPort := testServer.Listener.Addr().String()
		resp, err := http.Get(fmt.Sprintf("http://%s/debug/jobs/archiving", hostPort))
		Ω(err).Should(BeNil())
		bs, err := ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusOK))

		var respBody map[string]*memstore.ArchiveJobDetail
		Ω(json.Unmarshal(bs, &respBody)).Should(BeNil())
		Ω(bs).Should(MatchJSON(expectedStatus))
	})

	ginkgo.It("ShowJobStatus for backfill job should work", func() {
		expectedStatus := string(`
			{
				"test|1|backfill": {
				  "status": "succeeded",
				  "stage": "complete",
				  "nextRun": "0001-01-01T00:00:00Z",
				  "lastStartTime": "1970-01-01T00:01:40Z",
				  "lastRun": "0001-01-01T00:00:00Z",
				  "redologFile": 0,
                  "batchOffset": 0
				}
			 }
      	`)

		scheduler.On("RLock").Return()
		scheduler.On("RUnlock").Return()
		scheduler.On("GetJobDetails", memCom.BackfillJobType).Return(map[string]*memstore.BackfillJobDetail{
			"test|1|backfill": {
				JobDetail: memstore.JobDetail{
					Status:        memstore.JobSucceeded,
					LastStartTime: time.Unix(100, 0).UTC(),
				},
				Stage: memstore.BackfillComplete,
			}})
		hostPort := testServer.Listener.Addr().String()
		resp, err := http.Get(fmt.Sprintf("http://%s/debug/jobs/backfill", hostPort))
		Ω(err).Should(BeNil())
		bs, err := ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusOK))

		var respBody map[string]*memstore.BackfillJobDetail
		Ω(json.Unmarshal(bs, &respBody)).Should(BeNil())
		Ω(bs).Should(MatchJSON(expectedStatus))
	})

	ginkgo.It("ShowJobStatus for snapshot job should work", func() {
		expectedStatus := string(`
			{
				"test|1|snapshot": {
				  "status": "succeeded",
				  "nextRun": "0001-01-01T00:00:00Z",
				  "lastRun": "0001-01-01T00:00:00Z",
				  "lastStartTime": "1970-01-01T00:01:40Z",
				  "numMutations": 0,
				  "numBatches": 0,
				  "redologFile": 0,
				  "batchOffset": 0,
				  "stage": ""
				}
			 }
      	`)

		scheduler.On("RLock").Return()
		scheduler.On("RUnlock").Return()
		scheduler.On("GetJobDetails", memCom.SnapshotJobType).Return(map[string]*memstore.SnapshotJobDetail{
			"test|1|snapshot": {
				JobDetail: memstore.JobDetail{
					Status:        memstore.JobSucceeded,
					LastStartTime: time.Unix(100, 0).UTC(),
				},
			}})
		hostPort := testServer.Listener.Addr().String()
		resp, err := http.Get(fmt.Sprintf("http://%s/debug/jobs/snapshot", hostPort))
		Ω(err).Should(BeNil())
		bs, err := ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusOK))

		var respBody map[string]*memstore.SnapshotJobDetail
		Ω(json.Unmarshal(bs, &respBody)).Should(BeNil())
		Ω(bs).Should(MatchJSON(expectedStatus))
	})

	ginkgo.It("ShowHostMemory should work", func() {
		memoryUsages := map[string]memstore.TableShardMemoryUsage{
			"table1": {
				PrimaryKeyMemory: 10,
			},
		}

		expectedResponse := string(`
			{
			"table1": {
			  "cols": null,
			  "pk": 10
			}
		  }
      	`)

		memStore.On("GetMemoryUsageDetails").Return(memoryUsages, nil)

		hostPort := testServer.Listener.Addr().String()
		resp, err := http.Get(fmt.Sprintf("http://%s/debug/host-memory", hostPort))
		Ω(err).Should(BeNil())
		bs, err := ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusOK))

		var respBody map[string]memstore.TableShardMemoryUsage
		Ω(json.Unmarshal(bs, &respBody)).Should(BeNil())
		Ω(bs).Should(MatchJSON(expectedResponse))
	})

	ginkgo.It("ReadBackfillQueueUpsertBatch should work", func() {
		builder := memCom.NewUpsertBatchBuilder()
		builder.AddRow()
		err := builder.AddColumn(1, memCom.Uint8)
		Ω(err).Should(BeNil())
		builder.SetValue(0, 0, uint8(135))
		buffer, err := builder.ToByteArray()
		Ω(err).Should(BeNil())
		upsertBatch, err := memCom.NewUpsertBatch(buffer)
		Ω(upsertBatch).ShouldNot(BeNil())
		Ω(err).Should(BeNil())

		testShard, _ := memStore.GetTableShard(testTableName, testTableShardID)
		Ω(testShard.LiveStore.BackfillManager.Append(upsertBatch, 0, 0)).Should(BeFalse())
		expectedRows := [][]interface{}{{135}}
		expectedColumnNames := []string{"c1"}
		hostPort := testServer.Listener.Addr().String()
		resp, err := http.Get(
			fmt.Sprintf("http://%s/debug/%s/%d/backfill-manager/upsertbatches/%d"+
				"?draw=%d&start=%d&length=%d",
				hostPort, testTableName, testTableShardID, 0, 0, 0, 2))
		Ω(err).Should(BeNil())
		bs, err := ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusOK))

		respBody := ReadUpsertBatchResponse{
			Draw:            0,
			Data:            expectedRows,
			ColumnNames:     expectedColumnNames,
			RecordsTotal:    1,
			RecordsFiltered: 1,
		}

		jsonBytes, _ := json.Marshal(respBody)
		Ω(string(bs)).Should(MatchJSON(string(jsonBytes)))

		// shard does not exist
		resp, err = http.Get(
			fmt.Sprintf("http://%s/debug/%s/%d/backfill-manager/upsertbatches/%d"+
				"?draw=%d&start=%d&length=%d",
				hostPort, redoLogTableName, testTableShardID, 0, 0, 0, 2))
		Ω(err).Should(BeNil())
		bs, err = ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusBadRequest))
		Ω(string(bs)).Should(ContainSubstring("Failed to get shard"))
	})

	ginkgo.It("ShowDeviceStatus should work", func() {
		expectedStatus := string(`
			{
			"deviceInfos": [
			  {
				"deviceID": 0,
				"queryCount": 0,
				"totalMemory": 25576865792,
				"totalAvailableMemory": 23019177984,
				"totalFreeMemory": 23019177984
			  }
			],
			"timeout": 5,
			"maxAvailableMemory": 23019177984
		  }
      	`)

		hostPort := testServer.Listener.Addr().String()
		resp, err := http.Get(fmt.Sprintf("http://%s/debug/devices", hostPort))
		Ω(err).Should(BeNil())
		bs, err := ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusOK))

		var respBody query.DeviceManager
		Ω(json.Unmarshal(bs, &respBody)).Should(BeNil())
		Ω(bs).Should(MatchJSON(expectedStatus))
	})

	ginkgo.It("Backfill request should work", func() {
		hostPort := testServer.Listener.Addr().String()
		request := &BackfillRequest{}
		bs, err := json.Marshal(request)
		Ω(err).Should(BeNil())
		job := new(memMocks.Job)
		scheduler.On("NewBackfillJob", mock.Anything, mock.Anything, mock.Anything).Return(job)
		scheduler.On("SubmitJob", job).Return(nil, nil)
		job.On("Run", mock.Anything).Return(nil)
		correctURL := fmt.Sprintf("http://%s/debug/%s/%d/backfill", hostPort, testTableName, testTableShardID)
		contentType := "application/json"
		resp, err := http.Post(correctURL, contentType, bytes.NewReader(bs))
		Ω(err).Should(BeNil())
		bs, err = ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusOK))
		Ω(string(bs)).Should(ContainSubstring("Backfill job submitted"))

		// shard does not exist.
		resp, err = http.Post(
			fmt.Sprintf("http://%s/debug/%s/%d/backfill", hostPort, testTableName, 2), contentType, nil)
		Ω(err).Should(BeNil())
		bs, err = ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusBadRequest))
		Ω(string(bs)).Should(ContainSubstring("Failed to get shard"))
	})

	ginkgo.It("Snapshot request should work", func() {
		hostPort := testServer.Listener.Addr().String()
		request := &SnapshotRequest{}
		bs, err := json.Marshal(request)
		job := new(memMocks.Job)
		scheduler.On("NewSnapshotJob", mock.Anything, mock.Anything, mock.Anything).Return(job)
		scheduler.On("SubmitJob", job).Return(nil, nil)
		job.On("Run", mock.Anything).Return(nil)
		correctURL := fmt.Sprintf("http://%s/debug/%s/%d/snapshot", hostPort, testTableName, testTableShardID)
		contentType := "application/json"
		resp, err := http.Post(correctURL, contentType, bytes.NewReader(bs))
		Ω(err).Should(BeNil())
		bs, err = ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusOK))
		Ω(string(bs)).Should(ContainSubstring("Snapshot job submitted"))

		// shard does not exist.
		resp, err = http.Post(
			fmt.Sprintf("http://%s/debug/%s/%d/snapshot", hostPort, testTableName, 2), contentType, nil)
		Ω(err).Should(BeNil())
		bs, err = ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusBadRequest))
		Ω(string(bs)).Should(ContainSubstring("Failed to get shard"))
	})

	ginkgo.It("Purge request should work", func() {
		hostPort := testServer.Listener.Addr().String()
		request := &PurgeRequest{}
		bs, err := json.Marshal(request)
		job := new(memMocks.Job)
		scheduler.On("NewPurgeJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(job)
		scheduler.On("SubmitJob", job).Return(nil, nil)
		job.On("Run", mock.Anything).Return(nil)
		correctURL := fmt.Sprintf("http://%s/debug/%s/%d/purge", hostPort, testTableName, testTableShardID)
		contentType := "application/json"
		resp, err := http.Post(correctURL, contentType, RequestToBody(&request.Body))
		Ω(err).Should(BeNil())
		bs, err = ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusOK))
		Ω(string(bs)).Should(ContainSubstring("Purge job submitted"))

		// shard does not exist.
		resp, err = http.Post(
			fmt.Sprintf("http://%s/debug/%s/%d/purge", hostPort, testTableName, 2), contentType, RequestToBody(&request.Body))
		Ω(err).Should(BeNil())
		bs, err = ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusBadRequest))
		Ω(string(bs)).Should(ContainSubstring("Failed to get shard"))

		testSchema.Schema.Config.RecordRetentionInDays = 0
		request.Body.SafePurge = true
		resp, err = http.Post(
			fmt.Sprintf("http://%s/debug/%s/%d/purge", hostPort, testTableName, 1), contentType, RequestToBody(&request.Body))
		Ω(err).Should(BeNil())
		bs, err = ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusBadRequest))
		Ω(string(bs)).Should(ContainSubstring("safe purge attempted on table with infinite retention"))

		testSchema.Schema.Config.RecordRetentionInDays = 10
		request.Body.SafePurge = true
		resp, err = http.Post(
			fmt.Sprintf("http://%s/debug/%s/%d/purge", hostPort, testTableName, 1), contentType, RequestToBody(&request.Body))
		Ω(err).Should(BeNil())
		bs, err = ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusOK))
	})
})
