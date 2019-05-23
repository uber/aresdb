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

package metastore

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/testing"
	"github.com/uber/aresdb/utils/mocks"
	"github.com/uber/aresdb/utils"
)

var _ = ginkgo.Describe("disk metastore", func() {

	mockWriterCloser := &testing.TestReadWriteCloser{}

	testColumn0 := common.Column{
		Name: "column0",
		Type: common.Uint32,
		Config: common.ColumnConfig{
			PreloadingDays: 1,
			Priority:       3,
		},
	}

	testColumn1 := common.Column{
		Name:    "column1",
		Type:    common.BigEnum,
		Deleted: false,
		Config: common.ColumnConfig{
			PreloadingDays: 1,
			Priority:       3,
		},
	}

	testColumnConfig1 := common.ColumnConfig{
		PreloadingDays: 2,
		Priority:       3,
	}

	testColumn2 := common.Column{
		Name:    "column2",
		Type:    common.BigEnum,
		Deleted: false,
		Config: common.ColumnConfig{
			PreloadingDays: 1,
			Priority:       3,
		},
	}

	testColumn3 := common.Column{
		Name:    "column3",
		Type:    common.Int32,
		Deleted: false,
		Config: common.ColumnConfig{
			PreloadingDays: 1,
			Priority:       3,
		},
	}

	testColumn4 := common.Column{
		Name:    "column4",
		Type:    common.BigEnum,
		Deleted: false,
		Config: common.ColumnConfig{
			PreloadingDays: 1,
			Priority:       3,
		},
	}

	testColumn5 := common.Column{
		Name:    "column5",
		Type:    common.Int32,
		Deleted: true,
	}

	testTableA := common.Table{
		Name: "a",
		Columns: []common.Column{
			testColumn0,
			testColumn1,
			testColumn3,
			testColumn4,
			testColumn5,
		},
		Config: common.TableConfig{
			InitialPrimaryKeyNumBuckets: 0,
			BatchSize:                   2097152,
			ArchivingDelayMinutes:       1440,
			ArchivingIntervalMinutes:    180,
			BackfillIntervalMinutes:     60,
			BackfillMaxBufferSize:       4294967296,
			BackfillThresholdInBytes:    2097152,
			BackfillStoreBatchSize:      20000,
			RecordRetentionInDays:       90,
			SnapshotThreshold:           6291456,
			SnapshotIntervalMinutes:     1,
			RedoLogRotationInterval:     10800,
			MaxRedoLogFileSize:          1073741824,
		},
		IsFactTable:          true,
		PrimaryKeyColumns:    []int{1},
		ArchivingSortColumns: []int{2},
	}
	mockTableADir := &mocks.FileInfo{}
	mockTableADir.On("Name").Return("a")

	mockeTableAShard0 := &mocks.FileInfo{}
	mockeTableAShard0.On("Name").Return("0")
	testTableABytes, _ := json.MarshalIndent(testTableA, "", "  ")

	testTableB := common.Table{
		Name: "b",
		Columns: []common.Column{
			testColumn2,
		},
		IsFactTable:       false,
		PrimaryKeyColumns: []int{0},
		Config:            DefaultTableConfig,
	}

	mockTableBDir := &mocks.FileInfo{}
	mockTableBDir.On("Name").Return("b")
	mockeTableBShard0 := &mocks.FileInfo{}
	mockeTableBShard0.On("Name").Return("0")
	testTableBBytes, _ := json.MarshalIndent(testTableB, "", "  ")

	testTableC := common.Table{
		Name: "c",
		Columns: []common.Column{
			testColumn0,
			testColumn1,
		},
		IsFactTable:       true,
		PrimaryKeyColumns: []int{1},
		Config:            DefaultTableConfig,
	}
	testTableCBytes, _ := json.MarshalIndent(testTableC, "", "  ")

	mockFileSystem := &mocks.FileSystem{}
	mockFileSystem.On("ReadDir", "base").Return([]os.FileInfo{mockTableADir, mockTableBDir}, nil)
	mockFileSystem.On("Stat", "base/a/schema").Return(&mocks.FileInfo{}, nil)
	mockFileSystem.On("Stat", "base/b/schema").Return(&mocks.FileInfo{}, nil)
	mockFileSystem.On("Stat", "base/c/schema").Return(&mocks.FileInfo{}, nil)
	mockFileSystem.On("Stat", "base/read_fail/schema").Return(&mocks.FileInfo{}, nil)
	mockFileSystem.On("Stat", "base/unknown/schema").Return(nil, os.ErrNotExist)
	mockFileSystem.On("Stat", "base/error/schema").Return(nil, os.ErrPermission)
	mockFileSystem.On("Stat", "base/unknown_shard/schema").Return(&mocks.FileInfo{}, nil)
	mockFileSystem.On("Stat", "base/error_shard/schema").Return(nil, nil)

	mockFileSystem.On("Stat", "base/c/shards/0").Return(&mocks.FileInfo{}, nil)
	mockFileSystem.On("Stat", "base/a/shards/0").Return(&mocks.FileInfo{}, nil)
	mockFileSystem.On("Stat", "base/b/shards/0").Return(&mocks.FileInfo{}, nil)
	mockFileSystem.On("Stat", "base/read_fail/shards/0").Return(&mocks.FileInfo{}, nil)
	mockFileSystem.On("Stat", "base/unknown_shard/shards/0").Return(nil, os.ErrNotExist)
	mockFileSystem.On("Stat", "base/error_shard/shards/0").Return(nil, os.ErrPermission)

	mockFileSystem.On("ReadFile", "base/a/schema").Return(testTableABytes, nil)
	mockFileSystem.On("ReadFile", "base/b/schema").Return(testTableBBytes, nil)
	mockFileSystem.On("ReadFile", "base/c/schema").Return(testTableCBytes, nil)
	mockFileSystem.On("ReadFile", "base/read_fail/schema").Return(nil, os.ErrNotExist)

	mockFileSystem.On("ReadFile", "base/a/enums/column1").Return([]byte(fmt.Sprintf("foo%sbar", common.EnumDelimiter)), nil)
	mockFileSystem.On("ReadFile", "base/a/enums/column4").Return([]byte(fmt.Sprintf("foo%sbar", common.EnumDelimiter)), nil)
	mockFileSystem.On("ReadFile", "base/a/enums/bad_col").Return(nil, os.ErrNotExist)

	mockFileSystem.On("ReadFile", "base/a/shards/0/version").Return([]byte("1"), nil)
	mockFileSystem.On("ReadFile", "base/a/shards/0/redolog-offset").Return([]byte("1,0"), nil)
	mockFileSystem.On("ReadFile", "base/b/shards/0/snapshot").Return([]byte("1,0,-1,1"), nil)
	mockFileSystem.On("ReadFile", "base/c/shards/0/version").Return([]byte("1"), nil)
	mockFileSystem.On("ReadFile", "base/notexist/shards/0/redolog-offset").Return(nil, os.ErrNotExist)
	mockFileSystem.On("ReadFile", "base/nopermission/shards/0/redolog-offset").Return(nil, os.ErrPermission)
	mockFileSystem.On("ReadFile", "base/bad1/shards/0/redolog-offset").Return([]byte("10"), nil)
	mockFileSystem.On("ReadFile", "base/bad2/shards/0/redolog-offset").Return([]byte("a,0"), nil)
	mockFileSystem.On("ReadFile", "base/bad3/shards/0/redolog-offset").Return([]byte("1,a"), nil)

	mockFileSystem.On("OpenFileForWrite", "base/a/schema", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.FileMode(0644)).Return(mockWriterCloser, nil)
	mockFileSystem.On("OpenFileForWrite", "base/c/schema", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.FileMode(0644)).Return(mockWriterCloser, nil)
	mockFileSystem.On("OpenFileForWrite", "base/a/shards/0/version", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.FileMode(0644)).Return(mockWriterCloser, nil)
	mockFileSystem.On("OpenFileForWrite", "base/a/shards/0/redolog-offset", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.FileMode(0644)).Return(mockWriterCloser, nil)
	mockFileSystem.On("OpenFileForWrite", "base/b/shards/0/snapshot", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.FileMode(0644)).Return(mockWriterCloser, nil)
	mockFileSystem.On("OpenFileForWrite", "base/a/enums/column1", os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.FileMode(0644)).Return(mockWriterCloser, nil)
	mockFileSystem.On("OpenFileForWrite", "base/c/shards/0/batches/1", os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.FileMode(0644)).Return(mockWriterCloser, nil)
	mockFileSystem.On("OpenFileForWrite", "base/a/shards/0/redolog-offset", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.FileMode(0644)).Return(mockWriterCloser, nil)
	mockFileSystem.On("OpenFileForWrite", "base/b/shards/0/redolog-offset", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.FileMode(0644)).Return(nil, os.ErrPermission)

	mockFileSystem.On("MkdirAll", "base", os.FileMode(0755)).Return(nil)
	mockFileSystem.On("MkdirAll", "wrongpath", os.FileMode(0755)).Return(os.ErrInvalid)
	mockFileSystem.On("MkdirAll", "base/c", os.FileMode(0755)).Return(nil)
	mockFileSystem.On("MkdirAll", "base/c/shards/0/batches", os.FileMode(0755)).Return(nil)
	mockFileSystem.On("MkdirAll", "base/a/enums", os.FileMode(0755)).Return(nil)
	mockFileSystem.On("MkdirAll", "base/a/shards/0", os.FileMode(0755)).Return(nil)
	mockFileSystem.On("MkdirAll", "base/b/shards/0", os.FileMode(0755)).Return(nil)
	mockFileSystem.On("MkdirAll", "base/c/shards/0", os.FileMode(0755)).Return(nil)
	mockFileSystem.On("MkdirAll", "base/d/shards/0", os.FileMode(0755)).Return(os.ErrPermission)


	mockFileSystem.On("RemoveAll", "base/b").Return(nil)
	mockFileSystem.On("Remove", "base/a/enums/column4").Return(nil)

	var createDiskMetastore = func(basepath string) *diskMetaStore {
		diskMetaStore := &diskMetaStore{
			FileSystem:       mockFileSystem,
			writeLock:        sync.Mutex{},
			basePath:         basepath,
			enumDictWatchers: make(map[string]map[string]chan<- string),
			enumDictDone:     make(map[string]map[string]<-chan struct{}),
		}
		return diskMetaStore
	}

	ginkgo.BeforeEach(func() {
		mockWriterCloser.Reset()
	})

	ginkgo.It("ListTables", func() {
		diskMetaStore := createDiskMetastore("base")
		tables, err := diskMetaStore.ListTables()
		Ω(err).Should(BeNil())
		Ω(tables).Should(ContainElement("a"))
		Ω(tables).Should(ContainElement("b"))
	})

	ginkgo.It("GetTable", func() {
		diskMetaStore := createDiskMetastore("base")
		table, err := diskMetaStore.GetTable("a")
		Ω(err).Should(BeNil())
		Ω(*table).Should(Equal(testTableA))
		_, err = diskMetaStore.GetTable("unknown")
		Ω(err).Should(Equal(ErrTableDoesNotExist))
	})

	ginkgo.It("GetOwnedShards", func() {
		diskMetaStore := createDiskMetastore("base")
		res, err := diskMetaStore.GetOwnedShards("a")
		Ω(err).Should(BeNil())
		Ω(res[0]).Should(Equal(0))
	})

	ginkgo.It("GetEnumDict", func() {
		diskMetaStore := createDiskMetastore("base")
		enumCases, err := diskMetaStore.GetEnumDict("a", "column1")
		Ω(err).Should(BeNil())
		Ω(enumCases).Should(Equal([]string{"foo", "bar"}))

		enumCases, err = diskMetaStore.GetEnumDict("unknown", "column1")
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("GetArchivingCutoff", func() {
		diskMetaStore := createDiskMetastore("base")
		archivingCutoff, err := diskMetaStore.GetArchivingCutoff("a", 0)
		Ω(err).Should(BeNil())
		Ω(archivingCutoff).Should(Equal(uint32(1)))

		archivingCutoff, err = diskMetaStore.GetArchivingCutoff("unknown", 0)
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("GetSnapshotProgress", func() {
		diskMetaStore := createDiskMetastore("base")
		redoLogFile, offset, batchID, record, err := diskMetaStore.GetSnapshotProgress("b", 0)
		Ω(err).Should(BeNil())
		Ω(redoLogFile).Should(Equal(int64(1)))
		Ω(offset).Should(Equal(uint32(0)))
		Ω(batchID).Should(Equal(int32(-1)))
		Ω(record).Should(Equal(uint32(1)))

		redoLogFile, offset, batchID, record, err = diskMetaStore.GetSnapshotProgress("unknown", 0)
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("UpdateArchivingCutoff", func() {
		diskMetastore := createDiskMetastore("base")
		err := diskMetastore.UpdateArchivingCutoff("a", 0, 1)
		Ω(err).Should(BeNil())
		Ω(mockWriterCloser.Bytes()).Should(Equal([]byte("1")))

		err = diskMetastore.UpdateArchivingCutoff("unknown", 0, 1)
		Ω(err).ShouldNot(BeNil())
		err = diskMetastore.UpdateArchivingCutoff("unknown_shard", 0, 1)
		Ω(err).ShouldNot(BeNil())
		err = diskMetastore.UpdateArchivingCutoff("read_fail", 0, 1)
		Ω(err).ShouldNot(BeNil())
		err = diskMetastore.UpdateArchivingCutoff("b", 0, 1)
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("UpdateSnapshotProgress", func() {
		diskMetastore := createDiskMetastore("base")
		err := diskMetastore.UpdateSnapshotProgress("b", 0, 1, 0, 1, 1)
		Ω(err).Should(BeNil())
		Ω(mockWriterCloser.Bytes()).Should(Equal([]byte("1,0,1,1")))

		err = diskMetastore.UpdateArchivingCutoff("unknown", 0, 1)
		Ω(err).ShouldNot(BeNil())
		err = diskMetastore.UpdateSnapshotProgress("unknown_shard", 0, 1, 0, 1, 1)
		Ω(err).ShouldNot(BeNil())
		err = diskMetastore.UpdateSnapshotProgress("read_fail", 0, 1, 0, 1, 1)
		Ω(err).ShouldNot(BeNil())
		err = diskMetastore.UpdateSnapshotProgress("a", 0, 1, 0, 1, 1)
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("UpdateBackfillProgress", func() {
		diskMetastore := createDiskMetastore("base")
		err := diskMetastore.UpdateBackfillProgress("unknown_shard", 0, 1, 0)
		Ω(err).ShouldNot(BeNil())
		err = diskMetastore.UpdateBackfillProgress("read_fail", 0, 1, 0)
		Ω(err).ShouldNot(BeNil())
		err = diskMetastore.UpdateArchivingCutoff("b", 0, 1)
		Ω(err).ShouldNot(BeNil())

		err = diskMetastore.UpdateArchivingCutoff("a", 0, 1)
		Ω(err).Should(BeNil())
		Ω(mockWriterCloser.Bytes()).Should(Equal([]byte("1")))
	})

	ginkgo.It("GetBackfillProgressInfo", func() {
		diskMetastore := createDiskMetastore("base")
		redoLogFile, offset, err := diskMetastore.GetBackfillProgressInfo("a", 0)
		Ω(err).Should(BeNil())
		Ω(redoLogFile).Should(Equal(int64(1)))
		Ω(offset).Should(Equal(uint32(0)))

		_, _, err = diskMetastore.GetBackfillProgressInfo("unknown_shard", 0)
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("WatchTableListEvents", func() {
		diskMetastore := createDiskMetastore("base")
		events, done, err := diskMetastore.WatchTableListEvents()
		Ω(err).Should(BeNil())
		Ω(events).ShouldNot(BeNil())
		Ω(done).ShouldNot(BeNil())

		// rewatch will fail
		_, _, err = diskMetastore.WatchTableListEvents()
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("WatchTableSchemaEvents", func() {
		diskMetastore := createDiskMetastore("base")
		events, done, err := diskMetastore.WatchTableSchemaEvents()
		Ω(err).Should(BeNil())
		Ω(events).ShouldNot(BeNil())
		Ω(done).ShouldNot(BeNil())

		//rewatch will fail
		_, _, err = diskMetastore.WatchTableSchemaEvents()
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("WatchEnumDictEvents", func() {
		diskMetastore := createDiskMetastore("base")
		events, done, err := diskMetastore.WatchEnumDictEvents("a", "column1", 0)
		Ω(err).Should(BeNil())
		Ω(events).ShouldNot(BeNil())
		Ω(done).ShouldNot(BeNil())

		enumCases := []string{}
		enumCase := <-events
		enumCases = append(enumCases, enumCase)
		enumCase = <-events
		enumCases = append(enumCases, enumCase)
		Ω(enumCases).Should(Equal([]string{"foo", "bar"}))

		events, done, err = diskMetastore.WatchEnumDictEvents("unknown", "column1", 0)
		Ω(err).ShouldNot(BeNil())
		events, done, err = diskMetastore.WatchEnumDictEvents("a", "column1", 0)
		Ω(err).ShouldNot(BeNil())
		events, done, err = diskMetastore.WatchEnumDictEvents("a", "bad_col", 0)
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("WatchShardOwnershipEvents", func() {
		diskMetaStore := createDiskMetastore("base")
		events, done, err := diskMetaStore.WatchShardOwnershipEvents()
		Ω(err).Should(BeNil())
		Ω(events).ShouldNot(BeNil())
		Ω(done).ShouldNot(BeNil())

		//rewatch will fail
		_, _, err = diskMetaStore.WatchShardOwnershipEvents()
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("CreateTable", func() {
		diskMetaStore := createDiskMetastore("base")
		err := diskMetaStore.CreateTable(&testTableA)
		Ω(err).Should(Equal(ErrTableAlreadyExist))

		// should work without watchers
		err = diskMetaStore.CreateTable(&testTableC)
		Ω(err).Should(BeNil())
		Ω(mockWriterCloser.Bytes()).Should(Equal(testTableCBytes))
		mockWriterCloser.Reset()

		// watch schema change
		events, done, err := diskMetaStore.WatchTableSchemaEvents()
		Ω(err).Should(BeNil())
		var schemaEvent *common.Table
		go func() {
			schemaEvent = <-events
			done <- struct{}{}
		}()

		ownershipEvents, done2, err := diskMetaStore.WatchShardOwnershipEvents()
		Ω(err).Should(BeNil())
		go func() {
			<-ownershipEvents
			done2 <- struct{}{}
		}()

		err = diskMetaStore.CreateTable(&testTableC)
		Ω(err).Should(BeNil())
		Ω(mockWriterCloser.Bytes()).Should(Equal(testTableCBytes))

		// watcher should got the change before CreateTable return
		Ω(*schemaEvent).Should(Equal(testTableC))
	})

	ginkgo.It("DeleteTable", func() {
		diskMetaStore := createDiskMetastore("base")
		err := diskMetaStore.DeleteTable(testTableC.Name)
		Ω(err).Should(Equal(ErrTableDoesNotExist))

		// should work without watchers
		err = diskMetaStore.DeleteTable(testTableB.Name)
		Ω(err).Should(BeNil())

		events, done, err := diskMetaStore.WatchTableListEvents()
		Ω(err).Should(BeNil())
		var newTables []string
		go func() {
			newTables = <-events
			done <- struct{}{}
		}()

		err = diskMetaStore.DeleteTable(testTableB.Name)
		Ω(err).Should(BeNil())
		Ω(newTables).Should(Equal([]string{"a"}))
	})

	ginkgo.It("AddColumn", func() {
		diskMetaStore := createDiskMetastore("base")
		err := diskMetaStore.AddColumn("unknown", testColumn1, true)
		Ω(err).Should(Equal(ErrTableDoesNotExist))

		err = diskMetaStore.AddColumn(testTableA.Name, testColumn1, true)
		Ω(err).Should(Equal(ErrDuplicatedColumnName))

		err = diskMetaStore.AddColumn(testTableA.Name, testColumn2, true)
		Ω(err).Should(BeNil())

		var newTableA common.Table
		json.Unmarshal(mockWriterCloser.Bytes(), &newTableA)
		Ω(newTableA.Name).Should(Equal(testTableA.Name))
		Ω(newTableA.Columns).Should(Equal([]common.Column{testColumn0, testColumn1, testColumn3, testColumn4, testColumn5, testColumn2}))
		Ω(newTableA.ArchivingSortColumns).Should(Equal([]int{2, 5}))
	})

	ginkgo.It("AddEnumColumnWithDefaultValue", func() {
		col6DefaultValue := "default"
		testColumn6 := common.Column{
			Name:         "column6",
			Type:         common.BigEnum,
			DefaultValue: &col6DefaultValue,
		}

		mockWriterCloser2 := &testing.TestReadWriteCloser{}
		mockFileSystem.On("OpenFileForWrite", "base/a/enums/column6", os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.FileMode(0644)).Return(mockWriterCloser2, nil)

		diskMetaStore := createDiskMetastore("base")
		err := diskMetaStore.AddColumn(testTableA.Name, testColumn6, true)
		Ω(err).Should(BeNil())

		var newTableA common.Table
		json.Unmarshal(mockWriterCloser.Bytes(), &newTableA)
		Ω(newTableA.Name).Should(Equal(testTableA.Name))
		Ω(newTableA.Columns).Should(Equal([]common.Column{testColumn0, testColumn1, testColumn3, testColumn4, testColumn5, testColumn6}))
		Ω(newTableA.ArchivingSortColumns).Should(Equal([]int{2, 5}))

		Ω(string(mockWriterCloser2.Bytes())).Should(Equal("default\u0000\n"))
	})

	ginkgo.It("DeleteColumn", func() {
		diskMetaStore := createDiskMetastore("base")
		err := diskMetaStore.DeleteColumn("unknown", testColumn1.Name)
		Ω(err).Should(Equal(ErrTableDoesNotExist))

		err = diskMetaStore.DeleteColumn(testTableA.Name, "unknown")
		Ω(err).Should(Equal(ErrColumnDoesNotExist))

		err = diskMetaStore.DeleteColumn(testTableA.Name, testColumn0.Name)
		Ω(err).Should(Equal(ErrDeleteTimeColumn))

		events, done, err := diskMetaStore.WatchTableSchemaEvents()
		Ω(err).Should(BeNil())
		var newTable *common.Table
		go func(events <-chan *common.Table, done chan<- struct{}) {
			newTable = <-events
			done <- struct{}{}
		}(events, done)

		enumEvents, enumDone, err := diskMetaStore.WatchEnumDictEvents(testTableA.Name, testColumn4.Name, 0)
		Ω(err).Should(BeNil())
		go func(enumEvents <-chan string, done chan<- struct{}) {
			for range enumEvents {
			}
			close(enumDone)
		}(enumEvents, enumDone)

		err = diskMetaStore.DeleteColumn(testTableA.Name, testColumn4.Name)
		Ω(err).Should(BeNil())
		for _, column := range newTable.Columns {
			if column.Name == testColumn4.Name {
				Ω(column.Deleted).Should(BeTrue())
			}
		}
	})

	ginkgo.It("UpdateColumn", func() {
		diskMetaStore := createDiskMetastore("base")
		err := diskMetaStore.UpdateColumn("unknown", testColumn1.Name, testColumnConfig1)
		Ω(err).Should(Equal(ErrTableDoesNotExist))

		err = diskMetaStore.UpdateColumn(testTableA.Name, "unknown", testColumnConfig1)
		Ω(err).Should(Equal(ErrColumnDoesNotExist))

		err = diskMetaStore.UpdateColumn(testTableA.Name, testColumn5.Name, testColumnConfig1)
		Ω(err).Should(Equal(ErrColumnDoesNotExist))

		events, done, err := diskMetaStore.WatchTableSchemaEvents()
		Ω(err).Should(BeNil())
		var newTable *common.Table
		go func(events <-chan *common.Table, done chan<- struct{}) {
			newTable = <-events
			done <- struct{}{}
		}(events, done)

		enumEvents, enumDone, err := diskMetaStore.WatchEnumDictEvents(testTableA.Name, testColumn4.Name, 0)
		Ω(err).Should(BeNil())
		go func(enumEvents <-chan string, done chan<- struct{}) {
			for range enumEvents {
			}
			close(enumDone)
		}(enumEvents, enumDone)

		err = diskMetaStore.UpdateColumn(testTableA.Name, testColumn1.Name, testColumnConfig1)
		Ω(err).Should(BeNil())
		for _, column := range newTable.Columns {
			if column.Name == testColumn1.Name {
				Ω(column.Config).Should(Equal(testColumnConfig1))
			}
		}
	})

	ginkgo.It("ExtendEnumDict", func() {
		diskMetaStore := createDiskMetastore("base")
		enumIDs, err := diskMetaStore.ExtendEnumDict(testTableA.Name, testColumn1.Name, []string{"hello", "world"})
		Ω(err).Should(BeNil())
		Ω(enumIDs).Should(Equal([]int{2, 3}))
	})

	ginkgo.It("AddArchiveBatchVersion: seqNum is 0", func() {
		diskMetaStore := createDiskMetastore("base")
		// seqNum is 0
		err := diskMetaStore.AddArchiveBatchVersion(testTableC.Name, 0, 1, 1, 0, 10)
		Ω(err).Should(BeNil())
		Ω(mockWriterCloser.Bytes()).Should(Equal([]byte("1,10\n")))
	})

	ginkgo.It("AddArchiveBatchVersion: seqNum is not 0", func() {
		// seqNum is 2
		diskMetaStore := createDiskMetastore("base")
		err := diskMetaStore.AddArchiveBatchVersion(testTableC.Name, 0, 1, 1, 2, 15)
		Ω(err).Should(BeNil())
		Ω(mockWriterCloser.Bytes()).Should(Equal([]byte("1-2,15\n")))
	})

	ginkgo.It("GetArchiveBatchVersion", func() {
		diskMetaStore := createDiskMetastore("base")
		mockFileSystem.On("ReadFile", "base/c/shards/0/batches/1").Return([]byte("1,10\n2,20\n4,40\n"), nil).Once()
		version, seqNum, size, err := diskMetaStore.GetArchiveBatchVersion(testTableC.Name, 0, 1, 5)
		Ω(err).Should(BeNil())
		Ω(version).Should(Equal(uint32(4)))
		Ω(seqNum).Should(Equal(uint32(0)))
		Ω(size).Should(Equal(40))

		mockFileSystem.On("ReadFile", "base/c/shards/0/batches/1").Return([]byte("1,10\n2,20\n4,40\n"), nil).Once()
		version, seqNum, size, err = diskMetaStore.GetArchiveBatchVersion(testTableC.Name, 0, 1, 3)
		Ω(err).Should(BeNil())
		Ω(version).Should(Equal(uint32(2)))
		Ω(seqNum).Should(Equal(uint32(0)))
		Ω(size).Should(Equal(20))

		mockFileSystem.On("ReadFile", "base/c/shards/0/batches/1").Return([]byte("2,20\n4,40\n"), nil).Once()
		_, _, _, err = diskMetaStore.GetArchiveBatchVersion(testTableC.Name, 0, 1, 1)
		Ω(err).Should(BeNil())
	})

	ginkgo.It("UpdataTable", func() {
		diskMetaStore := createDiskMetastore("base")

		// should work without watchers
		err := diskMetaStore.UpdateTable(testTableC)
		Ω(err).Should(BeNil())
		Ω(mockWriterCloser.Bytes()).Should(Equal(testTableCBytes))
		mockWriterCloser.Reset()

		// watch schema change
		events, done, err := diskMetaStore.WatchTableSchemaEvents()
		Ω(err).Should(BeNil())
		var schemaEvent *common.Table
		go func() {
			schemaEvent = <-events
			done <- struct{}{}
		}()

		err = diskMetaStore.UpdateTable(testTableC)
		Ω(err).Should(BeNil())
		Ω(mockWriterCloser.Bytes()).Should(Equal(testTableCBytes))

		// watcher should got the change before UpdataTable return
		Ω(*schemaEvent).Should(Equal(testTableC))
	})

	ginkgo.It("UpdateTableConfig", func() {
		diskMetaStore := createDiskMetastore("base")
		updateConfig := common.TableConfig{
			ArchivingDelayMinutes:    60,
			ArchivingIntervalMinutes: 24 * 60,
			BackfillMaxBufferSize:    1 << 32,
			BackfillIntervalMinutes:  60,
			BackfillThresholdInBytes: 1 << 21,
			BackfillStoreBatchSize:   20000,
			BatchSize:                10,
			SnapshotThreshold:        10,
		}
		err := diskMetaStore.UpdateTableConfig(testTableA.Name, updateConfig)
		Ω(err).Should(BeNil())

		var newTable common.Table
		err = json.Unmarshal(mockWriterCloser.Bytes(), &newTable)
		Ω(err).Should(BeNil())
		Ω(newTable.Config).Should(Equal(updateConfig))
	})

	ginkgo.It("PurgeArchiveBatches", func() {
		diskMetaStore := createDiskMetastore("base")
		mockBatch1 := &mocks.FileInfo{}
		mockBatch2 := &mocks.FileInfo{}
		mockBatch1.On("Name").Return("1")
		mockBatch2.On("Name").Return("2")

		mockFileSystem.On("ReadDir", "base/c/shards/0/batches").Return([]os.FileInfo{mockBatch1, mockBatch2}, nil).Once()
		mockFileSystem.On("Remove", "base/c/shards/0/batches/1").Return(nil).Once()
		err := diskMetaStore.PurgeArchiveBatches(testTableC.Name, 0, 0, 2)
		Ω(err).Should(BeNil())

		mockFileSystem.On("ReadDir", "base/c/shards/0/batches").Return([]os.FileInfo{mockBatch1, mockBatch2}, nil).Once()
		mockFileSystem.On("Remove", "base/c/shards/0/batches/1").Return(os.ErrNotExist).Once()
		err = diskMetaStore.PurgeArchiveBatches(testTableC.Name, 0, 0, 2)
		Ω(err).Should(BeNil())

		mockFileSystem.On("ReadDir", "base/c/shards/0/batches").Return(nil, os.ErrNotExist).Once()
		err = diskMetaStore.PurgeArchiveBatches(testTableC.Name, 0, 0, 2)
		Ω(err).Should(BeNil())
	})

	ginkgo.It("readRedoLogFileAndOffset", func() {
		diskMetaStore := createDiskMetastore("base")
		redoLogFile, offset, err := diskMetaStore.readRedoLogFileAndOffset("base/notexist/shards/0/redolog-offset")
		Ω(redoLogFile).Should(Equal(int64(0)))
		Ω(offset).Should(Equal(uint32(0)))
		redoLogFile, offset, err = diskMetaStore.readRedoLogFileAndOffset("base/nopermission/shards/0/redolog-offset")
		Ω(err).ShouldNot(BeNil())

		redoLogFile, offset, err = diskMetaStore.readRedoLogFileAndOffset("base/bad1/shards/0/redolog-offset")
		Ω(err).ShouldNot(BeNil())
		redoLogFile, offset, err = diskMetaStore.readRedoLogFileAndOffset("base/bad2/shards/0/redolog-offset")
		Ω(err).ShouldNot(BeNil())
		redoLogFile, offset, err = diskMetaStore.readRedoLogFileAndOffset("base/bad3/shards/0/redolog-offset")
		Ω(err).ShouldNot(BeNil())

		redoLogFile, offset, err = diskMetaStore.readRedoLogFileAndOffset("base/a/shards/0/redolog-offset")
		Ω(err).Should(BeNil())
	})

	ginkgo.It("writeRedoLogVersionAndOffset", func() {
		diskMetaStore := createDiskMetastore("base")
		err := diskMetaStore.writeRedoLogVersionAndOffset("base/a/shards/0/redolog-offset", 1, 1)
		Ω(err).Should(BeNil())
		err = diskMetaStore.writeRedoLogVersionAndOffset("base/b/shards/0/redolog-offset", 1, 1)
		Ω(err).ShouldNot(BeNil())
		err = diskMetaStore.writeRedoLogVersionAndOffset("base/d/shards/0/redolog-offset", 1, 1)
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("enumColumnExists", func() {
		diskMetaStore := createDiskMetastore("base")
		err := diskMetaStore.enumColumnExists("unknown", "col1")
		Ω(err).ShouldNot(BeNil())

		err = diskMetaStore.enumColumnExists("error", "col1")
		Ω(err).ShouldNot(BeNil())

		err = diskMetaStore.enumColumnExists("read_fail", "col1")
		Ω(err).ShouldNot(BeNil())

		err = diskMetaStore.enumColumnExists("a", "column0")
		Ω(err).ShouldNot(BeNil())

		err = diskMetaStore.enumColumnExists("a", "column1")
		Ω(err).Should(BeNil())

		err = diskMetaStore.enumColumnExists("a", "column5")
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("shardExists", func() {
		diskMetaStore := createDiskMetastore("base")
		err := diskMetaStore.shardExists("a", 0)
		Ω(err).Should(BeNil())

		err = diskMetaStore.shardExists("unknown", 0)
		Ω(err).ShouldNot(BeNil())

		err = diskMetaStore.shardExists("error", 0)
		Ω(err).ShouldNot(BeNil())

		err = diskMetaStore.shardExists("unknown_shard", 0)
		Ω(err).ShouldNot(BeNil())

		err = diskMetaStore.shardExists("error_shard", 0)
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("CreateShard", func() {
		diskMetaStore := createDiskMetastore("base")
		err := diskMetaStore.createShard("c", true, 0)
		Ω(err).Should(BeNil())
		err = diskMetaStore.createShard("b", false, 0)
		Ω(err).Should(BeNil())
		err = diskMetaStore.createShard("d", false, 0)
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("NewDiskMetaStore", func() {
		diskMetaStore, err := NewDiskMetaStore("/tmp/ares_testdir")
		Ω(err).Should(BeNil())
		Ω(diskMetaStore).ShouldNot(BeNil())

		fs := utils.OSFileSystem{}
		fs.RemoveAll("/tmp/ares_testdir")
	})
})
