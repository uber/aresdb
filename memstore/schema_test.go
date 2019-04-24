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
	"errors"

	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	memCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/metastore"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/metastore/mocks"
)

var _ = ginkgo.Describe("memStoreImpl schema", func() {
	testColumn1 := metaCom.Column{
		Name:    "col1",
		Type:    metaCom.Bool,
		Deleted: false,
	}

	testColumn2 := metaCom.Column{
		Name:    "col2",
		Type:    metaCom.SmallEnum,
		Deleted: false,
	}

	testDeleteColumn2 := metaCom.Column{
		Name:    "col2",
		Type:    metaCom.SmallEnum,
		Deleted: true,
	}

	testColumn3 := metaCom.Column{
		Name:    "col3",
		Type:    metaCom.BigEnum,
		Deleted: false,
	}

	testColumn4 := metaCom.Column{
		Name:    "col4",
		Type:    metaCom.BigEnum,
		Deleted: false,
	}

	defaultBigEnumValue := "b"

	testColumn5 := metaCom.Column{
		Name:         "col5",
		Type:         metaCom.BigEnum,
		Deleted:      false,
		DefaultValue: &defaultBigEnumValue,
	}

	defaultIntValue := "123"

	testColumn6 := metaCom.Column{
		Name:         "col6",
		Type:         metaCom.Int32,
		Deleted:      false,
		DefaultValue: &defaultIntValue,
	}

	defaultInvalidValue := "123s"

	testColumn7 := metaCom.Column{
		Name:         "col7",
		Type:         metaCom.Int32,
		Deleted:      false,
		DefaultValue: &defaultInvalidValue,
	}

	defaultSmallEnumValue := "s"

	testColumn8 := metaCom.Column{
		Name:         "col8",
		Type:         metaCom.SmallEnum,
		Deleted:      false,
		DefaultValue: &defaultSmallEnumValue,
	}

	testTable := metaCom.Table{
		Name: "testTable",
		Columns: []metaCom.Column{
			testColumn1,
			testColumn2,
			testColumn3,
		},
		PrimaryKeyColumns: []int{1},
	}

	testNewTable := metaCom.Table{
		Name: "testTableNew",
		Columns: []metaCom.Column{
			testColumn1,
			testColumn2,
			testColumn3,
			testColumn4,
		},
		PrimaryKeyColumns: []int{1},
	}

	testModifiedTable := metaCom.Table{
		Name: "testTable",
		Columns: []metaCom.Column{
			testColumn1,
			testDeleteColumn2,
			testColumn3,
			testColumn4,
		},
		PrimaryKeyColumns: []int{1},
	}

	testTableWithDefaultValue := metaCom.Table{
		Name: "testTable",
		Columns: []metaCom.Column{
			testColumn1,
			testColumn5,
			testColumn6,
			testColumn7,
			testColumn8,
		},
		PrimaryKeyColumns: []int{1},
	}

	testColumn2EnumCases := []string{"a", "b", "c"}
	testColumn3EnumCases := []string{"d", "e"}

	mockMetastore := &mocks.MetaStore{}
	mockDiskstore := CreateMockDiskStore()
	var testHostMemoryManager *hostMemoryManager

	var getTestMemstore = func() *memStoreImpl {
		testMemstore := memStoreImpl{
			TableSchemas: make(map[string]*TableSchema),
			TableShards:  make(map[string]map[int]*TableShard),
			metaStore:    mockMetastore,
			diskStore:    mockDiskstore,
		}

		testMemstore.scheduler = newScheduler(&testMemstore)

		tableSchema := NewTableSchema(&testTable)
		tableSchema.createEnumDict(testColumn2.Name, testColumn2EnumCases)
		tableSchema.createEnumDict(testColumn3.Name, testColumn3EnumCases)
		testMemstore.TableSchemas[testTable.Name] = tableSchema

		testTableShard := NewTableShard(tableSchema, mockMetastore, mockDiskstore,
			NewHostMemoryManager(&testMemstore, 1<<32), 0)

		testMemstore.TableShards[testTable.Name] = map[int]*TableShard{
			0: testTableShard,
		}

		testHostMemoryManager = NewHostMemoryManager(&testMemstore, int64(1000)).(*hostMemoryManager)
		testMemstore.HostMemManager = testHostMemoryManager

		return &testMemstore
	}

	var destroyTestMemstore = func(testMemstore *memStoreImpl) {
		for _, shards := range testMemstore.TableShards {
			for _, shard := range shards {
				shard.Destruct()
			}
		}
	}

	ginkgo.It("NewTableSchema should work", func() {
		tableSchema := NewTableSchema(&testTable)
		Ω(tableSchema.Schema).Should(Equal(testTable))
		Ω(tableSchema.ColumnIDs).Should(Equal(map[string]int{
			testColumn1.Name: 0,
			testColumn2.Name: 1,
			testColumn3.Name: 2,
		}))
		Ω(tableSchema.ValueTypeByColumn).Should(Equal([]memCom.DataType{
			memCom.Bool,
			memCom.SmallEnum,
			memCom.BigEnum,
		}))
		Ω(tableSchema.PrimaryKeyBytes).Should(Equal(1))
		Ω(tableSchema.PrimaryKeyColumnTypes).Should(Equal([]memCom.DataType{memCom.SmallEnum}))
	})

	ginkgo.It("GetColumnDeletions should work", func() {
		tableSchema := NewTableSchema(&testModifiedTable)
		Ω(tableSchema.GetColumnDeletions()).Should(BeEquivalentTo([]bool{false, true, false, false}))

		tableSchema = NewTableSchema(&testTable)
		Ω(tableSchema.GetColumnDeletions()).Should(BeEquivalentTo([]bool{false, false, false}))
	})

	ginkgo.It("SetEnumDict should work", func() {
		tableSchema := NewTableSchema(&testTable)
		tableSchema.createEnumDict(testColumn2.Name, testColumn2EnumCases)
		tableSchema.createEnumDict(testColumn3.Name, testColumn3EnumCases)

		Ω(tableSchema.EnumDicts).Should(Equal(
			map[string]EnumDict{
				testColumn2.Name: {
					Capacity: 0x100,
					Dict: map[string]int{
						"a": 0,
						"b": 1,
						"c": 2,
					},
					ReverseDict: []string{"a", "b", "c"},
				},
				testColumn3.Name: {
					Capacity: 0x10000,
					Dict: map[string]int{
						"d": 0,
						"e": 1,
					},
					ReverseDict: []string{"d", "e"},
				},
			},
		))
	})

	ginkgo.It("FetchSchema should succeed with no metaStore error", func() {
		memstore := memStoreImpl{
			TableSchemas: make(map[string]*TableSchema),
			TableShards:  make(map[string]map[int]*TableShard),
			metaStore:    mockMetastore,
		}

		tableListEvents := make(chan []string, 1)
		tableSchemaEvents := make(chan *metaCom.Table, 1)
		enumCol2ChangeEvents := make(chan string, 1)
		enumCol3ChangeEvents := make(chan string, 1)
		doneChannel := make(chan struct{})

		var recvTableListEvents <-chan []string = tableListEvents
		var recvTableSchemaEvents <-chan *metaCom.Table = tableSchemaEvents
		var recvEnumCol2ChangeEvents <-chan string = enumCol2ChangeEvents
		var recvEnumCol3ChangeEvents <-chan string = enumCol3ChangeEvents
		var sendDoneChannel chan<- struct{} = doneChannel

		mockMetastore.On("ListTables").Return([]string{"testTable"}, nil).Once()
		mockMetastore.On("GetTable", testTable.Name).Return(&testTable, nil).Once()
		mockMetastore.On("GetEnumDict", testTable.Name, testColumn2.Name).Return(testColumn2EnumCases, nil).Once()
		mockMetastore.On("GetEnumDict", testTable.Name, testColumn3.Name).Return(testColumn3EnumCases, nil).Once()

		mockMetastore.On("WatchTableListEvents").Return(recvTableListEvents, sendDoneChannel, nil).Once()
		mockMetastore.On("WatchTableSchemaEvents").Return(recvTableSchemaEvents, sendDoneChannel, nil).Once()
		mockMetastore.On("WatchEnumDictEvents", testTable.Name, testColumn2.Name, len(testColumn2EnumCases)).Return(recvEnumCol2ChangeEvents, sendDoneChannel, nil).Once()
		mockMetastore.On("WatchEnumDictEvents", testTable.Name, testColumn3.Name, len(testColumn3EnumCases)).Return(recvEnumCol3ChangeEvents, sendDoneChannel, nil).Once()

		err := memstore.FetchSchema()
		Ω(err).Should(BeNil())
		Ω(memstore.TableSchemas[testTable.Name].Schema).Should(Equal(testTable))
		Ω(memstore.TableSchemas[testTable.Name].ColumnIDs).Should(Equal(map[string]int{
			testColumn1.Name: 0,
			testColumn2.Name: 1,
			testColumn3.Name: 2,
		}))
		Ω(memstore.TableSchemas[testTable.Name].ValueTypeByColumn).Should(Equal([]memCom.DataType{memCom.Bool, memCom.SmallEnum, memCom.BigEnum}))
		Ω(memstore.TableSchemas[testTable.Name].EnumDicts).Should(Equal(
			map[string]EnumDict{
				testColumn2.Name: {
					Capacity: 0x100,
					Dict: map[string]int{
						"a": 0,
						"b": 1,
						"c": 2,
					},
					ReverseDict: []string{"a", "b", "c"},
				},
				testColumn3.Name: {
					Capacity: 0x10000,
					Dict: map[string]int{
						"d": 0,
						"e": 1,
					},
					ReverseDict: []string{"d", "e"},
				},
			},
		))
	})

	ginkgo.It("FetchSchema should fail with metaStore errors", func() {
		memstore := memStoreImpl{
			TableSchemas: make(map[string]*TableSchema),
			TableShards:  make(map[string]map[int]*TableShard),
			metaStore:    mockMetastore,
		}

		tableListEvents := make(chan []string, 1)
		tableSchemaEvents := make(chan *metaCom.Table, 1)
		enumCol3ChangeEvents := make(chan string, 1)

		var recvTableListEvents <-chan []string = tableListEvents
		var recvTableSchemaEvents <-chan *metaCom.Table = tableSchemaEvents
		var recvEnumCol3ChangeEvents <-chan string = enumCol3ChangeEvents
		var doneChan chan<- struct{}

		// failed with ListTables error
		mockMetastore.On("ListTables").Return(nil, errors.New("Failure ListTables")).Once()
		err := memstore.FetchSchema()
		Ω(err).ShouldNot(BeNil())

		// failed with GetTable error
		mockMetastore.On("ListTables").Return([]string{testTable.Name}, nil).Once()
		mockMetastore.On("GetTable", testTable.Name).Return(nil, errors.New("Failure GetTable")).Once()
		err = memstore.FetchSchema()
		Ω(err).ShouldNot(BeNil())

		// failed with table no exist error
		mockMetastore.On("ListTables").Return([]string{testTable.Name}, nil).Once()
		mockMetastore.On("GetTable", testTable.Name).Return(nil, metastore.ErrTableDoesNotExist).Once()
		doneChan = make(chan struct{})
		mockMetastore.On("WatchTableListEvents").Return(recvTableListEvents, doneChan, nil).Once()
		doneChan = make(chan struct{})
		mockMetastore.On("WatchTableSchemaEvents").Return(recvTableSchemaEvents, doneChan, nil).Once()
		err = memstore.FetchSchema()
		Ω(err).Should(BeNil())

		// failed with GetEnumDict error
		mockMetastore.On("ListTables").Return([]string{"testTable"}, nil).Once()
		mockMetastore.On("GetTable", testTable.Name).Return(&testTable, nil).Once()
		mockMetastore.On("GetEnumDict", testTable.Name, testColumn2.Name).Return(nil, errors.New("Failure GetEnumDict")).Once()
		err = memstore.FetchSchema()
		Ω(err).ShouldNot(BeNil())

		// failed with column does not exist error
		mockMetastore.On("ListTables").Return([]string{"testTable"}, nil).Once()
		mockMetastore.On("GetTable", testTable.Name).Return(&testTable, nil).Once()
		mockMetastore.On("GetEnumDict", testTable.Name, testColumn2.Name).Return(nil, metastore.ErrColumnDoesNotExist).Once()
		mockMetastore.On("GetEnumDict", testTable.Name, testColumn3.Name).Return(testColumn3EnumCases, nil).Once()
		doneChan = make(chan struct{})
		mockMetastore.On("WatchTableListEvents").Return(recvTableListEvents, doneChan, nil).Once()
		doneChan = make(chan struct{})
		mockMetastore.On("WatchTableSchemaEvents").Return(recvTableSchemaEvents, doneChan, nil).Once()
		doneChan = make(chan struct{})
		mockMetastore.On("WatchEnumDictEvents", testTable.Name, testColumn3.Name, len(testColumn3EnumCases)).Return(recvEnumCol3ChangeEvents, doneChan, nil).Once()
		err = memstore.FetchSchema()
		Ω(err).Should(BeNil())

		close(tableListEvents)
		close(tableSchemaEvents)
		close(enumCol3ChangeEvents)
	})

	ginkgo.It("applyTableList should work", func() {
		testMemstore := getTestMemstore()
		mockDiskstore.On("DeleteTableShard", testTable.Name, 0).Return(nil)
		testMemstore.applyTableList([]string{})
		Ω(testMemstore.TableSchemas).Should(Equal(map[string]*TableSchema{}))
		Ω(testMemstore.TableShards).Should(Equal(map[string]map[int]*TableShard{}))
		destroyTestMemstore(testMemstore)
	})

	ginkgo.It("handleTableListChange should work", func() {
		testMemstore := getTestMemstore()

		tableListEvents := make(chan []string)
		doneChannel := make(chan struct{})

		var recvTableListEvents <-chan []string = tableListEvents
		var sendDoneChannel chan<- struct{} = doneChannel

		go testMemstore.handleTableListChange(recvTableListEvents, sendDoneChannel)

		tableListEvents <- []string{}
		// block until done
		<-doneChannel

		Ω(testMemstore.TableSchemas).Should(Equal(map[string]*TableSchema{}))
		Ω(testMemstore.TableShards).Should(Equal(map[string]map[int]*TableShard{}))
		destroyTestMemstore(testMemstore)
	})

	ginkgo.It("applyTableSchema should work with change events to existing table", func() {
		testMemstore := getTestMemstore()

		expectedColumnIDs := map[string]int{
			testColumn1.Name: 0,
			testColumn3.Name: 2,
			testColumn4.Name: 3,
		}

		expectedEnumDict := map[string]EnumDict{
			testColumn4.Name: {
				Capacity: 0x10000,
				Dict:     map[string]int{},
			},
			testColumn3.Name: {
				Capacity:    0x10000,
				ReverseDict: []string{"d", "e"},
				Dict: map[string]int{
					"d": 0,
					"e": 1,
				},
			},
		}

		enumCol4ChangeEvents := make(chan string, 1)
		var recvEnumCol4ChangeEvents <-chan string = enumCol4ChangeEvents
		doneChannel := make(chan struct{})
		var sendDoneChannel chan<- struct{} = doneChannel

		// test modifying a table
		mockMetastore.On("WatchEnumDictEvents", testModifiedTable.Name, testColumn4.Name, 0).Return(recvEnumCol4ChangeEvents, sendDoneChannel, nil).Once()
		testMemstore.applyTableSchema(&testModifiedTable)

		Ω(testMemstore.TableSchemas[testTable.Name].Schema).Should(Equal(testModifiedTable))
		Ω(testMemstore.TableSchemas[testTable.Name].ColumnIDs).Should(Equal(expectedColumnIDs))
		Ω(testMemstore.TableSchemas[testTable.Name].ValueTypeByColumn).Should(Equal([]memCom.DataType{memCom.Bool, memCom.SmallEnum, memCom.BigEnum, memCom.BigEnum}))
		Ω(testMemstore.TableSchemas[testTable.Name].EnumDicts).Should(Equal(expectedEnumDict))

		close(enumCol4ChangeEvents)
		destroyTestMemstore(testMemstore)
	})

	ginkgo.It("applyTableSchema should work with new table schema", func() {
		testMemstore := getTestMemstore()

		enumColChangeEvents := make(chan string, 1)

		var recvEnumColChangeEvents <-chan string = enumColChangeEvents
		var doneChan chan<- struct{}

		// test adding a new table

		doneChan = make(chan struct{})
		mockMetastore.On("WatchEnumDictEvents", testNewTable.Name, testColumn2.Name, 0).Return(recvEnumColChangeEvents, doneChan, nil).Once()
		doneChan = make(chan struct{})
		mockMetastore.On("WatchEnumDictEvents", testNewTable.Name, testColumn3.Name, 0).Return(recvEnumColChangeEvents, doneChan, nil).Once()
		doneChan = make(chan struct{})
		mockMetastore.On("WatchEnumDictEvents", testNewTable.Name, testColumn4.Name, 0).Return(recvEnumColChangeEvents, doneChan, nil).Once()

		testMemstore.applyTableSchema(&testNewTable)
		tableSchema, exist := testMemstore.TableSchemas[testNewTable.Name]
		Ω(exist).Should(BeTrue())
		Ω(tableSchema.Schema).Should(Equal(testNewTable))

		close(enumColChangeEvents)
		destroyTestMemstore(testMemstore)
	})

	ginkgo.It("handleTableSchema should work", func() {
		testMemstore := getTestMemstore()

		testTableChangeEvents := make(chan *metaCom.Table)
		enumCol4ChangeEvents := make(chan string)
		var recvTableChagneEvents <-chan *metaCom.Table = testTableChangeEvents
		var recvEnumCol4ChangeEvents <-chan string = enumCol4ChangeEvents

		var doneChannel1 chan<- struct{} = make(chan struct{})
		mockMetastore.On("WatchEnumDictEvents", testModifiedTable.Name, testColumn4.Name, 0).Return(recvEnumCol4ChangeEvents, doneChannel1, nil).Once()

		var doneChannel2 = make(chan struct{})
		go testMemstore.handleTableSchemaChange(recvTableChagneEvents, doneChannel2)
		testTableChangeEvents <- &testModifiedTable
		// block until processing is done
		<-doneChannel2

		expectedColumnIDs := map[string]int{
			testColumn1.Name: 0,
			testColumn3.Name: 2,
			testColumn4.Name: 3,
		}

		expectedEnumDict := map[string]EnumDict{
			testColumn4.Name: {
				Capacity: 0x10000,
				Dict:     map[string]int{},
			},
			testColumn3.Name: {
				Capacity:    0x10000,
				ReverseDict: []string{"d", "e"},
				Dict: map[string]int{
					"d": 0,
					"e": 1,
				},
			},
		}

		Ω(testMemstore.TableSchemas[testTable.Name].Schema).Should(Equal(testModifiedTable))
		Ω(testMemstore.TableSchemas[testTable.Name].ColumnIDs).Should(Equal(expectedColumnIDs))
		Ω(testMemstore.TableSchemas[testTable.Name].ValueTypeByColumn).Should(Equal([]memCom.DataType{memCom.Bool, memCom.SmallEnum, memCom.BigEnum, memCom.BigEnum}))
		Ω(testMemstore.TableSchemas[testTable.Name].EnumDicts).Should(Equal(expectedEnumDict))

		close(testTableChangeEvents)
		close(enumCol4ChangeEvents)
		destroyTestMemstore(testMemstore)
	})

	ginkgo.It("applyEnumCase should work", func() {
		testMemstore := getTestMemstore()

		oldEnumDict := EnumDict{
			Capacity: 0x100,
			Dict: map[string]int{
				"a": 0,
				"b": 1,
				"c": 2,
			},
			ReverseDict: []string{"a", "b", "c"},
		}

		newEnumDict := EnumDict{
			Capacity: 0x100,
			Dict: map[string]int{
				"a": 0,
				"b": 1,
				"c": 2,
				"d": 3,
			},
			ReverseDict: []string{"a", "b", "c", "d"},
		}

		testMemstore.applyEnumCase("unknown", testColumn2.Name, "d")
		Ω(testMemstore.TableSchemas[testTable.Name].EnumDicts[testColumn2.Name]).Should(Equal(oldEnumDict))
		testMemstore.applyEnumCase(testTable.Name, "unknown", "d")
		Ω(testMemstore.TableSchemas[testTable.Name].EnumDicts[testColumn2.Name]).Should(Equal(oldEnumDict))
		testMemstore.applyEnumCase(testTable.Name, testColumn2.Name, "d")
		Ω(testMemstore.TableSchemas[testTable.Name].EnumDicts[testColumn2.Name]).Should(Equal(newEnumDict))
		destroyTestMemstore(testMemstore)
	})

	ginkgo.It("handleEnumDictChange should work", func() {
		testMemstore := getTestMemstore()

		enumDictChanges := make(chan string)
		doneChannel := make(chan struct{})
		var recvEnumDictChanges <-chan string = enumDictChanges
		var sendDoneChannel chan<- struct{} = doneChannel

		go testMemstore.handleEnumDictChange(testTable.Name, testColumn2.Name, recvEnumDictChanges, sendDoneChannel)
		enumDictChanges <- "d"
		// block until processing done
		close(enumDictChanges)
		<-doneChannel
		newEnumDict := EnumDict{
			Capacity: 0x100,
			Dict: map[string]int{
				"a": 0,
				"b": 1,
				"c": 2,
				"d": 3,
			},
			ReverseDict: []string{"a", "b", "c", "d"},
		}
		Ω(testMemstore.TableSchemas[testTable.Name].EnumDicts[testColumn2.Name]).Should(Equal(newEnumDict))
		destroyTestMemstore(testMemstore)
	})

	ginkgo.It("SetDefaultValue should work", func() {
		testMemstore := getTestMemstore()
		tableSchema := NewTableSchema(&testTableWithDefaultValue)
		testMemstore.TableSchemas[tableSchema.Schema.Name] = tableSchema

		Ω(tableSchema.DefaultValues).Should(HaveLen(5))
		Ω(tableSchema.DefaultValues).Should(Equal([]*memCom.DataValue{nil,
			nil,
			nil,
			nil,
			nil,
		}))
		tableSchema.SetDefaultValue(0)
		Ω(tableSchema.DefaultValues[0]).ShouldNot(Equal(BeNil()))
		Ω(tableSchema.DefaultValues[0].Valid).Should(BeFalse())

		tableSchema.createEnumDict(testColumn5.Name, []string{defaultBigEnumValue})
		tableSchema.SetDefaultValue(1)
		Ω(tableSchema.DefaultValues[1].Valid).ShouldNot(BeFalse())
		Ω(*(*uint16)(tableSchema.DefaultValues[1].OtherVal)).Should(Equal(uint16(0)))

		tableSchema.SetDefaultValue(2)
		Ω(tableSchema.DefaultValues[2].Valid).ShouldNot(BeFalse())
		Ω(*(*int32)(tableSchema.DefaultValues[2].OtherVal)).Should(Equal(int32(123)))

		Ω(func() { tableSchema.SetDefaultValue(3) }).Should(Panic())

		tableSchema.createEnumDict(testColumn8.Name, []string{defaultSmallEnumValue})
		tableSchema.SetDefaultValue(4)
		Ω(tableSchema.DefaultValues[4].Valid).ShouldNot(BeFalse())
		Ω(*(*uint8)(tableSchema.DefaultValues[4].OtherVal)).Should(Equal(uint8(0)))
		Ω(tableSchema.GetColumnDeletions()).Should(BeEquivalentTo([]bool{false, false, false, false, false}))
		Ω(tableSchema.GetColumnIfNonNilDefault()).Should(BeEquivalentTo([]bool{false, true, true, true, true}))
		destroyTestMemstore(testMemstore)

	})

})
