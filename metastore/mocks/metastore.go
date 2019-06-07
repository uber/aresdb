// Code generated by mockery v1.0.0. DO NOT EDIT.
package mocks

import common "github.com/uber/aresdb/metastore/common"
import mock "github.com/stretchr/testify/mock"

// MetaStore is an autogenerated mock type for the MetaStore type
type MetaStore struct {
	mock.Mock
}

// AddArchiveBatchVersion provides a mock function with given fields: table, shard, batchID, version, seqNum, batchSize
func (_m *MetaStore) AddArchiveBatchVersion(table string, shard int, batchID int, version uint32, seqNum uint32, batchSize int) error {
	ret := _m.Called(table, shard, batchID, version, seqNum, batchSize)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, int, int, uint32, uint32, int) error); ok {
		r0 = rf(table, shard, batchID, version, seqNum, batchSize)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// AddColumn provides a mock function with given fields: table, column, appendToArchivingSortOrder
func (_m *MetaStore) AddColumn(table string, column common.Column, appendToArchivingSortOrder bool) error {
	ret := _m.Called(table, column, appendToArchivingSortOrder)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, common.Column, bool) error); ok {
		r0 = rf(table, column, appendToArchivingSortOrder)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// CreateTable provides a mock function with given fields: table
func (_m *MetaStore) CreateTable(table *common.Table) error {
	ret := _m.Called(table)

	var r0 error
	if rf, ok := ret.Get(0).(func(*common.Table) error); ok {
		r0 = rf(table)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// DeleteColumn provides a mock function with given fields: table, column
func (_m *MetaStore) DeleteColumn(table string, column string) error {
	ret := _m.Called(table, column)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, string) error); ok {
		r0 = rf(table, column)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// DeleteTable provides a mock function with given fields: name
func (_m *MetaStore) DeleteTable(name string) error {
	ret := _m.Called(name)

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(name)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ExtendEnumDict provides a mock function with given fields: table, column, enumCases
func (_m *MetaStore) ExtendEnumDict(table string, column string, enumCases []string) ([]int, error) {
	ret := _m.Called(table, column, enumCases)

	var r0 []int
	if rf, ok := ret.Get(0).(func(string, string, []string) []int); ok {
		r0 = rf(table, column, enumCases)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]int)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, string, []string) error); ok {
		r1 = rf(table, column, enumCases)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetArchiveBatchVersion provides a mock function with given fields: table, shard, batchID, cutoff
func (_m *MetaStore) GetArchiveBatchVersion(table string, shard int, batchID int, cutoff uint32) (uint32, uint32, int, error) {
	ret := _m.Called(table, shard, batchID, cutoff)

	var r0 uint32
	if rf, ok := ret.Get(0).(func(string, int, int, uint32) uint32); ok {
		r0 = rf(table, shard, batchID, cutoff)
	} else {
		r0 = ret.Get(0).(uint32)
	}

	var r1 uint32
	if rf, ok := ret.Get(1).(func(string, int, int, uint32) uint32); ok {
		r1 = rf(table, shard, batchID, cutoff)
	} else {
		r1 = ret.Get(1).(uint32)
	}

	var r2 int
	if rf, ok := ret.Get(2).(func(string, int, int, uint32) int); ok {
		r2 = rf(table, shard, batchID, cutoff)
	} else {
		r2 = ret.Get(2).(int)
	}

	var r3 error
	if rf, ok := ret.Get(3).(func(string, int, int, uint32) error); ok {
		r3 = rf(table, shard, batchID, cutoff)
	} else {
		r3 = ret.Error(3)
	}

	return r0, r1, r2, r3
}

// GetArchiveBatches provides a mock function with given fields: table, shard, start, end
func (_m *MetaStore) GetArchiveBatches(table string, shard int, start int32, end int32) ([]int, error) {
	ret := _m.Called(table, shard, start, end)

	var r0 []int
	if rf, ok := ret.Get(0).(func(string, int, int32, int32) []int); ok {
		r0 = rf(table, shard, start, end)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]int)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, int, int32, int32) error); ok {
		r1 = rf(table, shard, start, end)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetArchivingCutoff provides a mock function with given fields: table, shard
func (_m *MetaStore) GetArchivingCutoff(table string, shard int) (uint32, error) {
	ret := _m.Called(table, shard)

	var r0 uint32
	if rf, ok := ret.Get(0).(func(string, int) uint32); ok {
		r0 = rf(table, shard)
	} else {
		r0 = ret.Get(0).(uint32)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, int) error); ok {
		r1 = rf(table, shard)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetBackfillProgressInfo provides a mock function with given fields: table, shard
func (_m *MetaStore) GetBackfillProgressInfo(table string, shard int) (int64, uint32, error) {
	ret := _m.Called(table, shard)

	var r0 int64
	if rf, ok := ret.Get(0).(func(string, int) int64); ok {
		r0 = rf(table, shard)
	} else {
		r0 = ret.Get(0).(int64)
	}

	var r1 uint32
	if rf, ok := ret.Get(1).(func(string, int) uint32); ok {
		r1 = rf(table, shard)
	} else {
		r1 = ret.Get(1).(uint32)
	}

	var r2 error
	if rf, ok := ret.Get(2).(func(string, int) error); ok {
		r2 = rf(table, shard)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// GetEnumDict provides a mock function with given fields: table, column
func (_m *MetaStore) GetEnumDict(table string, column string) ([]string, error) {
	ret := _m.Called(table, column)

	var r0 []string
	if rf, ok := ret.Get(0).(func(string, string) []string); ok {
		r0 = rf(table, column)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]string)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, string) error); ok {
		r1 = rf(table, column)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetOwnedShards provides a mock function with given fields: table
func (_m *MetaStore) GetOwnedShards(table string) ([]int, error) {
	ret := _m.Called(table)

	var r0 []int
	if rf, ok := ret.Get(0).(func(string) []int); ok {
		r0 = rf(table)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]int)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(table)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetRedoLogCheckpointOffset provides a mock function with given fields: table, shard
func (_m *MetaStore) GetRedoLogCheckpointOffset(table string, shard int) (int64, error) {
	ret := _m.Called(table, shard)

	var r0 int64
	if rf, ok := ret.Get(0).(func(string, int) int64); ok {
		r0 = rf(table, shard)
	} else {
		r0 = ret.Get(0).(int64)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, int) error); ok {
		r1 = rf(table, shard)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetRedoLogCommitOffset provides a mock function with given fields: table, shard
func (_m *MetaStore) GetRedoLogCommitOffset(table string, shard int) (int64, error) {
	ret := _m.Called(table, shard)

	var r0 int64
	if rf, ok := ret.Get(0).(func(string, int) int64); ok {
		r0 = rf(table, shard)
	} else {
		r0 = ret.Get(0).(int64)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, int) error); ok {
		r1 = rf(table, shard)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetSnapshotProgress provides a mock function with given fields: table, shard
func (_m *MetaStore) GetSnapshotProgress(table string, shard int) (int64, uint32, int32, uint32, error) {
	ret := _m.Called(table, shard)

	var r0 int64
	if rf, ok := ret.Get(0).(func(string, int) int64); ok {
		r0 = rf(table, shard)
	} else {
		r0 = ret.Get(0).(int64)
	}

	var r1 uint32
	if rf, ok := ret.Get(1).(func(string, int) uint32); ok {
		r1 = rf(table, shard)
	} else {
		r1 = ret.Get(1).(uint32)
	}

	var r2 int32
	if rf, ok := ret.Get(2).(func(string, int) int32); ok {
		r2 = rf(table, shard)
	} else {
		r2 = ret.Get(2).(int32)
	}

	var r3 uint32
	if rf, ok := ret.Get(3).(func(string, int) uint32); ok {
		r3 = rf(table, shard)
	} else {
		r3 = ret.Get(3).(uint32)
	}

	var r4 error
	if rf, ok := ret.Get(4).(func(string, int) error); ok {
		r4 = rf(table, shard)
	} else {
		r4 = ret.Error(4)
	}

	return r0, r1, r2, r3, r4
}

// GetTable provides a mock function with given fields: name
func (_m *MetaStore) GetTable(name string) (*common.Table, error) {
	ret := _m.Called(name)

	var r0 *common.Table
	if rf, ok := ret.Get(0).(func(string) *common.Table); ok {
		r0 = rf(name)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*common.Table)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(name)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ListTables provides a mock function with given fields:
func (_m *MetaStore) ListTables() ([]string, error) {
	ret := _m.Called()

	var r0 []string
	if rf, ok := ret.Get(0).(func() []string); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]string)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// PurgeArchiveBatches provides a mock function with given fields: table, shard, batchIDStart, batchIDEnd
func (_m *MetaStore) PurgeArchiveBatches(table string, shard int, batchIDStart int, batchIDEnd int) error {
	ret := _m.Called(table, shard, batchIDStart, batchIDEnd)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, int, int, int) error); ok {
		r0 = rf(table, shard, batchIDStart, batchIDEnd)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// UpdateArchivingCutoff provides a mock function with given fields: table, shard, cutoff
func (_m *MetaStore) UpdateArchivingCutoff(table string, shard int, cutoff uint32) error {
	ret := _m.Called(table, shard, cutoff)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, int, uint32) error); ok {
		r0 = rf(table, shard, cutoff)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// UpdateBackfillProgress provides a mock function with given fields: table, shard, redoLogFile, offset
func (_m *MetaStore) UpdateBackfillProgress(table string, shard int, redoLogFile int64, offset uint32) error {
	ret := _m.Called(table, shard, redoLogFile, offset)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, int, int64, uint32) error); ok {
		r0 = rf(table, shard, redoLogFile, offset)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// UpdateColumn provides a mock function with given fields: table, column, config
func (_m *MetaStore) UpdateColumn(table string, column string, config common.ColumnConfig) error {
	ret := _m.Called(table, column, config)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, string, common.ColumnConfig) error); ok {
		r0 = rf(table, column, config)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// UpdateRedoLogCheckpointOffset provides a mock function with given fields: table, shard, offset
func (_m *MetaStore) UpdateRedoLogCheckpointOffset(table string, shard int, offset int64) error {
	ret := _m.Called(table, shard, offset)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, int, int64) error); ok {
		r0 = rf(table, shard, offset)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// UpdateRedoLogCommitOffset provides a mock function with given fields: table, shard, offset
func (_m *MetaStore) UpdateRedoLogCommitOffset(table string, shard int, offset int64) error {
	ret := _m.Called(table, shard, offset)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, int, int64) error); ok {
		r0 = rf(table, shard, offset)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// UpdateSnapshotProgress provides a mock function with given fields: table, shard, redoLogFile, upsertBatchOffset, lastReadBatchID, lastReadBatchOffset
func (_m *MetaStore) UpdateSnapshotProgress(table string, shard int, redoLogFile int64, upsertBatchOffset uint32, lastReadBatchID int32, lastReadBatchOffset uint32) error {
	ret := _m.Called(table, shard, redoLogFile, upsertBatchOffset, lastReadBatchID, lastReadBatchOffset)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, int, int64, uint32, int32, uint32) error); ok {
		r0 = rf(table, shard, redoLogFile, upsertBatchOffset, lastReadBatchID, lastReadBatchOffset)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// UpdateTable provides a mock function with given fields: table
func (_m *MetaStore) UpdateTable(table common.Table) error {
	ret := _m.Called(table)

	var r0 error
	if rf, ok := ret.Get(0).(func(common.Table) error); ok {
		r0 = rf(table)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// UpdateTableConfig provides a mock function with given fields: table, config
func (_m *MetaStore) UpdateTableConfig(table string, config common.TableConfig) error {
	ret := _m.Called(table, config)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, common.TableConfig) error); ok {
		r0 = rf(table, config)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// WatchEnumDictEvents provides a mock function with given fields: table, column, startCase
func (_m *MetaStore) WatchEnumDictEvents(table string, column string, startCase int) (<-chan string, chan<- struct{}, error) {
	ret := _m.Called(table, column, startCase)

	var r0 <-chan string
	if rf, ok := ret.Get(0).(func(string, string, int) <-chan string); ok {
		r0 = rf(table, column, startCase)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(<-chan string)
		}
	}

	var r1 chan<- struct{}
	if rf, ok := ret.Get(1).(func(string, string, int) chan<- struct{}); ok {
		r1 = rf(table, column, startCase)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(chan<- struct{})
		}
	}

	var r2 error
	if rf, ok := ret.Get(2).(func(string, string, int) error); ok {
		r2 = rf(table, column, startCase)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// WatchShardOwnershipEvents provides a mock function with given fields:
func (_m *MetaStore) WatchShardOwnershipEvents() (<-chan common.ShardOwnership, chan<- struct{}, error) {
	ret := _m.Called()

	var r0 <-chan common.ShardOwnership
	if rf, ok := ret.Get(0).(func() <-chan common.ShardOwnership); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(<-chan common.ShardOwnership)
		}
	}

	var r1 chan<- struct{}
	if rf, ok := ret.Get(1).(func() chan<- struct{}); ok {
		r1 = rf()
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(chan<- struct{})
		}
	}

	var r2 error
	if rf, ok := ret.Get(2).(func() error); ok {
		r2 = rf()
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// WatchTableListEvents provides a mock function with given fields:
func (_m *MetaStore) WatchTableListEvents() (<-chan []string, chan<- struct{}, error) {
	ret := _m.Called()

	var r0 <-chan []string
	if rf, ok := ret.Get(0).(func() <-chan []string); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(<-chan []string)
		}
	}

	var r1 chan<- struct{}
	if rf, ok := ret.Get(1).(func() chan<- struct{}); ok {
		r1 = rf()
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(chan<- struct{})
		}
	}

	var r2 error
	if rf, ok := ret.Get(2).(func() error); ok {
		r2 = rf()
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// WatchTableSchemaEvents provides a mock function with given fields:
func (_m *MetaStore) WatchTableSchemaEvents() (<-chan *common.Table, chan<- struct{}, error) {
	ret := _m.Called()

	var r0 <-chan *common.Table
	if rf, ok := ret.Get(0).(func() <-chan *common.Table); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(<-chan *common.Table)
		}
	}

	var r1 chan<- struct{}
	if rf, ok := ret.Get(1).(func() chan<- struct{}); ok {
		r1 = rf()
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(chan<- struct{})
		}
	}

	var r2 error
	if rf, ok := ret.Get(2).(func() error); ok {
		r2 = rf()
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}
