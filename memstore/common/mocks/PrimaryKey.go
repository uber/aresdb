// Code generated by mockery v1.0.0. DO NOT EDIT.
package mocks

import common "github.com/uber/aresdb/memstore/common"
import mock "github.com/stretchr/testify/mock"

// PrimaryKey is an autogenerated mock type for the PrimaryKey type
type PrimaryKey struct {
	mock.Mock
}

// AllocatedBytes provides a mock function with given fields:
func (_m *PrimaryKey) AllocatedBytes() uint {
	ret := _m.Called()

	var r0 uint
	if rf, ok := ret.Get(0).(func() uint); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(uint)
	}

	return r0
}

// Capacity provides a mock function with given fields:
func (_m *PrimaryKey) Capacity() uint {
	ret := _m.Called()

	var r0 uint
	if rf, ok := ret.Get(0).(func() uint); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(uint)
	}

	return r0
}

// Delete provides a mock function with given fields: key
func (_m *PrimaryKey) Delete(key common.Key) {
	_m.Called(key)
}

// Destruct provides a mock function with given fields:
func (_m *PrimaryKey) Destruct() {
	_m.Called()
}

// Find provides a mock function with given fields: key
func (_m *PrimaryKey) Find(key common.Key) (common.RecordID, bool) {
	ret := _m.Called(key)

	var r0 common.RecordID
	if rf, ok := ret.Get(0).(func(common.Key) common.RecordID); ok {
		r0 = rf(key)
	} else {
		r0 = ret.Get(0).(common.RecordID)
	}

	var r1 bool
	if rf, ok := ret.Get(1).(func(common.Key) bool); ok {
		r1 = rf(key)
	} else {
		r1 = ret.Get(1).(bool)
	}

	return r0, r1
}

// FindOrInsert provides a mock function with given fields: key, value, eventTime
func (_m *PrimaryKey) FindOrInsert(key common.Key, value common.RecordID, eventTime uint32) (bool, common.RecordID, error) {
	ret := _m.Called(key, value, eventTime)

	var r0 bool
	if rf, ok := ret.Get(0).(func(common.Key, common.RecordID, uint32) bool); ok {
		r0 = rf(key, value, eventTime)
	} else {
		r0 = ret.Get(0).(bool)
	}

	var r1 common.RecordID
	if rf, ok := ret.Get(1).(func(common.Key, common.RecordID, uint32) common.RecordID); ok {
		r1 = rf(key, value, eventTime)
	} else {
		r1 = ret.Get(1).(common.RecordID)
	}

	var r2 error
	if rf, ok := ret.Get(2).(func(common.Key, common.RecordID, uint32) error); ok {
		r2 = rf(key, value, eventTime)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// GetEventTimeCutoff provides a mock function with given fields:
func (_m *PrimaryKey) GetEventTimeCutoff() uint32 {
	ret := _m.Called()

	var r0 uint32
	if rf, ok := ret.Get(0).(func() uint32); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(uint32)
	}

	return r0
}

// LockForTransfer provides a mock function with given fields:
func (_m *PrimaryKey) LockForTransfer() common.PrimaryKeyData {
	ret := _m.Called()

	var r0 common.PrimaryKeyData
	if rf, ok := ret.Get(0).(func() common.PrimaryKeyData); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(common.PrimaryKeyData)
	}

	return r0
}

// Size provides a mock function with given fields:
func (_m *PrimaryKey) Size() uint {
	ret := _m.Called()

	var r0 uint
	if rf, ok := ret.Get(0).(func() uint); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(uint)
	}

	return r0
}

// UnlockAfterTransfer provides a mock function with given fields:
func (_m *PrimaryKey) UnlockAfterTransfer() {
	_m.Called()
}

// Update provides a mock function with given fields: key, value
func (_m *PrimaryKey) Update(key common.Key, value common.RecordID) bool {
	ret := _m.Called(key, value)

	var r0 bool
	if rf, ok := ret.Get(0).(func(common.Key, common.RecordID) bool); ok {
		r0 = rf(key, value)
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}

// UpdateEventTimeCutoff provides a mock function with given fields: eventTimeCutoff
func (_m *PrimaryKey) UpdateEventTimeCutoff(eventTimeCutoff uint32) {
	_m.Called(eventTimeCutoff)
}