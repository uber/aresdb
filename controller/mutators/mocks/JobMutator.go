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
// Code generated by mockery v1.0.0
package mocks

import (
	"github.com/stretchr/testify/mock"
	"github.com/uber/aresdb/controller/models"
)

// JobMutator is an autogenerated mock type for the JobMutator type
type JobMutator struct {
	mock.Mock
}

// AddJob provides a mock function with given fields: namespace, job
func (_m *JobMutator) AddJob(namespace string, job models.JobConfig) error {
	ret := _m.Called(namespace, job)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, models.JobConfig) error); ok {
		r0 = rf(namespace, job)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// DeleteJob provides a mock function with given fields: namespace, name
func (_m *JobMutator) DeleteJob(namespace string, name string) error {
	ret := _m.Called(namespace, name)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, string) error); ok {
		r0 = rf(namespace, name)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// GetHash provides a mock function with given fields: namespace
func (_m *JobMutator) GetHash(namespace string) (string, error) {
	ret := _m.Called(namespace)

	var r0 string
	if rf, ok := ret.Get(0).(func(string) string); ok {
		r0 = rf(namespace)
	} else {
		r0 = ret.Get(0).(string)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(namespace)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetJob provides a mock function with given fields: namespace, name
func (_m *JobMutator) GetJob(namespace string, name string) (models.JobConfig, error) {
	ret := _m.Called(namespace, name)

	var r0 models.JobConfig
	if rf, ok := ret.Get(0).(func(string, string) models.JobConfig); ok {
		r0 = rf(namespace, name)
	} else {
		r0 = ret.Get(0).(models.JobConfig)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, string) error); ok {
		r1 = rf(namespace, name)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetJobs provides a mock function with given fields: namespace
func (_m *JobMutator) GetJobs(namespace string) ([]models.JobConfig, error) {
	ret := _m.Called(namespace)

	var r0 []models.JobConfig
	if rf, ok := ret.Get(0).(func(string) []models.JobConfig); ok {
		r0 = rf(namespace)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]models.JobConfig)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(namespace)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// UpdateJob provides a mock function with given fields: namespace, job
func (_m *JobMutator) UpdateJob(namespace string, job models.JobConfig) error {
	ret := _m.Called(namespace, job)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, models.JobConfig) error); ok {
		r0 = rf(namespace, job)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
