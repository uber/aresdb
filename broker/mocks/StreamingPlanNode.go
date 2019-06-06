// Code generated by mockery v1.0.0
package mocks

import io "io"
import mock "github.com/stretchr/testify/mock"

// StreamingPlanNode is an autogenerated mock type for the StreamingPlanNode type
type StreamingPlanNode struct {
	mock.Mock
}

// Run provides a mock function with given fields: writer
func (_m *StreamingPlanNode) Run(writer io.Writer) error {
	ret := _m.Called(writer)

	var r0 error
	if rf, ok := ret.Get(0).(func(io.Writer) error); ok {
		r0 = rf(writer)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
