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

package utils

var (
	// TestingT is the global singleton for TestingTMock.
	TestingT = &TestingTMock{}
)

// TestingTMock is the  mock class for testingT used in mockery.
type TestingTMock struct {
}

// Logf is the implementation of mockery.testingT interface.
func (*TestingTMock) Logf(format string, args ...interface{}) {
	GetLogger().Infof(format, args...)
}

// Errorf is the implementation of mockery.testingT interface.
func (*TestingTMock) Errorf(format string, args ...interface{}) {
	GetLogger().Errorf(format, args...)
}

// FailNow is the implementation of mockery.testingT interface.
func (*TestingTMock) FailNow() {
	GetLogger().Panic("Mockery fail now")
}

func (*TestingTMock) Fatalf(format string, args ...interface{}) {
	GetLogger().Fatalf(format, args...)
}