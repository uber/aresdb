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

package testing

import "bytes"

// TestReadWriteCloser implements a in-memory io.ReadWriteCloser and utils.ReaderSeekerCloser for testing files.
type TestReadWriteCloser struct {
	bytes.Buffer
}

// Close implements io.ReadWriteCloser.Close.
func (TestReadWriteCloser) Close() error {
	return nil
}

// Seek implements utils.ReaderSeekerCloser.Seek.
func (t TestReadWriteCloser) Seek(offset int64, whence int) (int64, error) {
	return 0, nil
}
