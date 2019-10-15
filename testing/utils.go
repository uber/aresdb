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

import (
	"bytes"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	"github.com/onsi/ginkgo"
)

// TestReadWriteSyncCloser implements a in-memory io.ReadWriteCloser and utils.ReaderSeekerCloser for testing files.
type TestReadWriteSyncCloser struct {
	bytes.Buffer
}

// Close implements io.ReadWriteCloser.Close.
func (t *TestReadWriteSyncCloser) Close() error {
	return nil
}

func (t *TestReadWriteSyncCloser) Write(bytes []byte) (n int, err error) {
	return t.Buffer.Write(bytes)
}

func (t *TestReadWriteSyncCloser) Sync() error {
	return nil
}

// Seek implements utils.ReaderSeekerCloser.Seek.
func (t *TestReadWriteSyncCloser) Seek(offset int64, whence int) (int64, error) {
	return 0, nil
}

type GinkgoTestReporter struct{}

func (g GinkgoTestReporter) Errorf(format string, args ...interface{}) {
	ginkgo.Fail(fmt.Sprintf(format, args...))
}

func (g GinkgoTestReporter) Fatalf(format string, args ...interface{}) {
	ginkgo.Fail(fmt.Sprintf(format, args...))
}

func MockKafkaConsumerFunc(brokers []string) (sarama.Consumer, error) {
	var t GinkgoTestReporter
	config := sarama.NewConfig()
	config.ChannelBufferSize = 2 * 5000
	return mocks.NewConsumer(t, config), nil
}
