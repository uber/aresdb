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
	"github.com/uber/aresdb/metastore/mocks"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/mock"
)

type countJob struct {
	jobFunc func() error
}

func (j *countJob) Run() error {
	return j.jobFunc()
}

func (j *countJob) GetIdentifier() string {
	return "count"
}

func (j *countJob) String() string {
	return "count"
}

var _ = ginkgo.Describe("scheduler", func() {
	var counter int

	mockErr := errors.New("UpdateArchivingCutoff fails")
	m := getFactory().NewMockMemStore()
	(m.metaStore).(*mocks.MetaStore).On(
		"UpdateArchivingCutoff", mock.Anything, mock.Anything, mock.Anything).Return(mockErr)

	ginkgo.BeforeEach(func() {
		counter = 0
	})

	ginkgo.It("Test newScheduler", func() {
		scheduler := newScheduler(m)
		Ω(scheduler).Should(Not(BeNil()))
		Ω(scheduler.memStore).Should(Equal(m))
		Ω(scheduler.schedulerStopChan).Should(Not(BeNil()))
	})

	ginkgo.It("Test newScheduler Executor, every job should run sequentially", func() {
		scheduler := newScheduler(m)
		scheduler.Start()
		for i := 0; i < 10; i++ {
			expectedCount := i + 1
			resChan := scheduler.SubmitJob(&countJob{
				jobFunc: func() error {
					counter++
					if counter != expectedCount {
						return errors.New("Count is not equal to expectedCount")
					}
					return nil
				},
			})
			err := <-resChan
			Ω(err).Should(BeNil())
		}
		scheduler.Stop()
	})
})
