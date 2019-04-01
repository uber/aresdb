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

package tools

import (
	"sync"
	"time"
)

// Batcher batches a sequence of tasks and send them to workers for execution asynchronously.
// A batch of size 2^x is flushed when either maxDelay has elapsed since the oldest task was added,
// or a max number of tasks have been queued.
type Batcher struct {
	workerWG     sync.WaitGroup
	taskQueue    chan batcherTask
	batchQueue   chan []interface{}
	maxBatchSize int
	maxDelay     time.Duration
	now          func() time.Time
}

type batcherTask struct {
	task interface{}
	time time.Time
}

// NewBatcher creates and starts a batcher with no worker.
// User must call StartWorker before any task can be processed.
func NewBatcher(maxBatchSize int, maxDelay time.Duration, now func() time.Time) *Batcher {
	batcher := &Batcher{
		taskQueue:    make(chan batcherTask),
		batchQueue:   make(chan []interface{}),
		maxBatchSize: maxBatchSize,
		maxDelay:     maxDelay,
		now:          now,
	}
	go batcher.run()
	return batcher
}

// StartWorker adds a worker to the batcher and runs it asynchronously.
// The worker func shall receive task batches from the channel, and notify the wait group when it quits.
func (b *Batcher) StartWorker(run func(chan []interface{}, *sync.WaitGroup)) {
	b.workerWG.Add(1)
	go run(b.batchQueue, &b.workerWG)
}

// Add adds a task to the batcher for asynchronous processing.
func (b *Batcher) Add(task interface{}, time time.Time) {
	b.taskQueue <- batcherTask{task, time}
}

// Close signals all workers to quit and blocks until all queued tasks are processed.
func (b *Batcher) Close() {
	close(b.taskQueue)
	b.workerWG.Wait()
}

func createBatch(buffer []batcherTask) []interface{} {
	// Largest power of 2 that fits within buffer size.
	batchSize := 1
	for batchSize<<1 <= len(buffer) {
		batchSize <<= 1
	}

	batch := make([]interface{}, batchSize)
	for i := range batch {
		batch[i] = buffer[i].task
	}
	return batch
}

func (b *Batcher) run() {
	var buffer []batcherTask

	timeout := time.Tick(10 * time.Millisecond)

	for {
		select {
		case task, ok := <-b.taskQueue:
			if !ok {
				// Flush all remaining tasks on quit.
				for len(buffer) > 0 {
					batch := createBatch(buffer)
					b.batchQueue <- batch
					buffer = buffer[len(batch):]
				}
				close(b.batchQueue)
				return
			}
			buffer = append(buffer, task)
		case <-timeout:
		}

		// Flush when either max batch size is reached, or max delay is reached.
		for len(buffer) >= b.maxBatchSize || (len(buffer) > 0 && b.now().Sub(buffer[0].time) >= b.maxDelay) {
			batch := createBatch(buffer)
			b.batchQueue <- batch
			buffer = buffer[len(batch):]
		}
	}
}
