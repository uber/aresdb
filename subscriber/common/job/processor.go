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

package job

import (
	"sync"
	"time"

	"fmt"
	"github.com/uber/aresdb/subscriber/common/tools"
)

// Processor is a interface that all processor needs to implement to work with Driver
type Processor interface {

	// GetId will return ID of this processor
	GetID() int

	// GetContext will return the processor context
	GetContext() *ProcessorContext

	// Run will start the processor and run until shutdown
	// is triggered for close for some other reason
	Run()

	// Stop will stop the processor and close all connections
	// to kafka consumer group and storage layer
	Stop()

	// Restart will stop and start current processor
	Restart()
}

// ProcessorContext holds information about total messages processed,
// number of failed messages, number of waiting messages in batcher and
// last updated timestamp for this information
type ProcessorContext struct {
	sync.RWMutex

	StartTime      time.Time       `json:"startTime"`
	TotalMessages  int64           `json:"totalMessages"`
	FailedMessages int64           `json:"failedMessages"`
	Stopped        bool            `json:"stopped"`
	Shutdown       bool            `json:"shutdown"`
	Errors         processorErrors `json:"errors"`
	LastUpdated    time.Time       `json:"lastUpdated"`
	RestartCount   int64           `json:"restartCount"`
	Restarting     bool            `json:"restarting"`
	RestartTime    int64           `json:"restartTime"`
}

type processorErrors struct {
	errors   []ProcessorError
	errorIdx int
}

// ProcessorError will define the error and ID of processor that generated it
type ProcessorError struct {
	// ID of the processor
	ID int
	// Timestamp defines time when this error
	// happened
	Timestamp int64
	// Error generated
	Error error
}

// ErrorToJSON converts error to json format
func (p ProcessorError) ErrorToJSON() string {
	return tools.ToJSON(fmt.Sprintf(`{"error": "%s"}`, p.Error.Error()))
}
