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
package models

import (
	"github.com/uber/aresdb/metastore/common"
)

// meaningful defaults of Kafka configurations.
const (
	KafkaClusterFile                         = "/etc/uber/kafka8/clusters.yaml"
	KafkaTopicType                           = "heatpipe"
	KafkaLatestOffset                        = true
	KafkaErrorThreshold                      = 10
	KafkaStatusCheckInterval                 = 60
	KafkaAutoRecoveryThreshold               = 8
	KafkaProcessorCount                      = 1
	KafkaBatchSize                           = 32768
	KafkaMaxBatchDelayMS                     = 10000
	KafkaMegaBytePerSec                      = 600
	KafkaRestartOnFailure                    = true
	KafkaRestartInterval                     = 300
	FailureHandlerType                       = "retry"
	FailureHandlerInitRetryIntervalInSeconds = 60
	FailureHandlerMultiplier                 = 1
	FailureHandlerMaxRetryMinutes            = 525600
)

// TableConfig is the table part of job config
type TableConfig struct {
	Name    string        `json:"name"`
	Cluster string        `json:"cluster"`
	Schema  *common.Table `json:"schema,omitempty"`
}

// KafkaConfig is the kafka part of job config
type KafkaConfig struct {
	Topic           string         `json:"topic"`
	Cluster         string         `json:"kafkaClusterName"`
	KafkaVersion    string         `json:"kafkaVersion"`
	File            string         `json:"kafkaClusterFile,omitempty"`
	Type            string         `json:"topicType,omitempty"`
	Offset          bool           `json:"latestOffset,omitempty"`
	ErrorThreshold  int            `json:"errorThreshold,omitempty"`
	SCInterval      int            `json:"statusCheckInterval,omitempty"`
	ARThreshold     int            `json:"autoRecoveryThreshold,omitempty"`
	ProcessorCount  int            `json:"processorCount,omitempty"`
	BatchSize       int            `json:"batchSize,omitempty"`
	BatchDelayMS    int            `json:"maxBatchDelayMS,omitempty"`
	BytePerSec      int            `json:"megaBytePerSec,omitempty"`
	Restart         bool           `json:"restartOnFailure,omitempty"`
	RestartInterval int            `json:"restartInterval,omitempty"`
	FailureHandler  FailureHandler `json:"failureHandler,omitempty"`
}

// JobConfig is job's config
type JobConfig struct {
	Name    string      `json:"job"`
	Version int         `json:"version"`
	Table   TableConfig `json:"aresTableConfig"`
	Kafka   KafkaConfig `json:"streamConfig"`
}

// FailureHandler is kafka's failure handler
type FailureHandler struct {
	Type   string               `json:"type,omitempty"`
	Config FailureHandlerConfig `json:"config,omitempty"`
}

// FailureHandlerConfig is Kafka's failure handler config
type FailureHandlerConfig struct {
	Interval   int `json:"initRetryIntervalInSeconds,omitempty"`
	Multiplier int `json:"multiplier,omitempty"`
	MaxRetry   int `json:"maxRetryMinutes,omitempty"`
}
