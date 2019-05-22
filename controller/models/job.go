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
	metaCom "github.com/uber/aresdb/metastore/common"
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
	Name       string            `json:"name"`
	Cluster    string            `json:"cluster"`
	Table      *metaCom.Table    `json:"schema,omitempty"`
	UpdateMode map[string]string `json:"updateMode,omitempty"`
}

// KafkaConfig is the kafka part of job config
type KafkaConfig struct {
	Topic               string         `json:"topic"`
	Cluster             string         `json:"kafkaClusterName"`
	KafkaVersion        string         `json:"kafkaVersion"`
	File                string         `json:"kafkaClusterFile,omitempty"`
	TopicType           string         `json:"topicType,omitempty"`
	LatestOffset        bool           `json:"latestOffset,omitempty"`
	ErrorThreshold      int            `json:"errorThreshold,omitempty"`
	StatusCheckInterval int            `json:"statusCheckInterval,omitempty"`
	ARThreshold         int            `json:"autoRecoveryThreshold,omitempty"`
	ProcessorCount      int            `json:"processorCount,omitempty"`
	BatchSize           int            `json:"batchSize,omitempty"`
	MaxBatchDelayMS     int            `json:"maxBatchDelayMS,omitempty"`
	MegaBytePerSec      int            `json:"megaBytePerSec,omitempty"`
	RestartOnFailure    bool           `json:"restartOnFailure,omitempty"`
	RestartInterval     int            `json:"restartInterval,omitempty"`
	FailureHandler      FailureHandler `json:"failureHandler,omitempty"`

	// confluent kafka
	KafkaBroker       string `json:"kafkaBroker" yaml:"kafkaBroker"`
	MaxPollIntervalMs int    `json:"maxPollIntervalMs" yaml:"maxPollIntervalMs" default:"300000"`
	SessionTimeoutNs  int    `json:"sessionTimeoutNs" yaml:"sessionTimeoutNs" default:"10000"`
	ChannelBufferSize uint   `json:"channelBufferSize" yaml:"channelBufferSize" default:"256"`
}

// JobConfig is job's config
type JobConfig struct {
	Name            string      `json:"job"`
	Version         int         `json:"version"`
	AresTableConfig TableConfig `json:"aresTableConfig"`
	StreamingConfig KafkaConfig `json:"streamConfig"`
}

// FailureHandler is kafka's failure handler
type FailureHandler struct {
	Type   string               `json:"type,omitempty"`
	Config FailureHandlerConfig `json:"config,omitempty"`
}

// FailureHandlerConfig is Kafka's failure handler config
type FailureHandlerConfig struct {
	InitRetryIntervalInSeconds int     `json:"initRetryIntervalInSeconds,omitempty"`
	Multiplier                 float32 `json:"multiplier,omitempty"`
	MaxRetryMinutes            int     `json:"maxRetryMinutes,omitempty"`
}
