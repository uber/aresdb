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

// meaningful defaults of Kafka configurations.
const (
	KafkaTopicType                           = "json"
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
	FailureHandlerMaxRetryMinutes            = 15
)

var (
	// DefaultKafkaConfig  defines the default job
	DefaultKafkaConfig = KafkaConfig{
		TopicType:           KafkaTopicType,
		LatestOffset:        KafkaLatestOffset,
		ErrorThreshold:      KafkaErrorThreshold,
		StatusCheckInterval: KafkaStatusCheckInterval,
		ARThreshold:         KafkaAutoRecoveryThreshold,
		ProcessorCount:      KafkaProcessorCount,
		BatchSize:           KafkaBatchSize,
		MaxBatchDelayMS:     KafkaMaxBatchDelayMS,
		MegaBytePerSec:      KafkaMegaBytePerSec,
		RestartOnFailure:    KafkaRestartOnFailure,
		RestartInterval:     KafkaRestartInterval,
		FailureHandler: FailureHandler{
			Type: FailureHandlerType,
			Config: FailureHandlerConfig{
				InitRetryIntervalInSeconds: FailureHandlerInitRetryIntervalInSeconds,
				Multiplier:                 FailureHandlerMultiplier,
				MaxRetryMinutes:            FailureHandlerMaxRetryMinutes,
			},
		},
	}
)
