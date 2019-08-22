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
