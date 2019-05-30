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

package kafka

import (
	"fmt"
	"sync"

	"github.com/uber/aresdb/subscriber/common/consumer"

	"strconv"

	kafkaConfluent "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/subscriber/common/rules"
	"github.com/uber/aresdb/subscriber/config"
	"github.com/uber/aresdb/utils"
	"go.uber.org/zap"
)

// KafkaConsumer implements Consumer interface
type KafkaConsumer struct {
	*kafkaConfluent.Consumer
	kafkaConfluent.ConfigMap
	sync.Mutex

	TopicArray []string
	Logger     *zap.Logger
	Scope      tally.Scope
	ErrCh      chan error
	MsgCh      chan consumer.Message

	// WARNING: The following channels should not be closed by the lib users
	CloseAttempted bool
	CloseErr       error
	CloseCh        chan struct{}
}

// KafkaMessage implements Message interface
type KafkaMessage struct {
	*kafkaConfluent.Message

	Consumer    consumer.Consumer
	ClusterName string
}

// NewKafkaConsumer creates kafka consumer by using https://github.com/confluentinc/confluent-kafka-go.
func NewKafkaConsumer(jobConfig *rules.JobConfig, serviceConfig config.ServiceConfig) (consumer.Consumer, error) {
	offsetReset := "earliest"
	if jobConfig.StreamingConfig.LatestOffset {
		offsetReset = "latest"
	}
	cfg := kafkaConfluent.ConfigMap{
		"bootstrap.servers":               jobConfig.StreamingConfig.KafkaBroker,
		"group.id":                        GetConsumerGroupName(serviceConfig.Environment.Deployment, jobConfig.Name, jobConfig.AresTableConfig.Cluster),
		"max.poll.interval.ms":            jobConfig.StreamingConfig.MaxPollIntervalMs,
		"session.timeout.ms":              jobConfig.StreamingConfig.SessionTimeoutNs,
		"go.events.channel.enable":        false,
		"go.application.rebalance.enable": false,
		"enable.partition.eof":            true,
		"auto.offset.reset":               offsetReset,
	}
	serviceConfig.Logger.Info("Kafka consumer",
		zap.String("job", jobConfig.Name),
		zap.String("broker", jobConfig.StreamingConfig.KafkaBroker),
		zap.Any("config", cfg))

	c, err := kafkaConfluent.NewConsumer(&cfg)
	if err != nil {
		return nil, utils.StackError(err, "Unable to initialize Kafka consumer")
	}

	err = c.Subscribe(jobConfig.StreamingConfig.Topic, nil)
	if err != nil {
		return nil, utils.StackError(err, fmt.Sprintf("Unable to subscribe to topic: %s", jobConfig.StreamingConfig.Topic))
	}

	logger := serviceConfig.Logger.With(
		zap.String("kafkaBroker", jobConfig.StreamingConfig.KafkaBroker),
		zap.String("topic", jobConfig.StreamingConfig.Topic),
	)

	scope := serviceConfig.Scope.Tagged(map[string]string{
		"broker": jobConfig.StreamingConfig.KafkaBroker,
	})

	kc := KafkaConsumer{
		Consumer:   c,
		ConfigMap:  cfg,
		TopicArray: []string{jobConfig.StreamingConfig.Topic},
		Logger:     logger,
		Scope:      scope,
		ErrCh:      make(chan error, jobConfig.StreamingConfig.ChannelBufferSize),
		MsgCh:      make(chan consumer.Message, jobConfig.StreamingConfig.ChannelBufferSize),
		CloseCh:    make(chan struct{}),
	}

	go kc.startConsuming()
	return &kc, nil
}

// Name returns the name of this consumer group.
func (c *KafkaConsumer) Name() string {
	return c.ConfigMap["group.id"].(string)
}

// Topics returns the names of the topics being consumed.
func (c *KafkaConsumer) Topics() []string {
	return append([]string(nil), c.TopicArray...)
}

// Errors returns a channel of errors for the topic. To prevent deadlocks,
// users must read from the error channel.
//
// All errors returned from this channel can be safely cast to the
// consumer.Error interface, which allows structured access to the topic
// name and partition number.
func (c *KafkaConsumer) Errors() <-chan error {
	return c.ErrCh
}

// Closed returns a channel that unblocks when the consumer successfully shuts
// down.
func (c *KafkaConsumer) Closed() <-chan struct{} {
	return c.CloseCh
}

// Messages returns a channel of messages for the topic.
//
// If the consumer is not configured with nonzero buffer size, the Errors()
// channel must be read in conjunction with Messages() to prevent deadlocks.
func (c *KafkaConsumer) Messages() <-chan consumer.Message {
	return c.MsgCh
}

// CommitUpTo marks this message and all previous messages in the same partition
// as processed. The last processed offset for each partition is periodically
// flushed to ZooKeeper; on startup, consumers begin processing after the last
// stored offset.
func (c *KafkaConsumer) CommitUpTo(msg consumer.Message) error {
	if concreteMsg, ok := msg.(*KafkaMessage); ok {
		// Just unwrap the underlying message.
		c.CommitMessage(concreteMsg.Message)
	} else {
		topic := msg.Topic()
		c.CommitOffsets([]kafkaConfluent.TopicPartition{
			{
				Topic:     &topic,
				Partition: msg.Partition(),
				Offset:    kafkaConfluent.Offset(msg.Offset()),
			},
		})
	}
	return nil
}

func (c *KafkaConsumer) startConsuming() {
	c.Logger.Debug("Start consumption goroutine")

	// those four Metrics are of the format {"<topic name>":{<partition id>: <offset>, ...}, ...}
	msgCounter := make(map[string]map[int32]tally.Counter)
	msgByteCounter := make(map[string]map[int32]tally.Counter)
	msgOffsetGauge := make(map[string]map[int32]tally.Gauge)
	msgLagGauge := make(map[string]map[int32]tally.Gauge)

	// initialize outer map
	for _, topic := range c.TopicArray {
		msgCounter[topic] = make(map[int32]tally.Counter)
		msgByteCounter[topic] = make(map[int32]tally.Counter)
		msgOffsetGauge[topic] = make(map[int32]tally.Gauge)
		msgLagGauge[topic] = make(map[int32]tally.Gauge)
	}

	for run := true; run; {
		select {
		case _ = <-c.CloseCh:
			c.Logger.Info("Received close Signal")
			run = false
		case event := <-c.Events():
			switch e := event.(type) {
			case *kafkaConfluent.Message:
				c.processMsg(e, msgCounter, msgByteCounter, msgOffsetGauge, msgLagGauge)
			case kafkaConfluent.Error:
				c.ErrCh <- e
				c.Logger.Error("Received error event", zap.Error(e))
			default:
				c.Logger.Info("Ignored consumer event", zap.Any("event", e))
			}
		}
	}
}

func (c *KafkaConsumer) processMsg(msg *kafkaConfluent.Message,
	msgCounter map[string]map[int32]tally.Counter,
	msgByteCounter map[string]map[int32]tally.Counter,
	msgOffsetGauge map[string]map[int32]tally.Gauge,
	msgLagGauge map[string]map[int32]tally.Gauge) {

	c.Logger.Debug("Received nessage event", zap.Any("message", msg))
	c.MsgCh <- &KafkaMessage{
		Message:  msg,
		Consumer: c,
	}

	topic := *msg.TopicPartition.Topic
	partition := msg.TopicPartition.Partition
	pncm := msgCounter[topic]
	nCounter, ok := pncm[partition]
	if !ok {
		nCounter = c.Scope.Tagged(map[string]string{"topic": topic, "partition": strconv.Itoa(int(partition))}).Counter("messages-count")
		pncm[partition] = nCounter
	}
	nCounter.Inc(1)

	pbcm := msgByteCounter[topic]
	bCounter, ok := pbcm[partition]
	if !ok {
		bCounter = c.Scope.Tagged(map[string]string{"topic": topic, "partition": strconv.Itoa(int(partition))}).Counter("message-bytes-count")
		pbcm[partition] = bCounter
	}
	bCounter.Inc(int64(len(msg.Value)))

	pogm := msgOffsetGauge[topic]
	oGauge, ok := pogm[partition]
	if !ok {
		oGauge = c.Scope.Tagged(map[string]string{"topic": topic, "partition": strconv.Itoa(int(partition))}).Gauge("latest-offset")
		pogm[partition] = oGauge
	}
	oGauge.Update(float64(msg.TopicPartition.Offset))

	plgm := msgLagGauge[topic]
	lGauge, ok := plgm[partition]
	if !ok {
		lGauge = c.Scope.Tagged(map[string]string{"topic": topic, "partition": strconv.Itoa(int(partition))}).Gauge("offset-lag")
	}

	_, offset, _ := c.Consumer.QueryWatermarkOffsets(topic, partition, 100)

	if offset > int64(msg.TopicPartition.Offset) {
		lGauge.Update(float64(offset - int64(msg.TopicPartition.Offset) - 1))
	} else {
		lGauge.Update(0)
	}
}

func (c *KafkaConsumer) Close() error {
	c.Lock()
	defer c.Unlock()

	if c.CloseAttempted {
		return fmt.Errorf("Close attempted again on consumer group %s", c.ConfigMap["group.id"].(string))
	}

	c.Logger.Debug("Attempting to close consumer",
		zap.String("consumerGroup", c.ConfigMap["group.id"].(string)))
	c.CloseErr = c.Consumer.Close()
	if c.CloseErr != nil {
		c.Logger.With(zap.NamedError("error", c.CloseErr)).Error("Failed to close consumer",
			zap.String("consumerGroup", c.ConfigMap["group.id"].(string)))
	} else {
		c.Logger.Debug("Started to close consumer",
			zap.String("consumerGroup", c.ConfigMap["group.id"].(string)))
	}
	close(c.CloseCh)
	c.CloseAttempted = true
	return c.CloseErr
}

func (m *KafkaMessage) Key() []byte {
	return m.Message.Key
}

func (m *KafkaMessage) Value() []byte {
	return m.Message.Value
}

func (m *KafkaMessage) Topic() string {
	return *m.TopicPartition.Topic
}

func (m *KafkaMessage) Partition() int32 {
	return m.TopicPartition.Partition
}

func (m *KafkaMessage) Offset() int64 {
	return int64(m.TopicPartition.Offset)
}

func (m *KafkaMessage) Ack() {
	if m.Consumer != nil {
		m.Consumer.CommitUpTo(m)
	}
}

func (m *KafkaMessage) Nack() {
	// No op for now since Kafka based DLQ is not implemented
}

func (m *KafkaMessage) Cluster() string {
	return m.ClusterName
}

// GetConsumerGroupName will return the consumer group name to use or being used
// for given deployment and job name
func GetConsumerGroupName(deployment, jobName string, aresCluster string) string {
	return fmt.Sprintf("ares-subscriber_%s_%s_%s_streaming", deployment, jobName, aresCluster)
}
