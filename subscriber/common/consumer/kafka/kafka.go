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
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/subscriber/common/consumer"
	"github.com/uber/aresdb/subscriber/common/rules"
	"github.com/uber/aresdb/subscriber/config"
	"github.com/uber/aresdb/utils"
	"go.uber.org/zap"
	"strconv"
	"strings"
	"sync"
	"time"
)

// KafkaConsumer implements Consumer interface
type KafkaConsumer struct {
	sarama.ConsumerGroup
	*sarama.Config
	sync.Mutex

	group      string
	topicArray []string
	logger     *zap.Logger
	scope      tally.Scope
	msgCh      chan consumer.Message

	// WARNING: The following channels should not be closed by the lib users
	closeAttempted bool
	closeCh        chan struct{}
}

// KafkaMessage implements Message interface
type KafkaMessage struct {
	*sarama.ConsumerMessage

	consumer    consumer.Consumer
	clusterName string
	session     sarama.ConsumerGroupSession
}

// CGHandler represents a Sarama consumer group handler
type CGHandler struct {
	consumer       *KafkaConsumer
	msgCounter     map[string]map[int32]tally.Counter
	msgByteCounter map[string]map[int32]tally.Counter
	msgOffsetGauge map[string]map[int32]tally.Gauge
	msgLagGauge    map[string]map[int32]tally.Gauge
}

// GetConsumerGroupName will return the consumer group name to use or being used
// for given deployment and job name
func GetConsumerGroupName(deployment, jobName string, aresCluster string) string {
	return fmt.Sprintf("ares-subscriber_%s_%s_%s_streaming", deployment, jobName, aresCluster)
}

func getKafkaVersion(v string) sarama.KafkaVersion {
	switch v {
	case "V0_10_2_0":
		return sarama.V0_10_2_0
	case "V0_10_2_1":
		return sarama.V0_10_2_1
	case "V0_11_0_0":
		return sarama.V0_11_0_0
	case "V0_11_0_1":
		return sarama.V0_11_0_1
	case "V0_11_0_2":
		return sarama.V0_11_0_2
	case "V1_0_0_0":
		return sarama.V1_0_0_0
	case "V1_1_0_0":
		return sarama.V1_1_0_0
	case "V1_1_1_0":
		return sarama.V1_1_1_0
	case "V2_0_0_0":
		return sarama.V2_0_0_0
	case "V2_0_1_0":
		return sarama.V2_0_1_0
	case "V2_1_0_0":
		return sarama.V2_1_0_0
	case "V2_2_0_0":
		return sarama.V2_2_0_0
	default:
		return sarama.V0_10_2_0
	}
}

// NewKafkaConsumer creates kafka consumer
func NewKafkaConsumer(jobConfig *rules.JobConfig, serviceConfig config.ServiceConfig) (consumer.Consumer, error) {
	cfg := sarama.NewConfig()
	if jobConfig.StreamingConfig.SessionTimeoutMs > 0 {
		cfg.Consumer.Group.Session.Timeout = time.Duration(jobConfig.StreamingConfig.SessionTimeoutMs) * time.Millisecond
	}
	offsetReset := sarama.OffsetOldest
	if jobConfig.StreamingConfig.LatestOffset {
		offsetReset = sarama.OffsetNewest
	}
	cfg.Consumer.Offsets.Initial = offsetReset
	cfg.Consumer.Return.Errors = true
	if jobConfig.StreamingConfig.ReblanceTimeoutSec > 0 {
		cfg.Consumer.Group.Rebalance.Timeout = time.Duration(jobConfig.StreamingConfig.ReblanceTimeoutSec) * time.Second
	}
	cfg.Version = getKafkaVersion(jobConfig.StreamingConfig.KafkaVersion)

	serviceConfig.Logger.Info("Kafka consumer",
		zap.String("job", jobConfig.Name),
		zap.String("broker", jobConfig.StreamingConfig.KafkaBroker),
		zap.Any("config", cfg))

	group := GetConsumerGroupName(serviceConfig.Environment.Deployment, jobConfig.Name, jobConfig.AresTableConfig.Cluster)
	c, err := sarama.NewConsumerGroup(strings.Split(jobConfig.StreamingConfig.KafkaBroker, ","), group, cfg)
	if err != nil {
		return nil, utils.StackError(err, "Unable to initialize Kafka consumer")
	}

	logger := serviceConfig.Logger.With(
		zap.String("kafkaBroker", jobConfig.StreamingConfig.KafkaBroker),
		zap.String("topic", jobConfig.StreamingConfig.Topic),
	)

	scope := serviceConfig.Scope.Tagged(map[string]string{
		"broker": jobConfig.StreamingConfig.KafkaBroker,
	})

	kc := KafkaConsumer{
		ConsumerGroup: c,
		Config:        cfg,
		group:         group,
		topicArray:    []string{jobConfig.StreamingConfig.Topic},
		logger:        logger,
		scope:         scope,
		msgCh:         make(chan consumer.Message, jobConfig.StreamingConfig.ChannelBufferSize),
		closeCh:       make(chan struct{}),
	}

	cgHandler := CGHandler{
		consumer: &kc,
	}
	ctx := context.Background()
	go kc.startConsuming(ctx, &cgHandler)

	logger.Info("Consumer is up and running")
	return &kc, nil
}

// Name returns the name of this consumer group.
func (c *KafkaConsumer) Name() string {
	return c.group
}

// Topics returns the names of the topics being consumed.
func (c *KafkaConsumer) Topics() []string {
	return append([]string(nil), c.topicArray...)
}

// Errors returns a channel of errors for the topic. To prevent deadlocks,
// users must read from the error channel.
//
// All errors returned from this channel can be safely cast to the
// consumer.Error interface, which allows structured access to the topic
// name and partition number.
func (c *KafkaConsumer) Errors() <-chan error {
	return c.ConsumerGroup.Errors()
}

// Closed returns a channel that unblocks when the consumer successfully shuts
// down.
func (c *KafkaConsumer) Closed() <-chan struct{} {
	return c.closeCh
}

// SetClosed is used for testing
func (c *KafkaConsumer) SetClosed(closeCh chan struct{}) {
	c.closeCh = closeCh
}

// Messages returns a channel of messages for the topic.
//
// If the consumer is not configured with nonzero buffer size, the Errors()
// channel must be read in conjunction with Messages() to prevent deadlocks.
func (c *KafkaConsumer) Messages() <-chan consumer.Message {
	return c.msgCh
}

// SetMessages is used for testing
func (c *KafkaConsumer) SetMessages(msgCh chan consumer.Message) {
	c.msgCh = msgCh
}

// CommitUpTo marks this message and all previous messages in the same partition
// as processed. The last processed offset for each partition is periodically
// flushed to ZooKeeper; on startup, consumers begin processing after the last
// stored offset.
func (c *KafkaConsumer) CommitUpTo(msg consumer.Message) error {
	if concreteMsg, ok := msg.(*KafkaMessage); ok {
		if concreteMsg.session != nil {
			concreteMsg.session.MarkMessage(concreteMsg.ConsumerMessage, "")
		} else {
			return fmt.Errorf("Session is nil, msg:%v", msg)
		}
	} else {
		return fmt.Errorf("Failed to convert KafkaMessage, msg:%v", msg)
	}
	return nil
}

func (c *KafkaConsumer) startConsuming(ctx context.Context, cgHandler *CGHandler) {
	c.logger.Info("Start consumption goroutine")

	// those four Metrics are of the format {"<topic name>":{<partition id>: <offset>, ...}, ...}
	cgHandler.msgCounter = make(map[string]map[int32]tally.Counter)
	cgHandler.msgByteCounter = make(map[string]map[int32]tally.Counter)
	cgHandler.msgOffsetGauge = make(map[string]map[int32]tally.Gauge)
	cgHandler.msgLagGauge = make(map[string]map[int32]tally.Gauge)

	// initialize counter map
	for _, topic := range c.topicArray {
		cgHandler.msgCounter[topic] = make(map[int32]tally.Counter)
		cgHandler.msgByteCounter[topic] = make(map[int32]tally.Counter)
		cgHandler.msgOffsetGauge[topic] = make(map[int32]tally.Gauge)
		cgHandler.msgLagGauge[topic] = make(map[int32]tally.Gauge)
	}

	for run := true; run; {
		if err := c.ConsumerGroup.Consume(ctx, c.topicArray, cgHandler); err != nil {
			c.logger.Error("Received error from consumer", zap.Error(err))
			run = false
		}
		// check if context was cancelled, signaling that the consumer should stop
		if ctx.Err() != nil {
			run = false
			c.logger.Info("Received close Signal")
		}
		if !run {
			c.Close()
		}
	}
}

func (c *KafkaConsumer) processMsg(msg *sarama.ConsumerMessage, cgHandler *CGHandler,
	highWaterOffset int64, session sarama.ConsumerGroupSession) {
	c.Lock()
	defer c.Unlock()

	c.logger.Debug("Received nessage event", zap.Any("message", msg))
	c.msgCh <- &KafkaMessage{
		ConsumerMessage: msg,
		consumer:        c,
		session:         session,
	}

	topic := msg.Topic
	partition := msg.Partition
	pncm := cgHandler.msgCounter[topic]
	nCounter, ok := pncm[partition]
	if !ok {
		nCounter = c.scope.Tagged(map[string]string{"topic": topic, "partition": strconv.Itoa(int(partition))}).Counter("messages-count")
		pncm[partition] = nCounter
	}
	nCounter.Inc(1)

	pbcm := cgHandler.msgByteCounter[topic]
	bCounter, ok := pbcm[partition]
	if !ok {
		bCounter = c.scope.Tagged(map[string]string{"topic": topic, "partition": strconv.Itoa(int(partition))}).Counter("message-bytes-count")
		pbcm[partition] = bCounter
	}
	bCounter.Inc(int64(len(msg.Value)))

	pogm := cgHandler.msgOffsetGauge[topic]
	oGauge, ok := pogm[partition]
	if !ok {
		oGauge = c.scope.Tagged(map[string]string{"topic": topic, "partition": strconv.Itoa(int(partition))}).Gauge("latest-offset")
		pogm[partition] = oGauge
	}
	oGauge.Update(float64(msg.Offset))

	plgm := cgHandler.msgLagGauge[topic]
	lGauge, ok := plgm[partition]
	if !ok {
		lGauge = c.scope.Tagged(map[string]string{"topic": topic, "partition": strconv.Itoa(int(partition))}).Gauge("offset-lag")
	}

	if highWaterOffset > int64(msg.Offset) {
		lGauge.Update(float64(highWaterOffset - int64(msg.Offset) - 1))
	} else {
		lGauge.Update(0)
	}
}

func (c *KafkaConsumer) Close() error {
	c.Lock()
	defer c.Unlock()

	if c.closeAttempted {
		return fmt.Errorf("Close attempted again on consumer group %s", c.group)
	}
	c.logger.Info("Attempting to close consumer",
		zap.String("consumerGroup", c.group))
	err := c.ConsumerGroup.Close()
	if err != nil {
		c.logger.Error("Failed to close consumer",
			zap.String("consumerGroup", c.group),
			zap.Error(err))
	} else {
		c.logger.Info("Started to close consumer",
			zap.String("consumerGroup", c.group))
	}
	close(c.closeCh)
	c.closeAttempted = true
	return err
}

func (m *KafkaMessage) Key() []byte {
	return m.ConsumerMessage.Key
}

func (m *KafkaMessage) Value() []byte {
	return m.ConsumerMessage.Value
}

func (m *KafkaMessage) Topic() string {
	return m.ConsumerMessage.Topic
}

func (m *KafkaMessage) Partition() int32 {
	return m.ConsumerMessage.Partition
}

func (m *KafkaMessage) Offset() int64 {
	return m.ConsumerMessage.Offset
}

func (m *KafkaMessage) Ack() {
	if m.consumer != nil {
		m.consumer.CommitUpTo(m)
	}
}

func (m *KafkaMessage) Nack() {
	// No op for now since Kafka based DLQ is not implemented
}

func (m *KafkaMessage) Cluster() string {
	return m.clusterName
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (h *CGHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (h *CGHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (h *CGHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		h.consumer.processMsg(message, h, claim.HighWaterMarkOffset(), session)
	}

	return nil
}
