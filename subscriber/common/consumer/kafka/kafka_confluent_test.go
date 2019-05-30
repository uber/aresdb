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
	"os"

	kafkaConfluent "github.com/confluentinc/confluent-kafka-go/kafka"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/client"
	"github.com/uber/aresdb/subscriber/common/rules"
	"github.com/uber/aresdb/subscriber/common/tools"
	"github.com/uber/aresdb/subscriber/config"
	"github.com/uber/aresdb/utils"
	"go.uber.org/zap"
)

var _ = Describe("KafkaConsumer", func() {
	serviceConfig := config.ServiceConfig{
		Environment: utils.EnvironmentContext{
			Deployment:         "test",
			RuntimeEnvironment: "test",
			Zone:               "local",
		},
		Logger: zap.NewNop(),
		Scope:  tally.NoopScope,
	}
	serviceConfig.ActiveJobs = []string{"job1"}
	sinkConfig := config.SinkConfig{
		SinkModeStr:           "kafka",
		AresDBConnectorConfig: client.ConnectorConfig{Address: "localhost:8888"},
	}
	serviceConfig.ActiveAresClusters = map[string]config.SinkConfig{
		"dev01": sinkConfig,
	}

	rootPath := tools.GetModulePath("")
	os.Chdir(rootPath)
	jobConfigs := make(rules.JobConfigs)
	err := rules.AddLocalJobConfig(serviceConfig, jobConfigs)
	if err != nil {
		panic("Failed to AddLocalJobConfig")
	}
	if jobConfigs["job1"]["dev01"] == nil {
		panic("Failed to get (jobConfigs[\"job1\"][\"dev01\"]")
	} else {
		jobConfigs["job1"]["dev01"].AresTableConfig.Cluster = "dev01"
	}

	It("KafkaConsumer functions", func() {
		kc, err := NewKafkaConsumer(jobConfigs["job1"]["dev01"], serviceConfig)
		Ω(err).Should(BeNil())
		Ω(kc).ShouldNot(BeNil())

		groupId := kc.Name()
		Ω(groupId).Should(Equal("ares-subscriber_test_job1_dev01_streaming"))

		topics := kc.Topics()
		len := len(topics)
		Ω(len).Should(Equal(1))
		Ω(topics[0]).Should(Equal("job1-topic"))

		errCh := kc.Errors()
		Ω(errCh).ShouldNot(BeNil())

		msgCh := kc.Messages()
		Ω(msgCh).ShouldNot(BeNil())

		closeCh := kc.Closed()
		Ω(closeCh).ShouldNot(BeNil())

		go kc.(*KafkaConsumer).startConsuming()

		topic := "topic"
		msg := &kafkaConfluent.Message{
			TopicPartition: kafkaConfluent.TopicPartition{
				Topic:     &topic,
				Partition: int32(0),
				Offset:    0,
			},
			Value: []byte("value"),
			Key:   []byte("key"),
		}
		msgCounter := map[string]map[int32]tally.Counter{
			"topic": make(map[int32]tally.Counter),
		}
		msgByteCounter := map[string]map[int32]tally.Counter{
			"topic": make(map[int32]tally.Counter),
		}
		msgOffsetGauge := map[string]map[int32]tally.Gauge{
			"topic": make(map[int32]tally.Gauge),
		}
		msgLagGauge := map[string]map[int32]tally.Gauge{
			"topic": make(map[int32]tally.Gauge),
		}
		kc.(*KafkaConsumer).processMsg(msg, msgCounter, msgByteCounter, msgOffsetGauge, msgLagGauge)

		err = kc.(*KafkaConsumer).Close()
		Ω(err).Should(BeNil())

		err = kc.(*KafkaConsumer).Close()
		Ω(err).ShouldNot(BeNil())
	})

	It("KafkaMessage functions", func() {
		topic := "topic"
		message := &KafkaMessage{
			&kafkaConfluent.Message{
				TopicPartition: kafkaConfluent.TopicPartition{
					Topic:     &topic,
					Partition: int32(0),
					Offset:    0,
				},
				Value: []byte("value"),
				Key:   []byte("key"),
			},
			nil,
			"kafka-cluster1",
		}

		key := message.Key()
		Ω(string(key)).Should(Equal("key"))

		value := message.Value()
		Ω(string(value)).Should(Equal("value"))

		cluster := message.Cluster()
		Ω(cluster).Should(Equal("kafka-cluster1"))

		topic = message.Topic()
		Ω(topic).Should(Equal("topic"))

		offset := message.Offset()
		Ω(offset).Should(Equal(int64(0)))

		partition := message.Partition()
		Ω(partition).Should(Equal(int32(0)))

		message.Ack()
		message.Consumer, _ = NewKafkaConsumer(jobConfigs["job1"]["dev01"], serviceConfig)

		message.Ack()

		message.Nack()

		message.Consumer.Close()
	})
})
