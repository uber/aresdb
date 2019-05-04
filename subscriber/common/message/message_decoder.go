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

package message

import (
	"time"

	"github.com/uber/aresdb/subscriber/common/consumer"
	"github.com/uber/aresdb/subscriber/common/rules"
	"github.com/uber/aresdb/subscriber/config"
)

const (
	// MsgPrefix is prefix keyword of message body
	MsgPrefix = "msg"
	// MsgMetaDataUUID is message metadata uuid keyword
	MsgMetaDataUUID = "uuid"
	// MsgMetaDataTS is message metadata timestamp keyword
	MsgMetaDataTS = "ts"
)

// Decoder is a interface that Kafka message decoders
type Decoder interface {
	// DecodeMsg will decode the given message into out variable
	DecodeMsg(msg consumer.Message) (*Message, error)
}

// Message contains raw message read from Kafka and the decoded message
type Message struct {
	// MsgInSubTS is the timestamp when the message is consumed by the subscriber
	MsgInSubTS time.Time
	// MsgMetaDataTS is defined in encoder/decoder metadata
	MsgMetaDataTS time.Time
	// RawMessage is encoded message
	RawMessage consumer.Message
	// DecodedMessage is decoded message
	DecodedMessage map[string]interface{}
}

// stringMessage is an implementation of Message interface for testing.
type StringMessage struct {
	topic string
	msg   string
}

func NewStringMessage(topic, msg string) *StringMessage {
	return  &StringMessage {
		topic: topic,
		msg: msg,
	}
}

func (m *StringMessage) Key() []byte {
	return []byte("")
}

func (m *StringMessage) Value() []byte {
	return []byte(m.msg)
}

func (m *StringMessage) Topic() string {
	return m.topic
}

func (m *StringMessage) Partition() int32 {
	return 0
}

func (m *StringMessage) Offset() int64 {
	return 0
}

func (m *StringMessage) Ack() {
	return
}

func (m *StringMessage) Nack() {
	return
}

func (m *StringMessage) Cluster() string {
	return ""
}

// NewDefaultDecoder will initialize the json decoder based on the job type
func NewDefaultDecoder(
	jobConfig *rules.JobConfig, serviceConfig config.ServiceConfig) (decoder Decoder, err error) {
	switch jobConfig.StreamingConfig.TopicType {
	default:
		decoder = &JSONDecoder{}
	}

	return
}
