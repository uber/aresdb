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

	// MsgTypeHeatpipe is heatpipe message type
	MsgTypeHeatpipe = "heatpipe"
	// MsgTypeStreamio is streamio message type
	MsgTypeStreamio = "streamio"
	// MsgTypeJSON is json message type
	MsgTypeJSON = "json"
)

// Decoder is a interface that Kafka message decoders like
// heatpipe and streamio has to implement
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
type stringMessage struct {
	topic string
	msg   string
}

func (m *stringMessage) Key() []byte {
	return []byte("")
}

func (m *stringMessage) Value() []byte {
	return []byte(m.msg)
}

func (m *stringMessage) Topic() string {
	return m.topic
}

func (m *stringMessage) Partition() int32 {
	return 0
}

func (m *stringMessage) Offset() int64 {
	return 0
}

func (m *stringMessage) Ack() {
	return
}

func (m *stringMessage) Nack() {
	return
}

func (m *stringMessage) Cluster() string {
	return ""
}

// NewDefaultDecoder will initialize the heatpipe decoder, streamio decoder and json decoder based on the job type
func NewDefaultDecoder(
	jobConfig *rules.JobConfig, serviceConfig config.ServiceConfig) (decoder Decoder, err error) {
	switch jobConfig.StreamingConfig.TopicType {
	case MsgTypeStreamio:
		key := serviceConfig.Config.Get("streamio.key").String()
		decoder, err = NewStreamioDecoder(
			[]byte(key), serviceConfig.Logger, jobConfig.Name, jobConfig.AresTableConfig.Cluster)
	default:
		decoder = &JSONDecoder{}
	}

	return decoder, err
}
