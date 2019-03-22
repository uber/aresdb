package message

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/a8m/djson"
	"github.com/uber/aresdb/subscriber/common/consumer"
	"github.com/uber/aresdb/utils"
	"go.uber.org/zap"
)

// StreamioDecoder is an implementation of Decoder interface for decoding the messages from Kafka
// streamio topic
type StreamioDecoder struct {
	jobName string
	cluster string
	gcm     cipher.AEAD
	logger  *zap.Logger
}

// NewStreamioDecoder returns the StreamioDecoder instance for decoding
// streamio messages
func NewStreamioDecoder(base64Key []byte, logger *zap.Logger,
	jobName string, cluster string) (*StreamioDecoder, error) {
	decodedKey, err := base64.StdEncoding.DecodeString(string(base64Key))
	if err != nil {
		return nil, utils.StackError(err, "Unable to initialize streamio due to wrong key")
	}
	// initialize cipher
	block, err := aes.NewCipher(decodedKey)
	if err != nil {
		return nil, utils.StackError(err, "Unable to initialize streamio cipher")
	}
	// initialize GCM algorithm with custom nonce size. Streamio
	// uses the 16 byte IV for encryption, need same for decryption
	gcm, err := cipher.NewGCMWithNonceSize(block, aes.BlockSize)
	if err != nil {
		return nil, utils.StackError(err, "Unable to initialize streamio decoder")
	}
	return &StreamioDecoder{
		jobName: jobName,
		cluster: cluster,
		gcm:     gcm,
		logger:  logger,
	}, nil
}

// DecodeMsg will decode the given message and returns the Message
func (s *StreamioDecoder) DecodeMsg(msg consumer.Message) (*Message, error) {
	m := msg.Value()
	nonce := m[:aes.BlockSize]
	m = m[aes.BlockSize:]
	text, err := s.gcm.Open(nil, nonce, m, nil)
	if err != nil {
		return nil, err
	}

	decoder := djson.NewDecoder(text)
	decoder.AllocString()
	obj, err := decoder.DecodeObject()
	if err != nil {
		s.logger.Error("Unable to unmarshall decoded streamio message",
			zap.String("job", s.jobName),
			zap.String("cluster", s.cluster),
			zap.Any("msg", m),
			zap.Error(err))
		return nil, err
	}
	out := map[string]interface{}{
		MsgPrefix: obj,
	}

	ts, err := getTimestampFromStreamioMsg(obj)
	if err != nil {
		s.logger.Error("Unable to get message timestamp",
			zap.String("job", s.jobName),
			zap.String("cluster", s.cluster),
			zap.Any("msg", m),
			zap.Error(err))
	}
	message := &Message{
		MsgMetaDataTS:  ts,
		RawMessage:     msg,
		DecodedMessage: out,
	}
	return message, nil
}

func getTimestampFromStreamioMsg(msg map[string]interface{}) (ts time.Time, err error) {
	if val, ok := msg["_updated"]; ok {
		if timeStr, ok2 := val.(string); ok2 {
			// RFC3339 time format definition: https://golang.org/pkg/time/
			ts, err = time.Parse(time.RFC3339, timeStr)
		} else {
			errStr := fmt.Sprintf("Expecting time value to be string. But it's %T", val)
			err = errors.New(errStr)
		}
	}
	return ts, err
}
