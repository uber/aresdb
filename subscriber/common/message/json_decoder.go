package message

import (
	"encoding/json"
	"time"

	"github.com/uber/aresdb/subscriber/common/consumer"
)

// JSONDecoder is an implementation of Decoder interface for identity decoder
type JSONDecoder struct{}

// DecodeMsg will convert given JSON string to a map
func (j *JSONDecoder) DecodeMsg(msg consumer.Message) (*Message, error) {
	m := make(map[string]interface{})
	err := json.Unmarshal(msg.Value(), &m)
	if err != nil {
		return nil, err
	}

	out := map[string]interface{}{
		MsgPrefix: m,
	}

	var ts time.Time
	if val, ok := m[MsgMetaDataTS]; ok {
		ts = time.Unix(int64(val.(float64)), 0)
	} else {
		ts, _ = getTimestampFromStreamioMsg(m)
	}

	message := &Message{
		MsgMetaDataTS:  ts,
		RawMessage:     msg,
		DecodedMessage: out,
	}
	return message, err
}
