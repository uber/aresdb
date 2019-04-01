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
	}

	message := &Message{
		MsgMetaDataTS:  ts,
		RawMessage:     msg,
		DecodedMessage: out,
	}
	return message, err
}
