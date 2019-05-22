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
package consistenthasing

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConsistentHashing(t *testing.T) {
	ring := NewRing()
	for _, id := range []string{"0", "1", "2"} {
		err := ring.AddNode(id)
		assert.NoError(t, err)
	}
	assert.Len(t, ring.Nodes, 3)

	err := ring.AddNode("1")
	assert.Equal(t, err, ErrNodeIDExists)

	keys := []string{"foo", "foo", "bar", "bar", "test1", "test1"}
	for i := range keys {
		if i%2 != 0 {
			id1, nid1 := ring.Get(keys[i-1])
			id2, nid2 := ring.Get(keys[i])
			assert.Equal(t, id1, id2)
			assert.Equal(t, nid1, nid2)
		}
	}
}
