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
