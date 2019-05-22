package cluster

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/m3db/m3/src/cluster/kv"
)

// Transaction defines a Transaction
type Transaction struct {
	keys     []string
	versions []int
	values   []proto.Message
}

// NewTransaction creates a new transaction
func NewTransaction() *Transaction {
	return &Transaction{}
}

// AddKeyValue adds a tuple of (key, version, value) to the transaction
func (t *Transaction) AddKeyValue(key string, version int, value proto.Message) *Transaction {
	t.keys = append(t.keys, key)
	t.versions = append(t.versions, version)
	t.values = append(t.values, value)
	return t
}

// WriteTo writes the transaction to the transaction store
func (t *Transaction) WriteTo(store kv.TxnStore) error {
	if len(t.keys) != len(t.versions) || len(t.versions) != len(t.values) {
		return fmt.Errorf("length of keys (%d), versions (%d), values (%d) in Transaction do not match",
			len(t.keys), len(t.versions), len(t.values))
	}
	conditions := make([]kv.Condition, len(t.keys))
	ops := make([]kv.Op, len(t.keys))
	for i, key := range t.keys {
		conditions[i] = kv.NewCondition().
			SetTargetType(kv.TargetVersion).
			SetCompareType(kv.CompareEqual).
			SetKey(key).
			SetValue(t.versions[i])
		ops[i] = kv.NewSetOp(key, t.values[i])
	}
	_, err := store.Commit(conditions, ops)
	return err
}