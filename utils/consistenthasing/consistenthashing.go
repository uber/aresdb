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
	"errors"
	"hash/crc32"
	"sort"
)

var (
	// ErrNodeIDExists indicates a duplicated node was added to the ring
	ErrNodeIDExists = errors.New("Node with same id already exists")
)

// Node is a node in hash ring
type Node struct {
	ID     string
	HashID uint32
}

// Nodes is a slice of nodes
type Nodes []Node

// Len is for sorting nodes
func (n Nodes) Len() int { return len(n) }

// Less is for sorting nodes
func (n Nodes) Less(i, j int) bool {
	if n[i].HashID == n[j].HashID {
		return n[i].ID < n[j].ID
	}
	return n[i].HashID < n[j].HashID
}

// Swap  is for sorting nodes
func (n Nodes) Swap(i, j int) { n[i], n[j] = n[j], n[i] }

// Ring is a hashring
type Ring struct {
	Nodes Nodes
	idSet map[string]bool
}

// NewNode returns a new node
func NewNode(id string) *Node {
	return &Node{
		ID:     id,
		HashID: hashKey(id),
	}
}

// NewRing returns a new ring
func NewRing() *Ring {
	return &Ring{Nodes: Nodes{}, idSet: map[string]bool{}}
}

// AddNode adds a new node
func (r *Ring) AddNode(id string) error {
	if r.idSet[id] {
		return ErrNodeIDExists
	}
	node := NewNode(id)
	r.Nodes = append(r.Nodes, *node)
	sort.Sort(r.Nodes)
	r.idSet[id] = true
	return nil
}

// Get node id given key
func (r *Ring) Get(key string) (int, string) {
	searchfn := func(i int) bool {
		return r.Nodes[i].HashID >= hashKey(key)
	}
	i := sort.Search(r.Nodes.Len(), searchfn)
	if i >= r.Nodes.Len() {
		i = 0
	}
	return i, r.Nodes[i].ID
}

func hashKey(key string) uint32 {
	if len(key) < 64 {
		var scratch [64]byte
		copy(scratch[:], key)
		return crc32.ChecksumIEEE(scratch[:len(key)])
	}
	return crc32.ChecksumIEEE([]byte(key))
}
