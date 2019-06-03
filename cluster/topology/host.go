// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package topology

import (
	"errors"
	"fmt"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/cluster/shard"
	aresShard "github.com/uber/aresdb/cluster/shard"
)

var errInstanceHasNoShardsAssignment = errors.New("invalid instance with no shards assigned")

// host is the implementation of interface Host
type host struct {
	id      string
	address string
}

func (h *host) ID() string {
	return h.id
}

func (h *host) Address() string {
	return h.address
}

func (h *host) String() string {
	return fmt.Sprintf("Host<ID=%s, Address=%s>", h.id, h.address)
}

// NewHost creates a new host
func NewHost(id, address string) Host {
	return &host{id: id, address: address}
}

// hostShardSet is the implementation of the interface HostShardSet
type hostShardSet struct {
	host     Host
	shardSet aresShard.ShardSet
}

// NewHostShardSet creates a new host shard set
func NewHostShardSet(host Host, shardSet aresShard.ShardSet) HostShardSet {
	return &hostShardSet{host, shardSet}
}

func (h *hostShardSet) Host() Host {
	return h.host
}

func (h *hostShardSet) ShardSet() aresShard.ShardSet {
	return h.shardSet
}

// NewHostShardSetFromServiceInstance creates a new
// host shard set derived from a service instance
func NewHostShardSetFromServiceInstance(si services.ServiceInstance) (HostShardSet, error) {
	if si.Shards() == nil {
		return nil, errInstanceHasNoShardsAssignment
	}
	all := si.Shards().All()
	shards := make([]shard.Shard, len(all))
	copy(shards, all)
	shardSet := aresShard.NewShardSet(shards)

	return NewHostShardSet(NewHost(si.InstanceID(), si.Endpoint()), shardSet), nil
}
