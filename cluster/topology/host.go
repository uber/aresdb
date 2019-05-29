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
