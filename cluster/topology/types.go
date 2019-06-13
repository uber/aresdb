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
	"github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/services"
	m3Shard "github.com/m3db/m3/src/cluster/shard"
	"github.com/uber/aresdb/cluster/shard"
	"github.com/uber/aresdb/utils"
)

var (
	errUnownedShard = errors.New("unowned shard")
)

// Host is a container of a host in a topology
type Host interface {
	// ID is the identifier of the host
	ID() string

	// Address returns the address of the host
	Address() string

	// String returns a string representation of the host
	String() string
}

// HostShardSet is a container for a host and corresponding shard set
type HostShardSet interface {
	// Host returns the host
	Host() Host

	// ShardSet returns the shard set owned by the host
	ShardSet() shard.ShardSet
}

// Map describes a placement
type Map interface {
	// Hosts returns all hosts in the map
	Hosts() []Host

	// HostShardSets returns all HostShardSets in the map
	HostShardSets() []HostShardSet

	// LookupHostShardSet returns a HostShardSet for a host in the map
	LookupHostShardSet(hostID string) (HostShardSet, bool)

	// HostsLen returns the capacity of all hosts in the map
	HostsLen() int

	// ShardSet returns the shard set for the topology
	ShardSet() shard.ShardSet

	// RouteShard will route a given shard to a set of hosts
	RouteShard(shard int) ([]Host, error)

	// Replicas returns the number of replicas in the topology
	Replicas() int
}

type MapWatch interface {
	// WatchChan is a notification channel when a value becomes available
	C() <-chan struct{}

	// Get the current placement map
	Get() Map

	// Close the watch on the placement map
	Close()
}

// Initializer can init new instances of Topology
type Initializer interface {
	// Init will return a new topology
	Init() (Topology, error)

	// TopologyIsSet returns whether the topology is able to be
	// initialized immediately or if instead it will blocked
	// wait to be set on initialization
	TopologyIsSet() (bool, error)
}

// Topology is a container of a placement map and disseminates the placement map changes
type Topology interface {
	// Get the placement setting map
	Get() Map

	// Watch for the placement setting map
	Watch() (MapWatch, error)

	// Close will close the placement setting map
	Close()
}

// DynamicTopology is a topology that dynamically changes and as such
// adds functionality for a clustered database to call back and mark
// a shard as available once it completes bootstrapping
type DynamicTopology interface {
	Topology

	// MarkShardsAvailable marks a shard with the state of initializing as available
	MarkShardsAvailable(instanceID string, shardIDs ...uint32) error
}

// StaticConfiguration is used for standing up M3DB with a static topology
type StaticConfiguration struct {
	Shards   int               `yaml:"shards"`
	Replicas int               `yaml:"replicas"`
	Hosts    []HostShardConfig `yaml:"hosts"`
}

// HostShardConfig stores host information for fanout
type HostShardConfig struct {
	HostID        string `yaml:"hostID"`
	ListenAddress string `yaml:"listenAddress"`
}

// StaticOptions is a set of options for static topology
type StaticOptions interface {
	// Validate validates the options
	Validate() error

	// SetShardSet sets the ShardSet
	SetShardSet(value shard.ShardSet) StaticOptions

	// ShardSet returns the ShardSet
	ShardSet() shard.ShardSet

	// SetHostShardSets sets the hostShardSets
	SetHostShardSets(value []HostShardSet) StaticOptions

	// HostShardSets returns the hostShardSets
	HostShardSets() []HostShardSet

	// SetReplicas sets the replicas
	SetReplicas(value int) StaticOptions

	// Replicas returns the replicas
	Replicas() int
}

// DynamicOptions is a set of options for dynamic topology
type DynamicOptions interface {
	// Validate validates the options
	Validate() error

	// SetConfigServiceClient sets the client of ConfigService
	SetConfigServiceClient(c client.Client) DynamicOptions

	// ConfigServiceClient returns the client of ConfigService
	ConfigServiceClient() client.Client

	// SetServiceID sets the ServiceID for service discovery
	SetServiceID(s services.ServiceID) DynamicOptions

	// ServiceID returns the ServiceID for service discovery
	ServiceID() services.ServiceID

	// SetServicesOverrideOptions sets the override options for service discovery.
	SetServicesOverrideOptions(opts services.OverrideOptions) DynamicOptions

	// ServicesOverrideOptions returns the override options for service discovery.
	ServicesOverrideOptions() services.OverrideOptions

	// SetQueryOptions sets the ConfigService query options
	SetQueryOptions(value services.QueryOptions) DynamicOptions

	// QueryOptions returns the ConfigService query options
	QueryOptions() services.QueryOptions

	// SetInstrumentOptions sets the instrumentation options
	SetInstrumentOptions(value utils.Options) DynamicOptions

	// InstrumentOptions returns the instrumentation options
	InstrumentOptions() utils.Options
}

// MapProvider is an interface that can provide
// a topology map.
type MapProvider interface {
	// TopologyMap returns a topology map.
	TopologyMap() (Map, error)
}

// StateSnapshot represents a snapshot of the state of the topology at a
// given moment.
type StateSnapshot struct {
	Origin      Host
	ShardStates ShardStates
}

// ShardStates maps shard IDs to the state of each of the hosts that own
// that shard.
type ShardStates map[ShardID]map[HostID]HostShardState

// HostShardState contains the state of a shard as owned by a given host.
type HostShardState struct {
	Host       Host
	ShardState m3Shard.State
}

// HostID is the string representation of a host ID.
type HostID string

// ShardID is the ID of a shard.
type ShardID int
