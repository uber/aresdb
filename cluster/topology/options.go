package topology

import (
	"errors"
	"fmt"
	"github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/uber/aresdb/cluster/shard"
	"github.com/uber/aresdb/utils"
	"time"
)

const (
	defaultServiceName = "aresDB"
	defaultInitTimeout = 0 // Wait indefinitely by default for topology
	defaultReplicas    = 3
)

var (
	errNoConfigServiceClient = errors.New("no config service client")
	errInvalidReplicas       = errors.New("replicas must be equal to or greater than 1")
)

// staticOptions is the implementation of the interface StaticOptions
type staticOptions struct {
	shardSet      shard.ShardSet
	hostShardSets []HostShardSet
	replicas      int
}

// NewStaticOptions creates a new set of static topology options
func NewStaticOptions() StaticOptions {
	return &staticOptions{
		replicas: defaultReplicas,
	}
}

func (o *staticOptions) SetShardSet(value shard.ShardSet) StaticOptions {
	opts := *o
	opts.shardSet = value
	return &opts
}

func (o *staticOptions) ShardSet() shard.ShardSet {
	return o.shardSet
}

func (o *staticOptions) SetHostShardSets(value []HostShardSet) StaticOptions {
	opts := *o
	opts.hostShardSets = value
	return &opts
}

func (o *staticOptions) HostShardSets() []HostShardSet {
	return o.hostShardSets
}

func (o *staticOptions) SetReplicas(value int) StaticOptions {
	opts := *o
	opts.replicas = value
	return &opts
}

func (o *staticOptions) Replicas() int {
	return o.replicas
}

// dynamicOptions is the implementation of the interface DynamicOptions
type dynamicOptions struct {
	configServiceClient     client.Client
	serviceID               services.ServiceID
	servicesOverrideOptions services.OverrideOptions
	queryOptions            services.QueryOptions
	instrumentOptions       utils.Options
	initTimeout             time.Duration
}

// NewDynamicOptions creates a new set of dynamic topology options
func NewDynamicOptions() DynamicOptions {
	return &dynamicOptions{
		serviceID:               services.NewServiceID().SetName(defaultServiceName),
		servicesOverrideOptions: services.NewOverrideOptions(),
		queryOptions:            services.NewQueryOptions(),
		instrumentOptions:       utils.NewOptions(),
		initTimeout:             defaultInitTimeout,
	}
}

func (o *staticOptions) Validate() error {
	if o.replicas < 1 {
		return errInvalidReplicas
	}

	// Make a mapping of each shard to a set of hosts and check each
	// shard has at least the required replicas mapped to
	// NB(r): We allow greater than the required replicas in case
	// node is streaming in and needs to take writes
	totalShards := len(o.shardSet.AllIDs())
	hostAddressesByShard := make([]map[string]struct{}, totalShards)
	for i := range hostAddressesByShard {
		hostAddressesByShard[i] = make(map[string]struct{}, o.replicas)
	}
	for _, hostShardSet := range o.hostShardSets {
		hostAddress := hostShardSet.Host().Address()
		for _, shard := range hostShardSet.ShardSet().AllIDs() {
			hostAddressesByShard[shard][hostAddress] = struct{}{}
		}
	}
	for shard, hosts := range hostAddressesByShard {
		if len(hosts) < o.replicas {
			errorFmt := "shard %d has %d replicas, less than the required %d replicas"
			return fmt.Errorf(errorFmt, shard, len(hosts), o.replicas)
		}
	}

	return nil
}

func (o *dynamicOptions) SetConfigServiceClient(c client.Client) DynamicOptions {
	o.configServiceClient = c
	return o
}

func (o *dynamicOptions) ConfigServiceClient() client.Client {
	return o.configServiceClient
}

func (o *dynamicOptions) SetServiceID(s services.ServiceID) DynamicOptions {
	o.serviceID = s
	return o
}

func (o *dynamicOptions) ServiceID() services.ServiceID {
	return o.serviceID
}

func (o *dynamicOptions) SetServicesOverrideOptions(opts services.OverrideOptions) DynamicOptions {
	o.servicesOverrideOptions = opts
	return o
}

func (o *dynamicOptions) ServicesOverrideOptions() services.OverrideOptions {
	return o.servicesOverrideOptions
}

func (o *dynamicOptions) SetQueryOptions(qo services.QueryOptions) DynamicOptions {
	o.queryOptions = qo
	return o
}

func (o *dynamicOptions) QueryOptions() services.QueryOptions {
	return o.queryOptions
}

func (o *dynamicOptions) SetInstrumentOptions(io utils.Options) DynamicOptions {
	o.instrumentOptions = io
	return o
}

func (o *dynamicOptions) InstrumentOptions() utils.Options {
	return o.instrumentOptions
}

func (o *dynamicOptions) Validate() error {
	if o.ConfigServiceClient() == nil {
		return errNoConfigServiceClient
	}

	return nil
}
