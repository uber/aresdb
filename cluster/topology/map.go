package topology

import (
	xwatch "github.com/m3db/m3/src/x/watch"
	aresShard "github.com/uber/aresdb/cluster/shard"
)

// staticMap is the implementation of the interface Map
type staticMap struct {
	shardSet          aresShard.ShardSet
	hostShardSets     []HostShardSet
	hostShardSetsByID map[string]HostShardSet
	hostsByShard      [][]Host
	orderedHosts      []Host
	replicas          int
}

// NewStaticMap creates Map
func NewStaticMap(opts StaticOptions) Map {
	totalShards := len(opts.ShardSet().AllIDs())
	hostShardSets := opts.HostShardSets()
	staticMap := staticMap{
		shardSet:          opts.ShardSet(),
		hostShardSets:     hostShardSets,
		hostShardSetsByID: make(map[string]HostShardSet),
		hostsByShard:      make([][]Host, totalShards),
		orderedHosts:      make([]Host, 0, len(hostShardSets)),
		replicas:          opts.Replicas(),
	}

	for _, hostShardSet := range hostShardSets {
		host := hostShardSet.Host()
		staticMap.hostShardSetsByID[host.ID()] = hostShardSet
		staticMap.orderedHosts = append(staticMap.orderedHosts, host)
		for _, shard := range hostShardSet.ShardSet().AllIDs() {
			staticMap.hostsByShard[shard] = append(staticMap.hostsByShard[shard], host)
		}
	}
	return &staticMap
}

func (sm *staticMap) Hosts() []Host {
	return sm.orderedHosts
}

func (sm *staticMap) HostShardSets() []HostShardSet {
	return sm.hostShardSets
}

func (sm *staticMap) LookupHostShardSet(hostID string) (HostShardSet, bool) {
	v, ok := sm.hostShardSetsByID[hostID]
	return v, ok
}

func (sm *staticMap) HostsLen() int {
	return len(sm.orderedHosts)
}

func (sm *staticMap) ShardSet() aresShard.ShardSet {
	return sm.shardSet
}

func (sm *staticMap) RouteShard(shard int) ([]Host, error) {
	if int(shard) >= len(sm.hostsByShard) {
		return nil, errUnownedShard
	}
	return sm.hostsByShard[shard], nil
}

func (t *staticMap) Replicas() int {
	return t.replicas
}

// mapWatch is the implementation of the interface MapWatch
type mapWatch struct {
	xwatch.Watch
}

// NewMapWatch creates MapWatch
func NewMapWatch(w xwatch.Watch) MapWatch {
	return &mapWatch{w}
}

func (w *mapWatch) C() <-chan struct{} {
	return w.Watch.C()
}

func (w *mapWatch) Get() Map {
	value := w.Watch.Get()
	if value == nil {
		return nil
	}
	return value.(Map)
}

func (w *mapWatch) Close() {
	w.Watch.Close()
}
