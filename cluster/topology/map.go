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

func (sm *staticMap) RouteShard(shard uint32) ([]Host, error) {
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
