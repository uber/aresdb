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
	"github.com/uber/aresdb/utils"
	"sync"
	"time"
)

const (
	unhealthyRetryPeriodSeconds = 10
)

type healthiness struct {
	healthy             bool
	lastUpdateTimestamp time.Time
}

type healthTrackingDynamicTopoImpl struct {
	sync.RWMutex

	dynamicTopology  Topology
	hostsHealthiness map[Host]*healthiness
	closed           bool
}

func NewHealthTrackingDynamicTopology(opts DynamicOptions) (HealthTrackingDynamicTopoloy, error) {
	dynamicTopo, err := NewDynamicInitializer(opts).Init()
	if err != nil {
		return nil, err
	}

	topo := &healthTrackingDynamicTopoImpl{
		dynamicTopology:  dynamicTopo,
		hostsHealthiness: make(map[Host]*healthiness),
	}

	return topo, nil
}

func (ht *healthTrackingDynamicTopoImpl) Get() Map {
	ht.Lock()
	defer ht.Unlock()

	// filter known unhealthy hosts from dynamic topology
	dm := ht.dynamicTopology.Get()
	dhss := dm.HostShardSets()
	var hostShardSets []HostShardSet
	for _, hss := range dhss {
		h, found := ht.hostsHealthiness[hss.Host()]
		if !found {
			newHealthiness := &healthiness{
				healthy:             true,
				lastUpdateTimestamp: utils.Now(),
			}
			ht.hostsHealthiness[hss.Host()] = newHealthiness
			h = newHealthiness
		}
		if h.healthy || utils.Now().Sub(h.lastUpdateTimestamp).Seconds() > unhealthyRetryPeriodSeconds {
			hostShardSets = append(hostShardSets, hss)
		}
	}

	return NewStaticMap(NewStaticOptions().
		SetShardSet(dm.ShardSet()).
		SetReplicas(dm.Replicas()).
		SetHostShardSets(hostShardSets))
}

func (ht *healthTrackingDynamicTopoImpl) MarkHostHealthy(host Host) error {
	return ht.changeHostHealthState(host, true)
}

func (ht *healthTrackingDynamicTopoImpl) MarkHostUnhealthy(host Host) error {
	return ht.changeHostHealthState(host, false)
}

func (ht *healthTrackingDynamicTopoImpl) changeHostHealthState(host Host, healthy bool) error {
	ht.Lock()
	defer ht.Unlock()

	h, found := ht.hostsHealthiness[host]
	if !found {
		return utils.StackError(nil, "failed to change host health state, host not found. host: %s, healthiness %t", host, healthy)
	}
	h.healthy = healthy
	h.lastUpdateTimestamp = utils.Now()
	return nil
}

// dummy implementation, don't use
// TODO: implement when needed
func (ht *healthTrackingDynamicTopoImpl) Watch() (MapWatch, error) {
	return nil, nil
}

func (ht *healthTrackingDynamicTopoImpl) isClosed() bool {
	ht.RLock()
	closed := ht.closed
	ht.RUnlock()
	return closed
}

func (ht *healthTrackingDynamicTopoImpl) Close() {
	ht.Lock()
	defer ht.Unlock()

	if ht.closed {
		return
	}

	ht.closed = true

	ht.dynamicTopology.Close()
}
