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

package datanode

import (
	"github.com/uber/aresdb/cluster/topology"
	"github.com/uber/aresdb/common"
	"github.com/uber/aresdb/datanode/bootstrap"
	"github.com/uber/aresdb/datanode/client"
	"github.com/uber/aresdb/utils"
	"sync"
	"time"

	xerrors "github.com/m3db/m3/src/x/errors"
)

// bootstrapManagerImpl is the implementation of the interface databaseBootstrapManager
type bootstrapManagerImpl struct {
	sync.RWMutex

	opts                        Options
	log                         common.Logger
	origin                      string
	peerSource                  client.PeerSource
	bootstrapable               bootstrap.Bootstrapable
	state                       bootstrap.BootstrapState
	hasPending                  bool
	bootstrapTableShards        map[string]bootstrap.BootstrapDetails
	lastBootstrapCompletionTime time.Time
	topo                        topology.Topology
}

// NewBootstrapManager creates bootstrap manager
func NewBootstrapManager(origin string, bootstrappable bootstrap.Bootstrapable, opts Options, topo topology.Topology) BootstrapManager {
	peerSource, err := NewPeerSource(topo)
	if err != nil {
		opts.InstrumentOptions().Logger().With("error", err.Error()).Fatal("failed to initalize peer source")
	}

	return &bootstrapManagerImpl{
		origin:        origin,
		bootstrapable: bootstrappable,
		opts:          opts,
		log:           opts.InstrumentOptions().Logger(),
		peerSource:    peerSource,
		topo:          topo,
	}
}

func (m *bootstrapManagerImpl) IsBootstrapped() bool {
	m.RLock()
	state := m.state
	m.RUnlock()
	return state == bootstrap.Bootstrapped
}

func (m *bootstrapManagerImpl) LastBootstrapCompletionTime() (time.Time, bool) {
	return m.lastBootstrapCompletionTime, !m.lastBootstrapCompletionTime.IsZero()
}

func (m *bootstrapManagerImpl) Bootstrap() error {
	m.Lock()
	switch m.state {
	case bootstrap.Bootstrapping:
		// NB(r): Already bootstrapping, now a consequent bootstrap
		// request comes in - we queue this up to bootstrap again
		// once the current bootstrap has completed.
		// This is an edge case that can occur if during either an
		// initial bootstrap or a resharding bootstrap if a new
		// reshard occurs and we need to bootstrap more shards.
		m.hasPending = true
		m.Unlock()
		m.log.Info("bootstrap enqueued, datanode is in bootstrapping state")
		return nil
	default:
		m.state = bootstrap.Bootstrapping
	}
	m.Unlock()

	// Keep performing bootstraps until none pending
	multiErr := xerrors.NewMultiError()
	for {
		err := m.bootstrap()
		if err != nil {
			multiErr = multiErr.Add(err)
		}

		m.Lock()
		currPending := m.hasPending
		if currPending {
			// New bootstrap calls should now enqueue another pending bootstrap
			m.hasPending = false
		} else {
			m.state = bootstrap.Bootstrapped
		}
		m.Unlock()

		if !currPending {
			break
		}
	}

	m.lastBootstrapCompletionTime = utils.Now()
	return multiErr.FinalError()
}

func (m *bootstrapManagerImpl) bootstrap() error {
	startDatanodeBootstrap := utils.Now()
	topoStateSnapshot := newInitialTopologyState(m.topo)
	err := m.bootstrapable.Bootstrap(m.peerSource, m.origin, m.topo, topoStateSnapshot, m.opts.BootstrapOptions())
	took := utils.Now().Sub(startDatanodeBootstrap)
	if err != nil {
		m.log.With("datanode", m.origin).
			With("duration", took).
			With("error", err.Error()).
			Info("bootstrap finished with err")
		return err
	}
	m.log.With("datanode", m.origin).
		With("duration", took).
		Info("bootstrap finished")
	return nil
}

func newInitialTopologyState(topo topology.Topology) *topology.StateSnapshot {
	topoMap := topo.Get()

	var (
		hostShardSets = topoMap.HostShardSets()
		topologyState = &topology.StateSnapshot{
			ShardStates: topology.ShardStates{},
		}
	)

	for _, hostShardSet := range hostShardSets {
		for _, currShard := range hostShardSet.ShardSet().All() {
			shardID := topology.ShardID(currShard.ID())
			existing, ok := topologyState.ShardStates[shardID]
			if !ok {
				existing = map[topology.HostID]topology.HostShardState{}
				topologyState.ShardStates[shardID] = existing
			}

			hostID := topology.HostID(hostShardSet.Host().ID())
			existing[hostID] = topology.HostShardState{
				Host:       hostShardSet.Host(),
				ShardState: currShard.State(),
			}
		}
	}

	return topologyState
}
