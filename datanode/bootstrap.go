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
	"fmt"
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/cluster/topology"
	"github.com/uber/aresdb/common"
	"github.com/uber/aresdb/datanode/bootstrap"
	"github.com/uber/aresdb/utils"
	"go.uber.org/zap"

	xerrors "github.com/m3db/m3/src/x/errors"
	"sync"
	"time"
)

// bootstrapManagerImpl is the implementation of the interface databaseBootstrapManager
type bootstrapManagerImpl struct {
	sync.RWMutex

	datanode                    DataNode
	opts                        Options
	log                         common.Logger
	state                       bootstrap.BootstrapState
	hasPending                  bool
	status                      tally.Gauge
	lastBootstrapCompletionTime time.Time
	topo                        topology.Topology
}

func NewBootstrapManager(datanode DataNode, opts Options, topo topology.Topology) (BootstrapManager, error) {
	if topo == nil {
		return nil, fmt.Errorf("failed at NewBootstrapManager, reason: topo is nil")
	}
	scope := opts.InstrumentOptions().MetricsScope()
	return &bootstrapManagerImpl{
		datanode: datanode,
		opts:     opts,
		log:      opts.InstrumentOptions().Logger(),
		status:   scope.Gauge("bootstrapped"),
		topo:     topo,
	}, nil

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
		return bootstrap.ErrBootstrapEnqueued
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

func (m *bootstrapManagerImpl) Report() {
	if m.IsBootstrapped() {
		m.status.Update(1)
	} else {
		m.status.Update(0)
	}
}

func (m *bootstrapManagerImpl) bootstrap() error {
	starDatanodeBootstrap := utils.Now()
	err := m.datanode.Bootstrap(m.topo, m.opts.BootstrapOptions())
	took := utils.Now().Sub(starDatanodeBootstrap)
	m.log.Info("bootstrap finished",
		zap.String("datanode", m.datanode.Host().ID()),
		zap.Duration("duration", took),
	)

	return err
}
