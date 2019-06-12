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

package datanode

import (
	"fmt"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"
	xsync "github.com/m3db/m3/src/x/sync"
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/cluster"
	"github.com/uber/aresdb/cluster/shard"
	"github.com/uber/aresdb/cluster/topology"
	"github.com/uber/aresdb/common"
	"github.com/uber/aresdb/datanode/bootstrap"
	"github.com/uber/aresdb/datanode/client"
	"github.com/uber/aresdb/diskstore"
	"github.com/uber/aresdb/memstore"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/utils"
	"go.uber.org/zap"
	"sync"
)

// dataNode includes metastore, memstore and diskstore
type dataNode struct {
	sync.RWMutex

	host                 topology.Host
	peerSource           client.PeerSource
	namespace            cluster.Namespace
	metadata             metaCom.MetaStore
	memdata              memstore.MemStore
	diskdata             diskstore.DiskStore
	opts                 Options
	logger               common.Logger
	metrics              datanodeMetrics
	bootstrapState       bootstrap.BootstrapState
	bootstrapTableShards []*memstore.TableShard
}

type datanodeMetrics struct {
	bootstrap      instrument.MethodMetrics
	bootstrapStart tally.Counter
	bootstrapEnd   tally.Counter
}

func newDatanodeMetrics(scope tally.Scope, samplingRate float64) datanodeMetrics {
	return datanodeMetrics{
		bootstrap:      instrument.NewMethodMetrics(scope, "bootstrap", samplingRate),
		bootstrapStart: scope.Counter("bootstrap.start"),
		bootstrapEnd:   scope.Counter("bootstrap.end"),
	}
}

func NewDataNode(
	host topology.Host,
	namespace cluster.Namespace,
	memStore memstore.MemStore,
	metaStore metaCom.MetaStore,
	diskStore diskstore.DiskStore,
	opts Options) DataNode {
	iOpts := opts.InstrumentOptions()
	logger := iOpts.Logger().With(zap.String("datanode", host.ID()))

	scope := iOpts.MetricsScope().SubScope("namespace").
		Tagged(map[string]string{
			"datanode": host.ID(),
		})

	d := dataNode{
		host:      host,
		namespace: namespace,
		metadata:  metaStore,
		memdata:   memStore,
		diskdata:  diskStore,
		opts:      opts,
		logger:    logger,
		metrics:   newDatanodeMetrics(scope, opts.InstrumentOptions().MetricsSamplingRate()),
	}
	return &d
}

// Options returns the database options.
func (d *dataNode) Options() Options {
	return d.opts
}

// ShardSet returns the set of shards currently associated with this datanode.
func (d *dataNode) ShardSet() (shard.ShardSet, error) {
	t, err := d.namespace.Topology()
	if err != nil {
		return nil, err
	}
	hostShardSet, ok := t.Get().LookupHostShardSet(d.host.ID())
	if !ok {
		return nil, fmt.Errorf("datanode: %s not found in topology", d.host.ID())
	}
	return hostShardSet.ShardSet(), nil
}

// AssignNamespace sets the namespace
func (d *dataNode) AssignNamespace(namespace cluster.Namespace) {
	d.namespace = namespace
}

// Namespaces returns the namespace.
func (d *dataNode) Namespace() cluster.Namespace {
	return d.namespace
}

// Host returns the datanode host information.
func (d *dataNode) Host() topology.Host {
	return d.host
}

// Bootstrap bootstraps this datanode.
func (d *dataNode) Bootstrap(topo topology.Topology, options bootstrap.Options) error {
	callStart := utils.Now()

	d.Lock()
	if d.bootstrapState == bootstrap.Bootstrapping {
		d.Unlock()
		d.metrics.bootstrap.ReportError(utils.Now().Sub(callStart))
		return bootstrap.ErrDatanodeIsBootstrapping
	}
	d.bootstrapState = bootstrap.Bootstrapping
	d.Unlock()

	success := false
	defer func() {
		d.Lock()
		if success {
			d.bootstrapState = bootstrap.Bootstrapped
		} else {
			d.bootstrapState = bootstrap.BootstrapNotStarted
		}
		d.Unlock()
		d.metrics.bootstrapEnd.Inc(1)
	}()

	tables, err := d.metadata.ListTables()
	if err != nil {
		d.logger.With("error", err).Error("Failed at ListTables")
		return err
	}

	shards, err := d.ShardSet()
	if err != nil {
		d.logger.With("error", err).Error("Failed at ShardSet")
		return err
	}

	d.bootstrapTableShards = make([]*memstore.TableShard, 0, len(tables)*len(shards.AllIDs()))
	for _, tablename := range tables {
		table, err := d.metadata.GetTable(tablename)
		if err != nil {
			d.logger.
				With("error", err).
				With("table", tablename).
				Error("Failed at GetTable")
			return err
		}

		if !table.IsFactTable {
			// dimension table always has only 1 shard
			err := d.addTableShard(tablename, 0)
			if err != nil {
				return err
			}
		} else {
			for _, shardID := range shards.AllIDs() {
				err := d.addTableShard(tablename, shardID)
				if err != nil {
					return err
				}
			}
		}
	}

	if len(d.bootstrapTableShards) == 0 {
		success = true
		d.metrics.bootstrap.ReportSuccess(utils.Now().Sub(callStart))
		return nil
	}

	workers := xsync.NewWorkerPool(options.MaxConcurrentTableShards())
	workers.Init()

	var (
		multiErr = xerrors.NewMultiError()
		mutex    sync.Mutex
		wg       sync.WaitGroup
	)

	topoStateSnapshot := d.newInitialTopologyState(topo)
	for _, tableShard := range d.bootstrapTableShards {
		// capture table shard
		tableShard := tableShard
		wg.Add(1)
		workers.Go(func() {
			err := tableShard.Bootstrap(d.peerSource, d.host, topo, topoStateSnapshot, options)

			mutex.Lock()
			multiErr = multiErr.Add(err)
			mutex.Unlock()

			// unpin table shard after use
			tableShard.Users.Done()
			wg.Done()
		})
	}
	wg.Wait()

	d.metrics.bootstrap.Success.Inc(1)

	err = multiErr.FinalError()
	d.metrics.bootstrap.ReportSuccessOrError(err, utils.Now().Sub(callStart))
	success = err == nil

	return err
}

func (d *dataNode) TableShardsBootstrapState() bootstrap.TableShardsBootstrapState {
	d.RLock()
	tableShardsState := make(bootstrap.TableShardsBootstrapState)
	for _, tableShard := range d.bootstrapTableShards {
		if tableShard == nil {
			continue
		}
		if tableShardsState[tableShard.Schema.Schema.Name] == nil {
			tableShardsState[tableShard.Schema.Schema.Name] = make(map[uint32]bootstrap.BootstrapState)
		}
		tableShardsState[tableShard.Schema.Schema.Name][uint32(tableShard.ShardID)] = tableShard.BootstrapState()
	}
	d.RUnlock()
	return tableShardsState
}

func (d *dataNode) ShardsBootstrapState() bootstrap.ShardsBootstrapState {
	d.RLock()
	shardStates := make(bootstrap.ShardsBootstrapState)
	for _, tableShard := range d.bootstrapTableShards {
		if tableShard == nil {
			continue
		}
		if v, ok := shardStates[uint32(tableShard.ShardID)]; !ok {
			shardStates[uint32(tableShard.ShardID)] = tableShard.BootstrapState()
		} else {
			if v == bootstrap.Bootstrapped {
				shardStates[uint32(tableShard.ShardID)] = tableShard.BootstrapState()
			}
		}
	}
	d.RUnlock()
	return shardStates
}

func (d *dataNode) addTableShard(tablename string, shardID uint32) error {
	tableShard, err := d.memdata.GetTableShard(tablename, int(shardID))
	if err != nil {
		d.logger.
			With("error", err).
			With("table", tablename).
			With("shard", shardID).
			Error("Failed at GetTableShard")
		return err
	}
	if !tableShard.IsBootstrapped() {
		d.bootstrapTableShards = append(d.bootstrapTableShards, tableShard)
	}

	return nil
}

func (d *dataNode) newInitialTopologyState(topo topology.Topology) *topology.StateSnapshot {
	topoMap := topo.Get()

	var (
		hostShardSets = topoMap.HostShardSets()
		topologyState = &topology.StateSnapshot{
			Origin:      d.Host(),
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
