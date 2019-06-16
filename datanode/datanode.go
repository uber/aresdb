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
	"errors"
	"fmt"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/services"
	m3Shard "github.com/m3db/m3/src/cluster/shard"
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/api"
	"github.com/uber/aresdb/cluster"
	"github.com/uber/aresdb/cluster/shard"
	"github.com/uber/aresdb/cluster/topology"
	"github.com/uber/aresdb/common"
	"github.com/uber/aresdb/diskstore"
	"github.com/uber/aresdb/memstore"
	memCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/metastore"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/redolog"
	"github.com/uber/aresdb/utils"
	"go.uber.org/zap"
	"net/http"
	"net/http/pprof"
	"path/filepath"
	"sync"
	"time"
)

// dataNode includes metastore, memstore and diskstore
type dataNode struct {
	sync.RWMutex

	hostID     string
	startedAt  time.Time
	shardSet   shard.ShardSet
	clusterServices services.Services

	namespace  cluster.Namespace
	metaStore  metaCom.MetaStore
	memStore   memstore.MemStore
	diskStore  diskstore.DiskStore

	opts                 Options
	logger               common.Logger
	metrics              datanodeMetrics
	handlers 			 datanodeHandlers

	bootstraps int
	bootstrapManager     BootstrapManager

	mapWatch topology.MapWatch
	close 		  chan struct{}
}

type datanodeHandlers struct {
	schemaHandler *api.SchemaHandler
	enumHandler   *api.EnumHandler
	queryHandler  *api.QueryHandler
	dataHandler   *api.DataHandler
	nodeModuleHandler http.Handler
	debugStaticHandler http.Handler
	debugHandler  *api.DebugHandler
	healthCheckHandler *api.HealthCheckHandler
	swaggerHandler http.Handler
}

type datanodeMetrics struct {
	restartTimer tally.Timer
}

// NewDataNode creates a new data node
func NewDataNode(
	hostID string,
	namespace cluster.Namespace,
	redoLogManagerMaster *redolog.RedoLogManagerMaster,
	opts Options) (DataNode, error) {

	iOpts := opts.InstrumentOptions()
	logger := iOpts.Logger().With(zap.String("datanode", hostID))

	scope := iOpts.MetricsScope().SubScope("namespace").
		Tagged(map[string]string{
			"datanode": hostID,
		})

	metaStore, err := metastore.NewDiskMetaStore(filepath.Join(opts.ServerConfig().RootPath, "metastore"))
	if err != nil {
		return nil, utils.StackError(err, "failed to initialize local metastore")
	}
	diskStore := diskstore.NewLocalDiskStore(opts.ServerConfig().RootPath)
	memStore := memstore.NewMemStore(metaStore, diskStore, redoLogManagerMaster)

	d := &dataNode{
		hostID:    hostID,
		namespace: namespace,
		metaStore: metaStore,
		memStore:  memStore,
		diskStore: diskStore,
		opts:      opts,
		logger:    logger,
		metrics:   newDatanodeMetrics(scope),
	}
	d.handlers = d.newHandlers()
	d.bootstrapManager = NewBootstrapManager(d, opts, namespace.Topology())

	return d, nil
}

// Open data node for serving
func (d *dataNode) Open() error {
	d.startedAt = utils.Now()
	// memstore fetch local disk schema
	err := d.memStore.FetchSchema()
	if err != nil {
		return err
	}

	//TODO: 1. start schema watch

	// 2. start debug server
	go d.startDebugServer()

	// 3. first shard assignment
	d.mapWatch, err = d.namespace.Topology().Watch()
	if err != nil {
		return utils.StackError(err, "failed to watch topology")
	}

	select {
		case <-d.mapWatch.C():
			hostShardSet, ok := d.mapWatch.Get().LookupHostShardSet(d.hostID)
			if !ok {
				d.shardSet = shard.NewShardSet(nil)
			} else {
				d.shardSet = hostShardSet.ShardSet()
			}
			d.AssignShardSet(d.shardSet)
		default:
	}

	d.memStore.GetHostMemoryManager().Start()

	// 5. start scheduler
	if !d.opts.ServerConfig().SchedulerOff {
		d.opts.InstrumentOptions().Logger().Info("starting archiving scheduler")
		// disable archiving during redolog replay
		d.memStore.GetScheduler().EnableJobType(memCom.ArchivingJobType, false)
		// this will start scheduler of all jobs except archiving, archiving will be started individually
		d.memStore.GetScheduler().Start()
	} else {
		d.opts.InstrumentOptions().Logger().Info("scheduler is turned off")
	}

	// 6. start active topology watch
	go d.startActiveTopologyWatch()
	// 7. start analyzing shard availability
	go d.startAnalyzingShardAvailability()

	return nil
}

func (d *dataNode) Close() {
	close(d.close)
	d.mapWatch.Close()
}

func (d *dataNode) startDebugServer() {
	debugRouter := mux.NewRouter()
	debugRouter.PathPrefix("/node_modules/").Handler(d.handlers.nodeModuleHandler)
	debugRouter.PathPrefix("/static/").Handler(d.handlers.debugStaticHandler)
	debugRouter.HandleFunc("/debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
	debugRouter.HandleFunc("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
	debugRouter.HandleFunc("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))
	debugRouter.HandleFunc("/debug/pprof/trace", http.HandlerFunc(pprof.Trace))
	debugRouter.PathPrefix("/debug/pprof/").Handler(http.HandlerFunc(pprof.Index))

	d.handlers.debugHandler.Register(debugRouter.PathPrefix("/dbg").Subrouter())
	d.handlers.schemaHandler.RegisterForDebug(debugRouter.PathPrefix("/schema").Subrouter())

	d.opts.InstrumentOptions().Logger().Infof("Starting HTTP server on dbg-port %d", d.opts.ServerConfig().DebugPort)
	d.opts.InstrumentOptions().Logger().Fatal(http.ListenAndServe(fmt.Sprintf(":%d", d.opts.ServerConfig().DebugPort), debugRouter))
}

func (d *dataNode) startActiveTopologyWatch() {
	for {
		select {
		case <-d.close:
			d.mapWatch.Close()
		case <- d.mapWatch.C():
			topoMap := d.mapWatch.Get()
			hostShardSet, ok := topoMap.LookupHostShardSet(d.hostID)
			if ok {
				d.AssignShardSet(hostShardSet.ShardSet())
			}
		}
	}
}

func (d *dataNode) startAnalyzingShardAvailability() {
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <- ticker.C:
		case <- d.close:
			return
		}

		hostShardSet, ok := d.mapWatch.Get().LookupHostShardSet(d.hostID)
		if !ok {
			continue
		}

		initializing := make(map[uint32]m3Shard.Shard)
		for _, s := range hostShardSet.ShardSet().All() {
			if s.State() == m3Shard.Initializing {
				initializing[s.ID()] = s
			}
		}

		if len(initializing) == 0 {
			continue
		}

		dynamicTopo, ok := d.namespace.Topology().(topology.DynamicTopology)
		if !ok {
			d.logger.Error("cannot mark shard available", zap.Error(errors.New("topology is not dynamic")))
			continue
		}

		tables := d.Tables()
		availableShards := make([]uint32, 0, len(initializing))
		// check all whether all tables within initialing shards are bootstrapped
		for shardID := range initializing {
			numTablesBootstrapped := 0
			for _, table := range tables {
				tableShard, err := d.memStore.GetTableShard(table, int(shardID))
				if err != nil {
					d.logger.Error("cannot get table shard",
						zap.Error(err),
						zap.String("table", table),
						zap.Uint32("shard", shardID))
				}
				if tableShard.IsBootstrapped() {
					numTablesBootstrapped++
				}
				tableShard.Users.Done()
			}
			if numTablesBootstrapped == len(tables) {
				availableShards = append(availableShards, shardID)
			}
		}

		if len(availableShards) > 0 {
			if err := dynamicTopo.MarkShardsAvailable(d.hostID, availableShards...); err != nil {
				d.logger.Error("failed to mark shards as available", zap.Uint32s("shards", availableShards), zap.Error(err))
			} else {
				d.logger.Error("successfully marked shards as available", zap.Uint32s("shards", availableShards))
			}
		}
	}

}

// Options returns the database options.
func (d *dataNode) Options() Options {
	return d.opts
}

// ShardSet returns the set of shards currently associated with this datanode.
func (d *dataNode) ShardSet() shard.ShardSet {
	return d.shardSet
}

// Tables returns the actual tables owned by datanode
func (d *dataNode) Tables() (tables []string) {
	schemas := d.memStore.GetSchemas()
	for name := range schemas {
		tables = append(tables, name)
	}
	return
}

func (d *dataNode) ID() string {
	return d.hostID
}

func (d *dataNode) Serve() {
	// start advertising to the cluster
	d.advertise()

	router := mux.NewRouter()
	httpWrappers := append([]utils.HTTPHandlerWrapper{utils.WithMetricsFunc}, d.opts.HTTPWrappers()...)

	schemaRouter := router.PathPrefix("/schema")
	if d.opts.ServerConfig().Cluster.Enable {
		schemaRouter = schemaRouter.Methods(http.MethodGet)
	}

	d.handlers.schemaHandler.Register(schemaRouter.Subrouter(), httpWrappers...)
	d.handlers.enumHandler.Register(router.PathPrefix("/schema").Subrouter(), httpWrappers...)
	d.handlers.dataHandler.Register(router.PathPrefix("/data").Subrouter(), httpWrappers...)
	d.handlers.queryHandler.Register(router.PathPrefix("/query").Subrouter(), httpWrappers...)

	router.PathPrefix("/swagger/").Handler(d.handlers.swaggerHandler)
	router.PathPrefix("/node_modules/").Handler(d.handlers.nodeModuleHandler)
	router.HandleFunc("/health", utils.WithMetricsFunc(d.handlers.healthCheckHandler.HealthCheck))
	router.HandleFunc("/version", d.handlers.healthCheckHandler.Version)

	// Support CORS calls.
	allowOrigins := handlers.AllowedOrigins([]string{"*"})
	allowHeaders := handlers.AllowedHeaders([]string{"Accept", "Accept-Language", "Content-Language", "Origin", "Content-Type"})
	allowMethods := handlers.AllowedMethods([]string{"GET", "PUT", "POST", "DELETE", "OPTIONS"})

	// record time from data node started to actually serving
	d.metrics.restartTimer.Record(utils.Now().Sub(d.startedAt))
	d.opts.InstrumentOptions().Logger().Infof("Starting HTTP server on port %d with max connection %d", d.opts.ServerConfig().Port, d.opts.ServerConfig().HTTP.MaxConnections)
	utils.LimitServe(d.opts.ServerConfig().Port, handlers.CORS(allowOrigins, allowHeaders, allowMethods)(router), d.opts.ServerConfig().HTTP)
}

func (d *dataNode) advertise() {
	serviceID := services.NewServiceID().
		SetEnvironment(d.opts.ServerConfig().Env).
		SetZone(d.opts.ServerConfig().Zone).
		SetName(utils.DataNodeServiceName(d.opts.ServerConfig().Cluster.ClusterName))

	placementInstance := placement.NewInstance().SetID(d.hostID)
	ad := services.NewAdvertisement().
		SetServiceID(serviceID).
		SetPlacementInstance(placementInstance)

	err := d.clusterServices.Advertise(ad)
	if err != nil {
		d.opts.InstrumentOptions().Logger().Fatalf("failed to advertise data node",
			zap.String("id", d.opts.ServerConfig().InstanceConfig.ID),
			zap.Error(err))
	}
}

// GetTableShard returns the table shard from the datanode
func (d *dataNode) GetTableShard(table string, shardID uint32) (*memstore.TableShard, error) {
	return d.memStore.GetTableShard(table, int(shardID))
}

// Namespaces returns the namespace.
func (d *dataNode) Namespace() cluster.Namespace {
	return d.namespace
}

func (d *dataNode) AssignShardSet(shardSet shard.ShardSet) {
	d.Lock()

	tables := d.Tables()
	var (
		incoming = make(map[uint32]m3Shard.Shard, len(shardSet.All()))
		existing = make(map[uint32]struct{}, len(d.shardSet.AllIDs()))
		removing  []uint32
		adding    []m3Shard.Shard
	)

	for _, shard := range shardSet.All() {
		incoming[shard.ID()] = shard
	}

	for shardID := range incoming {
		existing[shardID] = struct{}{}
	}

	for shardID := range existing {
		if _, ok := incoming[shardID]; !ok {
			removing = append(removing, shardID)
		}
	}

	for shardID, shard := range incoming {
		if _, ok := existing[shardID]; !ok {
			adding = append(adding, shard)
		}
	}
	d.shardSet = shardSet

	for _, shardID := range removing {
		for _, table := range tables {
			d.memStore.RemoveTableShard(table, int(shardID))
		}
	}

	for _, shard := range adding {
		for _, table := range tables {
			d.memStore.AddTableShard(table, int(shard.ID()), shard.State() == m3Shard.Initializing)
		}
	}

	// only kick off bootstrap when the first bootstrap is done during shard assignment
	d.RLock()
	if d.bootstraps > 0 {
		go func() {
			if err := d.bootstrapManager.Bootstrap(); err != nil {
				d.logger.Error("error while bootstrapping", zap.Error(err))
			}
		}()
	}
	d.RUnlock()
}

func (d *dataNode) Bootstrap() error {
	d.Lock()
	d.bootstraps++
	d.Unlock()
	return d.bootstrapManager.Bootstrap()
}

func newDatanodeMetrics(scope tally.Scope) datanodeMetrics {
	return datanodeMetrics{
		restartTimer:        scope.Timer("restart"),
	}
}

func (d *dataNode) newHandlers() datanodeHandlers {
	healthCheckHandler := api.NewHealthCheckHandler()
	return datanodeHandlers{
		schemaHandler: api.NewSchemaHandler(d.metaStore),
		enumHandler: api.NewEnumHandler(d.memStore, d.metaStore),
		queryHandler:  api.NewQueryHandler(d.memStore, d.opts.ServerConfig().Query),
		dataHandler:   api.NewDataHandler(d.memStore),
		nodeModuleHandler: http.StripPrefix("/node_modules/", http.FileServer(http.Dir("./api/ui/node_modules/"))),
		debugStaticHandler: http.StripPrefix("/static/", utils.NoCache(http.FileServer(http.Dir("./api/ui/debug/")))),
		swaggerHandler: http.StripPrefix("/swagger/", http.FileServer(http.Dir("./api/ui/swagger/"))),
		healthCheckHandler: healthCheckHandler,
		debugHandler: api.NewDebugHandler(d.memStore, d.metaStore, d.handlers.queryHandler, healthCheckHandler),
	}
}

