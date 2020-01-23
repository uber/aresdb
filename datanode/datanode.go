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
	"github.com/uber/aresdb/common"
	"net/http"
	"net/http/pprof"
	"path/filepath"
	"sync"
	"time"

	"github.com/m3db/m3/src/x/instrument"
	controllerCli "github.com/uber/aresdb/controller/client"

	"strings"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/services"
	m3Shard "github.com/m3db/m3/src/cluster/shard"
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/api"
	"github.com/uber/aresdb/cluster/shard"
	"github.com/uber/aresdb/cluster/topology"
	mutatorsCom "github.com/uber/aresdb/controller/mutators/common"
	"github.com/uber/aresdb/datanode/bootstrap"
	"github.com/uber/aresdb/datanode/generated/proto/rpc"
	"github.com/uber/aresdb/diskstore"
	"github.com/uber/aresdb/memstore"
	memCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/metastore"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/redolog"
	"github.com/uber/aresdb/utils"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// dataNode includes metastore, memstore and diskstore
type dataNode struct {
	sync.RWMutex

	hostID          string
	startedAt       time.Time
	shardSet        shard.ShardSet
	clusterServices services.Services

	topoInitializer    topology.Initializer
	topology           topology.Topology
	numShardsInCluster int
	enumReader         mutatorsCom.EnumReader
	metaStore          metaCom.MetaStore
	memStore           memstore.MemStore
	diskStore          diskstore.DiskStore

	opts     Options
	logger   common.Logger
	metrics  datanodeMetrics
	handlers datanodeHandlers

	bootstrapManager     BootstrapManager
	redoLogManagerMaster *redolog.RedoLogManagerMaster
	grpcServer           *grpc.Server

	mapWatch topology.MapWatch
	close    chan struct{}

	readyCh chan struct{}
}

type datanodeHandlers struct {
	schemaHandler      *api.SchemaHandler
	enumHandler        *api.EnumHandler
	queryHandler       *api.QueryHandler
	dataHandler        *api.DataHandler
	nodeModuleHandler  http.Handler
	debugStaticHandler http.Handler
	debugHandler       *api.DebugHandler
	healthCheckHandler *api.HealthCheckHandler
	swaggerHandler     http.Handler
}

type datanodeMetrics struct {
	restartTimer tally.Timer
}

// NewDataNode creates a new data node
func NewDataNode(
	hostID string,
	topoInitializer topology.Initializer,
	enumReader mutatorsCom.EnumReader,
	opts Options) (DataNode, error) {

	iOpts := opts.InstrumentOptions()
	logger := iOpts.Logger().With("datanode", hostID)

	scope := iOpts.MetricsScope().SubScope("namespace").
		Tagged(map[string]string{
			"datanode": hostID,
		})

	metaStore, err := metastore.NewDiskMetaStore(filepath.Join(opts.ServerConfig().RootPath, "metastore"))
	if err != nil {
		return nil, utils.StackError(err, "failed to initialize local metastore")
	}
	diskStore := diskstore.NewLocalDiskStore(opts.ServerConfig().RootPath)

	bootstrapServer := bootstrap.NewPeerDataNodeServer(metaStore, diskStore)
	bootstrapToken := bootstrapServer.(memCom.BootStrapToken)

	redologCfg := opts.ServerConfig().RedoLogConfig
	redoLogManagerMaster, err := redolog.NewRedoLogManagerMaster(opts.ServerConfig().Cluster.Namespace, &redologCfg, diskStore, metaStore)
	if err != nil {
		return nil, utils.StackError(err, "failed to initialize redolog manager master")
	}

	memStore := memstore.NewMemStore(metaStore, diskStore,
		memstore.NewOptions(bootstrapToken, redoLogManagerMaster))

	grpcServer := grpc.NewServer()
	rpc.RegisterPeerDataNodeServer(grpcServer, bootstrapServer)
	reflection.Register(grpcServer)

	d := &dataNode{
		hostID:               hostID,
		topoInitializer:      topoInitializer,
		enumReader:           enumReader,
		metaStore:            metaStore,
		memStore:             memStore,
		diskStore:            diskStore,
		opts:                 opts,
		logger:               logger,
		metrics:              newDatanodeMetrics(scope),
		grpcServer:           grpcServer,
		redoLogManagerMaster: redoLogManagerMaster,
		shardSet:             shard.NewShardSet(nil),
		close:                make(chan struct{}),
		readyCh:              make(chan struct{}),
	}

	d.handlers = d.newHandlers()
	clusterClient, err := d.opts.ServerConfig().Cluster.Etcd.NewClient(instrument.NewOptions())
	if err != nil {
		return nil, utils.StackError(err, "failed to create etcd client")
	}
	d.clusterServices, err = clusterClient.Services(nil)
	if err != nil {
		return nil, utils.StackError(err, "failed to create cluster services client")
	}
	return d, nil
}

// Open data node for serving
func (d *dataNode) Open() error {
	d.startedAt = utils.Now()

	//1. start schema watch
	d.startSchemaWatch()

	// memstore fetch local disk schema
	err := d.memStore.FetchSchema()
	if err != nil {
		return err
	}

	// 2. start debug server
	go d.startDebugServer()

	// initialize topology, block wait for first topology in etcd
	d.topology, err = d.topoInitializer.Init()
	if err != nil {
		return utils.StackError(err, "failed to initialize topology")
	}
	d.bootstrapManager = NewBootstrapManager(d.hostID, d.memStore, d.opts.BootstrapOptions(), d.topology)

	// 3. first shard assignment
	d.mapWatch, err = d.topology.Watch()
	if err != nil {
		return utils.StackError(err, "failed to watch topology")
	}

	select {
	case <-d.mapWatch.C():
		topoMap := d.mapWatch.Get()
		d.numShardsInCluster = len(topoMap.ShardSet().AllIDs())
		hostShardSet, ok := topoMap.LookupHostShardSet(d.hostID)
		if ok {
			d.assignShardSet(hostShardSet.ShardSet())
		}
	default:
	}

	d.memStore.GetHostMemoryManager().Start()

	// 5. start scheduler
	if !d.opts.ServerConfig().SchedulerOff {
		d.logger.Info("starting scheduler")
		// disable archiving during redolog replay
		d.memStore.GetScheduler().EnableJobType(memCom.ArchivingJobType, false)
		// this will start scheduler of all jobs except archiving, archiving will be started individually
		d.memStore.GetScheduler().Start()
	} else {
		d.logger.Info("scheduler is turned off")
	}

	// 6. start table addition watch
	go d.startTableAdditionWatch()
	// 7. start active topology watch
	go d.startActiveTopologyWatch()
	// 8. start analyzing shard availability
	go d.startAnalyzingShardAvailability()
	// 9. start analyzing server readiness
	go d.startAnalyzingServerReadiness()
	// 10. start bootstrap retry watch
	go d.startBootstrapRetryWatch()

	return nil
}

func (d *dataNode) startSchemaWatch() {
	if d.opts.ServerConfig().Cluster.Enable {
		// TODO better to reuse the code directly in controller to talk to etcd
		if d.opts.ServerConfig().Cluster.Namespace == "" {
			d.logger.Fatal("Missing cluster name")
		}
		controllerClientCfg := d.opts.ServerConfig().Cluster.Controller
		if controllerClientCfg == nil {
			d.logger.Fatal("Missing controller client config")
		}
		if d.opts.ServerConfig().Cluster.InstanceID != "" {
			controllerClientCfg.Headers.Add(controllerCli.InstanceNameHeaderKey, d.opts.ServerConfig().Cluster.InstanceID)
		}

		controllerClient := controllerCli.NewControllerHTTPClient(controllerClientCfg.Address, time.Duration(controllerClientCfg.TimeoutSec)*time.Second, controllerClientCfg.Headers)
		schemaFetchJob := metastore.NewSchemaFetchJob(30, d.metaStore, nil, metastore.NewTableSchameValidator(), controllerClient, nil, d.opts.ServerConfig().Cluster.Namespace, "")
		// immediate initial fetch
		schemaFetchJob.FetchSchema()
		go schemaFetchJob.Run()
	}
}

func (d *dataNode) Close() {
	close(d.close)
	if d.mapWatch != nil {
		d.mapWatch.Close()
		d.mapWatch = nil
	}
	d.grpcServer.Stop()
	d.redoLogManagerMaster.Stop()
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

func (d *dataNode) startTableAdditionWatch() {
	shardOwnershipEvents, done, err := d.metaStore.WatchShardOwnershipEvents()
	if err != nil {
		utils.GetLogger().With("error", err.Error()).Fatal("failed to watch schema addition")
	}

	for {
		select {
		case <-d.close:
			return
		case event, ok := <-shardOwnershipEvents:
			if !ok {
				close(done)
				return
			}
			d.addTable(event.TableName)
			done <- struct{}{}
		}
	}
}

func (d *dataNode) startActiveTopologyWatch() {
	for {
		select {
		case <-d.close:
			return
		case _, ok := <-d.mapWatch.C():
			if !ok {
				return
			}
			topoMap := d.mapWatch.Get()
			hostShardSet, ok := topoMap.LookupHostShardSet(d.hostID)
			if ok {
				d.assignShardSet(hostShardSet.ShardSet())
			} else {
				// assign empty shard set when host does not appear in placement
				d.assignShardSet(shard.NewShardSet(nil))
			}
		}
	}
}

// checkShardReadiness check which of the shards are ready (meaning all tables within the shard are bootstrapped)
// this function will partition the input shards into two parts with all ready shards in the first part
// and return the number of ready shards
// eg. given input shards [0, 1, 2, 3, 4, 5, 6, 7], if all tables in shard 2, 6 are bootstrapped
// when the algorithm is finished,
// the input shards slice will become [2, 6, 0, 1, 3, 4, 5, 7],
// and numReadyShards returned is 2
func (d *dataNode) checkShardReadiness(tables []string, shards []uint32) (numReadyShards int) {
	for i := 0; i < len(shards); i++ {
		shardID := shards[i]
		numTablesBootstrapped := 0
		for _, table := range tables {
			tableShard, err := d.memStore.GetTableShard(table, int(shardID))
			if err != nil {
				d.logger.With(
					"error", err.Error(),
					"table", table,
					"shard", shardID).
					Error("cannot get table shard")
				continue
			}
			if tableShard.IsBootstrapped() {
				numTablesBootstrapped++
			}
			tableShard.Users.Done()
		}

		if numTablesBootstrapped == len(tables) {
			shards[i], shards[numReadyShards] = shards[numReadyShards], shards[i]
			numReadyShards++
		}
	}
	return
}

func (d *dataNode) startAnalyzingServerReadiness() {
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-ticker.C:
		case <-d.close:
			return
		}

		nonInitializing := make([]uint32, 0)

		// condition for serving readiness
		// 1. no shards owned by server
		hostShardSet, ok := d.mapWatch.Get().LookupHostShardSet(d.hostID)
		if !ok {
			close(d.readyCh)
			return
		}

		for _, s := range hostShardSet.ShardSet().All() {
			if s.State() != m3Shard.Initializing {
				nonInitializing = append(nonInitializing, s.ID())
			}
		}

		// 2. no nonInitializing (available/leaving) shards waiting for bootstrap
		if len(nonInitializing) == 0 {
			close(d.readyCh)
			return
		}

		factTables := make([]string, 0)
		dimTables := make([]string, 0)
		d.memStore.RLock()
		for table, schema := range d.memStore.GetSchemas() {
			if !schema.Schema.IsFactTable {
				dimTables = append(dimTables, table)
			} else {
				factTables = append(factTables, table)
			}
		}
		d.memStore.RUnlock()

		// 3. all nonInitializing (available/leaving) shards are bootstrapped
		// and all dimension table shards are bootstrapped (only one shard for dim table)
		if d.checkShardReadiness(factTables, nonInitializing) == len(nonInitializing) &&
			d.checkShardReadiness(dimTables, []uint32{0}) == 1 {
			close(d.readyCh)
			return
		}
	}
}

func (d *dataNode) startAnalyzingShardAvailability() {
	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-ticker.C:
		case <-d.close:
			return
		}

		hostShardSet, ok := d.mapWatch.Get().LookupHostShardSet(d.hostID)
		if !ok {
			continue
		}

		// initializing shards are shards with Initializing State
		// nonInitializing shards are shards with Available and Leaving State
		// we always bootstrap nonInitializing shards first
		// and wait for nonInitializing shards's readiness before serving traffic
		initializing := make([]uint32, 0)
		for _, s := range hostShardSet.ShardSet().All() {
			if s.State() == m3Shard.Initializing {
				initializing = append(initializing, s.ID())
			}
		}

		if len(initializing) > 0 {
			// snapshot fact tables
			factTables := make([]string, 0)
			d.memStore.RLock()
			for table, schema := range d.memStore.GetSchemas() {
				if schema.Schema.IsFactTable {
					factTables = append(factTables, table)
				}
			}
			d.memStore.RUnlock()

			dynamicTopo, ok := d.topology.(topology.DynamicTopology)
			if !ok {
				d.logger.Error("cannot mark shard available, topology is not dynamic")
				return
			}
			if numAvailable := d.checkShardReadiness(factTables, initializing); numAvailable > 0 {
				availableShards := initializing[:numAvailable]
				if err := dynamicTopo.MarkShardsAvailable(d.hostID, availableShards...); err != nil {
					d.logger.With(zap.Uint32s("shards", availableShards), "error", err.Error()).
						Error("failed to mark shards as available", zap.Error(err))
				} else {
					d.logger.With(zap.Uint32s("shards", availableShards)).Info("successfully marked shards as available")
				}
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

func (d *dataNode) ID() string {
	return d.hostID
}

func (d *dataNode) Serve() {
	// wait for server is ready to serve
	<-d.readyCh

	// start advertising to the cluster
	d.advertise()
	// enable archiving jobs
	if !d.opts.ServerConfig().SchedulerOff {
		d.memStore.GetScheduler().EnableJobType(memCom.ArchivingJobType, true)
		d.opts.InstrumentOptions().Logger().Info("archiving jobs enabled")
	}

	// start server
	router := mux.NewRouter()
	metricsLoggingMiddlewareProvider := utils.NewMetricsLoggingMiddleWareProvider(d.opts.InstrumentOptions().MetricsScope(), d.opts.InstrumentOptions().Logger())
	httpWrappers := append([]utils.HTTPHandlerWrapper{metricsLoggingMiddlewareProvider.WithMetrics}, d.opts.HTTPWrappers()...)
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
	router.HandleFunc("/health", utils.ApplyHTTPWrappers(d.handlers.healthCheckHandler.HealthCheck, metricsLoggingMiddlewareProvider.WithMetrics))
	router.HandleFunc("/version", d.handlers.healthCheckHandler.Version)

	// Support CORS calls.
	allowOrigins := handlers.AllowedOrigins([]string{"*"})
	allowHeaders := handlers.AllowedHeaders([]string{"Accept", "Accept-Language", "Content-Language", "Origin", "Content-Type"})
	allowMethods := handlers.AllowedMethods([]string{"GET", "PUT", "POST", "DELETE", "OPTIONS"})

	// record time from data node started to actually serving
	d.metrics.restartTimer.Record(utils.Now().Sub(d.startedAt))

	// start batch reporter
	batchStatsReporter := memstore.NewBatchStatsReporter(5*60, d.memStore, d)
	go batchStatsReporter.Run()

	d.opts.InstrumentOptions().Logger().Infof("Starting HTTP server on port %d with max connection %d", d.opts.ServerConfig().Port, d.opts.ServerConfig().HTTP.MaxConnections)
	utils.LimitServe(d.opts.ServerConfig().Port, handlers.CORS(allowOrigins, allowHeaders, allowMethods)(mixedHandler(d.grpcServer, router)), d.opts.ServerConfig().HTTP)
}

func (d *dataNode) advertise() {
	serviceID := services.NewServiceID().
		SetEnvironment(d.opts.ServerConfig().Cluster.Etcd.Env).
		SetZone(d.opts.ServerConfig().Cluster.Etcd.Zone).
		SetName(utils.DataNodeServiceName(d.opts.ServerConfig().Cluster.Namespace))

	err := d.clusterServices.SetMetadata(serviceID, services.NewMetadata().
		SetHeartbeatInterval(time.Duration(d.opts.ServerConfig().Cluster.HeartbeatConfig.Interval)*time.Second).
		SetLivenessInterval(time.Duration(d.opts.ServerConfig().Cluster.HeartbeatConfig.Timeout)*time.Second))
	if err != nil {
		d.logger.With("error", err.Error()).Fatalf("failed to set heart beat metadata")
	}

	placementInstance := placement.NewInstance().SetID(d.hostID)
	ad := services.NewAdvertisement().
		SetServiceID(serviceID).
		SetPlacementInstance(placementInstance)

	err = d.clusterServices.Advertise(ad)
	if err != nil {
		d.logger.With("error", err.Error()).Fatalf("failed to advertise data node")
	} else {
		d.logger.Info("start advertising datanode to cluster")
	}
}

func (d *dataNode) addTable(table string) {
	d.Lock()
	defer d.Unlock()

	d.memStore.RLock()
	schema := d.memStore.GetSchemas()[table]
	d.memStore.RUnlock()
	if schema == nil {
		d.logger.With("table", table).Error("schema does not exist")
		return
	}
	isFactTable := schema.Schema.IsFactTable

	if !isFactTable {
		d.logger.With("table", table, "shard", 0).Info("adding table shard on schema addition")
		// dimension table defaults shard to zero
		// new table does not need to copy data from peer, but need to purge old data
		d.memStore.AddTableShard(table, 0, 1, false, true)
	} else {
		for _, shardID := range d.shardSet.AllIDs() {
			d.logger.With("table", table, "shard", shardID).Info("adding table shard on schema addition")
			// new table does not need to copy data from peer, but need to purge old data
			d.memStore.AddTableShard(table, int(shardID), d.numShardsInCluster, false, true)
		}
	}

	go func() {
		if err := d.bootstrapManager.Bootstrap(); err != nil {
			d.logger.With("error", err.Error()).Error("error while bootstrapping")
		}
	}()
}

func (d *dataNode) assignShardSet(shardSet shard.ShardSet) {
	d.Lock()
	defer d.Unlock()

	// process fact tables first
	d.memStore.RLock()
	factTables := make([]string, 0)
	dimensionTables := make([]string, 0)
	for table, schema := range d.memStore.GetSchemas() {
		if schema.Schema.IsFactTable {
			factTables = append(factTables, table)
		} else {
			dimensionTables = append(dimensionTables, table)
		}
	}
	d.memStore.RUnlock()

	var (
		incoming           = make(map[uint32]m3Shard.Shard, len(shardSet.All()))
		existing           = make(map[uint32]struct{}, len(d.shardSet.AllIDs()))
		removing           []uint32
		adding             []m3Shard.Shard
		initializingShards = 0
		noExistingShards   bool
		noShardLeft        bool
	)

	for _, shard := range shardSet.All() {
		if shard.State() == m3Shard.Initializing {
			initializingShards++
		}
		incoming[shard.ID()] = shard
	}

	for _, shardID := range d.shardSet.AllIDs() {
		existing[shardID] = struct{}{}
	}

	noExistingShards = len(existing) == 0
	noShardLeft = len(incoming) == 0

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

	for _, shardID := range removing {
		for _, table := range factTables {
			d.logger.With("table", table, "shard", shardID).Info("removing fact table shard on placement change")
			d.memStore.RemoveTableShard(table, int(shardID))
			if err := d.metaStore.DeleteTableShard(table, int(shardID)); err != nil {
				d.logger.With("table", table, "shard", shardID).Error("failed to remove table shard metadata")
			}
			if err := d.diskStore.DeleteTableShard(table, int(shardID)); err != nil {
				d.logger.With("table", table, "shard", shardID).Error("failed to remove table shard data")
			}
		}
	}

	for _, shard := range adding {
		needPeerCopy := shard.State() == m3Shard.Initializing
		for _, table := range factTables {
			d.logger.With("table", table, "shard", shard.ID(), "state", shard.State()).Info("adding fact table shard on placement change")
			// when needPeerCopy is true, we also need to purge old data before adding new shard
			d.memStore.AddTableShard(table, int(shard.ID()), d.numShardsInCluster, needPeerCopy, needPeerCopy)
		}
	}

	// add/remove dimension tables with the following rules:
	// 1. add dimension tables when first shard is assigned to the data node
	// 2. remove dimension tables when the last shard is removed from the data node
	// 3. copy dimension table data from peer when all assigned new shards are initializing shards
	if noExistingShards && !noShardLeft {
		// only need to copy data from peer when all new shards are initializing shards
		// meaning no available/leaving shards ever owned by this data node
		needPeerCopy := initializingShards > 0 && len(incoming) == initializingShards
		for _, table := range dimensionTables {
			d.logger.With("table", table, "shard", 0).Info("adding dimension table shard on placement change")
			// only copy data from peer for dimension table
			// when from zero shards to all initialing shards
			// when needPeerCopy is true, we also need to purge old data before adding new shard
			d.memStore.AddTableShard(table, 0, 1, needPeerCopy, needPeerCopy)
		}
	}

	if !noExistingShards && noShardLeft {
		for _, table := range dimensionTables {
			d.logger.With("table", table, "shard", 0).Info("removing dimension table shard on placement change")
			d.memStore.RemoveTableShard(table, 0)
			if err := d.metaStore.DeleteTableShard(table, 0); err != nil {
				d.logger.With("table", table, "shard", 0).Error("failed to remove table shard metadata")
			}
			if err := d.diskStore.DeleteTableShard(table, 0); err != nil {
				d.logger.With("table", table, "shard", 0).Error("failed to remove table shard data")
			}
		}
	}
	d.shardSet = shardSet

	go func() {
		if err := d.bootstrapManager.Bootstrap(); err != nil {
			d.logger.With("error", err.Error()).Error("error while bootstrapping")
		}
	}()
}

// GetOwnedShards returns all shard ids the datanode owns
func (d *dataNode) GetOwnedShards() []int {
	d.RLock()
	defer d.RUnlock()
	shardIDs := d.shardSet.AllIDs()
	ids := make([]int, len(shardIDs))
	for i, shardID := range shardIDs {
		ids[i] = int(shardID)
	}
	return ids
}

func newDatanodeMetrics(scope tally.Scope) datanodeMetrics {
	return datanodeMetrics{
		restartTimer: scope.Timer("restart"),
	}
}

func (d *dataNode) newHandlers() datanodeHandlers {
	healthCheckHandler := api.NewHealthCheckHandler()
	return datanodeHandlers{
		schemaHandler:      api.NewSchemaHandler(d.metaStore),
		enumHandler:        api.NewEnumHandler(d.memStore, d.metaStore),
		queryHandler:       api.NewQueryHandler(d.memStore, d, d.opts.ServerConfig().Query, d.opts.ServerConfig().HTTP.MaxQueryConnections),
		dataHandler:        api.NewDataHandler(d.memStore, d.opts.ServerConfig().HTTP.MaxIngestionConnections),
		nodeModuleHandler:  http.StripPrefix("/node_modules/", http.FileServer(http.Dir("./api/ui/node_modules/"))),
		debugStaticHandler: http.StripPrefix("/static/", utils.NoCache(http.FileServer(http.Dir("./api/ui/debug/")))),
		swaggerHandler:     http.StripPrefix("/swagger/", http.FileServer(http.Dir("./api/ui/swagger/"))),
		healthCheckHandler: healthCheckHandler,
		debugHandler:       api.NewDebugHandler(d.opts.ServerConfig().Cluster.Namespace, d.memStore, d.metaStore, d.handlers.queryHandler, healthCheckHandler, d, d.enumReader),
	}
}

// mixed handler for both grpc and traditional http
func mixedHandler(grpcServer *grpc.Server, httpHandler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if grpcServer != nil && r.ProtoMajor == 2 && strings.Contains(r.Header.Get(utils.HTTPContentTypeHeaderKey), utils.HTTPContentTypeApplicationGRPC) {
			grpcServer.ServeHTTP(w, r)
		} else {
			httpHandler.ServeHTTP(w, r)
		}
	})
}

func (d *dataNode) startBootstrapRetryWatch() {
	for {
		select {
		case <-d.handlers.debugHandler.GetBootstrapRetryChan():
			go func() {
				err := d.bootstrapManager.Bootstrap()
				if err != nil {
					d.opts.InstrumentOptions().Logger().With("error", err.Error()).Error("error while retry bootstrapping")
				}
			}()
		}
	}
}
