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
	"github.com/m3db/m3/src/x/instrument"
	controllerCli "github.com/uber/aresdb/controller/client"
	"net/http"
	"net/http/pprof"
	"path/filepath"
	"sync"
	"time"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/services"
	m3Shard "github.com/m3db/m3/src/cluster/shard"
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/api"
	"github.com/uber/aresdb/cluster/shard"
	"github.com/uber/aresdb/cluster/topology"
	"github.com/uber/aresdb/common"
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
	"strings"
)

// dataNode includes metastore, memstore and diskstore
type dataNode struct {
	sync.RWMutex

	hostID          string
	startedAt       time.Time
	shardSet        shard.ShardSet
	clusterServices services.Services

	topo      topology.Topology
	metaStore metaCom.MetaStore
	memStore  memstore.MemStore
	diskStore diskstore.DiskStore

	opts     Options
	logger   common.Logger
	metrics  datanodeMetrics
	handlers datanodeHandlers

	bootstraps           int
	bootstrapManager     BootstrapManager
	redoLogManagerMaster *redolog.RedoLogManagerMaster
	grpcServer           *grpc.Server

	mapWatch topology.MapWatch
	close    chan struct{}
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
	topo topology.Topology,
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

	bootstrapServer := bootstrap.NewPeerDataNodeServer(metaStore, diskStore)
	bootstrapToken := bootstrapServer.(memCom.BootStrapToken)

	redologCfg := opts.ServerConfig().RedoLogConfig
	redoLogManagerMaster, err := redolog.NewRedoLogManagerMaster(&redologCfg, diskStore, metaStore)
	if err != nil {
		return nil, utils.StackError(err, "failed to initialize redolog manager master")
	}

	memStore := memstore.NewMemStore(metaStore, diskStore, memstore.NewOptions(bootstrapToken, redoLogManagerMaster))

	grpcServer := grpc.NewServer()
	rpc.RegisterPeerDataNodeServer(grpcServer, bootstrapServer)
	reflection.Register(grpcServer)

	d := &dataNode{
		hostID:               hostID,
		topo:                 topo,
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
	}
	d.handlers = d.newHandlers()
	d.bootstrapManager = NewBootstrapManager(d, opts, topo)
	clusterClient, err := d.opts.ServerConfig().InstanceConfig.Etcd.NewClient(instrument.NewOptions())
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

	// 3. first shard assignment
	d.mapWatch, err = d.topo.Watch()
	if err != nil {
		return utils.StackError(err, "failed to watch topology")
	}

	select {
	case <-d.mapWatch.C():
		hostShardSet, ok := d.mapWatch.Get().LookupHostShardSet(d.hostID)
		if ok {
			d.AssignShardSet(hostShardSet.ShardSet())
		}
	default:
	}

	d.memStore.GetHostMemoryManager().Start()

	// 5. start scheduler
	if !d.opts.ServerConfig().SchedulerOff {
		d.opts.InstrumentOptions().Logger().Info("starting scheduler")
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

func (d *dataNode) startSchemaWatch() {
	if d.opts.ServerConfig().Cluster.Enable {
		// TODO better to reuse the code directly in controller to talk to etcd
		if d.opts.ServerConfig().Cluster.ClusterName == "" {
			d.logger.Fatal("Missing cluster name")
		}
		controllerClientCfg := d.opts.ServerConfig().Gateway.Controller
		if controllerClientCfg == nil {
			d.logger.Fatal("Missing controller client config")
		}
		if d.opts.ServerConfig().Cluster.InstanceName != "" {
			controllerClientCfg.Headers.Add(controllerCli.InstanceNameHeaderKey, d.opts.ServerConfig().Cluster.InstanceName)
		}

		controllerClient := controllerCli.NewControllerHTTPClient(controllerClientCfg.Address, time.Duration(controllerClientCfg.TimeoutSec)*time.Second, controllerClientCfg.Headers)
		schemaFetchJob := metastore.NewSchemaFetchJob(5*60, d.metaStore, metastore.NewTableSchameValidator(), controllerClient, d.opts.ServerConfig().Cluster.ClusterName, "")
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

func (d *dataNode) startActiveTopologyWatch() {
	for {
		select {
		case <-d.close:
			return
		case <-d.mapWatch.C():
			topoMap := d.mapWatch.Get()
			hostShardSet, ok := topoMap.LookupHostShardSet(d.hostID)
			if ok {
				d.AssignShardSet(hostShardSet.ShardSet())
			} else {
				// assign empty shard set when host does not appear in placement
				d.AssignShardSet(shard.NewShardSet(nil))

			}
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

		initializing := make(map[uint32]m3Shard.Shard)
		for _, s := range hostShardSet.ShardSet().All() {
			if s.State() == m3Shard.Initializing {
				initializing[s.ID()] = s
			}
		}

		if len(initializing) == 0 {
			continue
		}

		dynamicTopo, ok := d.topo.(topology.DynamicTopology)
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
					continue
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
	d.memStore.RLock()
	defer d.memStore.RUnlock()
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
	// enable archiving jobs
	if !d.opts.ServerConfig().SchedulerOff {
		d.memStore.GetScheduler().EnableJobType(memCom.ArchivingJobType, true)
		d.opts.InstrumentOptions().Logger().Info("archiving jobs enabled")
	}

	// start server
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

	// start batch reporter
	batchStatsReporter := memstore.NewBatchStatsReporter(5*60, d.memStore, d)
	go batchStatsReporter.Run()

	d.opts.InstrumentOptions().Logger().Infof("Starting HTTP server on port %d with max connection %d", d.opts.ServerConfig().Port, d.opts.ServerConfig().HTTP.MaxConnections)
	utils.LimitServe(d.opts.ServerConfig().Port, handlers.CORS(allowOrigins, allowHeaders, allowMethods)(mixedHandler(d.grpcServer, router)), d.opts.ServerConfig().HTTP)
}

func (d *dataNode) advertise() {
	serviceID := services.NewServiceID().
		SetEnvironment(d.opts.ServerConfig().InstanceConfig.Etcd.Env).
		SetZone(d.opts.ServerConfig().InstanceConfig.Etcd.Zone).
		SetName(utils.DataNodeServiceName(d.opts.ServerConfig().InstanceConfig.Namespace))

	err := d.clusterServices.SetMetadata(serviceID, services.NewMetadata().
		SetHeartbeatInterval(time.Duration(d.opts.ServerConfig().InstanceConfig.HeartbeatConfig.Interval)*time.Second).
		SetLivenessInterval(time.Duration(d.opts.ServerConfig().InstanceConfig.HeartbeatConfig.Timeout)*time.Second))
	if err != nil {
		d.opts.InstrumentOptions().Logger().Fatalf("failed to set heart beat metadata",
			zap.String("id", d.hostID),
			zap.Error(err))
	}

	placementInstance := placement.NewInstance().SetID(d.hostID)
	ad := services.NewAdvertisement().
		SetServiceID(serviceID).
		SetPlacementInstance(placementInstance)

	err = d.clusterServices.Advertise(ad)
	if err != nil {
		d.opts.InstrumentOptions().Logger().Fatalf("failed to advertise data node",
			zap.String("id", d.hostID),
			zap.Error(err))
	} else {
		d.opts.InstrumentOptions().Logger().Info("start advertising datanode to cluster",
			zap.String("id", d.hostID))
	}
}

// GetTableShard returns the table shard from the datanode
func (d *dataNode) GetTableShard(table string, shardID uint32) (*memstore.TableShard, error) {
	return d.memStore.GetTableShard(table, int(shardID))
}

func (d *dataNode) AssignShardSet(shardSet shard.ShardSet) {
	d.Lock()
	defer d.Unlock()

	tables := d.Tables()
	var (
		incoming = make(map[uint32]m3Shard.Shard, len(shardSet.All()))
		existing = make(map[uint32]struct{}, len(d.shardSet.AllIDs()))
		removing []uint32
		adding   []m3Shard.Shard
	)

	for _, shard := range shardSet.All() {
		incoming[shard.ID()] = shard
	}

	for _, shardID := range d.shardSet.AllIDs() {
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

	for _, shardID := range removing {
		for _, table := range tables {
			d.logger.With("table", table, "shard", shardID).Info("removing table shard on placement change")
			d.memStore.RemoveTableShard(table, int(shardID))
		}
	}

	for _, shard := range adding {
		for _, table := range tables {
			d.logger.With("table", table, "shard", shard.ID(), "state", shard.State()).Info("adding table shard on placement change")
			d.memStore.AddTableShard(table, int(shard.ID()), shard.State() == m3Shard.Initializing)
		}
	}
	d.shardSet = shardSet

	// only kick off bootstrap when the first bootstrap is done during shard assignment
	if d.bootstraps > 0 {
		go func() {
			if err := d.bootstrapManager.Bootstrap(); err != nil {
				d.logger.Error("error while bootstrapping", zap.Error(err))
			}
		}()
	}
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

func (d *dataNode) Bootstrap() error {
	d.Lock()
	d.bootstraps++
	d.Unlock()
	return d.bootstrapManager.Bootstrap()
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
		queryHandler:       api.NewQueryHandler(d.memStore, d, d.opts.ServerConfig().Query),
		dataHandler:        api.NewDataHandler(d.memStore),
		nodeModuleHandler:  http.StripPrefix("/node_modules/", http.FileServer(http.Dir("./api/ui/node_modules/"))),
		debugStaticHandler: http.StripPrefix("/static/", utils.NoCache(http.FileServer(http.Dir("./api/ui/debug/")))),
		swaggerHandler:     http.StripPrefix("/swagger/", http.FileServer(http.Dir("./api/ui/swagger/"))),
		healthCheckHandler: healthCheckHandler,
		debugHandler:       api.NewDebugHandler(d.memStore, d.metaStore, d.handlers.queryHandler, healthCheckHandler, d),
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
