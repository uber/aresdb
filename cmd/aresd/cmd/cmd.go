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

package cmd

import (
	"fmt"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/spf13/cobra"
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/api"
	"github.com/uber/aresdb/cgoutils"
	"github.com/uber/aresdb/cluster/topology"
	"github.com/uber/aresdb/common"
	controllerCli "github.com/uber/aresdb/controller/client"
	"github.com/uber/aresdb/datanode"
	"github.com/uber/aresdb/datanode/bootstrap"
	"github.com/uber/aresdb/diskstore"
	"github.com/uber/aresdb/memstore"
	memCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/metastore"
	"github.com/uber/aresdb/redolog"
	"github.com/uber/aresdb/utils"
	"net/http"
	"net/http/pprof"
	"path/filepath"
	"time"
	"unsafe"
)

// Options represents options for executing command
type Options struct {
	DefaultCfg   map[string]interface{}
	ServerLogger common.Logger
	QueryLogger  common.Logger
	Metrics      common.Metrics
	HTTPWrappers []utils.HTTPHandlerWrapper
}

// Option is for setting option
type Option func(*Options)

// Execute executes command with options
func Execute(setters ...Option) {

	loggerFactory := common.NewLoggerFactory()
	options := &Options{
		ServerLogger: loggerFactory.GetDefaultLogger(),
		QueryLogger:  loggerFactory.GetLogger("query"),
		Metrics:      common.NewNoopMetrics(),
	}

	for _, setter := range setters {
		setter(options)
	}

	cmd := &cobra.Command{
		Use:     "ares",
		Short:   "AresDB",
		Long:    `AresDB is a GPU-powered real-time analytical engine`,
		Example: `./ares --config config/ares.yaml --port 9374 --debug_port 43202 --root_path ares-root`,
		Run: func(cmd *cobra.Command, args []string) {

			cfg, err := ReadConfig(options.DefaultCfg, cmd.Flags())
			if err != nil {
				options.ServerLogger.With("err", err.Error()).Fatal("failed to read configs")
			}

			start(
				cfg,
				options.ServerLogger,
				options.QueryLogger,
				options.Metrics,
				options.HTTPWrappers...,
			)
		},
	}
	AddFlags(cmd)
	cmd.Execute()
}

// start is the entry point of starting ares.
func start(cfg common.AresServerConfig, logger common.Logger, queryLogger common.Logger, metricsCfg common.Metrics, httpWrappers ...utils.HTTPHandlerWrapper) {
	logger.With("config", cfg).Info("Bootstrapping service")

	// Check whether we have a correct device running environment
	cgoutils.DeviceFree(unsafe.Pointer(nil), 0)

	// Pause profiler util requested
	cgoutils.CudaProfilerStop()

	scope, closer, err := metricsCfg.NewRootScope()
	if err != nil {
		logger.Fatal("Failed to create new root scope", err)
	}
	defer closer.Close()

	// Init common components.
	utils.Init(cfg, logger, queryLogger, scope)

	scope.Counter("restart").Inc(1)

	if cfg.Cluster.Distributed {
		startDataNode(cfg, logger, scope, httpWrappers...)
		return
	}

	// TODO keep this path for non-distributed mode, and to aovid code break
	// should be removed later after distributed mode is mature
	serverRestartTimer := scope.Timer("restart").Start()

	// Create MetaStore.
	metaStorePath := filepath.Join(cfg.RootPath, "metastore")
	metaStore, err := metastore.NewDiskMetaStore(metaStorePath)
	if err != nil {
		logger.Panic(err)
	}

	// Create DiskStore.
	diskStore := diskstore.NewLocalDiskStore(cfg.RootPath)

	// fetch schema from controller and start periodical job
	if cfg.Cluster.Enable {
		if cfg.Cluster.Namespace == "" {
			logger.Fatal("Missing cluster name")
		}
		controllerClientCfg := cfg.Cluster.Controller
		if controllerClientCfg == nil {
			logger.Fatal("Missing controller client config", err)
		}
		if cfg.Cluster.InstanceID != "" {
			controllerClientCfg.Headers.Add(controllerCli.InstanceNameHeaderKey, cfg.Cluster.InstanceID)
		}

		controllerClient := controllerCli.NewControllerHTTPClient(controllerClientCfg.Address, time.Duration(controllerClientCfg.TimeoutSec)*time.Second, controllerClientCfg.Headers)
		schemaFetchJob := metastore.NewSchemaFetchJob(5*60, metaStore, metastore.NewTableSchameValidator(), controllerClient, cfg.Cluster.Namespace, "")
		// immediate initial fetch
		schemaFetchJob.FetchSchema()
		go schemaFetchJob.Run()

	}

	bootstrapToken := bootstrap.NewPeerDataNodeServer(metaStore, diskStore).(memCom.BootStrapToken)

	redoLogManagerMaster, err := redolog.NewRedoLogManagerMaster(&cfg.RedoLogConfig, diskStore, metaStore)
	if err != nil {
		utils.GetLogger().Fatal(err)
	}

	// Create MemStore.
	memStore := memstore.NewMemStore(metaStore, diskStore, memstore.NewOptions(bootstrapToken, redoLogManagerMaster))

	// Read schema.
	utils.GetLogger().Infof("Reading schema from local MetaStore %s", metaStorePath)
	err = memStore.FetchSchema()
	if err != nil {
		utils.GetLogger().Fatal(err)
	}

	// create schema handler
	schemaHandler := api.NewSchemaHandler(metaStore)

	// create enum handler
	enumHandler := api.NewEnumHandler(memStore, metaStore)

	// create query hanlder.
	// static shard owner with non distributed version
	staticShardOwner := topology.NewStaticShardOwner([]int{0})
	queryHandler := api.NewQueryHandler(memStore, staticShardOwner, cfg.Query)

	// create health check handler.
	healthCheckHandler := api.NewHealthCheckHandler()

	nodeModulesHandler := http.StripPrefix("/node_modules/", http.FileServer(http.Dir("./api/ui/node_modules/")))

	// Start HTTP server for debugging.
	go func() {
		debugHandler := api.NewDebugHandler(memStore, metaStore, queryHandler, healthCheckHandler, staticShardOwner)

		debugStaticHandler := http.StripPrefix("/static/", utils.NoCache(
			http.FileServer(http.Dir("./api/ui/debug/"))))
		debugRouter := mux.NewRouter()
		debugHandler.Register(debugRouter.PathPrefix("/dbg").Subrouter())
		schemaHandler.RegisterForDebug(debugRouter.PathPrefix("/schema").Subrouter())

		debugRouter.PathPrefix("/node_modules/").Handler(nodeModulesHandler)
		debugRouter.PathPrefix("/static/").Handler(debugStaticHandler)
		debugRouter.HandleFunc("/debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
		debugRouter.HandleFunc("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
		debugRouter.HandleFunc("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))
		debugRouter.HandleFunc("/debug/pprof/trace", http.HandlerFunc(pprof.Trace))
		debugRouter.PathPrefix("/debug/pprof/").Handler(http.HandlerFunc(pprof.Index))

		utils.GetLogger().Infof("Starting HTTP server on dbg-port %d", cfg.DebugPort)
		utils.GetLogger().Fatal(http.ListenAndServe(fmt.Sprintf(":%d", cfg.DebugPort), debugRouter))
	}()

	// Init shards.
	utils.GetLogger().Infof("Initializing shards from local DiskStore %s", cfg.RootPath)
	memStore.InitShards(cfg.SchedulerOff, topology.NewStaticShardOwner([]int{0}))

	// Start serving.
	dataHandler := api.NewDataHandler(memStore)
	router := mux.NewRouter()

	httpWrappers = append([]utils.HTTPHandlerWrapper{utils.WithMetricsFunc}, httpWrappers...)

	schemaRouter := router.PathPrefix("/schema")
	if cfg.Cluster.Enable {
		schemaRouter = schemaRouter.Methods(http.MethodGet)
	}
	schemaHandler.Register(schemaRouter.Subrouter(), httpWrappers...)
	enumHandler.Register(router.PathPrefix("/schema").Subrouter(), httpWrappers...)
	dataHandler.Register(router.PathPrefix("/data").Subrouter(), httpWrappers...)
	queryHandler.Register(router.PathPrefix("/query").Subrouter(), httpWrappers...)

	swaggerHandler := http.StripPrefix("/swagger/", http.FileServer(http.Dir("./api/ui/swagger/")))
	router.PathPrefix("/swagger/").Handler(swaggerHandler)
	router.PathPrefix("/node_modules/").Handler(nodeModulesHandler)
	router.HandleFunc("/health", utils.WithMetricsFunc(healthCheckHandler.HealthCheck))
	router.HandleFunc("/version", healthCheckHandler.Version)

	// Support CORS calls.
	allowOrigins := handlers.AllowedOrigins([]string{"*"})
	allowHeaders := handlers.AllowedHeaders([]string{"Accept", "Accept-Language", "Content-Language", "Origin", "Content-Type"})
	allowMethods := handlers.AllowedMethods([]string{"GET", "PUT", "POST", "DELETE", "OPTIONS"})

	serverRestartTimer.Stop()

	batchStatsReporter := memstore.NewBatchStatsReporter(5*60, memStore, topology.NewStaticShardOwner([]int{0}))
	go batchStatsReporter.Run()

	utils.GetLogger().Infof("Starting HTTP server on port %d with max connection %d", cfg.Port, cfg.HTTP.MaxConnections)
	utils.LimitServe(cfg.Port, handlers.CORS(allowOrigins, allowHeaders, allowMethods)(router), cfg.HTTP)
	batchStatsReporter.Stop()
	redoLogManagerMaster.Stop()
}

// start datanode in distributed mode
func startDataNode(cfg common.AresServerConfig, logger common.Logger, scope tally.Scope, httpWrappers ...utils.HTTPHandlerWrapper) {
	serverRestartTimer := scope.Timer("restart").Start()

	opts := datanode.NewOptions().SetServerConfig(cfg).SetInstrumentOptions(utils.NewOptions()).SetBootstrapOptions(bootstrap.NewOptions()).SetHTTPWrappers(httpWrappers)

	var topo topology.Topology
	etcdCfg := cfg.Cluster.Etcd
	etcdCfg.Service = utils.DataNodeServiceName(cfg.Cluster.Namespace)
	configServiceCli, err := etcdCfg.NewClient(instrument.NewOptions())
	if err != nil {
		logger.Fatal("Failed to create etcd client,", err)
	}

	dynamicOptions := topology.NewDynamicOptions().
		SetConfigServiceClient(configServiceCli).
		SetQueryOptions(services.NewQueryOptions().SetIncludeUnhealthy(true)).
		SetServiceID(services.NewServiceID().
			SetZone(etcdCfg.Zone).
			SetName(etcdCfg.Service).
			SetEnvironment(etcdCfg.Env))
	topo, err = topology.NewDynamicInitializer(dynamicOptions).Init()
	if err != nil {
		logger.Fatal("Failed to initialize dynamic topology,", err)
	}

	dataNode, err := datanode.NewDataNode(cfg.Cluster.InstanceID, topo, opts)
	if err != nil {
		logger.Fatal("Failed to create datanode,", err)
	}
	defer dataNode.Close()

	// preparing
	err = dataNode.Open()
	if err != nil {
		logger.Fatal("Failed to open datanode,", err)
	}
	// bootstrap and recovery
	err = dataNode.Bootstrap()
	if err != nil {
		logger.Fatal("Failed to bootstrap datanode,", err)
	}

	serverRestartTimer.Stop()

	// start serving traffic
	dataNode.Serve()
}
