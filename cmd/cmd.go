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
	"flag"
	"fmt"
	"net/http"
	"net/http/pprof"
	"path/filepath"
	"unsafe"

	"github.com/uber/aresdb/api"
	"github.com/uber/aresdb/common"
	"github.com/uber/aresdb/diskstore"
	"github.com/uber/aresdb/memstore"
	"github.com/uber/aresdb/metastore"
	"github.com/uber/aresdb/utils"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/uber-common/bark"
	"github.com/uber/aresdb/clients"
	"github.com/uber/aresdb/memutils"
)

// StartService is the entry point of starting ares.
func StartService(cfg common.AresServerConfig, logger bark.Logger, queryLogger bark.Logger, metricsCfg common.Metrics, httpWrappers ...utils.HTTPHandlerWrapper) {
	logger.WithField("config", cfg).Info("Bootstrapping service")

	// Check whether we have a correct device running environment
	memutils.DeviceFree(unsafe.Pointer(nil), 0)

	// Pause profiler util requested
	memutils.CudaProfilerStop()

	scope, closer, err := metricsCfg.NewRootScope()
	if err != nil {
		logger.Fatal("Failed to create new root scope", err)
	}
	defer closer.Close()

	// Init common components.
	utils.Init(cfg, logger, queryLogger, scope)

	// Parse command line flags with default settings from the config file.
	port := utils.IntFlag("port", "p", utils.GetConfig().Port, "Ares service port")
	debugPort := utils.IntFlag("debug_port", "d", utils.GetConfig().DebugPort, "Ares service debug port")
	rootPath := utils.StringFlag("root_path", "r", utils.GetConfig().RootPath, "Root path of the data directory")
	schedulerOff := utils.BoolFlag("scheduler_off", "so", utils.GetConfig().SchedulerOff, "Start server with scheduler off, no archiving and backfill")

	flag.Parse()

	scope.Counter("restart").Inc(1)
	serverRestartTimer := scope.Timer("restart").Start()

	// Create MetaStore.
	metaStorePath := filepath.Join(*rootPath, "metastore")
	metaStore, err := metastore.NewDiskMetaStore(metaStorePath)
	if err != nil {
		logger.Panic(err)
	}

	// Create DiskStore.
	diskStore := diskstore.NewLocalDiskStore(*rootPath)

	// Create MemStore.
	memStore := memstore.NewMemStore(metaStore, diskStore)

	// Read schema.
	utils.GetLogger().Infof("Reading schema from local MetaStore %s", metaStorePath)
	err = memStore.FetchSchema()
	if err != nil {
		utils.GetLogger().Fatal(err)
	}

	// create schema handler
	schemaHandler := api.NewSchemaHandler(metaStore)

	// create enum handler
	enumHander := api.NewEnumHandler(memStore, metaStore)

	// create query hanlder.
	queryHandler := api.NewQueryHandler(memStore, cfg.Query)

	// create health check handler.
	healthCheckHandler := api.NewHealthCheckHandler()

	nodeModulesHandler := http.StripPrefix("/node_modules/", http.FileServer(http.Dir("./api/ui/node_modules/")))

	// Start HTTP server for debugging.
	go func() {
		debugHandler := api.NewDebugHandler(memStore, metaStore, queryHandler, healthCheckHandler)

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

		utils.GetLogger().Infof("Starting HTTP server on dbg-port %d", *debugPort)
		utils.GetLogger().Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *debugPort), debugRouter))
	}()

	// Init shards.
	utils.GetLogger().Infof("Initializing shards from local DiskStore %s", *rootPath)
	memStore.InitShards(*schedulerOff)

	// Start serving.
	dataHandler := api.NewDataHandler(memStore)
	router := mux.NewRouter()

	httpWrappers = append([]utils.HTTPHandlerWrapper{utils.WithMetricsFunc}, httpWrappers...)

	schemaHandler.Register(router.PathPrefix("/schema").Subrouter(), httpWrappers...)
	enumHander.Register(router.PathPrefix("/schema").Subrouter(), httpWrappers...)
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

	batchStatsReporter := memstore.NewBatchStatsReporter(5*60, memStore, metaStore)
	go batchStatsReporter.Run()

	if cfg.ClusterName != "" {
		// cluster mode
		controllerClientCfg := cfg.Clients.Controller
		if controllerClientCfg == nil {
			logger.Fatal("Missing controller client config", err)
		}
		controllerClient := clients.NewControllerHTTPClient(controllerClientCfg.Host, controllerClientCfg.Port, controllerClientCfg.Headers)
		schemaFetchJob := metastore.NewSchemaFetchJob(10, metaStore, metastore.NewTableSchameValidator(), controllerClient, cfg.ClusterName, "")
		go schemaFetchJob.Run()
	}

	utils.GetLogger().Infof("Starting HTTP server on port %d with max connection %d", *port, cfg.HTTP.MaxConnections)
	utils.LimitServe(*port, handlers.CORS(allowOrigins, allowHeaders, allowMethods)(router), cfg.HTTP)
	batchStatsReporter.Stop()
}
