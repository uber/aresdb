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
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/spf13/pflag"
	"github.com/mitchellh/mapstructure"
	"os"
)

var (
	cfgFile string
)

// AppParams contains for
type AppParams struct {
	DefaultCfg   map[string]interface{}
	ServerLogger bark.Logger
	QueryLogger bark.Logger
	Metrics common.Metrics
	HttpWrappers []utils.HTTPHandlerWrapper
}

func readConfig(defaultCfg map[string]interface{}, flags *pflag.FlagSet) common.AresServerConfig {
	v := viper.New()
	v.SetConfigType("yaml")
	v.BindPFlags(flags)

	v.SetEnvPrefix("ares")
	v.BindEnv("env")

	// set defaults
	v.SetDefault("root_path", "ares-root")

	hostname, err := os.Hostname()
	if err != nil {
		panic(utils.StackError(err, "cannot get host name"))
	}
	v.SetDefault("cluster", map[string]interface{}{
		 "instance_name": hostname,
	})

	v.MergeConfigMap(defaultCfg)

	// merge in config file
	if cfgFile != "" {
		v.SetConfigFile(cfgFile)
	} else {
		v.SetConfigName("ares")
		v.AddConfigPath("./config")
	}

	if err := v.MergeInConfig(); err == nil {
		fmt.Println("Using config file: ", v.ConfigFileUsed())
	}

	var cfg common.AresServerConfig
	err = v.Unmarshal(&cfg, func(config *mapstructure.DecoderConfig) {
		config.TagName = "yaml"
	})

	if err != nil {
		panic(err)
	}

	return cfg
}

// BuildCommand builds command
func BuildCommand(params AppParams) *cobra.Command {
	cmd := &cobra.Command{
		Use: "ares",
		Short: "AresDB",
		Long: `AresDB is a GPU-powered real-time analytical engine`,
		Example: `./ares --port 9374 --debug_port 43202 --root_path ares-root`,
		Run: func(cmd *cobra.Command, args []string) {
			start(
				readConfig(params.DefaultCfg, cmd.Flags()),
				params.ServerLogger,
				params.QueryLogger,
				params.Metrics,
				params.HttpWrappers...,
			)
		},
	}
	cmd.Flags().StringVar(&cfgFile, "config", "", "Ares config file")
	cmd.Flags().IntP("port", "p", 0, "Ares service port")
	cmd.Flags().IntP("debug_port", "d", 0, "Ares service debug port")
	cmd.Flags().StringP("root_path", "r", "ares-root", "Root path of the data directory")
	cmd.Flags().Bool("scheduler_off",false, "Start server with scheduler off, no archiving and backfill")
	return cmd
}

// start is the entry point of starting ares.
func start(cfg common.AresServerConfig, logger bark.Logger, queryLogger bark.Logger, metricsCfg common.Metrics, httpWrappers ...utils.HTTPHandlerWrapper) {
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

	scope.Counter("restart").Inc(1)
	serverRestartTimer := scope.Timer("restart").Start()

	// Create MetaStore.
	metaStorePath := filepath.Join(utils.GetConfig().RootPath, "metastore")
	metaStore, err := metastore.NewDiskMetaStore(metaStorePath)
	if err != nil {
		logger.Panic(err)
	}

	// fetch schema from controller and start periodical job
	if cfg.Cluster.Enable {
		if cfg.Cluster.ClusterName == "" {
			logger.Fatal("Missing cluster name")
		}
		controllerClientCfg := cfg.Clients.Controller
		if controllerClientCfg == nil {
			logger.Fatal("Missing controller client config", err)
		}
		if cfg.Cluster.InstanceName != "" {
			controllerClientCfg.Headers.Add(clients.InstanceNameHeaderKey, cfg.Cluster.InstanceName)
		}
		controllerClient := clients.NewControllerHTTPClient(controllerClientCfg.Host, controllerClientCfg.Port, controllerClientCfg.Headers)
		schemaFetchJob := metastore.NewSchemaFetchJob(5*60, metaStore, metastore.NewTableSchameValidator(), controllerClient, cfg.Cluster.ClusterName, "")
		// immediate initial fetch
		schemaFetchJob.FetchSchema()
		go schemaFetchJob.Run()
	}

	// Create DiskStore.
	diskStore := diskstore.NewLocalDiskStore(utils.GetConfig().RootPath)

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

		utils.GetLogger().Infof("Starting HTTP server on dbg-port %d", utils.GetConfig().DebugPort)
		utils.GetLogger().Fatal(http.ListenAndServe(fmt.Sprintf(":%d", utils.GetConfig().DebugPort), debugRouter))
	}()

	// Init shards.
	utils.GetLogger().Infof("Initializing shards from local DiskStore %s", utils.GetConfig().RootPath)
	memStore.InitShards(utils.GetConfig().SchedulerOff)

	// Start serving.
	dataHandler := api.NewDataHandler(memStore)
	router := mux.NewRouter()

	httpWrappers = append([]utils.HTTPHandlerWrapper{utils.WithMetricsFunc}, httpWrappers...)

	schemaRouter := router.PathPrefix("/schema")
	if cfg.Cluster.Enable {
		schemaRouter = schemaRouter.Methods(http.MethodGet)
	}
	schemaHandler.Register(schemaRouter.Subrouter(), httpWrappers...)
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

	utils.GetLogger().Infof("Starting HTTP server on port %d with max connection %d", utils.GetConfig().Port, cfg.HTTP.MaxConnections)
	utils.LimitServe(utils.GetConfig().Port, handlers.CORS(allowOrigins, allowHeaders, allowMethods)(router), cfg.HTTP)
	batchStatsReporter.Stop()
}
