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
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/uber/aresdb/broker"
	"github.com/uber/aresdb/broker/config"
	"github.com/uber/aresdb/cluster/topology"
	"github.com/uber/aresdb/cmd/aresd/cmd"
	"github.com/uber/aresdb/common"
	"github.com/uber/aresdb/controller/client"
	controllerEtcd "github.com/uber/aresdb/controller/mutators/etcd"
	dataNodeCli "github.com/uber/aresdb/datanode/client"
	"github.com/uber/aresdb/metastore"
	"github.com/uber/aresdb/utils"
	"go.uber.org/zap"
	"net/http"
	"time"
)

func Execute(setters ...cmd.Option) {
	loggerFactory := common.NewLoggerFactory()
	options := &cmd.Options{
		ServerLogger: loggerFactory.GetDefaultLogger(),
		QueryLogger:  loggerFactory.GetLogger("query"),
		Metrics:      common.NewNoopMetrics(),
	}

	for _, setter := range setters {
		setter(options)
	}

	cmd := &cobra.Command{
		Use:     "aresbrokerd",
		Short:   "AresDB broker",
		Long:    `AresDB broker is the gateway to send queries to AresDB`,
		Example: `./aresbrokerd --config config/ares-broker.yaml --port 9474`,
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
				options.HTTPWrapper,
				options.Middleware,
			)
		},
	}
	AddFlags(cmd)
	cmd.Execute()
}

func start(
	cfg config.BrokerConfig,
	logger common.Logger,
	queryLogger common.Logger,
	metricsCfg common.Metrics,
	httpWrapper utils.HTTPHandlerWrapper,
	middleware func(http.Handler) http.Handler,
) {
	logger.With("config", cfg).Info("Starting aresdb broker service")

	scope, closer, err := metricsCfg.NewRootScope()
	if err != nil {
		logger.Fatal("Failed to create new root scope", err)
	}
	defer closer.Close()

	// Init common components.
	utils.Init(common.AresServerConfig{}, logger, queryLogger, scope)

	scope.Counter("restart").Inc(1)
	serverRestartTimer := scope.Timer("restart").Start()
	defer serverRestartTimer.Stop()

	// fetch and keep syncing schema
	controllerClientCfg := cfg.Cluster.Controller
	if controllerClientCfg == nil {
		logger.Fatal("Missing controller client config", err)
	}

	var (
		topo        topology.HealthTrackingDynamicTopoloy
		clusterName = cfg.Cluster.Namespace
		serviceName = utils.BrokerServiceName(clusterName)
		store       kv.TxnStore
	)

	cfg.Cluster.Etcd.Service = serviceName
	configServiceCli, err := cfg.Cluster.Etcd.NewClient(
		instrument.NewOptions().SetLogger(zap.NewExample()))
	if err != nil {
		logger.Fatal("Failed to create config service client,", err)
	}

	controllerClient := client.NewControllerHTTPClient(
		controllerClientCfg.Address,
		time.Duration(controllerClientCfg.TimeoutSec)*time.Second,
		controllerClientCfg.Headers,
	)
	brokerSchemaMutator := broker.NewBrokerSchemaMutator()

	store, err = configServiceCli.Txn()
	if err != nil {
		logger.Fatal("Failed to get kv store")
	}

	schemaFetchJob := metastore.NewSchemaFetchJob(
		10,
		brokerSchemaMutator,
		brokerSchemaMutator,
		metastore.NewTableSchameValidator(),
		controllerClient,
		controllerEtcd.NewEnumMutator(
			store, controllerEtcd.NewTableSchemaMutator(
				store,
				zap.NewExample().Sugar(),
			),
		),
		clusterName,
		"",
	)
	schemaFetchJob.FetchSchema()
	schemaFetchJob.FetchEnum()
	go schemaFetchJob.Run()

	dynamicOptions := topology.NewDynamicOptions().SetConfigServiceClient(configServiceCli).SetServiceID(services.NewServiceID().SetZone(cfg.Cluster.Etcd.Zone).SetName(serviceName).SetEnvironment(cfg.Cluster.Etcd.Env))
	topo, err = topology.NewHealthTrackingDynamicTopology(dynamicOptions)
	if err != nil {
		logger.Fatal("Failed to create health tracking dynamic topology,", err)
	}

	// executor
	exec := broker.NewQueryExecutor(brokerSchemaMutator, topo, dataNodeCli.NewDataNodeQueryClient(logger))

	// init handlers
	queryHandler := broker.NewQueryHandler(exec, cfg.Cluster.InstanceID)

	// start HTTP server
	router := mux.NewRouter()
	metricsLoggingMiddlewareProvider := utils.NewMetricsLoggingMiddleWareProvider(scope, logger)
	httpWrappers := []utils.HTTPHandlerWrapper{metricsLoggingMiddlewareProvider.WithMetrics}
	if httpWrapper != nil {
		httpWrappers = append(httpWrappers, httpWrapper)
	}
	queryHandler.Register(router.PathPrefix("/query").Subrouter(), httpWrappers...)

	// Support CORS calls.
	allowOrigins := handlers.AllowedOrigins([]string{"*"})
	allowHeaders := handlers.AllowedHeaders([]string{"Accept", "Accept-Language", "Content-Language", "Origin", "Content-Type"})
	allowMethods := handlers.AllowedMethods([]string{"GET", "PUT", "POST", "DELETE", "OPTIONS"})

	utils.GetLogger().Infof("Starting HTTP server on port %d with max connection %d", cfg.Port, cfg.HTTP.MaxConnections)
	handler := handlers.CORS(allowOrigins, allowHeaders, allowMethods)(router)
	if middleware != nil {
		handler = middleware(handler)
	}
	utils.LimitServe(cfg.Port, handler, cfg.HTTP)
}

// AddFlags adds flags to command
func AddFlags(cmd *cobra.Command) {
	cmd.Flags().String("config", "config/ares-broker.yaml", "Ares broker config file")
	cmd.Flags().IntP("port", "p", 0, "Ares broker service port")
}

// ReadConfig populates BrokerConfig
func ReadConfig(defaultCfg map[string]interface{}, flags *pflag.FlagSet) (cfg config.BrokerConfig, err error) {
	v := viper.New()
	v.SetConfigType("yaml")
	// bind command flags
	v.BindPFlags(flags)

	utils.BindEnvironments(v)

	// set defaults
	v.MergeConfigMap(defaultCfg)

	// merge in config file
	if cfgFile, err := flags.GetString("config"); err == nil && cfgFile != "" {
		v.SetConfigFile(cfgFile)
	} else {
		v.SetConfigName("ares-broker")
		v.AddConfigPath("./config")
	}

	if err := v.MergeInConfig(); err == nil {
		fmt.Println("Using config file: ", v.ConfigFileUsed())
	}

	err = v.Unmarshal(&cfg, func(config *mapstructure.DecoderConfig) {
		config.TagName = "yaml"
	})
	return
}
