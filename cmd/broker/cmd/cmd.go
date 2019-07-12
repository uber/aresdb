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
	dataNodeCli "github.com/uber/aresdb/datanode/client"
	"github.com/uber/aresdb/metastore"
	"github.com/uber/aresdb/utils"
	"go.uber.org/zap"
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
				options.HTTPWrappers...,
			)
		},
	}
	AddFlags(cmd)
	cmd.Execute()
}

func start(cfg config.BrokerConfig, logger common.Logger, queryLogger common.Logger, metricsCfg common.Metrics, httpWrappers ...utils.HTTPHandlerWrapper) {
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
	controllerClientCfg := cfg.ControllerConfig
	if controllerClientCfg == nil {
		logger.Fatal("Missing controller client config", err)
	}

	clusterName := cfg.Cluster.Namespace
	controllerClient := client.NewControllerHTTPClient(controllerClientCfg.Address, time.Duration(controllerClientCfg.TimeoutSec)*time.Second, controllerClientCfg.Headers)
	schemaMutator := broker.NewBrokerSchemaMutator()
	schemaFetchJob := metastore.NewSchemaFetchJob(10, schemaMutator, metastore.NewTableSchameValidator(), controllerClient, clusterName, "")
	schemaFetchJob.FetchSchema()
	go schemaFetchJob.Run()

	var topo topology.Topology

	serviceName := utils.BrokerServiceName(clusterName)

	cfg.Etcd.Service = serviceName
	configServiceCli, err := cfg.Etcd.NewClient(
		instrument.NewOptions().SetLogger(zap.NewExample()))
	if err != nil {
		logger.Fatal("Failed to create config service client,", err)
	}
	dynamicOptions := topology.NewDynamicOptions().SetConfigServiceClient(configServiceCli).SetServiceID(services.NewServiceID().SetZone(cfg.Etcd.Zone).SetName(serviceName).SetEnvironment(cfg.Etcd.Env))
	topo, err = topology.NewDynamicInitializer(dynamicOptions).Init()
	if err != nil {
		logger.Fatal("Failed to initialize dynamic topology,", err)
	}

	// executor
	exec := broker.NewQueryExecutor(schemaMutator, topo, dataNodeCli.NewDataNodeQueryClient())

	// init handlers
	queryHandler := broker.NewQueryHandler(exec)

	// start HTTP server
	router := mux.NewRouter()
	httpWrappers = append([]utils.HTTPHandlerWrapper{utils.WithMetricsFunc}, httpWrappers...)
	queryHandler.Register(router.PathPrefix("/query").Subrouter(), httpWrappers...)

	// Support CORS calls.
	allowOrigins := handlers.AllowedOrigins([]string{"*"})
	allowHeaders := handlers.AllowedHeaders([]string{"Accept", "Accept-Language", "Content-Language", "Origin", "Content-Type"})
	allowMethods := handlers.AllowedMethods([]string{"GET", "PUT", "POST", "DELETE", "OPTIONS"})

	utils.GetLogger().Infof("Starting HTTP server on port %d with max connection %d", cfg.Port, cfg.HTTP.MaxConnections)
	utils.LimitServe(cfg.Port, handlers.CORS(allowOrigins, allowHeaders, allowMethods)(router), cfg.HTTP)
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
