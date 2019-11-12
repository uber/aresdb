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
	"github.com/spf13/cobra"
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/cluster/kvstore"
	"github.com/uber/aresdb/cmd/aresd/cmd"
	"github.com/uber/aresdb/common"
	"github.com/uber/aresdb/controller/handlers"
	"github.com/uber/aresdb/controller/mutators"
	"github.com/uber/aresdb/controller/tasks"
	"github.com/uber/aresdb/utils"
	cfgfx "go.uber.org/config"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"net/http"
)

var (
	// default config file
	cfgFile = "config/ares-controller.yaml"
	// default port
	port = 6708

	Module = fx.Provide(Init)
)

type Params struct {
	fx.In

	LifeCycle fx.Lifecycle
}

// Result contains all
type Result struct {
	fx.Out

	ConfigProvider cfgfx.Provider
	Logger         *zap.SugaredLogger
	Scope          tally.Scope
	EtcdClient     *kvstore.EtcdClient
	MetricsLoggingProvider utils.MetricsLoggingMiddleWareProvider
}

// AddFlags adds flags to command
func AddFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&cfgFile, "config", "c", "config/ares-controller.yaml", "Ares controller config file")
	cmd.Flags().IntVarP(&port, "port", "p", 6708, "Ares controller service port")
}

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

	command := &cobra.Command{
		Use:     "arescontrollerd",
		Short:   "AresDB Controller",
		Long:    `AresDB Controller is the administrative service for managing schema and configurations of the cluster`,
		Example: `./arescontrollerd --config config/ares-controller.yaml --port 6708`,
		Run: func(cmd *cobra.Command, args []string) {
			fx.New(Module,
				mutators.Module,
				handler.Module,
				tasks.Module,
				fx.Invoke(runServer),
			).Run()
		},
	}

	AddFlags(command)
	command.Execute()
}

func Init() Result {
	var result Result
	logger := zap.NewExample().Sugar()
	scope := tally.NewTestScope("test", nil)

	cfgProvider, err := cfgfx.NewYAML(cfgfx.Permissive(), cfgfx.File(cfgFile))
	if err != nil {
		logger.With("error", err.Error(), "path", cfgFile).Fatal("failed to read config file")
		return result
	}

	var etcdConfig kvstore.EtcdConfig
	if err := cfgProvider.Get("etcd").Populate(&etcdConfig); err != nil {
		logger.With("error", err.Error(), "path", cfgFile).Fatal("failed to read config file")
		return result
	}

	return Result{
		ConfigProvider: cfgProvider,
		Logger: logger,
		Scope:  scope,
		EtcdClient: kvstore.NewEtcdClient(logger, etcdConfig),
		MetricsLoggingProvider: utils.NewMetricsLoggingMiddleWareProvider(scope, logger),
	}
}

func runServer(logger *zap.SugaredLogger, handlerParams handler.ServerParams) {
	fallbackHandler := handler.NewCompositeHandler(handlerParams)
	logger.Fatal(http.ListenAndServe(fmt.Sprintf("localhost:%d", port), fallbackHandler))
}
