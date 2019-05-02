package main

import (
	"context"
	"fmt"
	"github.com/uber/aresdb/subscriber/common/sink"
	"os"
	"runtime"

	"github.com/spf13/cobra"
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/subscriber/common/consumer"
	"github.com/uber/aresdb/subscriber/common/job"
	"github.com/uber/aresdb/subscriber/common/message"
	"github.com/uber/aresdb/subscriber/common/rules"
	"github.com/uber/aresdb/subscriber/config"
	"github.com/uber/aresdb/utils"
	cfgfx "go.uber.org/config"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

type Params struct {
	fx.In

	Lifecycle fx.Lifecycle
}

type Result struct {
	fx.Out

	Environment utils.EnvironmentContext
	Provider    cfgfx.Provider
	Logger      *zap.Logger
	Scope       tally.Scope
	Consumer    job.NewConsumer
	Decoder     job.NewDecoder
	Sink        job.NewSink
}

func main() {
	module := fx.Provide(Init)
	Execute(module, config.Module, rules.Module, job.Module)
}

func Execute(opts ...fx.Option) {
	cmd := &cobra.Command{
		Use:     "ares-subscriber",
		Short:   "AresDB subscriber",
		Long:    `AresDB subscriber ingest data into AresDB`,
		Example: `ares-subscriber -c config/staging/staging.yaml -r config -j summary -a summaryProd`,
		Run: func(cmd *cobra.Command, args []string) {
			runtime.GOMAXPROCS(runtime.NumCPU())
			fx.New(opts...).Run()
		},
	}
	addFlags(cmd)
	cmd.Execute()
}

func addFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&config.ConfigRootPath, "r", "subscriber/config", "Config root path")
	cmd.Flags().StringVar(&config.ConfigFile, "c", "subscriber/config/staging/staging.yaml", "Config file path")
	cmd.Flags().StringVar(&config.ActiveJobNameSpace, "j", "jobNameSpace", "Job namespace")
	cmd.Flags().StringVar(&config.ActiveAresNameSpace, "a", "aresNameSpace", "Ares cluster namespace")

	if cmd.MarkFlagRequired("j") != nil {
		panic(`missing "-j jobNameSpace". Job namespace is required`)
	}

	if cmd.MarkFlagRequired("a") != nil {
		panic(`missing "-a aresNameSpace". Ares cluster namespace is required`)
	}

	fmt.Printf("jobNameSpace=%s, aresNameSpace=%s\n", config.ActiveJobNameSpace, config.ActiveAresNameSpace)
}

func newDefaultEnvironmentCtx() utils.EnvironmentContext {
	return utils.EnvironmentContext{
		Environment:        os.Getenv(utils.EnvironmentKey),
		RuntimeEnvironment: os.Getenv(utils.RuntimeEnvironmentKey),
		Zone:               os.Getenv(utils.ZoneKey),
		Hostname:           os.Getenv(utils.HostnameKey),
		Deployment:         os.Getenv(utils.DeploymentKey),
		SystemPort:         os.Getenv(utils.PortSystemKey),
		ApplicationID:      os.Getenv(utils.AppIDKey),
		InstanceID:         os.Getenv(utils.InstanceIDKey),
	}
}

func newDefaultConfig() cfgfx.Provider {
	if config.ConfigFile == "" {
		panic(`missing "-c configFile"`)
	}

	provider, err := cfgfx.NewYAMLProviderFromFiles(config.ConfigFile)
	if err != nil {
		panic(utils.StackError(err, fmt.Sprintf("Failed to load config %s", config.ConfigFile)))
	}
	return provider
}

func newDefaultLogger(params Params, env utils.EnvironmentContext) *zap.Logger {
	logger := zap.NewExample()
	undoGlobals := zap.ReplaceGlobals(logger)
	undoHijack := zap.RedirectStdLog(logger)
	params.Lifecycle.Append(fx.Hook{
		OnStart: func(context.Context) error {
			logger.Info("starting up with environment", zap.Any("environment", env.Environment))
			return nil
		},
		OnStop: func(context.Context) error {
			undoHijack()
			undoGlobals()
			logger.Sync()
			return nil
		},
	})
	return logger
}

func newDefaultScope() tally.Scope {
	return tally.NewTestScope("test", nil)
}

func Init(params Params) Result {
	env := newDefaultEnvironmentCtx()
	provider := newDefaultConfig()
	logger := newDefaultLogger(params, env)
	scope := newDefaultScope()

	return Result{
		Environment: env,
		Provider:    provider,
		Logger:      logger,
		Scope:       scope,
		Consumer:    consumer.NewKafkaConsumer,
		Decoder:     message.NewDefaultDecoder,
		Sink:        sink.NewAresDatabase,
	}
}
