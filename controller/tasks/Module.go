package tasks

import (
	"context"
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/cluster/kvstore"
	mutatorCom "github.com/uber/aresdb/controller/mutators/common"
	taskCom "github.com/uber/aresdb/controller/tasks/common"
	"github.com/uber/aresdb/controller/tasks/etcd"
	"go.uber.org/config"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// EtcdTaskParams wraps etcd params with fx
type EtcdTaskParams struct {
	fx.In

	ConfigProvider config.Provider
	Logger         *zap.SugaredLogger
	Scope          tally.Scope

	EtcdClient         *kvstore.EtcdClient
	NamespaceMutator   mutatorCom.NamespaceMutator
	JobMutator         mutatorCom.JobMutator
	SchemaMutator      mutatorCom.TableSchemaMutator
	SubscriberMutator  mutatorCom.SubscriberMutator
	AssignmentsMutator mutatorCom.IngestionAssignmentMutator
}

// InvokeEtcdTask invoke etcd based ingestion assignment task
func InvokeEtcdTask(params EtcdTaskParams, Lifecycle fx.Lifecycle) {
	if params.EtcdClient != nil {
		params.Logger.Info("initializing ingestion assignment task based on etcd")
		task := etcd.NewIngestionAssignmentTask(taskCom.IngestionAssignmentTaskParams{
			ConfigProvider:     params.ConfigProvider,
			Logger:             params.Logger,
			Scope:              params.Scope,
			EtcdClient:         params.EtcdClient,
			NamespaceMutator:   params.NamespaceMutator,
			SubscriberMutator:  params.SubscriberMutator,
			JobMutator:         params.JobMutator,
			SchemaMutator:      params.SchemaMutator,
			AssignmentsMutator: params.AssignmentsMutator,
		})
		go task.Run()
		Lifecycle.Append(fx.Hook{
			OnStop: func(context.Context) error {
				task.Done()
				return nil
			},
		})
	}
}

var Module = fx.Invoke(InvokeEtcdTask)