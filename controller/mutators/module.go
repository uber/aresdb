package mutators

import (
	"github.com/uber/aresdb/cluster/kvstore"
	"github.com/uber/aresdb/controller/mutators/common"
	"github.com/uber/aresdb/controller/mutators/etcd"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// Result holds all mutators
type Result struct {
	fx.Out

	JobMutator                 common.JobMutator
	NamespaceMutator           common.NamespaceMutator
	SubscriberMutator          common.SubscriberMutator
	SchemaMutator              common.TableSchemaMutator
	IngestionAssignmentMutator common.IngestionAssignmentMutator
	MembershipMutator          common.MembershipMutator
	EnumMutator                common.EnumMutator
}

// Params represents params to initialize all mutators
type Params struct {
	fx.In

	EtcdClient *kvstore.EtcdClient
	Logger     *zap.SugaredLogger
}

// InitMutators initialize mutators
func InitMutators(param Params) Result {
	if param.EtcdClient == nil {
		param.Logger.Fatal("Non usable clients provided")
	}

	result := Result{
		JobMutator: etcd.NewJobMutator(param.EtcdClient.TxnStore, param.Logger),
		SchemaMutator: etcd.NewTableSchemaMutator(param.EtcdClient.TxnStore, param.Logger),
		SubscriberMutator: etcd.NewSubscriberMutator(param.EtcdClient),
		IngestionAssignmentMutator: etcd.NewIngestionAssignmentMutator(param.EtcdClient.TxnStore),
		NamespaceMutator: etcd.NewNamespaceMutator(param.EtcdClient.TxnStore),
		MembershipMutator: etcd.NewMembershipMutator(param.EtcdClient),
	}
	result.EnumMutator = etcd.NewEnumMutator(param.EtcdClient.TxnStore, result.SchemaMutator)
	return result
}

// Module defines business module
var Module = fx.Provide(InitMutators)