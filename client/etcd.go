package client

import (
	"github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3x/instrument"
	"github.com/uber/aresdb/common"
	"github.com/uber/aresdb/utils"
)

// EtcdClient represents etcd client
type EtcdClient struct {
	Zone          string
	Environment   string
	ServiceName   string
	ClusterClient client.Client
	TxnStore      kv.TxnStore
	Services      services.Services
}

// NewEtcdClient returns a new EtcdClient
func NewEtcdClient(etcdConfig common.EtcdConfig) *EtcdClient {
	if !etcdConfig.Enabled {
		utils.GetLogger().Info("etcd not enabled")
		return nil
	}

	utils.GetLogger().Info("etcd enabled")
	csclient, err := etcdConfig.NewClient(instrument.NewOptions())

	if err != nil {
		utils.StackError(err, "Failed to initialize etcd client")
		return nil
	}

	txnStore, err := csclient.Txn()
	if err != nil {
		utils.StackError(err, "Failed to initialize txn store")
		return nil
	}

	svcs, err := csclient.Services(nil)
	if err != nil {
		utils.StackError(err, "Failed to initialize services")
		return nil
	}

	return &EtcdClient{
		ClusterClient: csclient,
		Zone:          etcdConfig.Zone,
		ServiceName:   etcdConfig.Service,
		Environment:   etcdConfig.Env,
		TxnStore:      txnStore,
		Services:      svcs,
	}
}
