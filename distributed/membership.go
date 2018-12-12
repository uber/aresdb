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

package distributed

import (
	"encoding/json"
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/uber/aresdb/clients"
	"github.com/uber/aresdb/common"
	"github.com/uber/aresdb/metastore"
	"os"
	"strings"
	"time"
)

//membership manager does several things:
//	1. it creates session based ephermal node in zookeeper, to indicate current node's activeness
//	2. it manages cluster/remote mode specific jobs
type MembershipManager interface {
	// Connect to an AresDB cluster, this can mean communicating to ares controller or zk.
	// It also starts all periodical jobs
	Connect() error
	// Disconnect from cluster and stops jobs properly (if necessary)
	Disconnect() error
}

// NewMembershipManager creates a new MembershipManager
func NewMembershipManager(cfg common.AresServerConfig, metaStore metastore.MetaStore) MembershipManager {
	return &membershipManagerImpl{
		cfg:       cfg,
		metaStore: metaStore,
	}
}

type membershipManagerImpl struct {
	cfg       common.AresServerConfig
	metaStore metastore.MetaStore
	zkc       *zk.Conn
}

func (mm *membershipManagerImpl) Connect() (err error) {
	// connect to zk
	if mm.zkc == nil {
		err = mm.initZKConnection()
		if err != nil {
			return
		}
	}

	// join cluster
	var instanceName, hostName, clusterName string
	var serverPort int

	instanceName = mm.cfg.Cluster.InstanceName
	if instanceName == "" {
		err = ErrInvalidInstanceName
		return
	}
	hostName, err = os.Hostname()
	if err != nil {
		return
	}
	serverPort = mm.cfg.Port

	instance := Instance{
		Name: instanceName,
		Host: hostName,
		Port: serverPort,
	}
	clusterName = mm.cfg.Cluster.ClusterName

	var instanceBytes []byte
	instanceBytes, err = json.Marshal(instance)
	if err != nil {
		return
	}

	_, err = mm.zkc.Create(getZNodePath(clusterName, instanceName), instanceBytes, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err != nil {
		return
	}

	// start jobs
	controllerClient := clients.NewControllerHTTPClient(mm.cfg.Clients.Controller.Host, mm.cfg.Clients.Controller.Port, mm.cfg.Clients.Controller.Headers)
	// TODO: (shz) rewrite schema fetch job to use zk watches
	schemaFetchJob := NewSchemaFetchJob(5*60, mm.metaStore, metastore.NewTableSchameValidator(), controllerClient, clusterName, "")
	schemaFetchJob.FetchSchema()
	go schemaFetchJob.Run()

	return
}

func (mm *membershipManagerImpl) Disconnect() (err error) {
	mm.zkc.Close()
	return
}

func (mm *membershipManagerImpl) initZKConnection() (err error) {
	zksStr := mm.cfg.Clients.ZK.ZKs
	zks := strings.Split(zksStr, ",")
	mm.zkc, _, err = zk.Connect(zks, time.Duration(mm.cfg.Clients.ZK.TimeoutSeconds)*time.Second)
	return
}

// getZNodePath returns path to instance znode givien its name and cluster name.
func getZNodePath(clusterName, instanceName string) string {
	return fmt.Sprintf("/ares_controller/%s/instances/%s", clusterName, instanceName)
}
