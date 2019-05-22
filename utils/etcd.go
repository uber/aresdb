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
package utils

import (
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/coreos/etcd/embed"
	"github.com/coreos/etcd/pkg/types"
	"github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/client/etcd"
	"github.com/m3db/m3x/instrument"
	"github.com/stretchr/testify/assert"
)

// SetUpEtcdTestServer set up embed etcd server for test
// returns clean up function
func SetUpEtcdTestServer(t *testing.T) (func(), int) {
	etcdDir := fmt.Sprintf("/tmp/etcd_%d", Now().UnixNano())
	config := embed.NewConfig()
	config.Dir = etcdDir

	randPort := int(rand.Int31n(6000) + 20000)
	peerPort := randPort + 1
	clientPort := randPort + 2
	cURLs, err := types.NewURLs([]string{fmt.Sprintf("http://localhost:%d", clientPort)})
	assert.NoError(t, err)
	pURLs, err := types.NewURLs([]string{fmt.Sprintf("http://localhost:%d", peerPort)})
	assert.NoError(t, err)
	config.Name = "0"
	config.APUrls = pURLs
	config.ACUrls = cURLs
	config.LPUrls = pURLs
	config.LCUrls = cURLs
	config.InitialCluster = fmt.Sprintf("0=%s", fmt.Sprintf("http://localhost:%d", peerPort))

	embedEtcd, err := embed.StartEtcd(config)
	assert.NoError(t, err)
	return func() {
		embedEtcd.Close()
		_ = os.RemoveAll(etcdDir)
	}, clientPort
}

// SetUpEtcdTestClient creates txStore client to etcd server
func SetUpEtcdTestClient(t *testing.T, port int) client.Client {
	config := etcd.Configuration{
		Zone:    "test",
		Env:     "test",
		Service: "test",
		ETCDClusters: []etcd.ClusterConfig{
			{
				Zone: "test",
				Endpoints: []string{
					fmt.Sprintf("localhost:%d", port),
				},
			},
		},
	}
	csClient, err := config.NewClient(instrument.NewOptions())
	assert.NoError(t, err)
	return csClient
}
