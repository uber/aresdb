package handler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"code.uber.internal/data/ares-controller/utils"
	"github.com/gorilla/mux"
	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/cluster/kvstore"
	"go.uber.org/zap"
)

func TestPlacementHandler(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	sugaredLogger := logger.Sugar()

	t.Run("Should work for placement handler", func(t *testing.T) {
		cleanUp, port := utils.SetUpEtcdTestServer(t)
		defer cleanUp()
		clusterClient := utils.SetUpEtcdTestClient(t, port)
		clusterServices, err := clusterClient.Services(nil)
		assert.NoError(t, err)

		txnStore, err := clusterClient.Txn()
		assert.NoError(t, err)

		client := kvstore.EtcdClient{
			Zone:          "test",
			Environment:   "test",
			ServiceName:   "test",
			ClusterClient: clusterClient,
			TxnStore:      txnStore,
			Services:      clusterServices,
		}

		initRequestBody := bytes.NewBuffer([]byte(`
		{
		  "numShards": 2,
		  "numReplica": 2,
		  "newInstances": [
			{
			  "id": "0",
			  "isolation_group": "rack-a",
			  "zone": "test",
			  "hostname": "host0",
              "endpoint": "http://host0:9374",
  			  "weight": 1,
			  "port": 9374
			},
			{
			  "id": "1",
			  "isolation_group": "rack-b",
			  "zone": "test",
			  "hostname": "host1",
              "endpoint": "http://host1:9374",
  			  "weight": 1,
			  "port": 9374
			}
		  ]
		}`))

		placementHandler := NewPlacementHandler(sugaredLogger, tally.NoopScope, &client)
		testRouter := mux.NewRouter()
		placementHandler.Register(testRouter)
		testServer := httptest.NewUnstartedServer(testRouter)
		testServer.Start()
		defer testServer.Close()

		hostPort := testServer.Listener.Addr().String()
		testNamespace := "test"

		// 1. initialize placement
		resp, err := http.Post(fmt.Sprintf("http://%s/%s/datanode/init", hostPort, testNamespace), "application/json", initRequestBody)
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		var initPlacement placementpb.Placement
		err = json.NewDecoder(resp.Body).Decode(&initPlacement)
		assert.NoError(t, err)

		// 2. get the current placement
		resp, err = http.Get(fmt.Sprintf("http://%s/%s/datanode", hostPort, testNamespace))
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		var pb0 placementpb.Placement
		err = json.NewDecoder(resp.Body).Decode(&pb0)
		assert.NoError(t, err)
		assert.Equal(t, pb0, initPlacement)

		p, _ := placement.NewPlacementFromProto(&pb0)
		assert.Equal(t, p.NumShards(), 2)
		assert.Equal(t, p.NumInstances(), 2)
		assert.Equal(t, p.ReplicaFactor(), 2)
		assert.Equal(t, p.IsSharded(), true)
		assert.Equal(t, p.IsMirrored(), false)
		assert.Equal(t, p.ReplicaFactor(), 2)

		instance0, exist := p.Instance("0")
		assert.True(t, exist, true)
		assert.True(t, instance0.IsInitializing(), true)
		instance1, exist := p.Instance("1")
		assert.True(t, exist, true)
		assert.True(t, instance1.IsInitializing(), true)

		// 3. mark namespace as available
		resp, err = http.Post(fmt.Sprintf("http://%s/%s/datanode/available", hostPort, testNamespace), "application/json", nil)
		assert.NoError(t, err)
		var pb1 placementpb.Placement
		err = json.NewDecoder(resp.Body).Decode(&pb1)
		assert.NoError(t, err)
		p, err = placement.NewPlacementFromProto(&pb1)
		assert.NoError(t, err)

		instance0, _ = p.Instance("0")
		assert.True(t, instance0.IsAvailable(), true)
		instance1, _ = p.Instance("1")
		assert.True(t, instance1.IsAvailable(), true)

		// 4. replace instance 0 with new instance 2 in with the same isolation group
		replaceInstanceRequestBody := bytes.NewBuffer([]byte(`
		{
		  "leavingInstances": ["0"],
		  "newInstances": [
			{
			  "id": "2",
			  "isolation_group": "rack-a",
			  "zone": "test",
			  "hostname": "host2",
              "endpoint": "http://host2:9374",
  			  "weight": 1,
			  "port": 9374
			}
		  ]
		}`))

		req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("http://%s/%s/datanode/instances", hostPort, testNamespace), replaceInstanceRequestBody)
		var pb2 placementpb.Placement
		resp, err = http.DefaultClient.Do(req)
		assert.NoError(t, err)
		err = json.NewDecoder(resp.Body).Decode(&pb2)
		assert.NoError(t, err)

		p, _ = placement.NewPlacementFromProto(&pb2)
		assert.Equal(t, p.NumInstances(), 3)
		instance0, _ = p.Instance("0")
		instance2, _ := p.Instance("2")
		assert.True(t, instance0.IsLeaving(), true)
		assert.True(t, instance2.IsInitializing(), true)

		// 5. mark available for instance 2
		markInstanceAvailableRequestBody := bytes.NewBuffer([]byte(`
		{
		  "all": true
		}`))

		resp, err = http.Post(fmt.Sprintf("http://%s/%s/datanode/instances/2/available", hostPort, testNamespace), "application/json", markInstanceAvailableRequestBody)
		assert.NoError(t, err)
		var pb3 placementpb.Placement
		err = json.NewDecoder(resp.Body).Decode(&pb3)
		assert.NoError(t, err)
		p, _ = placement.NewPlacementFromProto(&pb3)
		assert.Equal(t, p.NumInstances(), 2)
		instance1, _ = p.Instance("1")
		instance2, _ = p.Instance("2")
		assert.True(t, instance1.IsAvailable(), true)
		assert.True(t, instance2.IsAvailable(), true)

		// 6. add instance 0 back
		addInstanceRequestBody := bytes.NewBuffer([]byte(`
		{
		  "newInstances": [
			{
			  "id": "0",
			  "isolation_group": "rack-a",
			  "zone": "test",
			  "hostname": "host0",
              "endpoint": "http://host0:9374",
  			  "weight": 1,
			  "port": 9374
			}
		  ]
		}`))

		resp, err = http.Post(fmt.Sprintf("http://%s/%s/datanode/instances", hostPort, testNamespace), "application/json", addInstanceRequestBody)
		var pb4 placementpb.Placement
		assert.NoError(t, err)
		err = json.NewDecoder(resp.Body).Decode(&pb4)
		assert.NoError(t, err)

		p, _ = placement.NewPlacementFromProto(&pb4)
		assert.Equal(t, p.NumInstances(), 3)
		instance0, _ = p.Instance("0")
		instance2, _ = p.Instance("2")
		shard0, _ := instance0.Shards().Shard(0)
		assert.Equal(t, shard0.State(), shard.Initializing)
		shard0, _ = instance2.Shards().Shard(0)
		assert.Equal(t, shard0.State(), shard.Leaving)

		// 6. mark instance 0 shard 0 as available
		markInstanceAvailableRequestBody = bytes.NewBuffer([]byte(`
		{
		  "shards": [0]
		}`))
		resp, err = http.Post(fmt.Sprintf("http://%s/%s/datanode/instances/0/available", hostPort, testNamespace), "application/json", markInstanceAvailableRequestBody)
		assert.NoError(t, err)
		var pb5 placementpb.Placement
		err = json.NewDecoder(resp.Body).Decode(&pb5)
		assert.NoError(t, err)
		p, _ = placement.NewPlacementFromProto(&pb5)
		assert.Equal(t, p.NumInstances(), 3)
		instance0, _ = p.Instance("0")
		instance2, _ = p.Instance("2")
		shard0, exist = instance0.Shards().Shard(0)
		assert.True(t, exist)
		assert.Equal(t, shard0.State(), shard.Available)
		_, exist = instance2.Shards().Shard(0)
		assert.False(t, exist)

		// 7. remove instance 0 from the placement
		removeInstanceRequestBody := bytes.NewBuffer([]byte(`
		{
		  "leavingInstances": ["0"]
		}`))
		req, _ = http.NewRequest(http.MethodDelete, fmt.Sprintf("http://%s/%s/datanode/instances", hostPort, testNamespace), removeInstanceRequestBody)
		var pb6 placementpb.Placement
		resp, err = http.DefaultClient.Do(req)
		assert.NoError(t, err)
		err = json.NewDecoder(resp.Body).Decode(&pb6)
		assert.NoError(t, err)

		p, _ = placement.NewPlacementFromProto(&pb6)
		assert.Equal(t, p.NumInstances(), 3)
		instance0, _ = p.Instance("0")
		instance1, _ = p.Instance("1")
		instance2, _ = p.Instance("2")
		assert.True(t, instance0.IsLeaving(), true)
		assert.True(t, instance1.Shards().Contains(0))
		assert.True(t, instance1.Shards().Contains(1))
		assert.True(t, instance2.Shards().Contains(0))
		assert.True(t, instance2.Shards().Contains(1))
	})
}
