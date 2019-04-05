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

package job

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/curator-go/curator"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3x/instrument"
	"github.com/uber/aresdb/gateway"
	"github.com/uber/aresdb/subscriber/common/rules"
	"github.com/uber/aresdb/subscriber/config"
	"github.com/uber/aresdb/utils"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"os"
)

// Module configures Drivers and Controller.
var Module = fx.Options(
	fx.Provide(
		NewController,
	),
	fx.Invoke(StartController),
)

// Params defines the base objects for jobConfigs.
type Params struct {
	fx.In

	LifeCycle        fx.Lifecycle
	ServiceConfig    config.ServiceConfig
	JobConfigs       rules.JobConfigs
	ConsumerInitFunc NewConsumer
	DecoderInitFunc  NewDecoder
}

// Result defines the objects that the rules module provides.
type Result struct {
	fx.Out

	Controller *Controller
}

const (
	// defaultRefreshInterval is 10 minutes
	defaultRefreshInterval = 10
)

// Controller is responsible for syncing up with aresDB control
type Controller struct {
	sync.RWMutex

	serviceConfig config.ServiceConfig
	// aresControllerClient is aresDB controller client
	aresControllerClient gateway.ControllerClient
	// Drivers are all running jobs
	Drivers Drivers
	// jobNS is current active job namespace
	jobNS string
	// aresClusterNS is current active aresDB cluster namespace
	aresClusterNS string
	// assignmentHashCode is current assignment hash code
	assignmentHashCode string
	// zkClient is zookeeper client
	zkClient curator.CuratorFramework
	// etcdServices is etcd services client
	etcdServices services.Services
	// consumerInitFunc is func of NewConsumer
	consumerInitFunc NewConsumer
	// decoderInitFunc is func of NewDecoder
	decoderInitFunc NewDecoder
}

// ZKNodeSubscriber defines the information stored in ZKNode subscriber
type ZKNodeSubscriber struct {
	// Name is subscriber instanceId
	Name string `json:"name"`
	// Host is host name of subscriber
	Host string `json:"host"`
}

// NewController creates controller
func NewController(params Params) *Controller {
	params.ServiceConfig.Logger.Info("Creating Controller")

	drivers, err := NewDrivers(params)
	if err != nil {
		panic(err)
	}

	if params.ServiceConfig.ControllerConfig.RefreshInterval <= 0 {
		params.ServiceConfig.Logger.Info("Reset controller refreshInterval",
			zap.Int("from", params.ServiceConfig.ControllerConfig.RefreshInterval),
			zap.Int("to", defaultRefreshInterval))
		params.ServiceConfig.ControllerConfig.RefreshInterval = defaultRefreshInterval
	}

	aresControllerClient := gateway.NewControllerHTTPClient(params.ServiceConfig.ControllerConfig.Address,
		time.Duration(params.ServiceConfig.ControllerConfig.Timeout)*time.Second,
		http.Header{
			"RPC-Caller":  []string{os.Getenv("UDEPLOY_APP_ID")},
			"RPC-Service": []string{params.ServiceConfig.ControllerConfig.ServiceName},
		})

	controller := &Controller{
		serviceConfig:        params.ServiceConfig,
		aresControllerClient: aresControllerClient,
		Drivers:              drivers,
		jobNS:                config.ActiveJobNameSpace,
		aresClusterNS:        config.ActiveAresNameSpace,
		assignmentHashCode:   "",
	}

	if params.ServiceConfig.ControllerConfig.Enable {
		params.ServiceConfig.Logger.Info("aresDB Controller is enabled")

		if *params.ServiceConfig.HeartbeatConfig.Enabled {
			controller.etcdServices, err = connectEtcdServices(params)
			if err != nil {
				panic(utils.StackError(err, "Failed to createEtcdServices"))
			}
			if registerHeartBeatService(params, controller.etcdServices) != nil {
				panic(utils.StackError(err, "Failed to registerHeartBeatService"))
			}
		}

		controller.zkClient = createZKClient(params)
		err = controller.zkClient.Start()
		if err != nil {
			panic(utils.StackError(err, "Failed to start zkClient"))
		}
		params.ServiceConfig.Logger.Info("zkClient was started")

		err = controller.RegisterOnZK()
		if err != nil {
			panic(utils.StackError(err, "Failed to register subscriber"))
		}
		params.ServiceConfig.Logger.Info("Registered subscriber in zk")

		go controller.SyncUpJobConfigs()
	}

	params.ServiceConfig.Logger.Info("Controller created",
		zap.Any("controller", controller))
	return controller
}

func connectEtcdServices(params Params) (services.Services, error) {
	iopts := instrument.NewOptions().
		SetZapLogger(params.ServiceConfig.Logger).
		SetMetricsScope(params.ServiceConfig.Scope)

	// etcd key format: prefix/${env}/namespace/service/instanceId
	params.ServiceConfig.EtcdConfig.Service = fmt.Sprintf("%s/%s",
		config.ActiveAresNameSpace, params.ServiceConfig.EtcdConfig.Service)
	// create a config service client to access to the etcd cluster services.
	csClient, err := params.ServiceConfig.EtcdConfig.NewClient(iopts)
	if err != nil {
		return nil, err
	}

	servicesClient, err := csClient.Services(nil)
	if err != nil {
		return nil, err
	}

	return servicesClient, nil
}

func registerHeartBeatService(params Params, servicesClient services.Services) error {
	sid := services.NewServiceID().
		SetEnvironment(params.ServiceConfig.EtcdConfig.Env).
		SetZone(params.ServiceConfig.EtcdConfig.Zone).
		SetName(params.ServiceConfig.EtcdConfig.Service)

	pInstance := placement.NewInstance().SetID(params.ServiceConfig.Environment.InstanceID)

	ad := services.NewAdvertisement().
		SetServiceID(sid).
		SetPlacementInstance(pInstance)

	err := servicesClient.SetMetadata(sid, services.NewMetadata().
		SetHeartbeatInterval(*params.ServiceConfig.HeartbeatConfig.Interval).
		SetLivenessInterval(*params.ServiceConfig.HeartbeatConfig.Timeout))
	if err != nil {
		return err
	}

	err = servicesClient.Advertise(ad)
	return err
}

func createZKClient(params Params) curator.CuratorFramework {
	zkConfig := params.ServiceConfig.ZooKeeperConfig
	lc := params.LifeCycle
	retryPolicy := curator.NewExponentialBackoffRetry(
		zkConfig.BaseSleepTimeSeconds*time.Second,
		zkConfig.MaxRetries,
		zkConfig.MaxSleepSeconds*time.Second)

	// Using the CuratorFrameworkBuilder gives fine grained control over creation options
	builder := &curator.CuratorFrameworkBuilder{
		ConnectionTimeout: zkConfig.ConnectionTimeoutSeconds * time.Second,
		SessionTimeout:    zkConfig.SessionTimeoutSeconds * time.Second,
		RetryPolicy:       retryPolicy,
	}
	zkClient := builder.ConnectString(zkConfig.Server).Build()

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			params.ServiceConfig.Logger.Info("Close zkClient")
			return zkClient.Close()
		},
	})

	return zkClient
}

// RegisterOnZK registes aresDB subscriber instance in zookeeper as an ephemeral node
func (c *Controller) RegisterOnZK() error {
	path := fmt.Sprintf("/ares_controller/%s/subscribers/%s",
		c.jobNS, c.serviceConfig.Environment.InstanceID)
	subscriber, err := json.Marshal(ZKNodeSubscriber{
		Name: c.serviceConfig.Environment.InstanceID,
		Host: c.serviceConfig.Environment.Hostname,
	})
	if err != nil {
		return err
	}

	_, err = c.zkClient.Create().WithMode(curator.EPHEMERAL).ForPathWithData(path, subscriber)
	time.Sleep(time.Minute * 1)
	return err
}

// SyncUpJobConfigs sync up jobConfigs with aresDB controller
func (c *Controller) SyncUpJobConfigs() {
	c.Lock()
	defer c.Unlock()

	// Check if the hash of the assignment is changed or not
	updateHash, newAssignmentHash := c.updateAssignmentHash()
	if !updateHash {
		c.serviceConfig.Scope.Counter("syncUp.skipped").Inc(1)
		return
	}

	// Get assignment from aresDB controller since hash is changed
	assignment, err := c.aresControllerClient.GetAssignment(c.jobNS, c.serviceConfig.Environment.InstanceID)
	if err != nil {
		c.serviceConfig.Logger.Error("Failed to get assignment from aresDB controller",
			zap.String("jobNamespace", c.jobNS),
			zap.String("aresDB Controller", c.serviceConfig.ControllerConfig.Address),
			zap.Error(err))
		c.serviceConfig.Scope.Counter("syncUp.failed").Inc(1)
		return
	}
	c.serviceConfig.Logger.Info("Got assignment from aresDB controller",
		zap.String("jobNamespace", c.jobNS),
		zap.String("aresDB Controller", c.serviceConfig.ControllerConfig.Address))

	newJobs := make(map[string]*rules.JobConfig)
	// Add or Update jobs
	for _, jobConfig := range assignment.Jobs {
		newJobs[jobConfig.Name] = jobConfig
		if aresClusterDrivers, ok := c.Drivers[jobConfig.Name]; ok {
			// case1: existing jobConfig
			for aresCluster, driver := range aresClusterDrivers {
				if _, ok := assignment.AresClusters[aresCluster]; !ok {
					// case1.1: delete the driver because aresCluster is deleted
					c.deleteDriver(driver, aresCluster, aresClusterDrivers)
					continue
				}
				if driver.jobConfig.Version != jobConfig.Version {
					// case1.2: restart the driver because jobConfig version is changed,
					if !c.addDriver(jobConfig, aresCluster, aresClusterDrivers, true) {
						updateHash = false
					}
				}
			}
			for aresCluster, aresClusterObj := range assignment.AresClusters {
				if _, ok := aresClusterDrivers[aresCluster]; !ok {
					// case1.3 add a new driver because a new aresCluster is added
					c.serviceConfig.ActiveAresClusters[aresCluster] = aresClusterObj
					if !c.addDriver(jobConfig, aresCluster, aresClusterDrivers, false) {
						updateHash = false
					}
				}
			}
		} else {
			// case2: a new jobConfig
			aresClusterDrivers := make(map[string]*Driver)
			for aresCluster, aresClusterObj := range assignment.AresClusters {
				// case2.1: add a new driver for each aresCluster
				c.serviceConfig.ActiveAresClusters[aresCluster] = aresClusterObj
				if !c.addDriver(jobConfig, aresCluster, aresClusterDrivers, false) {
					updateHash = false
				}
			}
			c.Drivers[jobConfig.Name] = aresClusterDrivers
			for aresCluster := range c.serviceConfig.ActiveAresClusters {
				// case2.2: delete the aresCluster from ActiveAresClusters because it is deleted from assignment
				if _, ok := assignment.AresClusters[aresCluster]; !ok {
					delete(c.serviceConfig.ActiveAresClusters, aresCluster)
				}
			}
		}
	}

	// Delete jobs
	for jobName, aresClusterDrivers := range c.Drivers {
		if _, ok := newJobs[jobName]; !ok {
			// case3: jobConfig is deleted
			for aresCluster, driver := range aresClusterDrivers {
				c.deleteDriver(driver, aresCluster, aresClusterDrivers)
			}
			c.Drivers[jobName] = nil
			delete(c.Drivers, jobName)
			c.serviceConfig.Logger.Info("deleted all drivers",
				zap.String("job", jobName))
		}
	}

	// Update local hash codes
	if updateHash {
		c.serviceConfig.Logger.Info("Update assignment hash",
			zap.String("jobNamespace", c.jobNS),
			zap.String("aresDB Controller", c.serviceConfig.ControllerConfig.Address),
			zap.String("oldHash", c.assignmentHashCode),
			zap.String("newHash", newAssignmentHash))
		c.assignmentHashCode = newAssignmentHash
		c.serviceConfig.Scope.Counter("syncUp.succeeded").Inc(1)
	}
	c.serviceConfig.Scope.Counter("syncUp.failed").Inc(1)

	return
}

func (c *Controller) updateAssignmentHash() (update bool, newHash string) {
	// get the hash of the assignment
	oldHash := c.assignmentHashCode
	newHash, err := c.aresControllerClient.GetAssignmentHash(c.jobNS, c.serviceConfig.Environment.InstanceID)
	if err != nil {
		c.serviceConfig.Logger.Error("Failed to get assignment hash from aresDB controller",
			zap.String("jobNamespace", c.jobNS),
			zap.String("aresDB Controller", c.serviceConfig.ControllerConfig.Address),
			zap.Error(err))
		return false, ""
	}

	if strings.Compare(oldHash, newHash) == 0 {
		return false, newHash
	}
	c.serviceConfig.Logger.Info("Found assignment hash changed",
		zap.String("jobNamespace", c.jobNS),
		zap.String("aresDB Controller", c.serviceConfig.ControllerConfig.Address))

	return true, newHash
}

func (c *Controller) addDriver(
	jobConfig *rules.JobConfig, aresCluster string, aresClusterDrivers map[string]*Driver, stop bool) bool {
	if !c.startDriver(jobConfig, aresCluster, aresClusterDrivers, false) {
		c.serviceConfig.Scope.Tagged(map[string]string{
			"job":         jobConfig.Name,
			"aresCluster": aresCluster,
		}).Counter("errors.driver.new").Inc(1)
		return false
	}

	c.serviceConfig.Logger.Info("Added new driver",
		zap.String("job", jobConfig.Name),
		zap.String("aresCluster", aresCluster))
	return true
}

func (c *Controller) deleteDriver(driver *Driver, aresCluster string, aresClusterDrivers map[string]*Driver) {
	driver.Stop()
	aresClusterDrivers[aresCluster] = nil
	delete(aresClusterDrivers, aresCluster)
	delete(c.serviceConfig.ActiveAresClusters, aresCluster)
	c.serviceConfig.Logger.Info("deleted driver",
		zap.String("job", driver.JobName),
		zap.String("aresCluster", aresCluster))
}

func (c *Controller) startDriver(
	jobConfig *rules.JobConfig, aresCluster string, aresClusterDrivers map[string]*Driver, stop bool) bool {
	// 0. Clone jobConfig
	clonedJobConfig, err := rules.CloneJobConfig(jobConfig, c.serviceConfig, aresCluster)
	if err != nil {
		c.serviceConfig.Logger.Error("Failed to copy job config",
			zap.String("job", jobConfig.Name),
			zap.String("aresCluster", aresCluster),
			zap.Error(err))
		return false
	}

	// 1. Stop the job driver
	if stop {
		aresClusterDrivers[aresCluster].Stop()
		aresClusterDrivers[aresCluster] = nil
	}

	// 2. create a new driver
	driver, err :=
		NewDriver(clonedJobConfig, c.serviceConfig, NewStreamingProcessor, c.consumerInitFunc, c.decoderInitFunc)
	if err != nil {
		c.serviceConfig.Logger.Error("Failed to create driver",
			zap.String("job", jobConfig.Name),
			zap.String("cluster", aresCluster),
			zap.Error(err))
		return false
	}

	// 3. Start the job driver
	go driver.Start()
	aresClusterDrivers[aresCluster] = driver

	return true
}

// StartController starts periodically sync up with aresDB controller
func StartController(c *Controller) {
	if !c.serviceConfig.ControllerConfig.Enable {
		c.serviceConfig.Logger.Info("aresDB Controller is disabled")
		return
	}

	c.serviceConfig.Logger.Info("Start Controller")
	ticks := time.Tick(time.Duration(c.serviceConfig.ControllerConfig.RefreshInterval) * time.Minute)
	go func() {
		for {
			select {
			case <-ticks:
				c.serviceConfig.Logger.Info("Start sync up with aresDB controller")
				c.SyncUpJobConfigs()
				c.serviceConfig.Logger.Info("Done sync up with aresDB controller")
			}
		}
	}()
}
