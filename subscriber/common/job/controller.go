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

	"os"

	"github.com/curator-go/curator"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/x/instrument"
	controllerCli "github.com/uber/aresdb/controller/client"
	"github.com/uber/aresdb/subscriber/common/rules"
	"github.com/uber/aresdb/subscriber/config"
	"go.uber.org/fx"
	"go.uber.org/zap"
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
	SinkInitFunc     NewSink
	ConsumerInitFunc NewConsumer
	DecoderInitFunc  NewDecoder
}

// Result defines the objects that the job module provides.
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
	aresControllerClient controllerCli.ControllerClient
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
	// sinkInitFunc is func of NewSink
	sinkInitFunc NewSink
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

	aresControllerClient := controllerCli.NewControllerHTTPClient(params.ServiceConfig.ControllerConfig.Address,
		time.Duration(params.ServiceConfig.ControllerConfig.Timeout)*time.Second,
		http.Header{
			"RPC-Caller":  []string{os.Getenv("UDEPLOY_APP_ID")},
			"RPC-Service": []string{params.ServiceConfig.ControllerConfig.ServiceName},
		})
	aresControllerClient.SetNamespace(config.ActiveJobNameSpace)

	drivers, err := NewDrivers(params, aresControllerClient)
	if err != nil {
		params.ServiceConfig.Logger.Panic("Failed to NewDrivers", zap.Error(err))
	}

	if params.ServiceConfig.ControllerConfig.RefreshInterval <= 0 {
		params.ServiceConfig.Logger.Info("Reset controller refreshInterval",
			zap.Int("from", params.ServiceConfig.ControllerConfig.RefreshInterval),
			zap.Int("to", defaultRefreshInterval))
		params.ServiceConfig.ControllerConfig.RefreshInterval = defaultRefreshInterval
	}

	controller := &Controller{
		serviceConfig:        params.ServiceConfig,
		aresControllerClient: aresControllerClient,
		Drivers:              drivers,
		jobNS:                config.ActiveJobNameSpace,
		aresClusterNS:        config.ActiveAresNameSpace,
		assignmentHashCode:   "",
		sinkInitFunc:         params.SinkInitFunc,
		consumerInitFunc:     params.ConsumerInitFunc,
		decoderInitFunc:      params.DecoderInitFunc,
	}

	if params.ServiceConfig.ControllerConfig.Enable {
		params.ServiceConfig.Logger.Info("aresDB Controller is enabled")

		if params.ServiceConfig.HeartbeatConfig.Enabled {
			params.ServiceConfig.Logger.Info("heartbeat config",
				zap.Any("heartbeat", *params.ServiceConfig.HeartbeatConfig))
			controller.etcdServices, err = connectEtcdServices(params)
			if err != nil {
				params.ServiceConfig.Logger.Panic("Failed to createEtcdServices", zap.Error(err))
			}
			if registerHeartBeatService(params, controller.etcdServices) != nil {
				params.ServiceConfig.Logger.Panic("Failed to registerHeartBeatService", zap.Error(err))
			}
		} else {
			controller.zkClient = createZKClient(params)
			err = controller.zkClient.Start()
			if err != nil {
				params.ServiceConfig.Logger.Panic("Failed to start zkClient", zap.Error(err))
			}
			params.ServiceConfig.Logger.Info("zkClient was started")

			err = controller.RegisterOnZK()
			if err != nil {
				params.ServiceConfig.Logger.Panic("Failed to register subscriber", zap.Error(err))
			}
			params.ServiceConfig.Logger.Info("Registered subscriber in zk")
		}

		go controller.SyncUpJobConfigs()
	}

	params.ServiceConfig.Logger.Info("Controller created",
		zap.Any("controller", controller))
	return controller
}

func connectEtcdServices(params Params) (services.Services, error) {
	iopts := instrument.NewOptions().
		SetLogger(params.ServiceConfig.Logger).
		SetMetricsScope(params.ServiceConfig.Scope)

	// etcd key format: prefix/${env}/namespace/service/instanceId
	params.ServiceConfig.EtcdConfig.Env = fmt.Sprintf("%s/%s",
		params.ServiceConfig.EtcdConfig.Env, config.ActiveJobNameSpace)

	// create a config service client to access to the etcd cluster services.
	csClient, err := params.ServiceConfig.EtcdConfig.NewClient(iopts)
	if err != nil {
		params.ServiceConfig.Logger.Error("Failed to NewClient for etcd",
			zap.Error(err))
		return nil, err
	}

	servicesClient, err := csClient.Services(nil)
	if err != nil {
		params.ServiceConfig.Logger.Error("Failed to create services for etcd",
			zap.Error(err))
		return nil, err
	}

	return servicesClient, nil
}

func registerHeartBeatService(params Params, servicesClient services.Services) error {
	sid := services.NewServiceID().
		SetEnvironment(params.ServiceConfig.EtcdConfig.Env).
		SetZone(params.ServiceConfig.EtcdConfig.Zone).
		SetName(params.ServiceConfig.EtcdConfig.Service)

	err := servicesClient.SetMetadata(sid, services.NewMetadata().
		SetHeartbeatInterval(time.Duration(params.ServiceConfig.HeartbeatConfig.Interval)*time.Second).
		SetLivenessInterval(time.Duration(params.ServiceConfig.HeartbeatConfig.Timeout)*time.Second))
	if err != nil {
		params.ServiceConfig.Logger.Error("Failed to config heartbeart",
			zap.Error(err))
		return err
	}

	pInstance := placement.NewInstance().
		SetID(params.ServiceConfig.Environment.InstanceID)

	ad := services.NewAdvertisement().
		SetServiceID(sid).
		SetPlacementInstance(pInstance)

	params.ServiceConfig.Logger.Info("service, placement, and ad info",
		zap.Any("serviceID", sid),
		zap.Any("placement", pInstance),
		zap.Any("ad", ad))

	err = servicesClient.Advertise(ad)
	if err != nil {
		params.ServiceConfig.Logger.Error("Failed to advertise heartbeat service",
			zap.Error(err))
	} else {
		params.ServiceConfig.Logger.Info("advertised heartbeat")
	}

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
	assigned, err := c.aresControllerClient.GetAssignment(c.jobNS, c.serviceConfig.Environment.InstanceID)
	if err != nil {
		c.serviceConfig.Logger.Error("Failed to get assignment from aresDB controller",
			zap.String("jobNamespace", c.jobNS),
			zap.String("aresDB Controller", c.serviceConfig.ControllerConfig.Address),
			zap.Error(err))
		c.serviceConfig.Scope.Counter("syncUp.failed").Inc(1)
		return
	}

	assignment, err := rules.NewAssignmentFromController(assigned)
	if err != nil {
		c.serviceConfig.Logger.Error("Failed to populate assignment from controller assignment",
			zap.String("jobNamespace", c.jobNS),
			zap.String("aresDB Controller", c.serviceConfig.ControllerConfig.Address),
			zap.Error(err))
		c.serviceConfig.Scope.Counter("syncUp.failed").Inc(1)
		return
	}

	c.serviceConfig.Logger.Info("Got assignment from aresDB controller",
		zap.String("jobNamespace", c.jobNS),
		zap.String("aresDB Controller", c.serviceConfig.ControllerConfig.Address),
		zap.String("activeAresNameSpace", config.ActiveAresNameSpace),
		zap.Any("aresClusterNSConfig", c.serviceConfig.AresNSConfig),
		zap.Any("activeAresClusters", c.serviceConfig.ActiveAresClusters),
		zap.Any("assignement", assignment))

	newJobs := make(map[string]*rules.JobConfig)
	// Add or Update jobs
	for _, jobConfig := range assignment.Jobs {
		newJobs[jobConfig.Name] = jobConfig
		if aresClusterDrivers, ok := c.Drivers[jobConfig.Name]; ok {
			// case1: existing jobConfig
			for aresCluster, driver := range aresClusterDrivers {
				if _, ok := assignment.AresClusters[aresCluster]; !ok {
					// case1.1: delete the driver because aresCluster is deleted
					activeAresCluster, exist := c.serviceConfig.ActiveAresClusters[aresCluster]
					if exist && activeAresCluster.GetSinkMode() != config.Sink_Kafka {
						c.deleteDriver(driver, aresCluster, aresClusterDrivers)
						c.serviceConfig.Logger.Info("deleted driver due to the removed aresCluster",
							zap.String("job", jobConfig.Name),
							zap.String("aresCluster", aresCluster))
					}
					continue
				}
				if driver.jobConfig.Version != jobConfig.Version {
					// case1.2: restart the driver because jobConfig version is changed,
					if !c.addDriver(jobConfig, aresCluster, aresClusterDrivers, true) {
						updateHash = false
						c.serviceConfig.Logger.Info("restarted driver due to version changes",
							zap.String("job", jobConfig.Name),
							zap.String("aresCluster", aresCluster))
					}
				}
			}
			for aresCluster, aresClusterObj := range assignment.AresClusters {
				if _, ok := aresClusterDrivers[aresCluster]; !ok {
					// case1.3 add a new driver because a new aresCluster is added
					c.serviceConfig.ActiveAresClusters[aresCluster] = aresClusterObj
					if !c.addDriver(jobConfig, aresCluster, aresClusterDrivers, false) {
						updateHash = false
						c.serviceConfig.Logger.Info("added driver due to the new aresCluster",
							zap.String("job", jobConfig.Name),
							zap.String("aresCluster", aresCluster))
					}
				}
			}
		} else {
			// case2: a new jobConfig
			aresClusterDrivers := make(map[string]*Driver)
			if len(assignment.AresClusters) != 0 {
				for aresCluster, aresClusterObj := range assignment.AresClusters {
					// case2.1: add a new driver for each aresCluster
					c.serviceConfig.ActiveAresClusters[aresCluster] = aresClusterObj
					if !c.addDriver(jobConfig, aresCluster, aresClusterDrivers, false) {
						updateHash = false
						c.serviceConfig.Logger.Info("added driver (aresDB sink) due to the new job",
							zap.String("job", jobConfig.Name),
							zap.String("aresCluster", aresCluster))
					}
				}
			} else {
				for aresCluster, aresClusterObj := range c.serviceConfig.ActiveAresClusters {
					if aresClusterObj.GetSinkMode() == config.Sink_Kafka {
						if !c.addDriver(jobConfig, aresCluster, aresClusterDrivers, false) {
							updateHash = false
							c.serviceConfig.Logger.Info("added driver (kafka sink) due to the new job",
								zap.String("job", jobConfig.Name),
								zap.String("aresCluster", aresCluster))
						}
					} else {
						c.serviceConfig.Logger.Error("missing aresDB instance in assignment",
							zap.String("job", jobConfig.Name),
							zap.String("aresCluster", aresCluster))
					}
				}
			}
			c.Drivers[jobConfig.Name] = aresClusterDrivers
			for aresCluster, aresClusterObj := range c.serviceConfig.ActiveAresClusters {
				// case2.2: delete the aresCluster from ActiveAresClusters because it is deleted from assignment
				if _, ok := assignment.AresClusters[aresCluster]; !ok && aresClusterObj.GetSinkMode() != config.Sink_Kafka {
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
		NewDriver(clonedJobConfig, c.serviceConfig, c.aresControllerClient, NewStreamingProcessor, c.sinkInitFunc, c.consumerInitFunc, c.decoderInitFunc)
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
