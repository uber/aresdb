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

package config

import (
	"errors"
	"fmt"
	"time"

	"github.com/m3db/m3/src/cluster/client/etcd"
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/client"
	"github.com/uber/aresdb/utils"
	cfgfx "go.uber.org/config"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

var (
	// ActiveAresNameSpace is current namespace of a list of Ares Supported in current service
	ActiveAresNameSpace string

	// ActiveJobNameSpace is current namespace of a list of jobs Supported in current service
	ActiveJobNameSpace string

	// ConfigRootPath is the root path of config
	ConfigRootPath string = "config"

	// ConfigFile is the file path of config file
	ConfigFile string

	// Module configures an HTTP server.
	Module = fx.Options(
		fx.Provide(
			NewServiceConfig,
		),
	)
)

// Params defines the base objects for a service.
type Params struct {
	fx.In

	Environment utils.EnvironmentContext
	Logger      *zap.Logger
	Scope       tally.Scope
	Config      cfgfx.Provider
}

// Result defines the objects that the config module provides.
type Result struct {
	fx.Out

	ServiceConfig ServiceConfig
}

// ServiceConfig defines the service configuration.
type ServiceConfig struct {
	Environment utils.EnvironmentContext
	Logger      *zap.Logger
	Scope       tally.Scope
	Config      cfgfx.Provider

	Service            string                            `yaml:"service.name"`
	BackendPort        int                               `yaml:"rest.http.address"`
	AresNSConfig       AresNSConfig                      `yaml:"ares"`
	JobNSConfig        JobNSConfig                       `yaml:"jobs"`
	ActiveAresClusters map[string]client.ConnectorConfig `yaml:"-"`
	ActiveJobs         []string                          `yaml:"-"`
	ControllerConfig   *ControllerConfig                 `yaml:"controller"`
	ZooKeeperConfig    ZooKeeperConfig                   `yaml:"zookeeper"`
	EtcdConfig         *etcd.Configuration               `yaml:"etcd"`
	HeartbeatConfig    *HeartBeatConfig                  `yaml:"heartbeat"`
}

// HeartBeatConfig represents heartbeat config
type HeartBeatConfig struct {
	Enabled       *bool          `yaml:"enabled"`
	Timeout       *time.Duration `yaml:"timeout"`
	Interval      *time.Duration `yaml:"interval"`
	CheckInterval *time.Duration `yaml:"checkInterval"`
}

// AresNSConfig defines the mapping b/w ares namespace and its clusters
type AresNSConfig struct {
	AresNameSpaces map[string][]string               `yaml:"namespaces"`
	AresClusters   map[string]client.ConnectorConfig `yaml:"clusters"`
}

// JobNSConfig defines the mapping b/w job namespace and its clusters
type JobNSConfig struct {
	Jobs map[string][]string `yaml:"namespaces"`
}

// ControllerConfig defines aresDB controller configuration
type ControllerConfig struct {
	// Enable defines whether to enable aresDB controll or not
	Enable bool `yaml:"enable" default:"false"`
	// Address is aresDB controller address
	Address string `yaml:"address" default:"localhost:5436"`
	// Timeout is request sent to aresDB controller timeout in seconds
	Timeout int `yaml:"timeout" default:"30"`
	// RefreshInterval is the interval to sync up with aresDB controller in minutes
	RefreshInterval int `yaml:"refreshInterval" default:"10"`
	// ServiceName is aresDB controller name
	ServiceName string `yaml:"serviceName" default:"ares-controller"`
}

// ZooKeeperConfig defines the ZooKeeper client configuration
type ZooKeeperConfig struct {
	// Server defines zookeeper server addresses
	Server                   string        `yaml:"server"`
	SessionTimeoutSeconds    time.Duration `yaml:"sessionTimeoutSeconds" default:"60"`
	ConnectionTimeoutSeconds time.Duration `yaml:"connectionTimeoutSeconds" default:"15"`
	BaseSleepTimeSeconds     time.Duration `yaml:"exponentialBackoffRetryPolicy.baseSleepTimeSeconds" default:"1"`
	MaxRetries               int           `yaml:"exponentialBackoffRetryPolicy.maxRetries" default:"3"`
	MaxSleepSeconds          time.Duration `yaml:"exponentialBackoffRetryPolicy.maxSleepSeconds" default:"15"`
}

// NewServiceConfig constructs ServiceConfig.
func NewServiceConfig(p Params) (Result, error) {
	raw := p.Config.Get(cfgfx.Root)
	serviceConfig := ServiceConfig{}

	if err := raw.Populate(&serviceConfig); err != nil {
		return Result{
			ServiceConfig: serviceConfig,
		}, err
	}
	serviceConfig.Environment = p.Environment
	serviceConfig.Logger = p.Logger
	serviceConfig.Scope = p.Scope.Tagged(map[string]string{
		"deployment":  p.Environment.Deployment,
		"dc":          p.Environment.Zone,
		"application": p.Environment.ApplicationID,
	})
	serviceConfig.Config = p.Config
	serviceConfig.ActiveAresClusters = make(map[string]client.ConnectorConfig)

	// Skip local job and aresCluster config if controller is enabled
	if serviceConfig.ControllerConfig.Enable {
		return Result{
			ServiceConfig: serviceConfig,
		}, nil
	}

	// set serviceConfig.ActiveAresClusters
	if serviceConfig.AresNSConfig.AresClusters == nil || serviceConfig.AresNSConfig.AresNameSpaces == nil {
		return Result{
			ServiceConfig: serviceConfig,
		}, errors.New("Ares namespaces and clusters must be configured")
	}

	activeAresClusters := serviceConfig.AresNSConfig.AresNameSpaces[ActiveAresNameSpace]
	if activeAresClusters != nil {
		for _, cluster := range activeAresClusters {
			serviceConfig.ActiveAresClusters[cluster] = serviceConfig.AresNSConfig.AresClusters[cluster]
		}
		if len(serviceConfig.ActiveAresClusters) == 0 {
			return Result{
				ServiceConfig: serviceConfig,
			}, fmt.Errorf("No ares cluster configure is found for namespace %s", ActiveAresNameSpace)
		}
	} else {
		return Result{
			ServiceConfig: serviceConfig,
		}, fmt.Errorf("No ares clusters are defined for namespace %s", ActiveAresNameSpace)
	}

	// set serviceConfig.ActiveJobs
	if serviceConfig.JobNSConfig.Jobs == nil {
		return Result{
			ServiceConfig: serviceConfig,
		}, errors.New("Job namespace config not found")
	}
	serviceConfig.ActiveJobs = serviceConfig.JobNSConfig.Jobs[ActiveJobNameSpace]

	return Result{
		ServiceConfig: serviceConfig,
	}, nil
}
