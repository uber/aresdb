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

package common

import (
	"net/http"
)

// TimezoneConfig is the static config for timezone column support
type TimezoneConfig struct {
	// table to lookup timezone columns
	TableName string `yaml:"table_name"`
}

// QueryConfig is the static configuration for query.
type QueryConfig struct {
	// how much portion of the device memory we are allowed use
	DeviceMemoryUtilization float32 `yaml:"device_memory_utilization"`
	// timeout in seconds for choosing device
	DeviceChoosingTimeout int            `yaml:"device_choosing_timeout"`
	TimezoneTable         TimezoneConfig `yaml:"timezone_table"`
	EnableHashReduction   bool           `yaml:"enable_hash_reduction"`
}

// DiskStoreConfig is the static configuration for disk store.
type DiskStoreConfig struct {
	WriteSync bool `yaml:"write_sync"`
}

// HTTPConfig is the static configuration for main http server (query and schema).
type HTTPConfig struct {
	MaxConnections        int `yaml:"max_connections"`
	ReadTimeOutInSeconds  int `yaml:"read_time_out_in_seconds"`
	WriteTimeOutInSeconds int `yaml:"write_time_out_in_seconds"`
}

// ControllerConfig is the config for ares-controller client
type ControllerConfig struct {
	Address    string      `yaml:"address"`
	Headers    http.Header `yaml:"headers"`
	TimeoutSec int         `yaml:"timeout"`
}

// GatewayConfig is the config for all gateway
type GatewayConfig struct {
	Controller *ControllerConfig `yaml:"controller,omitempty"`
}

// InstanceConfig defines the config related to this instance in distributed cluster
type InstanceConfig struct {
	// ID represents the instance id of this datanode
	ID string `yaml:"id"`
	// Env represents the environment this data node belongs to
	// eg. dev, staging, sandbox, production, etc.
	Env string `yaml:"env"`
	// zone represent the zone this cluster belongs to
	Zone string `yaml:"zone"`
	// namespace is the namespace this instance belongs to
	Namespace string `yaml:"namespace"`
}

// ClusterConfig is the config for starting current instance with cluster mode
type ClusterConfig struct {
	// Enable controls whether to start in cluster mode
	Enable bool `yaml:"enable"`
	// ClusterName is the cluster to join
	ClusterName string `yaml:"cluster_name"`
	// InstanceName is the cluster wide unique name to identify current instance
	// it can be static configured in yaml, or dynamically set on start up
	InstanceName string `yaml:"instance_name"`
}

// local redolog config
type DiskRedoLogConfig struct {
	// disable local disk redolog, default will be enabled
	Disabled bool `yaml:"disabled"`
}

// Kafka source config
type KafkaRedoLogConfig struct {
	// enable redolog from kafka, default will be disabled
	Enabled bool `yaml:"enabled"`
	// kafka brokers
	Brokers []string `yaml:"brokers"`
}

// Configs related to data import and redolog option
type RedoLogConfig struct {
	// namespace or cluster named for this db
	Namespace string `yaml:"namespace"`
	// Disk redolog config
	DiskConfig DiskRedoLogConfig `yaml:"disk"`
	// Kafka redolog config
	KafkaConfig KafkaRedoLogConfig `yaml:"kafka"`
}

// AresServerConfig is config specific for ares server.
type AresServerConfig struct {
	// HTTP port for serving.
	Port int `yaml:"port"`

	// HTTP port for debugging.
	DebugPort int `yaml:"debug_port"`

	// Directory path that stores the data and schema on local disk.
	RootPath string `yaml:"root_path"`

	// Total memory size ares can use.
	TotalMemorySize int64 `yaml:"total_memory_size"`

	// Whether to turn off scheduler.
	SchedulerOff bool `yaml:"scheduler_off"`

	// Build version of the server currently running
	Version string `yaml:"version"`

	// environment
	Env string `yaml:"env"`

	Query     QueryConfig     `yaml:"query"`
	DiskStore DiskStoreConfig `yaml:"disk_store"`
	HTTP      HTTPConfig      `yaml:"http"`
	Cluster   ClusterConfig   `yaml:"cluster"`
	Gateway   GatewayConfig   `yaml:"gateway"`

	RedoLogConfig RedoLogConfig `yaml:"redolog"`

	// InstanceConfig defines instance config within distributed cluster
	InstanceConfig InstanceConfig `yaml:"instance"`
}
