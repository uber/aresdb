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
	Host    string      `yaml:"host"`
	Port    int         `yaml:"port"`
	Headers http.Header `yaml:"headers"`
}

// ClientsConfig is the config for all clients
type ClientsConfig struct {
	Controller *ControllerConfig `yaml:"controller,omitempty"`
}

type ClusterConfig struct {
	Enable      bool   `yaml:"enable"`
	ClusterName string `yaml:"cluster_name"`
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

	Query     QueryConfig     `yaml:"query"`
	DiskStore DiskStoreConfig `yaml:"disk_store"`
	HTTP      HTTPConfig      `yaml:"http"`
	Cluster   ClusterConfig   `yaml:"cluster"`
	Clients   ClientsConfig   `yaml:"clients"`
}
