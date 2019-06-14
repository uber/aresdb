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
	"github.com/m3db/m3/src/cluster/client/etcd"
	"github.com/uber/aresdb/common"
)

type BrokerConfig struct {
	// HTTP port for serving.
	Port int `yaml:"port"`

	ControllerConfig *common.ControllerConfig `yaml:"controller,omitempty"`
	HTTP             common.HTTPConfig        `yaml:"http"`
	Etcd             etcd.Configuration       `yaml:"etcd"`
	Cluster          common.ClusterConfig     `yaml:"cluster"`
}
