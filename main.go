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

package main

import (
	"github.com/uber/aresdb/cmd"
	"github.com/uber/aresdb/common"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
)

const defaultCfgPath = "config/ares.yaml"

func readConfig() (cfg common.AresServerConfig, err error) {
	var fileContent []byte
	fileContent, err = ioutil.ReadFile(defaultCfgPath)
	if err != nil {
		return
	}
	err = yaml.Unmarshal(fileContent, &cfg)
	return
}

func main() {
	logFactory := common.NewLoggerFactory()
	defaultLogger := logFactory.GetDefaultLogger()
	queryLogger := logFactory.GetLogger("query")
	defaultMetric := common.NewNoopMetrics()

	cfg, err := readConfig()
	if err != nil {
		defaultLogger.WithField("err", err).Fatal("Failed to read default config")
	}

	if cfg.Cluster.Enable && cfg.Cluster.InstanceName == "" {
		// use hostname as instance name if not configured
		hostName, err := os.Hostname()
		if err != nil {
			defaultLogger.Fatal("Failed to get host name:", err)
		}
		cfg.Cluster.InstanceName = hostName
	}

	cmd.StartService(cfg, defaultLogger, queryLogger, defaultMetric)
}
