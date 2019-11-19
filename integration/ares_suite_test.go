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

package integration

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gopkg.in/yaml.v2"
	"os"
	"testing"
	"time"

	"github.com/onsi/ginkgo/reporters"
	"github.com/uber/aresdb/cmd/aresd/cmd"
	"github.com/uber/aresdb/common"
	ex "github.com/uber/aresdb/examples/utils"
	"github.com/uber/aresdb/utils"
)

func TestAPI(t *testing.T) {
	RegisterFailHandler(Fail)
	junitReporter := reporters.NewJUnitReporter("junit.xml")
	RunSpecsWithDefaultAndCustomReporters(t, "Ares Integration Test Suite", []Reporter{junitReporter})
}

var aresd *cmd.AresD

var _ = BeforeSuite(func() {
	// customize the clock
	utils.SetCurrentTime(time.Unix(1560049867, 0))
	loggerFactory := common.NewLoggerFactory()

	options := &cmd.Options{
		ServerLogger: loggerFactory.GetDefaultLogger(),
		QueryLogger:  loggerFactory.GetLogger("query"),
		Metrics:      common.NewNoopMetrics(),
	}

	f, err := os.Open("config/ares.yaml")
	Ω(err).Should(BeNil())
	var cfg common.AresServerConfig
	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(&cfg)
	Ω(err).Should(BeNil())

	aresd = cmd.NewAresD(cfg, options)
	go aresd.Start()
	// wait for the aresd to start, it should panic out if the server can not start
	<- aresd.StartedChan

	tableSchemaPath := "./test-data/schema/arraytest.json"
	// create testing tables
	ex.CreateTable(testHost, testPort, "arraytest", tableSchemaPath)
	// ingest testing data
	ex.IngestDataForTable(testHost, testPort, "arraytest", "./test-data/data/arraytest.csv")
})

var _ = AfterSuite(func() {
	if aresd != nil {
		aresd.Shutdown()
	}
	// clean up local temp files
	os.RemoveAll("ares-root")
	// reset the clock
	utils.ResetClockImplementation()
})
