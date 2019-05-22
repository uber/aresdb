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

package sink

import (
	"fmt"
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/client"
	controllerCom "github.com/uber/aresdb/controller/client"
	"github.com/uber/aresdb/subscriber/common/rules"
	"github.com/uber/aresdb/subscriber/config"
	"github.com/uber/aresdb/utils"
	"go.uber.org/zap"
)

// AresDatabase is an implementation of Database interface for saving data to ares
type AresDatabase struct {
	ServiceConfig config.ServiceConfig
	JobConfig     *rules.JobConfig
	Scope         tally.Scope
	ClusterName   string
	Connector     client.Connector
}

// NewAresDatabase initialize an AresDatabase cluster
func NewAresDatabase(
	serviceConfig config.ServiceConfig, jobConfig *rules.JobConfig, cluster string,
	sinkCfg config.SinkConfig, aresControllerClient controllerCom.ControllerClient) (Sink, error) {
	if sinkCfg.GetSinkMode() != config.Sink_AresDB {
		return nil, fmt.Errorf("Failed to NewAresDatabase, wrong sinkMode=%d", sinkCfg.GetSinkMode())
	}

	connector, err := sinkCfg.AresDBConnectorConfig.NewConnector(serviceConfig.Logger.Sugar(), serviceConfig.Scope.Tagged(map[string]string{
		"job":         jobConfig.Name,
		"aresCluster": cluster,
	}))
	if err != nil {
		return nil, utils.StackError(err, "failed to create ares connector")
	}
	return &AresDatabase{
		ServiceConfig: serviceConfig,
		JobConfig:     jobConfig,
		Scope: serviceConfig.Scope.Tagged(map[string]string{
			"job":         jobConfig.Name,
			"aresCluster": cluster,
		}),
		ClusterName: cluster,
		Connector:   connector,
	}, nil
}

// Shutdown will clean up resources that needs to be cleaned up
func (db *AresDatabase) Shutdown() {}

// Save saves a batch of row objects into a destination
func (db *AresDatabase) Save(destination Destination, rows []client.Row) error {
	db.Scope.Gauge("batchSize").Update(float64(len(rows)))

	saveStart := utils.Now()
	db.ServiceConfig.Logger.Debug("saving", zap.Any("rows", rows))
	rowsInserted, err := db.Connector.
		Insert(destination.Table, destination.ColumnNames, rows, destination.AresUpdateModes...)
	if err != nil {
		db.Scope.Counter("errors.insert").Inc(1)
		return utils.StackError(err, fmt.Sprintf("Failed to save rows in table %s, columns: %+v",
			destination.Table, destination.ColumnNames))
	}
	db.Scope.Timer("latency.ares.save").Record(utils.Now().Sub(saveStart))
	db.Scope.Counter("rowsWritten").Inc(int64(rowsInserted))
	db.Scope.Counter("rowsIgnored").Inc(int64(len(rows)) - int64(rowsInserted))
	db.Scope.Gauge("upsertBatchSize").Update(float64(rowsInserted))
	return nil
}

// Cluster returns the DB cluster name
func (db *AresDatabase) Cluster() string {
	return db.ClusterName
}
