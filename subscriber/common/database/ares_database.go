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

package database

import (
	"fmt"
	"time"

	"github.com/uber-go/tally"
	"github.com/uber/aresdb/client"
	"github.com/uber/aresdb/subscriber/config"
	"github.com/uber/aresdb/utils"
	"go.uber.org/zap"
)

// AresDatabase is an implementation of Database interface for saving data to ares
type AresDatabase struct {
	ServiceConfig config.ServiceConfig
	Scope         tally.Scope
	ClusterName   string
	Connector     client.Connector
	JobName       string
}

// NewAresDatabase initialize an AresDatabase cluster
func NewAresDatabase(
	serviceConfig config.ServiceConfig, jobName string, cluster string,
	config client.ConnectorConfig) (*AresDatabase, error) {
	connector, err := config.NewConnector(serviceConfig.Logger.Sugar(), serviceConfig.Scope.Tagged(map[string]string{
		"job":         jobName,
		"aresCluster": cluster,
	}))
	if err != nil {
		return nil, utils.StackError(err, "failed to create ares connector")
	}
	return &AresDatabase{
		ServiceConfig: serviceConfig,
		Scope: serviceConfig.Scope.Tagged(map[string]string{
			"job":         jobName,
			"aresCluster": cluster,
		}),
		ClusterName: cluster,
		Connector:   connector,
		JobName:     jobName,
	}, nil
}

// Shutdown will clean up resources that needs to be cleaned up
func (db *AresDatabase) Shutdown() {}

// Save saves a batch of row objects into a destination
func (db *AresDatabase) Save(destination Destination, rows []client.Row) error {
	db.Scope.Gauge("batchSize").Update(float64(len(rows)))
	aresRows := make([]client.Row, 0, len(rows))
	for _, row := range rows {
		aresRows = append(aresRows, client.Row(row))
	}

	saveStart := time.Now()
	db.ServiceConfig.Logger.Debug("saving", zap.Any("rows", aresRows))
	rowsInserted, err := db.Connector.
		Insert(destination.Table, destination.ColumnNames, aresRows, destination.AresUpdateModes...)
	if err != nil {
		db.Scope.Counter("errors.insert").Inc(1)
		return utils.StackError(err, fmt.Sprintf("Failed to save rows in table %s, columns: %+v",
			destination.Table, destination.ColumnNames))
	}
	db.Scope.Timer("latency.ares.save").Record(time.Now().Sub(saveStart))
	db.Scope.Counter("rowsWritten").Inc(int64(rowsInserted))
	db.Scope.Counter("rowsIgnored").Inc(int64(len(aresRows)) - int64(rowsInserted))
	db.Scope.Gauge("upsertBatchSize").Update(float64(rowsInserted))
	return nil
}

// Cluster returns the DB cluster name
func (db *AresDatabase) Cluster() string {
	return db.ClusterName
}
