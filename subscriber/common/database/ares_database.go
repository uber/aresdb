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
	serviceConfig config.ServiceConfig
	scope         tally.Scope
	cluster       string
	connector     client.Connector
	jobName       string
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
		serviceConfig: serviceConfig,
		scope: serviceConfig.Scope.Tagged(map[string]string{
			"job":         jobName,
			"aresCluster": cluster,
		}),
		cluster:   cluster,
		connector: connector,
		jobName:   jobName,
	}, nil
}

// Shutdown will clean up resources that needs to be cleaned up
func (db *AresDatabase) Shutdown() {}

// Save saves a batch of row objects into a destination
func (db *AresDatabase) Save(destination Destination, rows []Row) error {
	db.scope.Gauge("batchSize").Update(float64(len(rows)))
	aresRows := make([]client.Row, 0, len(rows))
	for _, row := range rows {
		aresRows = append(aresRows, client.Row(row))
	}

	saveStart := time.Now()
	db.serviceConfig.Logger.Debug("saving", zap.Any("rows", aresRows))
	rowsInserted, err := db.connector.
		Insert(destination.Table, destination.ColumnNames, aresRows, destination.AresUpdateModes...)
	if err != nil {
		db.scope.Counter("errors.insert").Inc(1)
		return utils.StackError(err, fmt.Sprintf("Failed to save rows in table %s, columns: %+v",
			destination.Table, destination.ColumnNames))
	}
	db.scope.Timer("latency.ares.save").Record(time.Now().Sub(saveStart))
	db.scope.Counter("rowsWritten").Inc(int64(rowsInserted))
	db.scope.Counter("rowsIgnored").Inc(int64(len(aresRows)) - int64(rowsInserted))
	db.scope.Gauge("upsertBatchSize").Update(float64(rowsInserted))
	return nil
}

// Cluster returns the DB cluster name
func (db *AresDatabase) Cluster() string {
	return db.cluster
}
