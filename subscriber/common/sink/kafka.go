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
	"github.com/Shopify/sarama"
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/client"
	"github.com/uber/aresdb/subscriber/common/rules"
	"github.com/uber/aresdb/subscriber/config"
	"github.com/uber/aresdb/utils"
	"go.uber.org/zap"
	"strings"
	"time"
)

type KafkaPublisher struct {
	sarama.SyncProducer
	client.UpsertBatchBuilderImpl

	ServiceConfig config.ServiceConfig
	JobConfig     *rules.JobConfig
	Scope         tally.Scope
	ClusterName   string
}

func NewKafkaPublisher(jobConfig *rules.JobConfig, serviceConfig config.ServiceConfig, cluster string, kpCfg config.KafkaProducerConfig) (Sink, error) {
	addresses := strings.Split(kpCfg.Brokers, ",")
	serviceConfig.Logger.Info("Kafka borkers address", zap.Any("brokers", addresses))

	cfg := sarama.NewConfig()
	cfg.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	cfg.Producer.Retry.Max = kpCfg.RetryMax
	cfg.Producer.Timeout = time.Second * time.Duration(kpCfg.TimeoutInSec)
	cfg.Producer.Return.Successes = true

	p, err := sarama.NewSyncProducer(addresses, cfg)
	if err != nil {
		return nil, utils.StackError(err, "Unable to initialize Kafka publisher")
	}
	upsertBatchBuilderImpl := client.UpsertBatchBuilderImpl{
		/* TODO: need export parameters
		cfg:                      serviceConfig.Logger.Sugar(),
		logger:                   serviceConfig.Logger.Sugar(),
		metricScope:              serviceConfig.Scope.Tagged(map[string]string{
			"job":         jobConfig.Name,
			"aresCluster": cluster,
		}),
		schemas:                  make(map[string]*client.TableSchema),
		enumMappings:             make(map[string]map[int]client.EnumDict),
		enumDefaultValueMappings: make(map[string]map[int]int),
		*/
	}
	kp := KafkaPublisher{
		SyncProducer:           p,
		UpsertBatchBuilderImpl: upsertBatchBuilderImpl,
		ServiceConfig:          serviceConfig,
		JobConfig:              jobConfig,
		Scope: serviceConfig.Scope.Tagged(map[string]string{
			"job":         jobConfig.Name,
			"aresCluster": cluster,
		}),
		ClusterName: cluster,
	}

	return &kp, nil
}

// Shutdown will clean up resources that needs to be cleaned up
func (kp *KafkaPublisher) Shutdown() {}

// Save saves a batch of row objects into a destination
func (kp *KafkaPublisher) Save(destination Destination, rows []client.Row) error {
	shards := Sharding(rows, destination, kp.JobConfig)
	kp.Scope.Gauge("batchSize").Update(float64(len(rows)))
	aresRows := make([]client.Row, 0, len(rows))
	for _, row := range rows {
		aresRows = append(aresRows, row)
	}

	saveStart := time.Now()
	kp.ServiceConfig.Logger.Debug("saving", zap.Any("rows", aresRows))
	rowsInserted, err := kp.Connector.
		Insert(destination.Table, destination.ColumnNames, aresRows, destination.AresUpdateModes...)
	if err != nil {
		kp.Scope.Counter("errors.insert").Inc(1)
		return utils.StackError(err, fmt.Sprintf("Failed to save rows in table %s, columns: %+v",
			destination.Table, destination.ColumnNames))
	}
	kp.Scope.Timer("latency.ares.save").Record(time.Now().Sub(saveStart))
	kp.Scope.Counter("rowsWritten").Inc(int64(rowsInserted))
	kp.Scope.Counter("rowsIgnored").Inc(int64(len(aresRows)) - int64(rowsInserted))
	kp.Scope.Gauge("upsertBatchSize").Update(float64(rowsInserted))
	return nil
}

// Cluster returns the DB cluster name
func (kp *KafkaPublisher) Cluster() string {
	return kp.ClusterName
}
