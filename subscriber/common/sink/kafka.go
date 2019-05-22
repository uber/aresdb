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
	controllerCom "github.com/uber/aresdb/controller/client"
	memCom "github.com/uber/aresdb/memstore/common"
	"github.com/uber/aresdb/subscriber/common/rules"
	"github.com/uber/aresdb/subscriber/config"
	"github.com/uber/aresdb/utils"
	"go.uber.org/zap"
	"strings"
	"time"
)

type KafkaPublisher struct {
	sarama.SyncProducer
	client.UpsertBatchBuilder

	ServiceConfig config.ServiceConfig
	JobConfig     *rules.JobConfig
	Scope         tally.Scope
	ClusterName   string
}

func NewKafkaPublisher(serviceConfig config.ServiceConfig, jobConfig *rules.JobConfig, cluster string,
	sinkCfg config.SinkConfig, aresControllerClient controllerCom.ControllerClient) (Sink, error) {
	if sinkCfg.GetSinkMode() != config.Sink_Kafka {
		return nil, fmt.Errorf("Failed to NewKafkaPublisher, wrong sinkMode=%d", sinkCfg.GetSinkMode())
	}

	addresses := strings.Split(sinkCfg.KafkaProducerConfig.Brokers, ",")
	serviceConfig.Logger.Info("Kafka borkers address", zap.Any("brokers", addresses))

	cfg := sarama.NewConfig()
	cfg.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	if sinkCfg.KafkaProducerConfig.RetryMax > 0 {
		cfg.Producer.Retry.Max = sinkCfg.KafkaProducerConfig.RetryMax
	}
	if sinkCfg.KafkaProducerConfig.TimeoutInSec > 0 {
		cfg.Producer.Timeout = time.Second * time.Duration(sinkCfg.KafkaProducerConfig.TimeoutInSec)
	}
	cfg.Producer.Return.Successes = true

	p, err := sarama.NewSyncProducer(addresses, cfg)
	if err != nil {
		return nil, utils.StackError(err, "Unable to initialize Kafka producer")
	}

	// replace httpSchemaFetcher with gateway client
	// httpSchemaFetcher := NewHttpSchemaFetcher(httpClient, cfg.Address, metricScope)
	cachedSchemaHandler := client.NewCachedSchemaHandler(
		serviceConfig.Logger.Sugar(),
		serviceConfig.Scope.Tagged(map[string]string{
			"job":         jobConfig.Name,
			"aresCluster": cluster,
		}), aresControllerClient)

	// schema refresh is based on job assignment refresh, so disable at here
	err = cachedSchemaHandler.Start(0)
	if err != nil {
		return nil, err
	}

	kp := KafkaPublisher{
		SyncProducer: p,
		UpsertBatchBuilder: client.NewUpsertBatchBuilderImpl(
			serviceConfig.Logger.Sugar(),
			serviceConfig.Scope.Tagged(map[string]string{
				"job":         jobConfig.Name,
				"aresCluster": cluster,
			}),
			cachedSchemaHandler),
		ServiceConfig: serviceConfig,
		JobConfig:     jobConfig,
		Scope: serviceConfig.Scope.Tagged(map[string]string{
			"job":         jobConfig.Name,
			"aresCluster": cluster,
		}),
		ClusterName: cluster,
	}

	return &kp, nil
}

// Shutdown will clean up resources that needs to be cleaned up
func (kp *KafkaPublisher) Shutdown() {
	kp.SyncProducer.Close()
}

// Save saves a batch of row objects into a destination
func (kp *KafkaPublisher) Save(destination Destination, rows []client.Row) error {
	kp.Scope.Gauge("batchSize").Update(float64(len(rows)))

	shards, rowsIgnored := Shard(rows, destination, kp.JobConfig)
	if rowsIgnored != 0 {
		kp.Scope.Counter("errors.shard").Inc(int64(rowsIgnored))
	}

	msgs := make([]*sarama.ProducerMessage, 0, len(shards))
	if shards == nil {
		// case1: no sharding --  publish rows to random kafka partition
		kp.buildKafkaMessage(msgs, &rowsIgnored, destination.Table, -1, destination.ColumnNames, rows, destination.AresUpdateModes...)
	} else {
		// case2: sharding -- publish rows to specified partition
		for shardID, rowsInShard := range shards {
			kp.buildKafkaMessage(msgs, &rowsIgnored, destination.Table, int32(shardID), destination.ColumnNames, rowsInShard, destination.AresUpdateModes...)
		}
	}

	saveStart := utils.Now()
	kp.ServiceConfig.Logger.Debug("saving", zap.Any("rows", rows))
	err := kp.SyncProducer.SendMessages(msgs)
	if err != nil {
		kp.Scope.Counter("errors.insert").Inc(1)
		return utils.StackError(err, fmt.Sprintf("Failed to publish rows in table %s, columns: %+v",
			destination.Table, destination.ColumnNames))
	}
	numRows := len(rows) - rowsIgnored
	kp.Scope.Timer("latency.ares.save").Record(utils.Now().Sub(saveStart))
	kp.Scope.Counter("rowsWritten").Inc(int64(numRows))
	kp.Scope.Counter("rowsIgnored").Inc(int64(rowsIgnored))
	kp.Scope.Gauge("upsertBatchSize").Update(float64(numRows))

	return nil
}

// Cluster returns the DB cluster name
func (kp *KafkaPublisher) Cluster() string {
	return kp.ClusterName
}

func (kp *KafkaPublisher) buildKafkaMessage(msgs []*sarama.ProducerMessage, rowsIgnored *int, tableName string, shardID int32, columnNames []string, rows []client.Row,
	updateModes ...memCom.ColumnUpdateMode) {
	bytes, numRows, err := kp.UpsertBatchBuilder.PrepareUpsertBatch(tableName, columnNames, updateModes, rows)
	if err != nil {
		kp.Scope.Counter("errors.upsertBatchBuild").Inc(1)
		utils.StackError(err, "Failed to prepare rows in table %s, columns: %+v",
			tableName, columnNames)
	}

	msg := sarama.ProducerMessage{
		Topic: fmt.Sprintf("%s-%s", kp.Cluster(), tableName),
		Value: sarama.ByteEncoder(bytes),
	}

	if shardID >= 0 {
		msg.Partition = shardID
	}

	msgs = append(msgs, &msg)
	*rowsIgnored = *rowsIgnored + (len(rows) - numRows)
	return
}
