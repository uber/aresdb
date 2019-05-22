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

package job

import (
	"fmt"
	controllerCom "github.com/uber/aresdb/controller/client"
	"github.com/uber/aresdb/subscriber/common/sink"
	"strconv"
	"sync"
	"time"

	"github.com/uber-go/tally"
	"github.com/uber/aresdb/client"
	"github.com/uber/aresdb/subscriber/common/consumer"
	"github.com/uber/aresdb/subscriber/common/message"
	"github.com/uber/aresdb/subscriber/common/rules"
	"github.com/uber/aresdb/subscriber/common/tools"
	"github.com/uber/aresdb/subscriber/config"
	"github.com/uber/aresdb/utils"
	"go.uber.org/zap"
)

// NewConsumer is the type of function each consumer that implements Consumer should provide for initialization.
type NewConsumer func(jobConfig *rules.JobConfig, serviceConfig config.ServiceConfig) (consumer.Consumer, error)

// NewDecoder is the type of function each decoder that implements decoder should provide for initialization.
type NewDecoder func(jobConfig *rules.JobConfig, serviceConfig config.ServiceConfig) (decoder message.Decoder, err error)

// NewSink is the type of function each decoder that implements sink should provide for initialization.
type NewSink func(
	serviceConfig config.ServiceConfig, jobConfig *rules.JobConfig, cluster string,
	sinkCfg config.SinkConfig, aresControllerClient controllerCom.ControllerClient) (sink.Sink, error)

// StreamingProcessor defines a individual processor that connects to a Kafka high level consumer,
// processes the messages based on the type of job and saves to database
type StreamingProcessor struct {
	ID                   int
	context              *ProcessorContext
	jobConfig            *rules.JobConfig
	cluster              string
	serviceConfig        config.ServiceConfig
	scope                tally.Scope
	aresControllerClient controllerCom.ControllerClient
	sink                 sink.Sink
	sinkInitFunc         NewSink
	highLevelConsumer    consumer.Consumer
	consumerInitFunc     NewConsumer
	parser               *message.Parser
	decoder              message.Decoder
	batcher              *tools.Batcher
	msgSizes             chan int64
	shutdown             chan bool
	close                chan bool
	errors               chan ProcessorError
	failureHandler       FailureHandler
}

// NewStreamingProcessor returns Processor to consume, process and save data to db.
func NewStreamingProcessor(id int, jobConfig *rules.JobConfig, aresControllerClient controllerCom.ControllerClient, sinkInitFunc NewSink, consumerInitFunc NewConsumer, decoderInitFunc NewDecoder,
	errors chan ProcessorError, msgSizes chan int64, serviceConfig config.ServiceConfig) (Processor, error) {
	cluster := jobConfig.AresTableConfig.Cluster
	// Initialize downstream DB
	db, err := initSink(jobConfig, serviceConfig, aresControllerClient, sinkInitFunc)
	if err != nil {
		return nil, utils.StackError(err,
			fmt.Sprintf("Failed to initialize database connection for job: %s, cluster: %s",
				jobConfig.Name, cluster))
	}
	// initialize failure handler
	failureHandler := initFailureHandler(serviceConfig, jobConfig, db)

	// Initialize Kafka consumer
	hlConsumer, err := consumerInitFunc(jobConfig, serviceConfig)
	if err != nil {
		return nil, utils.StackError(err, fmt.Sprintf(
			"Unable to initialize Kafka consumer for job: %s, cluster: %s", jobConfig.Name, cluster))
	}

	// Initialize the decoder based on topic
	decoder, err := decoderInitFunc(jobConfig, serviceConfig)
	if err != nil {
		return nil, utils.StackError(err,
			fmt.Sprintf("Unable to initialize Kafka message decoder for job: %s, cluster: %s",
				jobConfig.Name, cluster))
	}

	// Initialize message parser
	parser := message.NewParser(jobConfig, serviceConfig)

	processor := &StreamingProcessor{
		ID:            id,
		jobConfig:     jobConfig,
		cluster:       cluster,
		serviceConfig: serviceConfig,
		scope: serviceConfig.Scope.Tagged(map[string]string{
			"job":         jobConfig.Name,
			"aresCluster": jobConfig.AresTableConfig.Cluster,
		}),
		aresControllerClient: aresControllerClient,
		sink:                 db,
		sinkInitFunc:         sinkInitFunc,
		failureHandler:       failureHandler,
		highLevelConsumer:    hlConsumer,
		consumerInitFunc:     consumerInitFunc,
		msgSizes:             msgSizes,
		parser:               parser,
		decoder:              decoder,
		shutdown:             make(chan bool),
		close:                make(chan bool),
		errors:               errors,
		context: &ProcessorContext{
			StartTime: time.Now(),
			Errors: processorErrors{
				errors: make([]ProcessorError, jobConfig.StreamingConfig.ErrorThreshold*10),
			},
		},
	}

	processor.initBatcher()

	return processor, nil
}

func initFailureHandler(serviceConfig config.ServiceConfig,
	jobConfig *rules.JobConfig, db sink.Sink) FailureHandler {
	if jobConfig.StreamingConfig.FailureHandler.Type == retryHandler {
		return NewRetryFailureHandler(
			jobConfig.StreamingConfig.FailureHandler.Config,
			serviceConfig, db, jobConfig.Name)
	}
	return nil
}

// initDatabase will initialize the database for writing ingest data
func initSink(
	jobConfig *rules.JobConfig, serviceConfig config.ServiceConfig, aresControllerClient controllerCom.ControllerClient, sinkInitFunc NewSink) (sink.Sink, error) {
	cluster := jobConfig.AresTableConfig.Cluster
	serviceConfig.Logger.Info("Initialize database",
		zap.String("job", jobConfig.Name),
		zap.String("cluster", cluster))

	var aresConfig config.SinkConfig
	var ok bool
	if aresConfig, ok = serviceConfig.ActiveAresClusters[cluster]; !ok {
		return nil, fmt.Errorf("Failed to get ares config for job: %s, cluster: %s",
			jobConfig.Name, cluster)
	}

	return sinkInitFunc(serviceConfig, jobConfig, cluster, aresConfig, aresControllerClient)
}

// GetID will return ID of this processor
func (s *StreamingProcessor) GetID() int {
	return s.ID
}

// GetContext will return context of this processor
func (s *StreamingProcessor) GetContext() *ProcessorContext {
	return s.context
}

// Stop will stop the processor
func (s *StreamingProcessor) Stop() {
	s.context.Lock()
	if s.context.Restarting {
		s.context.Restarting = false
	} else if !s.context.Shutdown {
		close(s.shutdown)
	}
	s.context.Unlock()
}

// Restart will stop the processor and start the process again in the case failure detected
func (s *StreamingProcessor) Restart() {
	s.context.Lock()
	if s.context.Restarting {
		s.serviceConfig.Logger.Info("Restarting: processor already in restarting",
			zap.String("job", s.jobConfig.Name),
			zap.String("cluster", s.cluster))
		s.context.Unlock()
		return
	}
	if s.context.Stopped || s.context.Shutdown {
		s.serviceConfig.Logger.Info("Restarting: processor already stopped",
			zap.String("job", s.jobConfig.Name),
			zap.String("cluster", s.cluster))
		s.context.Unlock()
		return
	}
	s.context.Restarting = true
	s.context.RestartCount++
	s.context.RestartTime = time.Now().UnixNano() / int64(time.Millisecond)
	s.context.Unlock()

	s.serviceConfig.Logger.Info(
		"Restarting processor (Stop original processor)",
		zap.Int("ID", s.ID),
		zap.String("job", s.jobConfig.Name),
		zap.String("cluster", s.cluster))
	// not calling Stop() due to different flag setting
	close(s.shutdown)
	// wating for the original routine to stop
	for {
		time.Sleep(time.Millisecond)
		if s.context.Stopped {
			break
		}
	}

	// wating for some time to avoid keep restarting in short of period, or quit if stop is called during restart
	timeTick := time.NewTicker(time.Second)
	timer := time.NewTimer(time.Second * time.Duration(s.jobConfig.StreamingConfig.RestartInterval))
	defer func() {
		s.context.Lock()
		s.context.Restarting = false
		timeTick.Stop()
		timer.Stop()
		s.context.Unlock()
	}()

loop:
	for {
		select {
		case <-timeTick.C:
			if !s.context.Restarting {
				s.serviceConfig.Logger.Info("Restarting interrupted, give up",
					zap.String("job", s.jobConfig.Name),
					zap.String("cluster", s.cluster))
				return
			}
		case <-timer.C:
			break loop
		}
	}

	err := s.reInitialize()
	if err == nil {
		s.serviceConfig.Logger.Info(
			"Restarting processor(Re-initialize)",
			zap.Int("ID", s.ID),
			zap.String("job", s.jobConfig.Name),
			zap.String("cluster", s.cluster))
		// restart
		go s.Run()
	} else {
		// quit restarting if failed to re-initialized database or consumer
		s.serviceConfig.Logger.Error(
			"Failed to restart processor",
			zap.Int("ID", s.ID),
			zap.String("job", s.jobConfig.Name),
			zap.String("cluster", s.cluster),
			zap.Error(err))
	}
}

func (s *StreamingProcessor) reInitialize() error {
	// maybe we can try to re-initialize if anything failed
	s.serviceConfig.Logger.Info("Restarting processor(Re-initialize)",
		zap.Int("ID", s.ID),
		zap.String("job", s.jobConfig.Name),
		zap.String("cluster", s.cluster))
	db, err := initSink(s.jobConfig, s.serviceConfig, s.aresControllerClient, s.sinkInitFunc)
	if err != nil {
		err = utils.StackError(err, "Unable to initialize Database")
		return err
	}

	// Initialize Kafka consumer
	hlConsumer, err := s.consumerInitFunc(s.jobConfig, s.serviceConfig)
	if err != nil {
		err = utils.StackError(err, "Unable to initialize Kafka consumer")
		db.Shutdown()
		return err
	}

	s.sink = db
	s.highLevelConsumer = hlConsumer
	s.failureHandler = initFailureHandler(s.serviceConfig, s.jobConfig, s.sink)
	s.initBatcher()
	s.shutdown = make(chan bool)

	return nil
}

// Run will start the Processor that reads from high level kafka consumer,
// decodes the message and add the row to batcher for saving to ares.
func (s *StreamingProcessor) Run() {
	s.serviceConfig.Logger.Info("Starting Job",
		zap.String("job", s.jobConfig.Name),
		zap.String("cluster", s.cluster),
		zap.Any("config", s.jobConfig))
	// reset back the running flag
	s.context.Lock()
	s.context.Shutdown = false
	s.context.Stopped = false
	s.context.Restarting = false
	s.context.Unlock()

	defer func() {
		s.highLevelConsumer.Close()
		s.batcher.Close()
		s.sink.Shutdown()
		s.context.Lock()
		s.context.Stopped = true
		s.context.Unlock()
	}()

	for {
		select {
		case msg := <-s.highLevelConsumer.Messages():
			msgInSubTS := time.Now()
			if msg != nil {
				// Update message count in context
				s.context.Lock()
				s.context.TotalMessages++
				s.context.Unlock()
				s.scope.Counter("message.totalCount").Inc(1)

				// log message size and report for throttling
				msgLength := int64(len(msg.Value()))
				s.scope.Gauge("message.size").Update(float64(msgLength))
				s.msgSizes <- msgLength

				// decode message and add to batcher for parse and save
				message, err := s.decodeMessage(msg)
				message.MsgInSubTS = msgInSubTS
				if err == nil {
					s.batcher.Add(message, time.Now())
					s.reportMessageAge(message)
				}
			} else {
				s.scope.Counter("errors.kafka.nilMessages").Inc(1)
			}
		case err := <-s.highLevelConsumer.Errors():
			if err != nil {
				s.serviceConfig.Logger.Error("Error reading from consumer",
					zap.String("job", s.jobConfig.Name),
					zap.String("cluster", s.cluster),
					zap.String("name", message.GetFuncName()),
					zap.Error(err))
				s.scope.Counter("errors.kafka.consumer").Inc(1)
			} else {
				s.scope.Counter("errors.kafka.nilErrors").Inc(1)
			}
		case <-s.highLevelConsumer.Closed():
			s.serviceConfig.Logger.Info("Consumer closed. Shutting down processor",
				zap.String("job", s.jobConfig.Name),
				zap.String("cluster", s.cluster))
			return
		case <-s.shutdown:
			s.serviceConfig.Logger.Info("Processor shutdown requested. Shutting down processor",
				zap.String("job", s.jobConfig.Name),
				zap.String("cluster", s.cluster))
			s.context.Lock()
			s.context.Shutdown = true
			s.context.Unlock()
			return
		}
	}
}

// initBatcher will initialize the batcher with 1 worker per processor, so to
// maintain the order of offset commits
func (s *StreamingProcessor) initBatcher() {
	s.serviceConfig.Logger.Info("Initialize batcher",
		zap.String("job", s.jobConfig.Name),
		zap.String("cluster", s.cluster))
	batcher := tools.NewBatcher(s.jobConfig.StreamingConfig.BatchSize,
		time.Duration(s.jobConfig.StreamingConfig.MaxBatchDelayMS)*time.Millisecond,
		time.Now)
	batcher.StartWorker(s.saveToDB)
	s.batcher = batcher
}

// decodeMessage will decode the given Kafka message and return Message, which defines
// actual raw Kafka message, decoded message and timestamp of the message
func (s *StreamingProcessor) decodeMessage(msg consumer.Message) (*message.Message, error) {
	message, err := s.decoder.DecodeMsg(msg)
	if err != nil {
		s.serviceConfig.Logger.Error("Unable to decode message",
			zap.String("job", s.jobConfig.Name),
			zap.String("cluster", s.cluster),
			zap.Error(err))
		s.scope.Counter("errors.messageDecode").Inc(1)
		return nil, err
	}
	return message, nil
}

// saveToDestination will parse given decoded message based on transformations in JobConfig
// and save it to configured destination
func (s *StreamingProcessor) saveToDestination(batch []interface{}, destination sink.Destination) {
	s.scope.Gauge("batcherBatchSize").Update(float64(len(batch)))

	rows := []client.Row{}
	for _, b := range batch {
		msg := b.(*message.Message).DecodedMessage[message.MsgPrefix].(map[string]interface{})
		if s.parser.IsMessageValid(msg, destination) != nil {
			s.serviceConfig.Logger.Debug("Invalid message", zap.Any("msg", msg))
			continue
		}
		row, err := s.parser.ParseMessage(msg, destination)
		if err == nil &&
			s.parser.CheckPrimaryKeys(destination, row) == nil &&
			s.parser.CheckTimeColumnExistence(
				s.jobConfig.AresTableConfig.Table, s.jobConfig.GetColumnDict(), destination, row) == nil {
			rows = append(rows, row)
		} else {
			s.context.Lock()
			s.context.FailedMessages++
			s.context.LastUpdated = time.Now()
			s.context.Unlock()
		}
	}

	size := len(batch)
	if size > 0 {
		s.scope.Timer("lag.ingestion").Record(time.Now().Sub(batch[size-1].(*message.Message).MsgInSubTS))
		s.writeRow(rows, destination)
	}

}

func (s *StreamingProcessor) writeRow(rows []client.Row, destination sink.Destination) {
	err := s.sink.Save(destination, rows)
	if err != nil {
		s.serviceConfig.Logger.Error(
			"Unable to save rows to database",
			zap.String("job", s.jobConfig.Name),
			zap.String("cluster", s.cluster),
			zap.String("name", message.GetFuncName()),
			zap.Error(err))
		if s.failureHandler != nil {
			err = s.failureHandler.HandleFailure(destination, rows)
		}
		if err != nil {
			pe := ProcessorError{
				ID:        s.ID,
				Timestamp: time.Now().UnixNano() / int64(time.Millisecond),
				Error:     err,
			}
			s.context.Lock()
			s.context.Errors.errorIdx = (s.context.Errors.errorIdx + 1) % len(s.context.Errors.errors)
			s.context.Errors.errors[s.context.Errors.errorIdx] = pe
			s.context.FailedMessages += int64(len(rows))
			s.context.Unlock()
			s.errors <- pe
		}
	}
	s.context.Lock()
	s.context.LastUpdated = time.Now()
	s.context.Unlock()
}

// saveToDB will parse and save given batches
func (s *StreamingProcessor) saveToDB(batches chan []interface{}, wg *sync.WaitGroup) {
	for batch := range batches {
		s.saveToDestination(batch, s.parser.Destination)
		for _, batchObj := range batch {
			msg := batchObj.(*message.Message)
			err := s.highLevelConsumer.CommitUpTo(msg.RawMessage)
			if err == nil {
				s.scope.Counter("message.commited").Inc(1)
			}
		}
	}
	wg.Done()
}

// reportMessageAge will report the message age for the message
func (s *StreamingProcessor) reportMessageAge(msg *message.Message) {
	if !msg.MsgMetaDataTS.IsZero() {
		s.scope.Tagged(map[string]string{
			"partition": strconv.Itoa(int(msg.RawMessage.Partition())),
		}).Timer("message.age").Record(time.Now().Sub(msg.MsgMetaDataTS))
	}
}
