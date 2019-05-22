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
	"bytes"
	"encoding/json"
	"fmt"
	controllerCom "github.com/uber/aresdb/controller/client"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/uber-go/tally"
	"github.com/uber/aresdb/subscriber/common/rules"
	"github.com/uber/aresdb/subscriber/config"
	"go.uber.org/zap"
)

// Drivers contains information about job, ares cluster and its driver
type Drivers map[string]map[string]*Driver

// Driver will initialize and start the Processor's based on the JobConfig provided
type Driver struct {
	sync.RWMutex

	Topic                string
	StartTime            time.Time
	Shutdown             bool
	JobName              string
	AresCluster          string
	TotalProcessors      int
	RunningProcessors    int
	StoppedProcessors    int
	FailedProcessors     int
	RestartingProcessors int
	ProcessorContext     map[string]*ProcessorContext

	// driver related variable
	waitGroup           *sync.WaitGroup
	shutdown            chan bool
	jobConfig           *rules.JobConfig
	serviceConfig       config.ServiceConfig
	scope               tally.Scope
	errors              chan ProcessorError
	errorThreshold      int
	statusCheckInterval int

	// processors related variables
	processorInitFunc    NewProcessor
	processorCounter     uint
	processors           []Processor
	processorMsgCount    map[int]int64
	processorMsgSizes    chan int64
	aresControllerClient controllerCom.ControllerClient
	sinkInitFunc         NewSink
	consumerInitFunc     NewConsumer
	decoderInitFunc      NewDecoder
}

// NewProcessor is the type of function each processor that implements Processor should provide for initialization
// This function implementation should always return a new instance of the processor
type NewProcessor func(id int, jobConfig *rules.JobConfig, aresControllerClient controllerCom.ControllerClient, sinkInitFunc NewSink, consumerInitFunc NewConsumer, decoderInitFunc NewDecoder,
	errors chan ProcessorError, msgSizes chan int64, serviceConfig config.ServiceConfig) (Processor, error)

// NewDrivers return Drivers
func NewDrivers(params Params, aresControllerClient controllerCom.ControllerClient) (Drivers, error) {
	drivers := make(Drivers)

	// iterate local job configs
	for jobName, jobAresConfig := range params.JobConfigs {
		// iterate all active ares clusters
		aresClusterDriverTable := make(map[string]*Driver)
		for aresCluster, jobConfig := range jobAresConfig {
			driver, err := NewDriver(jobConfig, params.ServiceConfig, aresControllerClient, NewStreamingProcessor, params.SinkInitFunc,
				params.ConsumerInitFunc, params.DecoderInitFunc)
			if err != nil {
				params.ServiceConfig.Logger.Error("Failed to create driver",
					zap.String("job", jobName),
					zap.String("cluster", jobConfig.AresTableConfig.Cluster),
					zap.Error(err))
				return nil,
					fmt.Errorf("Failed to create Drivers, reason: job %s cluster %s failed to create driver",
						jobName, aresCluster)
			}
			go driver.Start()
			aresClusterDriverTable[aresCluster] = driver
		}
		drivers[jobName] = aresClusterDriverTable
	}

	return drivers, nil
}

// NewDriver will return a new Driver instance to start a ingest job
func NewDriver(
	jobConfig *rules.JobConfig, serviceConfig config.ServiceConfig, aresControllerClient controllerCom.ControllerClient, processorInitFunc NewProcessor, sinkInitFunc NewSink,
	consumerInitFunc NewConsumer, decoderInitFunc NewDecoder) (*Driver, error) {
	return &Driver{
		Topic:                jobConfig.StreamingConfig.Topic,
		JobName:              jobConfig.Name,
		AresCluster:          jobConfig.AresTableConfig.Cluster,
		StartTime:            time.Now(),
		Shutdown:             false,
		TotalProcessors:      jobConfig.StreamingConfig.ProcessorCount,
		StoppedProcessors:    0,
		RestartingProcessors: 0,
		ProcessorContext:     make(map[string]*ProcessorContext),
		waitGroup:            &sync.WaitGroup{},
		shutdown:             make(chan bool),
		jobConfig:            jobConfig,
		serviceConfig:        serviceConfig,
		scope: serviceConfig.Scope.Tagged(map[string]string{
			"job":         jobConfig.Name,
			"aresCluster": jobConfig.AresTableConfig.Cluster,
		}),
		errors:               make(chan ProcessorError),
		errorThreshold:       jobConfig.StreamingConfig.ErrorThreshold,
		statusCheckInterval:  jobConfig.StreamingConfig.StatusCheckInterval,
		processorInitFunc:    processorInitFunc,
		processorCounter:     uint(0),
		processors:           []Processor{},
		processorMsgCount:    make(map[int]int64),
		processorMsgSizes:    make(chan int64),
		aresControllerClient: aresControllerClient,
		sinkInitFunc:         sinkInitFunc,
		consumerInitFunc:     consumerInitFunc,
		decoderInitFunc:      decoderInitFunc,
	}, nil
}

// Start will start the Driver, which starts Processor's to read from Kafka consumer, process
// and save to database
func (d *Driver) Start() {
	go d.monitorStatus(time.NewTicker(time.Duration(d.statusCheckInterval) * time.Second))
	go d.monitorErrors()
	go d.limitRate()
	d.addProcessors(d.jobConfig.StreamingConfig.ProcessorCount)
}

func (d *Driver) addProcessors(count int) {
	for i := 0; i < count; i++ {
		err := d.AddProcessor()
		if err != nil {
			d.serviceConfig.Scope.Counter("errors.driver.addprocessor").Inc(1)
		}
		// adding too many consumers at once can make some consumers stuck with no messages
		time.Sleep(500 * time.Millisecond)
	}
}

// AddProcessor will add a new processor to driver
func (d *Driver) AddProcessor() (err error) {
	d.Lock()
	defer func() {
		if err != nil {
			d.processorCounter--
		}
		d.Unlock()
	}()
	d.serviceConfig.Logger.Info("Adding a new processor",
		zap.String("job", d.JobName),
		zap.String("cluster", d.AresCluster))

	// get a new ID for processor, we start at 1
	d.processorCounter++
	ID := int(d.processorCounter)

	processor, err := d.processorInitFunc(ID, d.jobConfig, d.aresControllerClient, d.sinkInitFunc, d.consumerInitFunc, d.decoderInitFunc,
		d.errors, d.processorMsgSizes, d.serviceConfig)
	if err != nil {
		d.serviceConfig.Logger.Error("Failed to initialize Processor",
			zap.String("job", d.JobName),
			zap.String("cluster", d.AresCluster),
			zap.Error(err))
		return
	}

	// add processor
	d.processors = append(d.processors, processor)
	d.ProcessorContext[strconv.Itoa(ID)] = processor.GetContext()

	// start processor
	go processor.Run()

	// Update context
	d.TotalProcessors++
	d.RunningProcessors++
	return
}

// RemoveProcessor will remove a processor from driver.
func (d *Driver) RemoveProcessor(ID int) bool {
	d.Lock()
	defer d.Unlock()

	return d.removeProcessor(ID)
}

func (d *Driver) removeProcessor(ID int) bool {
	// if no processors running, nothing to remove
	if d.RunningProcessors <= 0 {
		return false
	}

	if ID <= 0 {
		// pick last running processor
		ID = len(d.processors) - 1
		for d.processors[ID] == nil || d.processors[ID].GetContext().Stopped {
			ID--
		}
	} else {
		ID = ID - 1
	}
	d.serviceConfig.Logger.Info("Removing a processor", zap.Int("ID", ID+1),
		zap.String("job", d.JobName),
		zap.String("cluster", d.AresCluster))
	// stop the processor
	if !d.processors[ID].GetContext().Stopped {
		d.processors[ID].Stop()
	} else {
		d.serviceConfig.Logger.Warn("Processor already stopped", zap.Int("ID", ID),
			zap.String("job", d.JobName),
			zap.String("cluster", d.AresCluster))
		return false
	}

	// update context
	pc := d.ProcessorContext[strconv.Itoa(ID+1)]
	pc.Lock()
	pc.Stopped = true
	pc.Unlock()
	d.StoppedProcessors++
	d.TotalProcessors--
	d.RunningProcessors--

	// remove the processor
	d.processors[ID] = nil

	// remove the processor id from message count map
	delete(d.processorMsgCount, ID+1)

	return true
}

// restartProcessor will restart a processor from driver.
func (d *Driver) restartProcessor(ID int) {
	d.Lock()
	defer d.Unlock()
	if d.processors[ID-1] != nil {
		go d.processors[ID-1].Restart()
	}
}

// MarshalJSON marshal driver into json
func (d *Driver) MarshalJSON() ([]byte, error) {
	d.RLock()
	defer d.RUnlock()
	buf := new(bytes.Buffer)
	startTimeJSON, err := d.StartTime.MarshalJSON()
	if err != nil {
		return nil, err
	}
	processorContextJSON, err := json.Marshal(d.ProcessorContext)
	if err != nil {
		return nil, err
	}
	buf.WriteString(fmt.Sprintf(`{"environment":"%s","topic":"%s","startTime":%s,`+
		`"jobName":"%s","totalProcessors":%d,"runningProcessors":%d,`+
		`"stoppedProcessors":%d,"failedProcessors":%d,"restartingProcessors":%d,"processorContext":%s}`,
		d.serviceConfig.Environment.Deployment,
		d.Topic,
		string(startTimeJSON),
		d.JobName,
		d.TotalProcessors,
		d.RunningProcessors,
		d.StoppedProcessors,
		d.FailedProcessors,
		d.RestartingProcessors,
		string(processorContextJSON),
	))
	return buf.Bytes(), nil
}

// WriteContext writes context
func (d *Driver) WriteContext(w http.ResponseWriter) {
	ctxStr, err := d.MarshalJSON()
	if err != nil {
		d.serviceConfig.Logger.Error("Unable to marshal context", zap.Error(err))
	}
	w.Write(ctxStr)
}

// limitRate will rate limit the amount of data read from Kafka, so to
// not saturate the network bandwidth on mesos compute hosts
func (d *Driver) limitRate() {
	d.waitGroup.Add(1)
	defer d.waitGroup.Done()

	// only one goroutine read/write this value
	var cumulativeSizeInBytes int64
	var mask int64 = 1<<20 - 1

	maxTokens := d.jobConfig.StreamingConfig.MegaBytePerSec
	tokens := make(chan bool, maxTokens)
	ticks := time.NewTicker(time.Second)

	d.waitGroup.Add(1)
	go func() {
		defer d.waitGroup.Done()
		defer ticks.Stop()

		// drain tokens every second to allow more
		for {
			select {
			case <-ticks.C:
				numExistingTokens := len(tokens)
				for i := 0; i < numExistingTokens; i++ {
					<-tokens
				}
			case <-d.shutdown:
				return
			}
		}
	}()

	for {
		select {
		case msgSize := <-d.processorMsgSizes:
			cumulativeSizeInBytes += msgSize
			tokensToIssue := int(cumulativeSizeInBytes >> 20)
			cumulativeSizeInBytes = cumulativeSizeInBytes & mask
			// try produce tokens
			for i := 0; i < tokensToIssue; i++ {
				tokens <- true
			}
		case <-d.shutdown:
			d.serviceConfig.Logger.Info("Shutdown driver limitRate:",
				zap.String("job", d.JobName),
				zap.String("cluster", d.AresCluster))
			return
		}
	}
}

// monitorStatus will monitor the status of all Processor's and report them to graphite
func (d *Driver) monitorStatus(ticker *time.Ticker) {
	d.waitGroup.Add(1)
	defer d.waitGroup.Done()
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			failedProcessors := 0
			stoppedProcessors := 0
			restartingProcessors := 0
			// first add processors in case processor failed to be initialized at the begining
			d.addProcessors(d.jobConfig.StreamingConfig.ProcessorCount - len(d.ProcessorContext))
			d.Lock()
			for i, p := range d.ProcessorContext {
				p.RLock()
				// if shutdown not intentional, but processor is dead, trouble!
				if !p.Shutdown && p.Stopped {
					failedProcessors++
				}
				if p.Shutdown && p.Stopped {
					stoppedProcessors++
				}
				if p.Restarting {
					restartingProcessors++
				}

				// Check if messages are processed by all processors
				id, _ := strconv.Atoi(i)
				// Update new message count for comparing with next check
				d.processorMsgCount[id] = p.TotalMessages
				p.RUnlock()
			}
			d.scope.Gauge("processors.failed").Update(float64(failedProcessors))
			total := len(d.ProcessorContext) - stoppedProcessors + restartingProcessors
			healthy := total - failedProcessors
			d.RunningProcessors = healthy - restartingProcessors
			d.TotalProcessors = total
			d.RestartingProcessors = restartingProcessors
			d.serviceConfig.Logger.Debug("Processors:",
				zap.Int("Configured", d.jobConfig.StreamingConfig.ProcessorCount),
				zap.String("job", d.JobName),
				zap.String("cluster", d.AresCluster),
				zap.Int("Total", total),
				zap.Int("Running", healthy))
			if diff := healthy - d.jobConfig.StreamingConfig.ProcessorCount; diff < 0 {
				d.scope.Gauge("numProcessors").Update(float64(-diff))
			} else {
				d.scope.Gauge("numProcessors").Update(0)
			}
			d.Unlock()
		case <-d.shutdown:
			d.serviceConfig.Logger.Info("Shutdown driver monitorStatus:",
				zap.String("job", d.JobName),
				zap.String("cluster", d.AresCluster))
			return
		}
	}
}

// monitorErrors will monitor errors generated by each Processor and kill them if exceeds
// configured threshold number of errors per processor
func (d *Driver) monitorErrors() {
	d.waitGroup.Add(1)
	defer d.waitGroup.Done()

	processorErrorCount := make(map[int]int)
	for {
		select {
		case err := <-d.errors:
			if d.processors[err.ID-1] != nil {
				if err.Timestamp > d.processors[err.ID-1].GetContext().RestartTime {
					// skip errors which are before restart
					if val, ok := processorErrorCount[err.ID]; !ok {
						processorErrorCount[err.ID] = 1
					} else {
						processorErrorCount[err.ID] = val + 1
					}
				}
			} else {
				d.serviceConfig.Logger.Warn("No processor exist for",
					zap.Int("ID", err.ID),
					zap.String("job", d.JobName),
					zap.String("cluster", d.AresCluster))
			}

			if processorErrorCount[err.ID] == d.errorThreshold {
				d.serviceConfig.Logger.Info("Restarting processor for reaching error threshold",
					zap.Int("ID", err.ID),
					zap.String("job", d.JobName),
					zap.String("cluster", d.AresCluster))
				if d.jobConfig.StreamingConfig.RestartOnFailure {
					// reset processor error count
					processorErrorCount[err.ID] = 0
					d.restartProcessor(err.ID)
					time.Sleep(time.Millisecond * 10)
				} else {
					d.RemoveProcessor(err.ID)
				}
			}
		case <-d.shutdown:
			d.serviceConfig.Logger.Info("Shutdown driver monitorErrors:",
				zap.String("job", d.JobName),
				zap.String("cluster", d.AresCluster))
			return
		}
	}
}

// GetErrors returns errors
func (d *Driver) GetErrors() chan ProcessorError {
	return d.errors
}

// Stop will shutdown driver and its processors
func (d *Driver) Stop() {
	defer func() {
		d.Shutdown = true
		d.Unlock()
	}()

	d.Lock()
	// Shutdown all processors
	for _, processor := range d.processors {
		if processor == nil {
			continue
		}
		d.removeProcessor(processor.GetID())
	}

	if !d.Shutdown {
		close(d.shutdown)
	}
	d.waitGroup.Wait()
}
