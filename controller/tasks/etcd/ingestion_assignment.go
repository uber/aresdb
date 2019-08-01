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
package etcd

import (
	"math/rand"
	"os"
	"reflect"
	"time"

	"github.com/uber/aresdb/controller/models"
	mutators "github.com/uber/aresdb/controller/mutators/common"
	"github.com/uber/aresdb/controller/tasks/common"

	"github.com/m3db/m3/src/cluster/services"
	"github.com/uber-go/tally"
	metaCom "github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/utils"
	"github.com/uber/aresdb/utils/consistenthasing"
	"go.uber.org/zap"
)

const (
	ingestionAssignmentConfigKey = "ingestionAssignmentTask"
	taskTagValue                 = "ingestionAssignmentTask"
	assignmentChangeMetricName   = "ingestion_assignment_changed"
	assignmentErrorMetricName    = "ingestion_assignment_error"
	assignmentSuccessMetricName  = "ingestion_assignment_success"
)

type ingestionAssignmentTaskConfig struct {
	IntervalInSeconds int `yaml:"intervalInSeconds"`
}

// ingestionAssignmentTask calculates ingestion jobs assignment to subscriber instances
// given current state of the cluster
type ingestionAssignmentTask struct {
	zone        string
	environment string

	intervalSeconds int
	logger          *zap.SugaredLogger
	scope           tally.Scope
	// closing stopChan will:
	// stop ongoing leader election, which will clean up leader election states
	// stop ongoing task assignment calculation loop
	stopChan chan struct{}

	namespaceMutator   mutators.NamespaceMutator
	jobMutator         mutators.JobMutator
	schemaMutator      mutators.TableSchemaMutator
	subscriberMutator  mutators.SubscriberMutator
	assignmentsMutator mutators.IngestionAssignmentMutator

	etcdServices   services.Services
	leaderElection LeaderElector
	configHashes   map[string]configHash
}

type jobSubscriberState struct {
	namespace   string
	subscribers []models.Subscriber
	jobs        []models.JobConfig
}

type configHash struct {
	jobsHash       string
	schemaHash     string
	subscriberHash string
}

// NewIngestionAssignmentTask creates a new instance of ingestionAssignmentTask
func NewIngestionAssignmentTask(p common.IngestionAssignmentTaskParams) common.Task {
	var iaconfig ingestionAssignmentTaskConfig
	if err := p.ConfigProvider.Get(ingestionAssignmentConfigKey).Populate(&iaconfig); err != nil {
		p.Logger.Fatal("failed to load config for ingestionAssignmentTask")
	}

	serviceID := services.NewServiceID().
		SetEnvironment(p.EtcdClient.Environment).
		SetZone(p.EtcdClient.Zone).
		SetName(p.EtcdClient.ServiceName)
	leaderService, err := p.EtcdClient.Services.LeaderService(serviceID, nil)
	if err != nil {
		p.Logger.Fatal("failed to create leader service for ingestionAssignmentTask")
	}

	task := &ingestionAssignmentTask{
		intervalSeconds: iaconfig.IntervalInSeconds,
		logger:          p.Logger,
		scope:           p.Scope,
		stopChan:        make(chan struct{}, 1),

		zone:        p.EtcdClient.Zone,
		environment: p.EtcdClient.Environment,

		etcdServices:       p.EtcdClient.Services,
		leaderElection:     NewLeaderElector(leaderService),
		namespaceMutator:   p.NamespaceMutator,
		jobMutator:         p.JobMutator,
		schemaMutator:      p.SchemaMutator,
		assignmentsMutator: p.AssignmentsMutator,
		subscriberMutator:  p.SubscriberMutator,
		configHashes:       make(map[string]configHash),
	}
	return task
}

// Run starts the ingestionAssignmentTask
func (ia *ingestionAssignmentTask) Run() {
	hostName, _ := os.Hostname()

	// wait random interval to avoid herd effect electing for leader on cluster reboot
	waitSeconds := rand.Intn(5)
	time.Sleep(time.Duration(waitSeconds) * time.Second)

	ia.logger.With(
		"host", hostName,
		"waitedSeconds", waitSeconds,
	).Info("start ingestion assignment task after waiting")

	if err := ia.leaderElection.Start(); err != nil {
		ia.logger.With("host", hostName, "error", err.Error()).Error("failed to start leader election")
		ia.scope.Tagged(
			map[string]string{
				"task": taskTagValue,
			}).Counter("task_failed").Inc(1)
		return
	}

	defer func() {
		err := ia.leaderElection.Close()
		if err != nil {
			ia.logger.Error(err)
		} else {
			ia.logger.With("host", hostName).Infof("stopped leader election")
		}
	}()

	ia.logger.With("host", hostName).Infof("entering ingestion assignment calculation loop")
	ia.startIngestionAssignment(hostName)
	ia.logger.With("host", hostName).Infof("exited ingestion assignment calculation loop")
}

// Done stops the task
func (ia *ingestionAssignmentTask) Done() {
	ia.logger.Info("killing ingestion assignment task")
	close(ia.stopChan)
}

func (ia *ingestionAssignmentTask) startIngestionAssignment(hostName string) {
	for {
		select {
		// waiting for new election status change
		case <-ia.leaderElection.C():
			if !ia.isLeader() {
				continue
			}
		case <-ia.stopChan:
			return
		}

		ia.logger.With("host", hostName).Infof("elected as leader")
		tickerChan := time.NewTicker(time.Duration(ia.intervalSeconds) * time.Second).C
	loop:
		for {
			select {
			case <-ia.leaderElection.C():
				if !ia.isLeader() {
					ia.logger.With("host", hostName).Infof("host is no longer the leader")
					break loop
				}
			case <-tickerChan:
				if !ia.isLeader() {
					ia.logger.With("host", hostName).Infof("host is no longer the leader")
					break loop
				}
				ia.tryRecalculateAllNamespaces()
			case <-ia.stopChan:
				return
			}
		}
	}
}

func (ia *ingestionAssignmentTask) isLeader() bool {
	return ia.leaderElection.Status() == Leader
}

func (ia *ingestionAssignmentTask) checkConfigHashes(namespace string) (hashes configHash, err error) {
	hashes.jobsHash, err = ia.jobMutator.GetHash(namespace)
	if err != nil {
		return
	}
	hashes.schemaHash, err = ia.schemaMutator.GetHash(namespace)
	if err != nil {
		return
	}
	hashes.subscriberHash, err = ia.subscriberMutator.GetHash(namespace)
	if err != nil {
		return
	}
	return
}

func (ia *ingestionAssignmentTask) readCurrentState(namespace string) (jobSubscriberState, error) {
	state := jobSubscriberState{
		namespace: namespace,
	}
	subscriberServiceID := services.NewServiceID().
		SetName(utils.SubscriberServiceName(namespace)).
		SetZone(ia.zone).
		SetEnvironment(ia.environment)
	hbSvc, err := ia.etcdServices.HeartbeatService(subscriberServiceID)
	if err != nil {
		return state, err
	}
	subscriberInstances, err := hbSvc.Get()
	if err != nil {
		return state, err
	}

	for _, subscriberInstance := range subscriberInstances {
		state.subscribers = append(state.subscribers, models.Subscriber{
			Name: subscriberInstance,
		})
	}

	datanodeServiceID := services.NewServiceID().
		SetName(utils.DataNodeServiceName(namespace)).
		SetZone(ia.zone).
		SetEnvironment(ia.environment)
	placementService, err := ia.etcdServices.PlacementService(datanodeServiceID, nil)
	if err != nil {
		return state, err
	}
	placement, err := placementService.Placement()
	if err != nil {
		return state, err
	}
	numShards := placement.NumShards()

	state.jobs, err = ia.jobMutator.GetJobs(namespace)
	if err != nil {
		return state, err
	}

	for i, job := range state.jobs {
		var table *metaCom.Table
		table, err = ia.schemaMutator.GetTable(namespace, job.AresTableConfig.Name)
		if err != nil {
			return state, err
		}
		job.AresTableConfig.Table = table
		job.NumShards = numShards
		state.jobs[i] = job
	}

	if len(state.subscribers) == 0 {
		return state, common.ErrNotEnoughSubscribers
	}
	return state, nil
}

func (ia *ingestionAssignmentTask) recalculateForNamespace(ns string) {
	ia.logger.With(
		"namespace", ns,
	).Info("recalculating assignment for namespace")
	state, err := ia.readCurrentState(ns)
	if err != nil {
		ia.logger.Error(err)
		ia.scope.Counter(assignmentErrorMetricName).Inc(1)
		return
	}
	changes, errs := ia.processIngestionAssignment(state)
	if errs > 0 {
		ia.scope.Counter(assignmentErrorMetricName).Inc(int64(errs))
	} else {
		ia.scope.Counter(assignmentChangeMetricName).Inc(int64(changes))
	}
}

func (ia *ingestionAssignmentTask) tryRecalculateAllNamespaces() (errs int) {
	ia.logger.Info("try recalculating ingestion assignments")
	namespaces, err := ia.namespaceMutator.ListNamespaces()
	if err != nil {
		ia.reportError(err, &errs)
		return
	}
	for _, ns := range namespaces {
		hashes, err := ia.checkConfigHashes(ns)
		if err != nil {
			ia.reportError(err, &errs)
			return
		}
		if existingHashes, exist := ia.configHashes[ns]; !exist || hashes != existingHashes {
			ia.recalculateForNamespace(ns)
			ia.configHashes[ns] = hashes
		}
		ia.scope.Counter(assignmentSuccessMetricName).Inc(1)
	}
	ia.logger.Info("ingestion assignments calculation finished")
	return
}

func (ia *ingestionAssignmentTask) processIngestionAssignment(state jobSubscriberState) (changes, errs int) {
	// build consistent hash ring where a ring node is a subscriber
	// and resource key is the kafka topic name.
	// this will guarantee minimum change for a topic's ingestion assignment
	// when subscribers join/leave the cluster
	ring := consistenthasing.NewRing()
	for _, subscriber := range state.subscribers {
		err := ring.AddNode(subscriber.Name)
		if err != nil {
			ia.reportError(err, &errs)
			return
		}
	}

	jobAssignments := map[string][]models.JobConfig{}

	// TODO: take subscriber instance capacity into consideration when assigning jobs
	for _, job := range state.jobs {
		processorsNeeded := job.StreamingConfig.ProcessorCount
		if processorsNeeded <= 0 {
			continue
		}
		processorsPerSubscriber := processorsNeeded / len(state.subscribers)
		if processorsPerSubscriber < 1 {
			processorsPerSubscriber = 1
		}

		// calcualte starting node of task assignment base on kafka topic name
		startingIndex, _ := ring.Get(job.StreamingConfig.Topic)
		for i := 0; processorsNeeded > 0; i++ {
			processorsToAssign := processorsPerSubscriber
			if processorsNeeded < processorsToAssign {
				processorsToAssign = processorsNeeded
			}
			j := job
			j.StreamingConfig.ProcessorCount = processorsToAssign
			index := (startingIndex + i) % len(state.subscribers)
			subscriberName := ring.Nodes[index].ID
			jobAssignments[subscriberName] = append(jobAssignments[subscriberName], j)
			processorsNeeded -= processorsToAssign
		}
	}

	existingAssignments, err := ia.assignmentsMutator.GetIngestionAssignments(state.namespace)
	if err != nil {
		ia.reportError(err, &errs)
		return
	}

	existingAssignmentsMap := map[string]*models.IngestionAssignment{}
	for _, assignment := range existingAssignments {
		subscriber := assignment.Subscriber
		existingAssignmentsMap[subscriber] = &assignment
	}

	for _, subscriberObj := range state.subscribers {
		subscriber := subscriberObj.Name
		jobAssignment, newExists := jobAssignments[subscriber]
		existingAssignment, oldExist := existingAssignmentsMap[subscriber]
		if newExists {
			if oldExist {
				if !reflect.DeepEqual(existingAssignment.Jobs, jobAssignment) {
					// update existing assignment
					err = ia.assignmentsMutator.UpdateIngestionAssignment(state.namespace, models.IngestionAssignment{
						Subscriber: subscriber,
						Jobs:       jobAssignment,
					})
					if err != nil {
						ia.reportError(err, &errs)
					} else {
						changes++
					}
				}
				// mark not deleted
				existingAssignmentsMap[subscriber] = nil
			} else {
				// new assignment
				err = ia.assignmentsMutator.AddIngestionAssignment(state.namespace, models.IngestionAssignment{
					Subscriber: subscriber,
					Jobs:       jobAssignment,
				})
				if err != nil {
					ia.reportError(err, &errs)
				} else {
					changes++
				}
			}
		} else if !oldExist {
			// add dummy assignment
			err = ia.assignmentsMutator.AddIngestionAssignment(state.namespace, models.IngestionAssignment{
				Subscriber: subscriber,
				Jobs:       []models.JobConfig{},
			})
		}
	}

	for k, v := range existingAssignmentsMap {
		if v != nil {
			ia.logger.With(
				"assginment", k,
			).Info("deleting assignment")
			err = ia.assignmentsMutator.DeleteIngestionAssignment(state.namespace, k)
			if err != nil {
				ia.reportError(err, &errs)
			}
			changes++
		}
	}
	return
}

func (ia *ingestionAssignmentTask) reportError(err error, errCount *int) {
	ia.logger.Error(err)
	*errCount++
}
