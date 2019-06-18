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
package common

import (
	"github.com/uber/aresdb/controller/models"
	"github.com/uber/aresdb/metastore/common"
)

// TableSchemaMutator mutates table metadata
type TableSchemaMutator interface {
	ListTables(namespace string) ([]string, error)
	GetTable(namespace, name string) (*common.Table, error)
	CreateTable(namespace string, table *common.Table, force bool) error
	DeleteTable(namespace, name string) error
	UpdateTable(namespace string, table common.Table, force bool) error
	GetHash(namespace string) (string, error)
}

// SubscriberMutator defines rw operations
// only read ops needed for now, creating and removing subscriber should be done by each subscriber process
type SubscriberMutator interface {
	// GetSubscriber returns a subscriber
	GetSubscriber(namespace, subscriberName string) (models.Subscriber, error)
	// GetSubscribers returns a list of subscribers
	GetSubscribers(namespace string) ([]models.Subscriber, error)
	// GetHash returns hash of all subscribers
	GetHash(namespace string) (string, error)
}

// NamespaceMutator mutates table metadata
type NamespaceMutator interface {
	CreateNamespace(namespace string) error
	ListNamespaces() ([]string, error)
}

// JobMutator defines rw operations
type JobMutator interface {
	GetJob(namespace, name string) (job models.JobConfig, err error)
	GetJobs(namespace string) (job []models.JobConfig, err error)
	DeleteJob(namespace, name string) error
	UpdateJob(namespace string, job models.JobConfig) error
	AddJob(namespace string, job models.JobConfig) error
	GetHash(namespace string) (string, error)
}

// IngestionAssignmentMutator defines rw operations
type IngestionAssignmentMutator interface {
	GetIngestionAssignment(namespace, name string) (IngestionAssignment models.IngestionAssignment, err error)
	GetIngestionAssignments(namespace string) (IngestionAssignment []models.IngestionAssignment, err error)
	DeleteIngestionAssignment(namespace, name string) error
	UpdateIngestionAssignment(namespace string, IngestionAssignment models.IngestionAssignment) error
	AddIngestionAssignment(namespace string, IngestionAssignment models.IngestionAssignment) error
	GetHash(namespace, subscriber string) (string, error)
}

// EnumMutator defines EnumMutator interface
type EnumMutator interface {
	// ExtendEnumCases try to extend new enum cases to given column
	ExtendEnumCases(namespace, table, column string, enumCases []string) ([]int, error)
	// GetEnumCases get all enum cases for the given table column
	GetEnumCases(namespace, table, column string) ([]string, error)
}
