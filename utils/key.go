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
package utils

import (
	"path"
	"strconv"
)

const (
	// AresController sets the name for ares controller
	AresController = "ares-controller"
	// AresSubscriber sets the name for ares subscriber
	AresSubscriber = "ares-subscriber"
	// AresDataNode sets the name for ares datanode
	AresDataNode = "ares-datanode"
)

// etcd keys

// NamespaceListKey builds key for namespace list
func NamespaceListKey() string {
	return path.Join(AresController, "namespace")
}

// NamespaceKey builds key for namespace
func NamespaceKey(namespace string) string {
	return path.Join(NamespaceListKey(), namespace)
}

// SchemaListKey builds key for schema list
func SchemaListKey(namespace string) string {
	return path.Join(NamespaceKey(namespace), "schema")
}

// JobListKey builds key for job list
func JobListKey(namespace string) string {
	return path.Join(NamespaceKey(namespace), "job_config")
}

// JobAssignmentsListKey builds key for job assignments
func JobAssignmentsListKey(namespace string) string {
	return path.Join(NamespaceKey(namespace), "job_assignments")
}

// InstanceListKey builds key for job assignments
func InstanceListKey(namespace string) string {
	return path.Join(NamespaceKey(namespace), "instances")
}

// SchemaKey builds key for schema
func SchemaKey(namespace, name string) string {
	return path.Join(SchemaListKey(namespace), name)
}

// JobKey builds key for job config
func JobKey(namespace, name string) string {
	return path.Join(JobListKey(namespace), name)
}

// JobAssignmentsKey builds key for job assignments
func JobAssignmentsKey(namespace, name string) string {
	return path.Join(JobAssignmentsListKey(namespace), name)
}

// InstanceKey builds key for instance
func InstanceKey(namespace, name string) string {
	return path.Join(InstanceListKey(namespace), name)
}

// EnumNodeListKey builds the key for enum node list
func EnumNodeListKey(namespace, table string, incarnation, columnID int) string {
	return path.Join(NamespaceKey(namespace), "enum_cases", table, strconv.Itoa(incarnation), strconv.Itoa(columnID))
}

// EnumNodeKey builds the key for enum node
func EnumNodeKey(namespace, table string, incarnation int, columnID, nodeID int) string {
	return path.Join(EnumNodeListKey(namespace, table, incarnation, columnID), strconv.Itoa(nodeID))
}

// SubscriberServiceName builds the subscriber service name
func SubscriberServiceName(namespace string) string {
	return path.Join(namespace, AresSubscriber)
}

// DataNodeServiceName builds the subscriber service name
func DataNodeServiceName(namespace string) string {
	return path.Join(namespace, AresDataNode)
}
