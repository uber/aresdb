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

import "fmt"

// ares redolog kafka topic prefix
const aresRedologKafkaTopicPrefix = "ares-redolog"

// GetTopicFromTable get the topic name for namespace and table name
func GetTopicFromTable(namespace, table string) string {
	return fmt.Sprintf("%s-%s-%s", aresRedologKafkaTopicPrefix, namespace, table)
}
