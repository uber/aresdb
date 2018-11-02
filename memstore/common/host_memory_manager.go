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

// HostMemoryManager manages archive batch storage in host memory.
// Specifically, it keeps track of memory usage of archive batches and makes
// preloading and eviction decisions based on retention config.
//
// The space available to archive batches is defined as maxMem - unmanagedMem,
// where unmanagedMem accounts for C allocated buffers in live batches and
// primary keys, which changes over time.
// Eviction of archive batches is configured at column level using two configs:
// preloadingDays and priorities.
//
// Data eviction policy is defined as such:
// Always evict data not in preloading zone first; Preloading data won’t be
// evicted until all the non-preloading data are evicted and server is still
// in short of memory.
// For data within the same zone, eviction will happen based on column priority
// For data with same priority, eviction will happen based on data time,
// older data will be evicted first, for same old data, larger size columns
// will be evicted first;
//
// HostMemoryManger will also maintain two go routines. One for preloading data
// and another for eviction. Calling start to start those goroutines and call
// stop to stop them. Stop is a blocking call.
//
// Both TriggerPreload and TriggerEviction are asynchronous calls.
type HostMemoryManager interface {
	ReportUnmanagedSpaceUsageChange(bytes int64)
	ReportManagedObject(table string, shard, batchID, columnID int, bytes int64)
	GetArchiveMemoryUsageByTableShard() (map[string]map[string]*ColumnMemoryUsage, error)
	TriggerEviction()
	TriggerPreload(tableName string, columnID int,
		oldPreloadingDays int, newPreloadingDays int)
	Start()
	Stop()
}

// ColumnMemoryUsage contains column memory usage
type ColumnMemoryUsage struct {
	Preloaded    uint `json:"preloaded"`
	NonPreloaded uint `json:"nonPreloaded"`
	Live         uint `json:"live"`
}
