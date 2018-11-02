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

package memstore

import (
	"code.uber.internal/data/ares/utils"
	"sync"
	"time"
)

// PurgeManager manages the purge related stats and progress.
type PurgeManager struct {
	sync.RWMutex `json:"-"`

	// Job Trigger Condition related fields
	// Last purge time.
	LastPurgeTime time.Time `json:"lastPurgeTime"`

	PurgeInterval time.Duration `json:"purgeInterval"`

	// for convenience.
	shard *TableShard
}

// NewPurgeManager creates a new PurgeManager instance.
func NewPurgeManager(shard *TableShard) *PurgeManager {
	return &PurgeManager{
		shard:         shard,
		PurgeInterval: 24 * time.Hour,
		LastPurgeTime: utils.Now(),
	}
}

// QualifyForPurge tells whether we can trigger a purge job.
func (p *PurgeManager) QualifyForPurge() bool {
	p.RLock()
	defer p.RUnlock()
	now := utils.Now()
	return now.After(p.LastPurgeTime.Add(p.PurgeInterval))
}
