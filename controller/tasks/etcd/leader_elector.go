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
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/cluster/services/leader/campaign"
	xwatch "github.com/m3db/m3x/watch"
)

// ElectionStatus represents the leader election status
type ElectionStatus int

const (
	// Unknown status
	Unknown ElectionStatus = iota
	// Leader status
	Leader
	// Follower status
	Follower
)

// LeaderElector is the interface for start leader election
type LeaderElector interface {
	// Start starts leader election
	Start() error
	// Status check current leader election status
	Status() ElectionStatus
	// Resign leader status if leader
	Resign() error
	// Close election
	Close() error
}

type leaderElector struct {
	leaderService   services.LeaderService
	statusWatchable xwatch.Watchable
}

func (l *leaderElector) Start() error {
	campaignOpts, err := services.NewCampaignOptions()
	if err != nil {
		return err
	}
	statusCh, err := l.leaderService.Campaign("", campaignOpts)
	if err != nil {
		return err
	}
	go func() {
		for status := range statusCh {
			_ = l.statusWatchable.Update(status)
		}
	}()
	return nil
}

func (l *leaderElector) Status() ElectionStatus {
	state := l.statusWatchable.Get()
	if state == nil {
		return Unknown
	}
	switch state.(campaign.Status).State {
	case campaign.Follower:
		return Follower
	case campaign.Leader:
		return Leader
	default:
		return Unknown
	}
}

func (l *leaderElector) Close() error {
	return l.leaderService.Close()
}

func (l *leaderElector) Resign() error {
	return l.leaderService.Resign("")
}

// NewLeaderElector creates a leader elector
func NewLeaderElector(service services.LeaderService) LeaderElector {
	return &leaderElector{
		leaderService:   service,
		statusWatchable: xwatch.NewWatchable(),
	}
}
