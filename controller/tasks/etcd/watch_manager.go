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
	"sync"

	xwatch "github.com/m3db/m3x/watch"
)

const stateChangeEventBufferSize = 6

// WatchFunc watches a namespace state change and creates a updatable
type WatchFunc func(namespace string) (xwatch.Updatable, error)

// WatchManager is the interface for manage updatable in added namespaces
type WatchManager interface {
	// C return update event notification channel
	C() <-chan string
	// AddNamespace add a managed namespace
	AddNamespace(namespace string) error
	// Close the watch manager
	Close()
}

type watchManager struct {
	sync.RWMutex

	namespaces       map[string]struct{}
	watchFunc        WatchFunc
	stateChangeEvent chan string
	done             chan struct{}
}

func (w *watchManager) C() <-chan string {
	return w.stateChangeEvent
}

func (w *watchManager) AddNamespace(namespace string) (err error) {
	var updatable xwatch.Updatable
	w.Lock()
	defer func() {
		w.Unlock()
		if updatable != nil {
			go w.handleUpdate(updatable.C(), func() {
				w.stateChangeEvent <- namespace
			}, func() {
				updatable.Close()
			})
		}
	}()

	if _, exist := w.namespaces[namespace]; exist {
		return
	}
	updatable, err = w.watchFunc(namespace)
	w.namespaces[namespace] = struct{}{}

	return
}

func (w *watchManager) handleUpdate(notifyCh <-chan struct{}, onUpdate, onClose func()) {
	for {
		select {
		case <-notifyCh:
			onUpdate()
		case <-w.done:
			onClose()
			return
		}
	}
}

func (w *watchManager) Close() {
	close(w.done)
	close(w.stateChangeEvent)
}

// NewWatchManager creates a new watch manager
func NewWatchManager(watchFunc WatchFunc) WatchManager {
	return &watchManager{
		namespaces:       make(map[string]struct{}),
		done:             make(chan struct{}),
		stateChangeEvent: make(chan string, stateChangeEventBufferSize),
		watchFunc:        watchFunc,
	}
}
