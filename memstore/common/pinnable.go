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
	"sync"
)
// Pinnable implements a vector party that support pin and release operations.
type Pinnable struct {
	// Used in archive batches to allow requesters to wait until the vector party
	// is fully loaded from disk.
	Loader sync.WaitGroup
	// For archive store only. Number of users currently using this vector party.
	// This field is protected by the batch lock.
	Pins int
	// For archive store only. The condition for pins to drop down to 0.
	AllUsersDone *sync.Cond
}

// Release releases the vector party from the archive store
// so that it can be evicted or deleted.
func (vp *Pinnable) Release() {
	vp.AllUsersDone.L.Lock()
	vp.Pins--
	if vp.Pins == 0 {
		vp.AllUsersDone.Broadcast()
	}
	vp.AllUsersDone.L.Unlock()
}

// Pin vector party for use, caller should lock archive batch before calling
func (vp *Pinnable) Pin() {
	vp.Pins++
}

// WaitForUsers wait for vector party user to finish and return true when all users are done
func (vp *Pinnable) WaitForUsers(blocking bool) bool {
	if blocking {
		for vp.Pins > 0 {
			vp.AllUsersDone.Wait()
		}
		return true
	}
	return vp.Pins == 0
}

// WaitForDiskLoad waits for vector party disk load to finish
func (vp *Pinnable) WaitForDiskLoad() {
	vp.Loader.Wait()
}
