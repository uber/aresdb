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
