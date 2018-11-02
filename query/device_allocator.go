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

package query

import (
	"code.uber.internal/data/ares/memutils"
	"code.uber.internal/data/ares/utils"
	"strconv"
	"sync"
	"sync/atomic"
	"unsafe"
)

//// deviceAllocatorImpl virtually allocates devices to queries.
//// It maintains device config and current query usage, allocates one or two
//// devices to a query upon request. All requests to the GPU devices must go
//// through this allocator in order for the virtual allocation to be effective.
//// Two devices are allocated at the same time for a single query when consumer
//// grade GPUs without ECC memory are used (to cross check for errors manually).
//type deviceAllocatorImpl interface {
//	// Allocate a device (or two) for a query.
//	// Returns the IDs of the allocated devices, or -1 in case of error.
//	// Returns the same device ID if only one device is allocated.
//	// Also returns the queryHandle for future references.
//	// Memory requirement is guaranteed to be satisfied, thread requirement is
//	// treated only as a hint. When all devices are temporarily busy, will block
//	// until the request can be satisfied.
//	DeviceAlloc(bytes, threads int) (deviceID0, deviceID1, queryHandle int)
//	// Adjust the requirements of an existing query on the allocated devices.
//	// This is used when a new batch of data (of different size) is about to be
//	// transferred and processed for a query. The call always succeeds and returns
//	// true when bytes decreases; however, when bytes increases, it may fail and
//	// return false when failFast=true, or block wait for an extended amount of
//	// time (it can still fail after the wait).
//	// One protocol to handle increased bytes is:
//	//   if DeviceRealloc(bytes=new, failFast=true) {
//	//     return success
//	//   }
//	//   // Keep the query result only and free up most memory.
//	//   DeviceRealloc(bytes=result_only)
//	//   // deviceAllocatorImpl will remember this query's intent for bigger memory.
//	//   if DeviceRealloc(bytes=new, failFast=false) {
//	//     return success
//	//   }
//	//   // fail the query due to unsatisfiable requirement.
//	//   // Free up all space.
//	//   // deviceFree()
//	//   return failure
//	DeviceRealloc(queryHandle, bytes, threads int, failFast bool) bool
//	// Free up the resources allocated on the devices for the specified query.
//	deviceFree(queryHandle int)
//}
var (
	nullDevicePointer = devicePointer{}
)

// devicePointer is the wrapper of actual device memory pointer plus the size it points to and which device
// it belongs to.
type devicePointer struct {
	bytes, device int
	pointer       unsafe.Pointer
	// whether this pointer points to beginning of an allocated address.
	allocated bool
}

func (p devicePointer) getPointer() unsafe.Pointer {
	return p.pointer
}

func (p devicePointer) isNull() bool {
	return p.pointer == nil
}

// offset returns another pointer points to the address of current ptr + offset.
func (p devicePointer) offset(offset int) devicePointer {
	return devicePointer{
		device:  p.device,
		pointer: memutils.MemAccess(p.getPointer(), offset),
	}
}

// deviceAllocatorImpl is the interface to allocate and deallocate device memory for a specific device.
// Note this allocator only tracks memory usage as golang side. Any memory allocation/deallocation at
// cuda side (either thrust code or our own code) is not tracked. So it's preferred to allocate the memory
// at golang side and pass on the pointer to cuda code.
type deviceAllocator interface {
	// deviceAllocate allocates the specified amount of memory on the device.
	deviceAllocate(bytes, device int) devicePointer
	// deviceFree frees the specified memory from the device.
	deviceFree(dp devicePointer)
}

var da deviceAllocator
var deviceAllocatorOnce sync.Once

// getDeviceAllocator returns singleton deviceAllocatorImpl instance.
func getDeviceAllocator() deviceAllocator {
	deviceAllocatorOnce.Do(func() {
		da = newDeviceAllocator()
	})
	return da
}

// deviceAllocate is the wrapper of deviceAllocate of deviceAllocatorImpl.
func deviceAllocate(bytes, device int) devicePointer {
	return getDeviceAllocator().deviceAllocate(bytes, device)
}

// deviceFreeAndSetNil frees the specified device pointer if it's not null and set the pointer it holds to null.
func deviceFreeAndSetNil(dp *devicePointer) {
	if dp != nil && !dp.isNull() && dp.allocated {
		getDeviceAllocator().deviceFree(*dp)
		*dp = nullDevicePointer
	}
}

// newDeviceAllocator returns a new device allocator instances.
func newDeviceAllocator() deviceAllocator {
	return &deviceAllocatorImpl{
		memoryUsage: make([]int64, memutils.GetDeviceCount()),
	}
}

// deviceAllocatorImpl maintains the memory space for each device and reports the updated memory every time an
// allocation/free request is issued.
type deviceAllocatorImpl struct {
	memoryUsage []int64
}

// deviceAllocate allocates the specified amount of memory on the device. **Slice bound is not checked!!**
func (d *deviceAllocatorImpl) deviceAllocate(bytes, device int) devicePointer {
	dp := devicePointer{
		device:    device,
		bytes:     bytes,
		pointer:   memutils.DeviceAllocate(bytes, device),
		allocated: true,
	}
	utils.GetRootReporter().GetChildGauge(map[string]string{
		"device": strconv.Itoa(device),
	}, utils.AllocatedDeviceMemory).Update(float64(
		atomic.AddInt64(&d.memoryUsage[device], int64(bytes))))
	return dp
}

// deviceFree frees the specified memory from the device. **Slice bound is not checked!!**
func (d *deviceAllocatorImpl) deviceFree(dp devicePointer) {
	memutils.DeviceFree(dp.pointer, dp.device)
	utils.GetRootReporter().GetChildGauge(map[string]string{
		"device": strconv.Itoa(dp.device),
	}, utils.AllocatedDeviceMemory).Update(float64(
		atomic.AddInt64(&d.memoryUsage[dp.device], int64(-dp.bytes))))
}
