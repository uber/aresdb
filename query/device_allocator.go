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
	"github.com/uber/aresdb/cgoutils"
	"github.com/uber/aresdb/utils"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

//// DeviceAllocator virtually allocates devices to queries.
//// It maintains device config and current query usage, allocates one or two
//// devices to a query upon request. All requests to the GPU devices must go
//// through this allocator in order for the virtual allocation to be effective.
//// Two devices are allocated at the same time for a single query when consumer
//// grade GPUs without ECC memory are used (to cross check for errors manually).
//type DeviceAllocator interface {
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
//	//   // DeviceAllocator will remember this query's intent for bigger memory.
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
	nullDevicePointer       = devicePointer{}
	memoryReportingInterval = time.Second * 10
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
		pointer: utils.MemAccess(p.getPointer(), offset),
	}
}

// deviceAllocator is the interface to allocate and deallocate device memory for a specific device.
// Note this allocator only tracks memory usage as golang side. Any memory allocation/deallocation at
// cuda side (either thrust code or our own code) is not tracked. So it's preferred to allocate the memory
// at golang side and pass on the pointer to cuda code.
type deviceAllocator interface {
	// deviceAllocate allocates the specified amount of memory on the device.
	deviceAllocate(bytes, device int) devicePointer
	// deviceFree frees the specified memory from the device.
	deviceFree(dp devicePointer)
	// getAllocatedMemory returns allocated memory for a specific device.
	getAllocatedMemory(device int) int64
}

var da deviceAllocator
var deviceAllocatorOnce sync.Once

// getDeviceAllocator returns singleton deviceAllocator instance.
func getDeviceAllocator() deviceAllocator {
	deviceAllocatorOnce.Do(func() {
		da = newDeviceAllocator()
	})
	return da
}

// deviceAllocate is the wrapper of deviceAllocate of deviceAllocator.
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

func reportAllocatedMemory(deviceCount int, da deviceAllocator) {
	// getAllocatedMemory may panic, therefore we should recover here
	defer func() {
		if r := recover(); r != nil {
			var err error
			switch x := r.(type) {
			case string:
				err = utils.StackError(nil, x)
			case error:
				err = utils.StackError(x, "Panic happens when reporting allocated memory")
			default:
				err = utils.StackError(nil, "Panic happens when reporting allocated memory %v", x)
			}
			utils.GetLogger().With("err", err).Error("Failed to report allocated memory")
		}
	}()

	for device := 0; device < deviceCount; device++ {
		utils.GetRootReporter().GetChildGauge(map[string]string{
			"device": strconv.Itoa(device),
		}, utils.AllocatedDeviceMemory).Update(float64(da.getAllocatedMemory(device)))
	}
}

// newDeviceAllocator returns a new device allocator instances.
func newDeviceAllocator() deviceAllocator {
	// init may panic and crash the service. This is expected.
	cgoutils.Init()
	var da deviceAllocator
	deviceCount := cgoutils.GetDeviceCount()
	if cgoutils.IsPooledMemory() {
		utils.GetLogger().Info("Using pooled device memory manager")
		da = &pooledDeviceAllocatorImpl{}
	} else {
		utils.GetLogger().Info("Using memory tracking device memory manager")
		da = &memoryTrackingDeviceAllocatorImpl{
			memoryUsage: make([]int64, deviceCount),
		}
	}

	// Start memory usage reporting go routine.
	// Report the allocated memory of each device per memoryReportingInterval.
	timer := time.NewTimer(memoryReportingInterval)
	go func() {
		for {
			select {
			case <-timer.C:
				reportAllocatedMemory(deviceCount, da)
				// Since we already receive the event from channel,
				// there is no need to stop it and we can directly reset the timer.
				timer.Reset(memoryReportingInterval)
			}
		}
	}()
	return da
}

// memoryTrackingDeviceAllocatorImpl maintains the memory space for each device and reports the updated memory every time an
// allocation/free request is issued.
type memoryTrackingDeviceAllocatorImpl struct {
	memoryUsage []int64
}

// deviceAllocate allocates the specified amount of memory on the device. **Slice bound is not checked!!**
func (d *memoryTrackingDeviceAllocatorImpl) deviceAllocate(bytes, device int) devicePointer {
	dp := devicePointer{
		device:    device,
		bytes:     bytes,
		pointer:   cgoutils.DeviceAllocate(bytes, device),
		allocated: true,
	}
	atomic.AddInt64(&d.memoryUsage[device], int64(bytes))
	return dp
}

// deviceFree frees the specified memory from the device. **Slice bound is not checked!!**
func (d *memoryTrackingDeviceAllocatorImpl) deviceFree(dp devicePointer) {
	cgoutils.DeviceFree(dp.pointer, dp.device)
	atomic.AddInt64(&d.memoryUsage[dp.device], int64(-dp.bytes))
}

// getAllocatedMemory returns memory allocated by this device allocator. Note this
// might be different from the actual device allocated for this device. As thrust
// memory allocation is not tracked here.
func (d *memoryTrackingDeviceAllocatorImpl) getAllocatedMemory(device int) int64 {
	return d.memoryUsage[device]
}

// pooledDeviceAllocatorImpl just delegates every call to underlying pooled memory manager.
type pooledDeviceAllocatorImpl struct {
}

// deviceAllocate allocates the specified amount of memory on the device. **Slice bound is not checked!!**
func (d *pooledDeviceAllocatorImpl) deviceAllocate(bytes, device int) devicePointer {
	dp := devicePointer{
		device:    device,
		bytes:     bytes,
		pointer:   cgoutils.DeviceAllocate(bytes, device),
		allocated: true,
	}
	return dp
}

// deviceFree frees the specified memory from the device. **Slice bound is not checked!!**
func (d *pooledDeviceAllocatorImpl) deviceFree(dp devicePointer) {
	cgoutils.DeviceFree(dp.pointer, dp.device)
}

// getAllocatedMemory returns memory allocated for a specific device.
func (d *pooledDeviceAllocatorImpl) getAllocatedMemory(device int) int64 {
	free, total := cgoutils.GetDeviceMemoryInfo(device)
	return int64(total - free)
}
