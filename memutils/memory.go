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

package memutils

// #cgo LDFLAGS: -L${SRCDIR}/../lib -lmem
// #include "memory.h"
import "C"
import (
	"github.com/uber/aresdb/cgoutils"
	"reflect"
	"unsafe"
)

// GetFlags return flags about the memory management.
func GetFlags() C.DeviceMemoryFlags {
	return C.GetFlags()
}

func IsDeviceMemoryImplementation() bool {
	return (GetFlags() & C.DEVICE_MEMORY_IMPLEMENTATION_FLAG) != 0
}

func IsPooledMemory() bool {
	return (GetFlags() & C.POOLED_MEMORY_FLAG) != 0
}

// Init will initialize the memory management.
func Init() {
	doCGoCall(func() C.CGoCallResHandle {
		return C.Init()
	})
}

// HostAlloc allocates memory in C.
func HostAlloc(bytes int) unsafe.Pointer {
	return unsafe.Pointer(doCGoCall(func() C.CGoCallResHandle {
		return C.HostAlloc(C.size_t(bytes))
	}))
}

// HostFree frees memory allocated in C.
func HostFree(p unsafe.Pointer) {
	doCGoCall(func() C.CGoCallResHandle {
		return C.HostFree(p)
	})
}

// MakeSliceFromCPtr make a slice that points to data that cptr points to.
// cptr must be a c-allocated pointer as the garbage collector will not update
// that uintptr's value if the golang object movee.
func MakeSliceFromCPtr(cptr uintptr, length int) []byte {
	h := reflect.SliceHeader{
		Data: cptr,
		Len:  length,
		Cap:  length,
	}
	return *(*[]byte)(unsafe.Pointer(&h))
}

// CreateCudaStream creates a Cuda stream.
func CreateCudaStream(device int) unsafe.Pointer {
	return unsafe.Pointer(doCGoCall(func() C.CGoCallResHandle {
		return C.CreateCudaStream(C.int(device))
	}))
}

// WaitForCudaStream block waits until all pending operations are finished on
// the specified Cuda stream.
func WaitForCudaStream(stream unsafe.Pointer, device int) {
	if stream != nil {
		doCGoCall(func() C.CGoCallResHandle {
			return C.WaitForCudaStream(stream, C.int(device))
		})
	}
}

// DestroyCudaStream destroys the specified Cuda stream.
func DestroyCudaStream(stream unsafe.Pointer, device int) {
	if stream != nil {
		doCGoCall(func() C.CGoCallResHandle {
			return C.DestroyCudaStream(stream, C.int(device))
		})
	}
}

// DeviceAllocate allocates the specified amount of memory on the device.
func DeviceAllocate(bytes, device int) unsafe.Pointer {
	return unsafe.Pointer(doCGoCall(func() C.CGoCallResHandle {
		return C.DeviceAllocate(C.size_t(bytes), C.int(device))
	}))
}

// DeviceFree frees the specified memory from the device.
func DeviceFree(ptr unsafe.Pointer, device int) {
	doCGoCall(func() C.CGoCallResHandle {
		return C.DeviceFree(ptr, C.int(device))
	})
}

// AsyncMemCopyFunc is a abstraction of DeviceToDevice, DeviceToHost, HostToDevice memcopy functions
type AsyncMemCopyFunc func(dst, src unsafe.Pointer, bytes int, stream unsafe.Pointer, device int)

// AsyncCopyHostToDevice asynchronously copies the host buffer to the device
// buffer on the specified stream.
func AsyncCopyHostToDevice(
	dst, src unsafe.Pointer, bytes int, stream unsafe.Pointer, device int) {
	doCGoCall(func() C.CGoCallResHandle {
		return C.AsyncCopyHostToDevice(dst, src, C.size_t(bytes), stream, C.int(device))
	})
}

// AsyncCopyDeviceToDevice asynchronously copies the src device buffer to the
// dst device buffer buffer on the specified stream.
func AsyncCopyDeviceToDevice(
	dst, src unsafe.Pointer, bytes int, stream unsafe.Pointer, device int) {
	doCGoCall(func() C.CGoCallResHandle {
		return C.AsyncCopyDeviceToDevice(dst, src, C.size_t(bytes), stream, C.int(device))
	})
}

// AsyncCopyDeviceToHost asynchronously copies the device buffer to the host
// buffer on the specified stream.
func AsyncCopyDeviceToHost(
	dst, src unsafe.Pointer, bytes int, stream unsafe.Pointer, device int) {
	doCGoCall(func() C.CGoCallResHandle {
		return C.AsyncCopyDeviceToHost(dst, src, C.size_t(bytes), stream, C.int(device))
	})
}

// GetDeviceCount returns the number of GPU devices
func GetDeviceCount() int {
	return int(doCGoCall(func() C.CGoCallResHandle {
		return C.GetDeviceCount()
	}))
}

// GetDeviceGlobalMemoryInMB returns the total global memory(MB) for a given device
func GetDeviceGlobalMemoryInMB(device int) int {
	return int(doCGoCall(func() C.CGoCallResHandle {
		return C.GetDeviceGlobalMemoryInMB(C.int(device))
	}))
}

// CudaProfilerStart starts/resumes the profiler.
func CudaProfilerStart() {
	doCGoCall(func() C.CGoCallResHandle {
		return C.CudaProfilerStart()
	})
}

// CudaProfilerStop stops/pauses the profiler.
func CudaProfilerStop() {
	doCGoCall(func() C.CGoCallResHandle {
		return C.CudaProfilerStop()
	})
}

// GetDeviceMemoryInfo returns information about total size and free size of device memory in bytes for a specfic
// device.
func GetDeviceMemoryInfo(device int) (int, int) {
	var freeSize, totalSize C.size_t
	doCGoCall(func() C.CGoCallResHandle {
		return C.GetDeviceMemoryInfo(&freeSize, &totalSize, C.int(device))
	})
	return int(freeSize), int(totalSize)
}

// doCGoCall does the cgo call by converting CGoCallResHandle to C.int and *C.char and calls doCGoCall.
// The reason to have this wrapper is because CGo types are bound to package name, thereby even C.int are different types
// under different packages.
func doCGoCall(f func() C.CGoCallResHandle) uintptr {
	return cgoutils.DoCGoCall(func() (uintptr, unsafe.Pointer) {
		ret := f()
		return uintptr(ret.res), unsafe.Pointer(ret.pStrErr)
	})
}
