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

package cgoutils

import (
	"unsafe"

	"fmt"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = ginkgo.Describe("memory utils", func() {

	ginkgo.It("MakeSliceFromCPtr should work", func() {
		// Allocate 5 ints.
		p := HostAlloc(5)
		b := uintptr(p)
		Ω(*(*int)(unsafe.Pointer(b))).Should(BeEquivalentTo(0))
		s := MakeSliceFromCPtr(b, 5)
		s[0] = 1
		Ω(*(*byte)(unsafe.Pointer(b))).Should(BeEquivalentTo(1))
		s[1] = 3
		Ω(*(*byte)(unsafe.Pointer(b + 1))).Should(BeEquivalentTo(3))
		s[4] = 5
		Ω(*(*byte)(unsafe.Pointer(b + 4))).Should(BeEquivalentTo(5))

		//// Index should be out of bound.
		Ω(func() { fmt.Println(s[5]) }).Should(Panic())
		HostFree(p)
	})

	ginkgo.It("CreateCudaStream should work", func() {
		Ω(CreateCudaStream(0)).Should(BeZero())
		Ω(CreateCudaStream(10)).Should(BeZero())
	})

	ginkgo.It("DestroyCudaStream should work", func() {
		Ω(func() { DestroyCudaStream(unsafe.Pointer(nil), 0) }).ShouldNot(Panic())
		Ω(func() { DestroyCudaStream(unsafe.Pointer(nil), 10) }).ShouldNot(Panic())
	})

	ginkgo.It("DeviceAllocate and DeviceFree should work", func() {
		p1 := DeviceAllocate(0, 0)
		Ω(p1).ShouldNot(BeZero())
		Ω(func() { DeviceFree(p1, 0) }).ShouldNot(Panic())

		p2 := DeviceAllocate(20, 0)
		Ω(p2).ShouldNot(BeZero())
		Ω(func() { DeviceFree(p2, 0) }).ShouldNot(Panic())
	})

	ginkgo.It("CudaProfilerStart and CudaProfilerStop should work", func() {
		Ω(func() { CudaProfilerStart() }).ShouldNot(Panic())
		Ω(func() { CudaProfilerStop() }).ShouldNot(Panic())
	})

	ginkgo.It("GetDeviceCount, GetDeviceMemoryInfo and GetDeviceGlobalMemoryInMB should work", func() {
		deviceCount := GetDeviceCount()
		Ω(deviceCount).Should(BeNumerically(">", 0))
		for device := 0; device < deviceCount; device++ {
			Ω(GetDeviceGlobalMemoryInMB(device)).Should(BeNumerically(">", 0))
			if IsPooledMemory() {
				free, total := GetDeviceMemoryInfo(device)
				Ω(free).Should(BeNumerically(">", 0))
				Ω(total).Should(BeNumerically(">", 0))
			} else {
				Ω(func() { GetDeviceMemoryInfo(device) }).Should(Panic())
			}
		}
	})

	ginkgo.It("GetFlags should work", func() {
		Ω(func() { GetFlags() }).ShouldNot(Panic())
		Ω(func() { IsDeviceMemoryImplementation() }).ShouldNot(Panic())
		Ω(func() { IsPooledMemory() }).ShouldNot(Panic())
	})

	ginkgo.It("CudaMemCopies should work", func() {
		srcHost := [10]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
		dstHost := [10]byte{}

		deviceMem1 := DeviceAllocate(10, 0)
		defer DeviceFree(deviceMem1, 0)
		deviceMem2 := DeviceAllocate(10, 0)
		defer DeviceFree(deviceMem2, 0)

		stream := CreateCudaStream(0)
		AsyncCopyHostToDevice(deviceMem1, unsafe.Pointer(&srcHost[0]), 10, stream, 0)
		AsyncCopyDeviceToDevice(deviceMem2, deviceMem1, 10, stream, 0)
		AsyncCopyDeviceToHost(unsafe.Pointer(&dstHost[0]), deviceMem2, 10, stream, 0)
		Ω(dstHost).Should(Equal(srcHost))
	})
})
