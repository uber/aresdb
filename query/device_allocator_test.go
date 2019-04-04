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
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Device Allocator", func() {

	ginkgo.It("deviceAllocate and deviceFree should work", func() {
		deviceAllocator := getDeviceAllocator()
		dp := deviceAllocator.deviceAllocate(12, 0)
		Ω(dp.bytes).Should(BeEquivalentTo(12))
		Ω(dp.device).Should(BeEquivalentTo(0))
		Ω(dp.allocated).Should(BeTrue())
		Ω(deviceAllocator.getAllocatedMemory(0)).Should(BeEquivalentTo(12))

		dp2 := deviceAllocator.deviceAllocate(24, 0)
		Ω(dp2.bytes).Should(BeEquivalentTo(24))
		Ω(dp2.device).Should(BeEquivalentTo(0))
		Ω(dp2.allocated).Should(BeTrue())
		Ω(deviceAllocator.getAllocatedMemory(0)).Should(BeEquivalentTo(36))

		deviceAllocator.deviceFree(dp)
		Ω(deviceAllocator.getAllocatedMemory(0)).Should(BeEquivalentTo(24))

		deviceAllocator.deviceFree(dp2)
		Ω(deviceAllocator.getAllocatedMemory(0)).Should(BeEquivalentTo(0))
	})

	ginkgo.It("deviceFreeAndSetNil should work", func() {
		dp := deviceAllocate(12, 0)
		Ω(dp.bytes).Should(BeEquivalentTo(12))
		Ω(dp.device).Should(BeEquivalentTo(0))
		Ω(dp.allocated).Should(BeTrue())
		da := getDeviceAllocator()
		Ω(da.getAllocatedMemory(0)).Should(BeNumerically(">", 0))

		deviceFreeAndSetNil(&dp)
		Ω(da.getAllocatedMemory(0)).Should(BeEquivalentTo(0))
	})

	ginkgo.It("reportAllocatedMemory should work", func() {
		dp := deviceAllocate(12, 0)
		da := getDeviceAllocator()
		Ω(da.getAllocatedMemory(0)).Should(BeNumerically(">", 0))
		Ω(func() { reportAllocatedMemory(0, da) }).ShouldNot(Panic())
		da.deviceFree(dp)
	})
})
