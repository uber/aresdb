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
package utils

import (
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"testing"
	"unsafe"
)

func byteToByteCopy(a unsafe.Pointer, b unsafe.Pointer, bytes int) bool {
	for i := 0; i < bytes; i++ {
		if *(*uint8)(MemAccess(a, i)) != *(*uint8)(MemAccess(b, i)) {
			return false
		}
	}
	return true
}

func BenchmarkMemEqual(b *testing.B) {
	key1 := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	key2 := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	kp1 := unsafe.Pointer(&key1[0])
	kp2 := unsafe.Pointer(&key2[0])
	for i := 0; i < b.N; i++ {
		MemEqual(kp1, kp2, 16)
	}
}

func BenchmarkMemEqual_ByteToByte(b *testing.B) {
	key1 := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	key2 := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	kp1 := unsafe.Pointer(&key1[0])
	kp2 := unsafe.Pointer(&key2[0])
	for i := 0; i < b.N; i++ {
		byteToByteCopy(kp1, kp2, 16)
	}
}

var _ = ginkgo.Describe("memory", func() {
	ginkgo.It("Memory equal should work", func() {
		key1 := []byte{1, 2, 3, 4}
		key2 := []byte{1, 2, 3, 4}
		key3 := []byte{5, 6, 7, 8}
		Ω(MemEqual(unsafe.Pointer(&key1[0]), unsafe.Pointer(&key2[0]), 4)).Should(BeTrue())
		Ω(MemEqual(unsafe.Pointer(&key1[0]), unsafe.Pointer(&key3[0]), 4)).Should(BeFalse())
	})

	ginkgo.It("Memory copy should work", func() {
		key1 := []byte{1, 1, 1, 1}
		key2 := []byte{2, 2, 2, 2}
		MemCopy(unsafe.Pointer(&key1[0]), unsafe.Pointer(&key2[0]), 4)
		Ω(key1).Should(Equal(key2))
	})

	ginkgo.It("Memory swap should work", func() {
		key1 := []byte{1, 1, 1, 1}
		key2 := []byte{2, 2, 2, 2}
		MemSwap(unsafe.Pointer(&key1[0]), unsafe.Pointer(&key2[0]), 4)
		Ω(key1).Should(Equal([]byte{2, 2, 2, 2}))
		Ω(key2).Should(Equal([]byte{1, 1, 1, 1}))
	})

	ginkgo.It("MemDist should work", func() {
		value := [20]byte{}
		Ω(MemDist(unsafe.Pointer(&value[0]), unsafe.Pointer(&value[0]))).Should(BeZero())
		Ω(MemDist(unsafe.Pointer(&value[1]), unsafe.Pointer(&value[0]))).Should(BeEquivalentTo(1))
		Ω(MemDist(unsafe.Pointer(&value[0]), unsafe.Pointer(&value[1]))).Should(BeEquivalentTo(-1))
		Ω(MemDist(unsafe.Pointer(&value[19]), unsafe.Pointer(&value[0]))).Should(BeEquivalentTo(19))
	})
})
