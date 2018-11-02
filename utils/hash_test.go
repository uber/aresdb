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
	"testing"
	"unsafe"

	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = ginkgo.Describe("MurmurHash32", func() {
	ginkgo.It("Should Hash Correctly for key with one byte", func() {
		key := [1]byte{1}
		Ω(Murmur3Sum32(unsafe.Pointer(&key[0]), 1, 0)).Should(Equal(uint32(3831157163)))
	})

	ginkgo.It("Should Hash Correctly for key with four bytes", func() {
		key := [4]byte{1, 2, 3, 4}
		Ω(Murmur3Sum32(unsafe.Pointer(&key[0]), 4, 0)).Should(Equal(uint32(1043635621)))
	})

	ginkgo.It("Should Hash Correctly for key with seven bytes", func() {
		key := [7]byte{1, 2, 3, 4, 5, 6, 7}
		Ω(Murmur3Sum32(unsafe.Pointer(&key[0]), 7, 0)).Should(Equal(uint32(4233664437)))
	})

	ginkgo.It("Should Hash Correctly for key with nine bytes", func() {
		key := [9]byte{1, 2, 3, 4, 5, 6, 7, 8, 9}
		Ω(Murmur3Sum32(unsafe.Pointer(&key[0]), 9, 0)).Should(Equal(uint32(2711154856)))
	})

})

func BenchmarkMurmur3Sum32(b *testing.B) {
	testMurmurKey := [4]byte{100, 0, 0, 0}
	for i := 0; i < b.N; i++ {
		Murmur3Sum32(unsafe.Pointer(&testMurmurKey[0]), 16, 0)
	}
}
