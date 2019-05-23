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

var _ = ginkgo.Describe("Hash should work", func() {

	ginkgo.It("MurmurHash32 should work", func() {
		tests := [][]interface{}{
			{[]byte{1}, 1, uint32(3831157163)},
			{[]byte{1, 2, 3, 4}, 4, uint32(1043635621)},
			{[]byte{1, 2, 3, 4, 5, 6, 7}, 7, uint32(4233664437)},
			{[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9}, 9, uint32(2711154856)},
		}

		for _, test := range tests {
			key := test[0].([]byte)
			numBytes := test[1].(int)
			expHash := test[2].(uint32)
			actHash := Murmur3Sum32(unsafe.Pointer(&key[0]), numBytes, 0)
			Ω(actHash).Should(Equal(expHash))
		}
	})

	ginkgo.It("MurmurHash128 should work", func() {
		tests := [][]interface{}{
			{[]byte{1}, 1, [2]uint64{8849112093580131862, 8613248517421295493}},
			{[]byte{1, 2, 3, 4}, 4, [2]uint64{720734999560851427, 16923441050003117939}},
			{[]byte{1, 2, 3, 4, 5, 6, 7}, 7, [2]uint64{17578618098293890537, 4405506937751715985}},
			{[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9}, 9, [2]uint64{13807401213100465550, 17366753655886151073}},
		}

		for _, test := range tests {
			key := test[0].([]byte)
			numBytes := test[1].(int)
			expHash := test[2].([2]uint64)
			actHash := Murmur3Sum128(unsafe.Pointer(&key[0]), numBytes, 0)
			Ω(actHash).Should(Equal(expHash))
		}
	})

	ginkgo.It("MurmurHash64 should work", func() {
		tests := [][]interface{}{
			{[]byte{1}, 1, uint64(8849112093580131862)},
			{[]byte{1, 2, 3, 4}, 4, uint64(720734999560851427)},
			{[]byte{1, 2, 3, 4, 5, 6, 7}, 7, uint64(17578618098293890537)},
			{[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9}, 9, uint64(13807401213100465550)},
		}

		for _, test := range tests {
			key := test[0].([]byte)
			numBytes := test[1].(int)
			expHash := test[2].(uint64)
			actHash := Murmur3Sum64(unsafe.Pointer(&key[0]), numBytes, 0)
			Ω(actHash).Should(Equal(expHash))
		}
	})
})

func BenchmarkMurmur3Sum32(b *testing.B) {
	testMurmurKey := [4]byte{100, 0, 0, 0}
	for i := 0; i < b.N; i++ {
		Murmur3Sum32(unsafe.Pointer(&testMurmurKey[0]), 16, 0)
	}
}
