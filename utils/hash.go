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

import "unsafe"

const (
	murmur3C1_32 uint32 = 0xcc9e2d51
	murmur3C2_32 uint32 = 0x1b873593
)

// Murmur3Sum32 implements Murmur3Sum32 hash algorithm
func Murmur3Sum32(key unsafe.Pointer, bytes int, seed uint32) uint32 {
	h1 := seed

	nblocks := bytes / 4
	var p uintptr
	if bytes > 0 {
		p = uintptr(key)
	}
	p1 := p + uintptr(4*nblocks)
	for ; p < p1; p += 4 {
		k1 := *(*uint32)(unsafe.Pointer(p))

		k1 *= murmur3C1_32
		k1 = (k1 << 15) | (k1 >> 17)
		k1 *= murmur3C2_32

		h1 ^= k1
		h1 = (h1 << 13) | (h1 >> 19)
		h1 = h1*5 + 0xe6546b64
	}

	tailBytes := bytes - nblocks*4
	tail := p1

	var k1 uint32
	switch tailBytes & 3 {
	case 3:
		k1 ^= uint32(*(*uint8)(unsafe.Pointer(tail + uintptr(2)))) << 16
		fallthrough
	case 2:
		k1 ^= uint32(*(*uint8)(unsafe.Pointer(tail + uintptr(1)))) << 8
		fallthrough
	case 1:
		k1 ^= uint32(*(*uint8)(unsafe.Pointer(tail + uintptr(0))))
		k1 *= murmur3C1_32
		k1 = (k1 << 15) | (k1 >> 17)
		k1 *= murmur3C2_32
		h1 ^= k1
	}

	h1 ^= uint32(bytes)

	h1 ^= h1 >> 16
	h1 *= 0x85ebca6b
	h1 ^= h1 >> 13
	h1 *= 0xc2b2ae35
	h1 ^= h1 >> 16

	return h1
}
