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

func rotl64(x uint64, r int8) uint64 {
	return (x << uint64(r)) | (x >> (64 - uint64(r)))
}

func fmix64(k uint64) uint64 {
	k ^= k >> 33
	k *= 0xff51afd7ed558ccd
	k ^= k >> 33
	k *= 0xc4ceb9fe1a85ec53
	k ^= k >> 33
	return k
}

// Murmur3Sum128 implements murmur3sum128 hash algorithm
func Murmur3Sum128(key unsafe.Pointer, bytes int, seed uint32) (out [2]uint64) {
	var data = uintptr(key)
	nblocks := bytes / 16

	var i int
	var h1 = uint64(seed)
	var h2 = uint64(seed)

	var c1 uint64 = 0x87c37b91114253d5
	var c2 uint64 = 0x4cf5ad432745937f

	blocks := data
	for i = 0; i < nblocks; i++ {
		k1 := *(*uint64)(unsafe.Pointer(blocks + uintptr(i * 2) * 8))
		k2 := *(*uint64)(unsafe.Pointer(blocks + uintptr(i * 2 + 1) * 8))

		k1 *= c1
		k1 = rotl64(k1, 31)
		k1 *= c2
		h1 ^= k1

		h1 = rotl64(h1, 27)
		h1 += h2
		h1 = h1 * 5 + 0x52dce729

		k2 *= c2
		k2 = rotl64(k2, 33)
		k2 *= c1
		h2 ^= k2

		h2 = rotl64(h2, 31)
		h2 += h1
		h2 = h2 * 5 + 0x38495ab5
	}

	tail := data + uintptr(nblocks) * 16
	var k1 uint64 = 0
	var k2 uint64 = 0

	switch bytes & 15 {
	case 15:k2 ^= uint64(*(*uint8)(unsafe.Pointer(tail + uintptr(14)))) << 48
		fallthrough
	case 14:k2 ^= uint64(*(*uint8)(unsafe.Pointer(tail + uintptr(13)))) << 40
		fallthrough
	case 13:k2 ^= uint64(*(*uint8)(unsafe.Pointer(tail + uintptr(12)))) << 32
		fallthrough
	case 12:k2 ^= uint64(*(*uint8)(unsafe.Pointer(tail + uintptr(11)))) << 24
		fallthrough
	case 11:k2 ^= uint64(*(*uint8)(unsafe.Pointer(tail + uintptr(10)))) << 16
		fallthrough
	case 10:k2 ^= uint64(*(*uint8)(unsafe.Pointer(tail + uintptr(9)))) << 8
		fallthrough
	case 9:k2 ^= uint64(*(*uint8)(unsafe.Pointer(tail + uintptr(8))))
		k2 *= c2
		k2 = rotl64(k2, 33)
		k2 *= c1
		h2 ^= k2
		fallthrough
	case 8:k1 ^= uint64(*(*uint8)(unsafe.Pointer(tail + uintptr(7)))) << 56
		fallthrough
	case 7:k1 ^= uint64(*(*uint8)(unsafe.Pointer(tail + uintptr(6)))) << 48
		fallthrough
	case 6:k1 ^= uint64(*(*uint8)(unsafe.Pointer(tail + uintptr(5)))) << 40
		fallthrough
	case 5:k1 ^= uint64(*(*uint8)(unsafe.Pointer(tail + uintptr(4)))) << 32
		fallthrough
	case 4:k1 ^= uint64(*(*uint8)(unsafe.Pointer(tail + uintptr(3)))) << 24
		fallthrough
	case 3:k1 ^= uint64(*(*uint8)(unsafe.Pointer(tail + uintptr(2)))) << 16
		fallthrough
	case 2:k1 ^= uint64(*(*uint8)(unsafe.Pointer(tail + uintptr(1)))) << 8
		fallthrough
	case 1:k1 ^= uint64(*(*uint8)(unsafe.Pointer(tail)))
		k1 *= c1
		k1 = rotl64(k1, 31)
		k1 *= c2
		h1 ^= k1
	}

	h1 ^= uint64(bytes)
	h2 ^= uint64(bytes)

	h1 += h2
	h2 += h1

	h1 = fmix64(h1)
	h2 = fmix64(h2)

	h1 += h2
	h2 += h1

	out[0] = h1
	out[1] = h2
	return
}
