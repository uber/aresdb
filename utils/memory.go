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

// MemAccess access memory location with starting pointer and an offset.
func MemAccess(p unsafe.Pointer, offset int) unsafe.Pointer {
	return unsafe.Pointer(uintptr(p) + uintptr(offset))
}

// MemDist returns the distance between two unsafe pointer.
func MemDist(p1 unsafe.Pointer, p2 unsafe.Pointer) int64 {
	return int64(uintptr(p1) - uintptr(p2))
}

// MemEqual performs byte to byte comparison.
func MemEqual(a unsafe.Pointer, b unsafe.Pointer, bytes int) bool {
	for i := 0; i < bytes; i++ {
		if *(*uint8)(MemAccess(a, i)) != *(*uint8)(MemAccess(b, i)) {
			return false
		}
	}
	return true
}

// MemCopy performs memory copy of specified bytes from src to dst
func MemCopy(dst unsafe.Pointer, src unsafe.Pointer, bytes int) {
	for i := 0; i < bytes; i++ {
		*(*uint8)(MemAccess(dst, i)) = *(*uint8)(MemAccess(src, i))
	}
}

// MemSwap performs memory copy of specified bytes from src to dst
func MemSwap(dst unsafe.Pointer, src unsafe.Pointer, bytes int) {
	for i := 0; i < bytes; i++ {
		tmp := *(*uint8)(MemAccess(dst, i))
		*(*uint8)(MemAccess(dst, i)) = *(*uint8)(MemAccess(src, i))
		*(*uint8)(MemAccess(src, i)) = tmp
	}
}
