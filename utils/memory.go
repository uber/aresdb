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