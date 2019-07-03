package common

import (
	"unsafe"
)

// SetValue sets the value as dataType at ith position from baseAddr.
func SetValue(baseAddr uintptr, i int, val unsafe.Pointer, dataType DataType) {
	idx := uintptr(i)
	unitBits := DataTypeBits(dataType)

	switch unitBits {
	case 8:
		*(*uint8)(unsafe.Pointer(baseAddr + idx)) = *(*uint8)(val)
	case 16:
		*(*uint16)(unsafe.Pointer(baseAddr + idx*2)) = *(*uint16)(val)
	case 32:
		*(*uint32)(unsafe.Pointer(baseAddr + idx*4)) = *(*uint32)(val)
	case 64:
		*(*uint64)(unsafe.Pointer(baseAddr + idx*8)) = *(*uint64)(val)
	case 128:
		*(*uint64)(unsafe.Pointer(baseAddr + idx*16)) = *(*uint64)(val)
		*(*uint64)(unsafe.Pointer(baseAddr + idx*16 + 8)) = *(*uint64)(unsafe.Pointer(uintptr(val) + 8))
	}
}

// SetBool sets the value as bool at ith position from baseAddr.
func SetBool(baseAddr uintptr, i int, val bool) {
	wordOffset := uintptr(i / 32 * 4)
	localBit := uint(i % 32)
	wordAddr := (*uint32)(unsafe.Pointer(baseAddr + wordOffset))

	if val {
		*wordAddr |= 1 << localBit
	} else {
		*wordAddr &^= 1 << localBit
	}
}

// GetValue reads value as dataType at ith position from baseAddr
func GetValue(baseAddr uintptr, i int, dataType DataType) unsafe.Pointer {
	unitBits := DataTypeBits(dataType)
	return unsafe.Pointer(baseAddr + uintptr(unitBits/8*i))
}

// GetBool reads the value as bool at ith position from baseAddr.
func GetBool(baseAddr uintptr, i int) bool {
	wordOffset := uintptr(i / 32 * 4)
	localBit := uint(i % 32)
	wordAddr := (*uint32)(unsafe.Pointer(baseAddr + wordOffset))
	return *wordAddr&(1<<localBit) != 0
}
