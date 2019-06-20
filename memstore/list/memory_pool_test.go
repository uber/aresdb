package list

import (
	"fmt"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"math/rand"
	"reflect"
	"testing"
	"time"
	"unsafe"
)

var (
	maxArraySize    = 20
	batchSize       = 2000000 // 2M
	nUpdate         = batchSize * 4
	maxElementValue = 20000
	nRandomArr      = 20000
	validateArray   = false
)

var _0 = ginkgo.Describe("HighLevelMemoryPool tests", func() {
	setUint8Val := func(addr uintptr, val uint8) {
		*(*uint8)(unsafe.Pointer(addr)) = val
	}

	getUint8Val := func(addr uintptr) uint8 {
		return *(*uint8)(unsafe.Pointer(addr))
	}

	var mp HighLevelMemoryPool
	ginkgo.BeforeEach(func() {
		mp = NewHighLevelMemoryPool(nil)
	})

	ginkgo.AfterEach(func() {
		mp.Destroy()
	})

	ginkgo.It("test basics", func() {
		addr := mp.Allocate(10)
		Ω(IsNilAddr(addr)).Should(BeFalse(), "Allocate should return non nil addr")

		setUint8Val(mp.Interpret(addr[0]), 11)
		Ω(getUint8Val(mp.Interpret(addr[0]))).Should(BeEquivalentTo(11))

		Ω(func() { mp.Free(addr) }).ShouldNot(Panic())

		newAddr := mp.Allocate(10)
		Ω(newAddr).Should(Equal(newAddr), "Allocate after free should return the same address")
	})

	ginkgo.It("test reallocation", func() {
		var oldBuf [2]uintptr
		var oldSize, newSize int
		// both oldBuf and newBuf are nils, no allocation happens
		Ω(IsNilAddr(oldBuf)).Should(BeTrue())
		newBuf := mp.Reallocate(oldBuf, oldSize, newSize)
		Ω(IsNilAddr(newBuf)).Should(BeTrue())

		// oldBuf nil, newBuf non nil, allocation happens
		newSize = 11
		newBuf = mp.Reallocate(oldBuf, oldSize, newSize)
		Ω(IsNilAddr(newBuf)).Should(BeFalse())
		Ω(newBuf).ShouldNot(Equal(oldBuf))

		// Both non nil and sizes are equal, no allocation happens
		oldSize = newSize
		oldBuf = newBuf
		newBuf = mp.Reallocate(oldBuf, oldSize, newSize)
		Ω(IsNilAddr(newBuf)).Should(BeFalse())
		Ω(newBuf).Should(Equal(oldBuf))

		// Sizes are not equal, allocation happens and
		// old memory is freed
		newSize = 12
		newBuf = mp.Reallocate(oldBuf, oldSize, newSize)
		Ω(IsNilAddr(newBuf)).Should(BeFalse())
	})
})

var _1 = ginkgo.Describe("NativeMemoryPool tests", func() {
	var nativeMP NativeMemoryPool
	ginkgo.BeforeEach(func() {
		nativeMP = NewNativeMemoryPool(nil)
	})

	ginkgo.AfterEach(func() {
		nativeMP.Destroy()
		nativeMP = nil
	})

	setUint8Val := func(addr uintptr, val uint8) {
		*(*uint8)(unsafe.Pointer(addr)) = val
	}

	getUint8Val := func(addr uintptr) uint8 {
		return *(*uint8)(unsafe.Pointer(addr))
	}

	ginkgo.It("test basics", func() {
		// Before first allocation, everything should be zero
		nativeMPImpl := (nativeMP).(*singleChunkNativeMemoryPool)
		Ω(nativeMPImpl.block).Should(BeZero())
		Ω(nativeMPImpl.allocatedSize).Should(BeZero())
		Ω(nativeMPImpl.totalSize).Should(BeZero())
		Ω(nativeMPImpl.nChunks).Should(BeZero())

		Ω(nativeMP.GetBaseAddr()).Should(BeZero())

		var totalMemory int64
		nativeMPImpl.hostMemoryReporter = func(bytes int64) {
			totalMemory = bytes
		}

		// First allocation should allocate a new chunk and return a zero offset.
		addr := nativeMP.Malloc(12)
		Ω(addr).Should(BeZero())
		Ω(nativeMPImpl.block).ShouldNot(BeZero())
		Ω(nativeMPImpl.allocatedSize).Should(Equal(12))
		Ω(nativeMPImpl.totalSize).Should(Equal(nativeChunkSize))
		Ω(nativeMPImpl.nChunks).Should(Equal(1))

		Ω(nativeMP.GetBaseAddr()).ShouldNot(BeZero())

		// test memory reporting
		Ω(totalMemory).Should(BeEquivalentTo(nativeChunkSize))

		// Allocate another memory within current chunk.
		Ω(nativeMP.Malloc(12)).Should(BeEquivalentTo(12))
		Ω(nativeMPImpl.allocatedSize).Should(Equal(24))
		Ω(nativeMPImpl.totalSize).Should(Equal(nativeChunkSize))
		Ω(nativeMPImpl.nChunks).Should(Equal(1))

		// set val at offset 0
		setUint8Val(addr+nativeMP.GetBaseAddr(), 11)
		oldBaseAddr := nativeMP.GetBaseAddr()
		// Allocate more memory that cannot fit in current chunk, old memory content
		// should be carried over.
		Ω(nativeMP.Malloc(nativeChunkSize)).Should(BeEquivalentTo(24))
		Ω(nativeMPImpl.allocatedSize).Should(Equal(24 + nativeChunkSize))
		Ω(nativeMPImpl.totalSize).Should(Equal(2 * nativeChunkSize))
		Ω(nativeMPImpl.nChunks).Should(Equal(2))

		Ω(nativeMP.GetBaseAddr()).ShouldNot(Equal(oldBaseAddr))
		Ω(getUint8Val(nativeMP.GetBaseAddr())).Should(BeEquivalentTo(11))

		Ω(totalMemory).Should(BeEquivalentTo(nativeChunkSize * 2))

		nativeMP.Destroy()
		Ω(totalMemory).Should(BeZero())
	})

	ginkgo.It("test large allocation", func() {
		Ω(nativeMP.Malloc(nativeChunkSize * 3)).Should(BeZero())

		tailAddr := uintptr(3*nativeChunkSize - 1)
		setUint8Val(nativeMP.GetBaseAddr()+tailAddr, 11)
		Ω(getUint8Val(nativeMP.GetBaseAddr() + tailAddr)).Should(BeEquivalentTo(11))
	})
})

func BenchmarkIntArray(b *testing.B) {
	mp := NewHighLevelMemoryPool(nil)
	defer mp.Destroy()

	rand.Seed(time.Now().UnixNano())
	stuff := make([][2]uintptr, batchSize)
	size := make([]int, batchSize)

	setRow := func(arr []int32, row int) {
		oldSize := size[row]
		newSize := len(arr)
		stuff[row] = mp.Reallocate(stuff[row], oldSize*4, newSize*4)
		size[row] = newSize

		for i := 0; i < newSize; i++ {
			*(*int32)(unsafe.Pointer(mp.Interpret(stuff[row][0]) + uintptr(4*i))) = arr[i]
		}
	}

	getRow := func(row int) []int32 {
		size := size[row]
		addr := stuff[row][0]

		if size == 0 {
			return nil
		}

		header := reflect.SliceHeader{
			Data: mp.Interpret(addr),
			Len:  size,
			Cap:  size,
		}

		return *(*[]int32)(unsafe.Pointer(&header))
	}

	randomArrs := make([][]int32, nRandomArr)
	for i := 0; i < nRandomArr; i++ {
		randomArrs[i] = generateRandomArray()
	}

	b.Run(fmt.Sprintf("Setting %d rows with %d updates ", batchSize, nUpdate), func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			row := rand.Intn(batchSize)
			arr := randomArrs[rand.Intn(nRandomArr)]

			setRow(arr, row)
			if validateArray {
				assertArrayEqual(getRow(row), arr, b)
			}
		}
	})
}

func generateRandomArray() []int32 {
	arrSize := rand.Intn(maxArraySize)
	arr := make([]int32, arrSize)

	for i := 0; i < arrSize; i++ {
		arr[i] = int32(rand.Intn(maxElementValue))
	}
	return arr
}

func assertArrayEqual(l []int32, r []int32, b *testing.B) {
	if len(l) != len(r) {
		b.Fatal("Arr length not equal")
	}

	for i := 0; i < len(l); i++ {
		if l[i] != r[i] {
			b.Fatalf("Element at %d not equal: l %d, r %d", i, l[i], r[i])
		}
	}
}
