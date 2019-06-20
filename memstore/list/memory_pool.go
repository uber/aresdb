package list

// #include "string.h"
// #include "stdlib.h"
import "C"
import (
	"github.com/uber/aresdb/cgoutils"
	"github.com/uber/aresdb/utils"
	"unsafe"
)

var (
	nativeChunkSize                    = 1 << 25 // 32MB
	defaultStartSlabAllocatorChunkSize = 16
	defaultSlabSize                    = 1024
	defaultSlabGrowthFactor            = 1.5
)

type HostMemoryReporter func(bytes int64);

// HighLevelMemoryPool manages memory requests on pooled memory. It underlying uses a
// slab allocator to manage free memory chunks. When it no longer can satisfy memory
// allocation request from customer, it will request more memory from underlying
// NativeMemoryPool. All address returned back to client are an 2 element array of offset
// where the first offset is the memory allocated to the caller and second offset is the
// footer offset to current slab. Note that allocate a memory chunk larger than slabSize
// will fail. For more information related how slab allocator works, please refer to
// https://github.com/couchbase/go-slab.
type HighLevelMemoryPool interface {
	// Allocate allocates size byte memory and return back to client.
	Allocate(size int) [2]uintptr
	// Reallocate reallocates memory according to the size of the old allocated
	// memory and size of new allocation requests. If the size is the same, it
	// does nothing and just return the old addr. Otherwise it will allocate a new
	// memory, copy the old content to it if oldSize is non-zero and returned back to
	// client. If the oldSize is not zero, it will also free the old memory.
	Reallocate(oldBuf [2]uintptr, oldSize int, newSize int) [2]uintptr
	// Return the memory back to memory pool.
	Free(buf [2]uintptr)
	// Return the actual memory address given offset.
	Interpret(offset uintptr) uintptr
	// Release the underlying memory.
	Destroy()
}

// slabMemoryPool implements the HighLevelMemoryPool using slab algorithm.
type slabMemoryPool struct {
	slabAllocator    *Arena
	nativeMemoryPool NativeMemoryPool
}

// NewHighLevelMemoryPool returns a default implementation of HighLevelMemoryPool.
func NewHighLevelMemoryPool(reporter HostMemoryReporter) HighLevelMemoryPool {
	nativeMP := NewNativeMemoryPool(reporter)
	slabAllocator :=
		NewArena(defaultStartSlabAllocatorChunkSize, defaultSlabSize, defaultSlabGrowthFactor, nativeMP)
	return slabMemoryPool{
		slabAllocator:    slabAllocator,
		nativeMemoryPool: nativeMP,
	}
}

// Allocate is the implementation of Allocate in HighLevelMemoryPool interface.
func (mp slabMemoryPool) Allocate(size int) [2]uintptr {
	if size == 0 {
		return [2]uintptr{0, 0}
	}
	return mp.slabAllocator.Alloc(size)
}

// Reallocate is the implementation of Reallocate in HighLevelMemoryPool interface.
func (mp slabMemoryPool) Reallocate(oldBuf [2]uintptr, oldSize int, newSize int) [2]uintptr {
	if oldSize == newSize {
		return oldBuf
	}

	if oldSize != 0 {
		mp.Free(oldBuf)
	}

	return mp.Allocate(newSize)
}

// Free is the implementation of Free in HighLevelMemoryPool interface.
func (mp slabMemoryPool) Free(buf [2]uintptr) {
	if !mp.slabAllocator.DecRef(buf) {
		utils.GetLogger().Panic("buf should be freed but not")
	}
}

// Interpret is the implementation of Interpret in HighLevelMemoryPool interface.
func (mp slabMemoryPool) Interpret(offset uintptr) uintptr {
	return offset + mp.nativeMemoryPool.GetBaseAddr()
}

// Destroy is the implementation of Free in HighLevelMemoryPool interface.
func (mp slabMemoryPool) Destroy() {
	mp.nativeMemoryPool.Destroy()
	mp.nativeMemoryPool = nil
}

// NativeMemoryPool is the interface to manage system memory to support high level memory pool
// allocation requests. All the pointer/address returned by this memory pool is relative to the
// base address fetched via GetBaseAddr.
type NativeMemoryPool interface {
	// Malloc returns a byte slice to caller, will allocate more memoryOffset if no enough space.
	// returned addresses are not aligned.
	Malloc(size int) uintptr
	// Destroy frees the memory managed by this memory pool. After destroy, any malloc call's
	// behaviour will be undefined.
	Destroy()
	// GetBaseAddr returns the base address managed by this pool.
	GetBaseAddr() uintptr
}

// singleChunkNativeMemoryPool manages the system memory as a single chunk of continuous memory address.
// When this memory pool can no longer satisfy allocation requests, it will allocate another memory chunk
// big enough to hold the old memory content while satisfying the new requests. Then it will make a memcpy
// to move the old data into the new location. A small performance penalty will be paid during this period.
type singleChunkNativeMemoryPool struct {
	block              uintptr
	allocatedSize      int
	totalSize          int
	nChunks            int
	hostMemoryReporter HostMemoryReporter
}

// GetBaseAddr is the implementation of GetBaseAddr in NativeMemoryPool interface.
func (mp *singleChunkNativeMemoryPool) GetBaseAddr() uintptr {
	return mp.block
}

// Malloc is the implementation of Malloc in NativeMemoryPool interface.
func (mp *singleChunkNativeMemoryPool) Malloc(size int) uintptr {
	remainingSize := mp.totalSize - mp.allocatedSize
	if size > remainingSize {
		newSize := (mp.allocatedSize + size + nativeChunkSize - 1) / nativeChunkSize * nativeChunkSize
		if mp.hostMemoryReporter != nil {
			mp.hostMemoryReporter(int64(newSize))
		}
		newBlock := uintptr(cgoutils.HostAlloc(newSize))
		// copy old content.
		if mp.block != 0 {
			cgoutils.HostMemCpy(unsafe.Pointer(newBlock), unsafe.Pointer(mp.block), mp.totalSize)
			cgoutils.HostFree(unsafe.Pointer(mp.block))
		}
		mp.block = newBlock
		mp.totalSize = newSize
		mp.nChunks = mp.totalSize / nativeChunkSize
	}
	addr := uintptr(mp.allocatedSize)
	mp.allocatedSize += size
	return addr
}

// Destroy is the implementation of Destroy in NativeMemoryPool interface.
func (mp *singleChunkNativeMemoryPool) Destroy() {
	cgoutils.HostFree(unsafe.Pointer(mp.block))
	if mp.hostMemoryReporter != nil {
		mp.hostMemoryReporter(0)
	}
	mp.block = 0
	mp.totalSize = 0
	mp.allocatedSize = 0
	mp.nChunks = 0
}

// NewNativeMemoryPool returns a default implementation of NativeMemoryPool.
func NewNativeMemoryPool(reporter HostMemoryReporter) NativeMemoryPool {
	return &singleChunkNativeMemoryPool{
		hostMemoryReporter: reporter,
	}
}
