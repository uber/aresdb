//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package list

// #include "string.h"
import "C"
import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"unsafe"
)

// An opaque reference to bytes managed by an Arena.  See
// Arena.BufToLoc/LocToBuf().  A Loc struct is GC friendly in that a
// Loc does not have direct pointer fields into the Arena's memoryOffset
// that the GC's scanner must traverse.
type Loc struct {
	slabClassIndex int
	slabIndex      int
	chunkIndex     int
	bufStart       int
	bufLen         int
}

// NilLoc returns a Loc where Loc.IsNil() is true.
func NilLoc() Loc {
	return nilLoc
}

var nilLoc = Loc{-1, -1, -1, -1, -1} // A sentinel.
var nilAddr = [2]uintptr{0, 0}

func IsNilAddr(addr [2]uintptr) bool {
	return addr == nilAddr
}

// IsNil returns true if the Loc came from NilLoc().
func (cl Loc) IsNil() bool {
	return cl.slabClassIndex < 0 && cl.slabIndex < 0 &&
		cl.chunkIndex < 0 && cl.bufStart < 0 && cl.bufLen < 0
}

// An Arena manages a set of slab classes and memoryOffset.
type Arena struct {
	mp           NativeMemoryPool
	growthFactor float64
	slabClasses  []slabClass // slabClasses's chunkSizes grow by growthFactor.
	slabMagic    int32       // Magic # suffix on each slab memoryOffset []byte.
	slabSize     int

	totAllocs           int64
	totAddRefs          int64
	totDecRefs          int64
	totDecRefZeroes     int64 // Inc'ed when a ref-count reaches zero.
	totGetNexts         int64
	totSetNexts         int64
	totMallocs          int64
	totMallocErrs       int64
	totTooBigErrs       int64
	totAddSlabErrs      int64
	totPushFreeChunks   int64 // Inc'ed when chunk added to free list.
	totPopFreeChunks    int64 // Inc'ed when chunk removed from free list.
	totPopFreeChunkErrs int64
}

type slabClass struct {
	slabs     []*slab // A growing array of slabs.
	chunkSize int     // Each slab is sliced into fixed-sized chunks.
	chunkFree Loc     // Chunks are tracked in a free-list per slabClass.

	numChunks     int64
	numChunksFree int64
}

type slab struct {
	// offset to arena.baseAddr
	memoryOffset uintptr
	// length of the memory allocated to this slab.
	length int
	// Matching array of chunk metadata, and len(memoryOffset) == len(chunks).
	chunks []chunk
}

// Based on slabClassIndex + slabIndex + slabMagic.
const slabMemoryFooterLen int = 4 + 4 + 4

type chunk struct {
	refs int32 // Ref-count.
	self Loc   // The self is the Loc for this chunk.
	next Loc   // Used when chunk is in the free-list or when chained.
}

// NewArena returns an Arena to manage byte slice memoryOffset based on a
// slab allocator approach.
//
// The startChunkSize and slabSize should be > 0.
// The growthFactor should be > 1.0.
func NewArena(startChunkSize int, slabSize int, growthFactor float64,
	mp NativeMemoryPool) *Arena {
	s := &Arena{
		growthFactor: growthFactor,
		slabMagic:    rand.Int31(),
		slabSize:     slabSize,
		mp:           mp,
	}
	s.addSlabClass(startChunkSize)
	return s
}

// Alloc may return nil on errors, such as if no more free chunks are
// available and new slab memoryOffset was not allocatable (such as if
// malloc() returns nil).  The returned buf may not be append()'ed to
// for growth.  The returned buf must be DecRef()'ed for memoryOffset reuse.
func (s *Arena) Alloc(bufLen int) [2]uintptr {
	sc, chunk := s.allocChunk(bufLen)
	if sc == nil || chunk == nil {
		return [2]uintptr{0, 0}
	}
	return sc.chunkMem(chunk)
}

// Owns returns true if this Arena owns the buf.
func (s *Arena) Owns(offsets [2]uintptr) bool {
	sc, c := s.bufChunk(offsets)
	return sc != nil && c != nil
}

// AddRef increase the ref count on a buf.  The input buf must be from
// an Alloc() from the same Arena.
func (s *Arena) AddRef(offsets [2]uintptr) {
	s.totAddRefs++
	sc, c := s.bufChunk(offsets)
	if sc == nil || c == nil {
		panic("buf not from this arena")
	}
	c.addRef()
}

// DecRef decreases the ref count on a buf.  The input buf must be
// from an Alloc() from the same Arena.  Once the buf's ref-count
// drops to 0, the Arena may reuse the buf.  Returns true if this was
// the last DecRef() invocation (ref count reached 0).
func (s *Arena) DecRef(buf [2]uintptr) bool {
	s.totDecRefs++
	sc, c := s.bufChunk(buf)
	if sc == nil || c == nil {
		panic("buf not from this arena")
	}
	return s.decRef(sc, c)
}

// ---------------------------------------------------------------

func (s *Arena) allocChunk(bufLen int) (*slabClass, *chunk) {
	s.totAllocs++

	if bufLen > s.slabSize {
		s.totTooBigErrs++
		return nil, nil
	}

	slabClassIndex := s.findSlabClassIndex(bufLen)
	sc := &(s.slabClasses[slabClassIndex])
	if sc.chunkFree.IsNil() {
		if !s.addSlab(slabClassIndex, s.slabSize, s.slabMagic) {
			s.totAddSlabErrs++
			return nil, nil
		}
	}

	s.totPopFreeChunks++
	chunk := sc.popFreeChunk()
	if chunk == nil {
		s.totPopFreeChunkErrs++
		return nil, nil
	}

	return sc, chunk
}

func (s *Arena) findSlabClassIndex(bufLen int) int {
	i := sort.Search(len(s.slabClasses),
		func(i int) bool { return bufLen <= s.slabClasses[i].chunkSize })
	if i >= len(s.slabClasses) {
		for {
			slabClass := &(s.slabClasses[len(s.slabClasses)-1])
			nextChunkSize := int(
				math.Ceil(float64(slabClass.chunkSize) * s.growthFactor))
			s.addSlabClass(nextChunkSize)
			if bufLen <= nextChunkSize {
				return len(s.slabClasses) - 1
			}
		}
	}
	return i
}

func (s *Arena) addSlabClass(chunkSize int) {
	s.slabClasses = append(s.slabClasses, slabClass{
		chunkSize: chunkSize,
		chunkFree: nilLoc,
	})
}

func (s *Arena) addSlab(
	slabClassIndex, slabSize int, slabMagic int32) bool {
	sc := &(s.slabClasses[slabClassIndex])

	chunksPerSlab := slabSize / sc.chunkSize
	if chunksPerSlab <= 0 {
		chunksPerSlab = 1
	}

	slabIndex := len(sc.slabs)

	s.totMallocs++
	// Re-multiplying to avoid any extra fractional chunk memoryOffset.
	memorySize := (sc.chunkSize * chunksPerSlab) + slabMemoryFooterLen
	memoryOffset := s.mp.Malloc(memorySize)
	slab := &slab{
		memoryOffset: uintptr(memoryOffset),
		length:       memorySize,
		chunks:       make([]chunk, chunksPerSlab),
	}

	memoryAddr := s.mp.GetBaseAddr() + slab.memoryOffset + uintptr(slab.length-slabMemoryFooterLen)
	*(*uint32)(unsafe.Pointer(memoryAddr)) = uint32(slabClassIndex)
	*(*uint32)(unsafe.Pointer(memoryAddr + uintptr(4))) = uint32(slabIndex)
	*(*uint32)(unsafe.Pointer(memoryAddr + uintptr(8))) = uint32(slabMagic)
	sc.slabs = append(sc.slabs, slab)

	for i := 0; i < len(slab.chunks); i++ {
		c := &(slab.chunks[i])
		c.self.slabClassIndex = slabClassIndex
		c.self.slabIndex = slabIndex
		c.self.chunkIndex = i
		c.self.bufStart = 0
		c.self.bufLen = sc.chunkSize
		sc.pushFreeChunk(c)
	}
	sc.numChunks += int64(len(slab.chunks))
	return true
}

func (sc *slabClass) pushFreeChunk(c *chunk) {
	if c.refs != 0 {
		panic(fmt.Sprintf("pushFreeChunk() non-zero refs: %v", c.refs))
	}
	c.next = sc.chunkFree
	sc.chunkFree = c.self
	sc.numChunksFree++
}

func (sc *slabClass) popFreeChunk() *chunk {
	if sc.chunkFree.IsNil() {
		panic("popFreeChunk() when chunkFree is nil")
	}
	c := sc.chunk(sc.chunkFree)
	if c.refs != 0 {
		panic(fmt.Sprintf("popFreeChunk() non-zero refs: %v", c.refs))
	}
	c.refs = 1
	sc.chunkFree = c.next
	c.next = nilLoc
	sc.numChunksFree--
	if sc.numChunksFree < 0 {
		panic("popFreeChunk() got < 0 numChunksFree")
	}
	return c
}

// chunkMem returns the offset of the memory address along with the chunk end offset.
// zero footer offset means invalid block.
func (sc *slabClass) chunkMem(c *chunk) [2]uintptr {
	if c == nil || c.self.IsNil() {
		return [2]uintptr{0, 0}
	}
	beg := uintptr(sc.chunkSize * c.self.chunkIndex)
	offset := sc.slabs[c.self.slabIndex].memoryOffset + beg
	chuckEndOffset := sc.slabs[c.self.slabIndex].memoryOffset + uintptr(sc.slabs[c.self.slabIndex].length)
	return [2]uintptr{offset, chuckEndOffset}
}

func (sc *slabClass) chunk(cl Loc) *chunk {
	if cl.IsNil() {
		return nil
	}
	return &(sc.slabs[cl.slabIndex].chunks[cl.chunkIndex])
}

func (s *Arena) chunk(cl Loc) (*slabClass, *chunk) {
	if cl.IsNil() {
		return nil, nil
	}
	sc := &(s.slabClasses[cl.slabClassIndex])
	return sc, sc.chunk(cl)
}

// Determine the slabClass & chunk for an Arena managed buf []byte.
func (s *Arena) bufChunk(offsets [2]uintptr) (*slabClass, *chunk) {
	offset := offsets[0]
	endOffset := offsets[1]
	if int(endOffset-offset) <= slabMemoryFooterLen {
		return nil, nil
	}

	footerDist := endOffset - offset - uintptr(slabMemoryFooterLen)
	footerAddr := s.mp.GetBaseAddr() + offset + footerDist

	slabClassIndex := *(*uint32)(unsafe.Pointer(footerAddr))
	slabIndex := *(*uint32)(unsafe.Pointer(footerAddr + uintptr(4)))
	slabMagic := *(*uint32)(unsafe.Pointer(footerAddr + uintptr(8)))
	if slabMagic != uint32(s.slabMagic) {
		return nil, nil
	}

	sc := &(s.slabClasses[slabClassIndex])
	slab := sc.slabs[slabIndex]
	chunkIndex := len(slab.chunks) -
		int(math.Ceil(float64(footerDist)/float64(sc.chunkSize)))
	return sc, &(slab.chunks[chunkIndex])
}

func (c *chunk) addRef() *chunk {
	c.refs++
	if c.refs <= 1 {
		panic(fmt.Sprintf("refs <= 1 during addRef: %#v", c))
	}
	return c
}

func (s *Arena) decRef(sc *slabClass, c *chunk) bool {
	c.refs--
	if c.refs < 0 {
		panic(fmt.Sprintf("refs < 0 during decRef: %#v", c))
	}
	if c.refs == 0 {
		s.totDecRefZeroes++
		scNext, cNext := s.chunk(c.next)
		if scNext != nil && cNext != nil {
			s.decRef(scNext, cNext)
		}
		c.next = nilLoc
		s.totPushFreeChunks++
		sc.pushFreeChunk(c)
		return true
	}
	return false
}

// Stats fills an input map with runtime metrics about the Arena.
func (s *Arena) Stats(m map[string]int64) map[string]int64 {
	m["totSlabClasses"] = int64(len(s.slabClasses))
	m["totAllocs"] = s.totAllocs
	m["totAddRefs"] = s.totAddRefs
	m["totDecRefs"] = s.totDecRefs
	m["totDecRefZeroes"] = s.totDecRefZeroes
	m["totGetNexts"] = s.totGetNexts
	m["totSetNexts"] = s.totSetNexts
	m["totMallocs"] = s.totMallocs
	m["totMallocErrs"] = s.totMallocErrs
	m["totTooBigErrs"] = s.totTooBigErrs
	m["totAddSlabErrs"] = s.totAddSlabErrs
	m["totPushFreeChunks"] = s.totPushFreeChunks
	m["totPopFreeChunks"] = s.totPopFreeChunks
	m["totPopFreeChunkErrs"] = s.totPopFreeChunkErrs
	for i, sc := range s.slabClasses {
		prefix := fmt.Sprintf("slabClass-%06d-", i)
		m[prefix+"numSlabs"] = int64(len(sc.slabs))
		m[prefix+"chunkSize"] = int64(sc.chunkSize)
		m[prefix+"numChunks"] = int64(sc.numChunks)
		m[prefix+"numChunksFree"] = int64(sc.numChunksFree)
		m[prefix+"numChunksInUse"] = int64(sc.numChunks - sc.numChunksFree)
	}
	return m
}
