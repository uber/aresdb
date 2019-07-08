package list

import (
	"fmt"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"testing"
	"unsafe"
)

var _ = ginkgo.Describe("slab tests", func() {
	setUint8Val := func(addr uintptr, mp NativeMemoryPool, val uint8) {
		*(*uint8)(unsafe.Pointer(addr + mp.GetBaseAddr())) = val
	}

	getUint8Val := func(addr uintptr, mp NativeMemoryPool) uint8 {
		return *(*uint8)(unsafe.Pointer(addr + mp.GetBaseAddr()))
	}

	var nativeMP NativeMemoryPool
	ginkgo.BeforeEach(func() {
		nativeMP = NewNativeMemoryPool(nil)
	})

	ginkgo.AfterEach(func() {
		nativeMP.Destroy()
		nativeMP = nil
	})

	ginkgo.It("test basics", func() {
		s := NewArena(1, 1024, 2, nativeMP)
		Ω(s).ShouldNot(BeNil(), "expected new slab arena to work")
		a := s.Alloc(1)
		Ω(IsNilAddr(a)).ShouldNot(BeTrue(), "expected alloc to work")
		Ω(a[0]).ShouldNot(Equal(a[1]), "expected alloc to give right size buf")
		Ω(a[0]+uintptr(1+slabMemoryFooterLen)).Should(Equal(a[1]),
			"expected alloc cap to match algorithm")

		setUint8Val(a[0], nativeMP, 66)

		Ω(s.DecRef(a)).Should(BeTrue(), "expected DecRef to be the last one")

		b := s.Alloc(1)
		Ω(IsNilAddr(b)).ShouldNot(BeTrue(), "expected alloc to work")
		Ω(b[0]).ShouldNot(Equal(b[1]), "expected alloc to give right size buf")
		Ω(b[0]+uintptr(1+slabMemoryFooterLen)).Should(Equal(b[1]),
			"expected alloc cap to match algorithm")

		Ω(getUint8Val(b[0], nativeMP)).Should(BeEquivalentTo(66),
			"expected alloc to return last freed buf")
	})

	ginkgo.It("test slab class growth", func() {
		s := NewArena(1, 8, 2, nativeMP)
		expectSlabClasses := func(numSlabClasses int) {
			Ω(s.slabClasses).Should(HaveLen(numSlabClasses), "expected "+
				"slab classes not match")
		}

		expectSlabClasses(1)
		s.Alloc(1)
		expectSlabClasses(1)
		s.Alloc(1)
		expectSlabClasses(1)
		s.Alloc(2)
		expectSlabClasses(2)
		s.Alloc(1)
		s.Alloc(2)
		expectSlabClasses(2)
		s.Alloc(3)
		s.Alloc(4)
		expectSlabClasses(3)
		s.Alloc(5)
		s.Alloc(8)
		expectSlabClasses(4)
	})

	ginkgo.It("test slab class growth", func() {
		s := NewArena(1, 8, 2, nativeMP)
		expectSlabClasses := func(numSlabClasses int) {
			if len(s.slabClasses) != numSlabClasses {
				Ω(s.slabClasses).Should(HaveLen(numSlabClasses),
					"expected slab classes not match")
			}
		}
		a := make([][2]uintptr, 128)
		for j := 0; j < 100; j++ {
			for i := 0; i < len(a); i++ {
				a[i] = s.Alloc(i % 8)
			}
			for i := 0; i < len(a); i++ {
				s.DecRef(a[i])
			}
		}
		expectSlabClasses(4)
	})

	ginkgo.It("test large allocation", func() {
		s := NewArena(1, 1, 2,
			nativeMP)
		a := s.Alloc(2)
		Ω(IsNilAddr(a)).Should(BeTrue(),
			"expected alloc larger than slab size to fail")
	})

	ginkgo.It("test empty chunk", func() {
		s := NewArena(1, 1, 2,
			nativeMP)
		sc := s.slabClasses[0]
		Ω(sc.chunk(nilLoc)).Should(BeNil(),
			"expected empty chunk to not have a chunk")
		sc1, c1 := s.chunk(nilLoc)
		Ω(sc1 == nil && c1 == nil).Should(BeTrue(),
			"expected empty chunk to not have a chunk")
	})

	ginkgo.It("test empty chunk mem", func() {
		s := NewArena(1, 1, 2, nativeMP)
		sc := s.slabClasses[0]
		Ω(IsNilAddr(sc.chunkMem(nil))).Should(BeTrue(),
			"expected nil chunk to not have a chunk")
		Ω(IsNilAddr(sc.chunkMem(&chunk{self: nilLoc}))).Should(BeTrue(),
			"expected empty chunk to not have a chunk")
	})

	ginkgo.It("test empty chunk mem", func() {
		s := NewArena(1, 1, 2, nativeMP)
		sc := s.slabClasses[0]
		Ω(IsNilAddr(sc.chunkMem(nil))).Should(BeTrue(),
			"expected nil chunk to not have a chunk")
		Ω(IsNilAddr(sc.chunkMem(&chunk{self: nilLoc}))).Should(BeTrue(),
			"expected empty chunk to not have a chunk")
	})

	ginkgo.It("Test AddRef on already released buf", func() {
		s := NewArena(1, 1, 2, nativeMP)
		a := s.Alloc(1)
		s.DecRef(a)
		var err interface{}
		func() {
			defer func() { err = recover() }()
			s.AddRef(a)
		}()

		Ω(err).ShouldNot(BeNil(),
			"expected panic on AddRef on already release buf")
	})

	ginkgo.It("Test DecRef on already released buf", func() {
		s := NewArena(1, 1, 2, nativeMP)
		a := s.Alloc(1)
		s.DecRef(a)
		var err interface{}
		func() {
			defer func() { err = recover() }()
			s.DecRef(a)
		}()
		Ω(err).ShouldNot(BeNil(),
			"expected panic on DecRef on already release buf")
	})

	ginkgo.It("Test pushFreeChunk On referenced chunk", func() {
		s := NewArena(1, 1, 2, nativeMP)
		sc := s.slabClasses[0]
		var err interface{}
		func() {
			defer func() { err = recover() }()
			sc.pushFreeChunk(&chunk{refs: 1})
		}()
		Ω(err).ShouldNot(BeNil(),
			"expected panic when free'ing a ref-counted chunk")
	})

	ginkgo.It("Test popFreeChunk on free chunk", func() {
		s := NewArena(1, 1, 2, nativeMP)
		sc := s.slabClasses[0]
		sc.chunkFree = nilLoc
		var err interface{}
		func() {
			defer func() { err = recover() }()
			sc.popFreeChunk()
		}()
		Ω(err).ShouldNot(BeNil(),
			"expected panic when popFreeChunk() on free chunk")
	})

	ginkgo.It("Test popFreeChunk on referenced free chunk", func() {
		s := NewArena(1, 1024, 2, nativeMP)
		s.Alloc(1)
		sc := s.slabClasses[0]
		sc.chunk(sc.chunkFree).refs = 1
		var err interface{}
		func() {
			defer func() { err = recover() }()
			sc.popFreeChunk()
		}()
		Ω(err).ShouldNot(BeNil(),
			"expected panic when popFreeChunk() on ref'ed chunk")
	})

	ginkgo.It("Test popFreeChunk on referenced free chunk", func() {
		s := NewArena(1, 1024, 2, nativeMP)
		s.Alloc(1)
		sc := s.slabClasses[0]
		sc.chunk(sc.chunkFree).refs = 1
		var err interface{}
		func() {
			defer func() { err = recover() }()
			sc.popFreeChunk()
		}()
		Ω(err).ShouldNot(BeNil(),
			"expected panic when popFreeChunk() on ref'ed chunk")
	})

	ginkgo.It("Test Arena chunk", func() {
		s := NewArena(1, 100, 2, nativeMP)
		s.Alloc(1)
		sc := &(s.slabClasses[0])
		c := sc.popFreeChunk()
		Ω(sc.chunk(c.self)).Should(Equal(c), "expected chunk to be the same")
		sc1, c1 := s.chunk(c.self)
		Ω(sc1 != sc || c1 != c).
			ShouldNot(BeTrue(), "expected chunk to be the same")
	})

	ginkgo.It("Test Arena chunkMem", func() {
		s := NewArena(1, 100, 2, nativeMP)
		s.Alloc(1)
		sc := s.slabClasses[0]
		c := sc.popFreeChunk()
		Ω(sc.chunkMem(c)).ShouldNot(BeNil(), "expected chunkMem to be non-nil")
	})

	ginkgo.It("Test findSlabClassIndex", func() {
		s := NewArena(1, 1024, 2, nativeMP)
		test := func(bufLen, idxExp int) {
			idxAct := s.findSlabClassIndex(bufLen)
			Ω(idxAct).Should(Equal(idxExp), fmt.Sprintf("expected slab class index: %v, got: %v, bufLen: %v",
				idxExp, idxAct, bufLen))
		}
		test(0, 0)
		test(1, 0)
		test(2, 1)
		test(3, 2)
		test(4, 2)
		test(5, 3)
		test(256, 8)
	})

	ginkgo.It("Test growth factors", func() {
		for gf := 1.1; gf < 16.7; gf = gf + 0.1 {
			s := NewArena(1, 1024, gf, nativeMP)
			a := s.Alloc(1024)
			setUint8Val(a[0], nativeMP, 123)
			s.DecRef(a)
			b := s.Alloc(1024)
			Ω(getUint8Val(b[0], nativeMP)).Should(BeEquivalentTo(123), "expected re-used alloc mem")
		}
	})
})

func BenchmarkAllocingSize1(b *testing.B) {
	benchmarkAllocingConstant(b, NewArena(1, 1024, 2, NewNativeMemoryPool(nil)), 1)
}

func BenchmarkAllocingSize128(b *testing.B) {
	benchmarkAllocingConstant(b, NewArena(1, 1024, 2, NewNativeMemoryPool(nil)), 128)
}

func BenchmarkAllocingSize256(b *testing.B) {
	benchmarkAllocingConstant(b, NewArena(1, 1024, 2, NewNativeMemoryPool(nil)), 256)
}

func benchmarkAllocingConstant(b *testing.B, a *Arena, allocSize int) {
	var stuff [][2]uintptr
	for i := 0; i < 1024; i++ {
		stuff = append(stuff, a.Alloc(allocSize))
	}
	for _, x := range stuff {
		a.DecRef(x)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.DecRef(a.Alloc(allocSize))
	}
}

func BenchmarkAllocingModSizes(b *testing.B) {
	benchmarkAllocingFunc(b, NewArena(1, 1024, 2, NewNativeMemoryPool(nil)),
		func(i int) int { return i % 1024 })
}

func BenchmarkAllocingModSizesGrowthFactor1Dot1(b *testing.B) {
	benchmarkAllocingFunc(b, NewArena(1, 1024, 1.1, NewNativeMemoryPool(nil)),
		func(i int) int { return i % 1024 })
}

func benchmarkAllocingFunc(b *testing.B, a *Arena,
	allocSize func(i int) int) {
	var stuff [][2]uintptr
	for i := 0; i < 1024; i++ {
		stuff = append(stuff, a.Alloc(allocSize(i)))
	}
	for _, x := range stuff {
		a.DecRef(x)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.DecRef(a.Alloc(allocSize(i)))
	}
}
