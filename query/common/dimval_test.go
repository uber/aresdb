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

package common

import (
	memCom "code.uber.internal/data/ares/memstore/common"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"time"
	"unsafe"
)

var _ = ginkgo.Describe("dimval", func() {
	ginkgo.It("GetDimensionStartOffsets should work", func() {
		numDimsPerDimWidth := DimCountsPerDimWidth{0, 0, 1, 1, 1}
		valueOffset, nullsOffset := GetDimensionStartOffsets(numDimsPerDimWidth, 0, 10)
		Ω([2]int{valueOffset, nullsOffset}).Should(Equal([2]int{0, 70}))
		valueOffset, nullsOffset = GetDimensionStartOffsets(numDimsPerDimWidth, 1, 10)
		Ω([2]int{valueOffset, nullsOffset}).Should(Equal([2]int{40, 80}))
		valueOffset, nullsOffset = GetDimensionStartOffsets(numDimsPerDimWidth, 2, 10)
		Ω([2]int{valueOffset, nullsOffset}).Should(Equal([2]int{60, 90}))

		numDimsPerDimWidth = DimCountsPerDimWidth{0, 0, 0, 1, 1}
		valueOffset, nullsOffset = GetDimensionStartOffsets(numDimsPerDimWidth, 0, 10)
		Ω([2]int{valueOffset, nullsOffset}).Should(Equal([2]int{0, 30}))
		valueOffset, nullsOffset = GetDimensionStartOffsets(numDimsPerDimWidth, 1, 10)
		Ω([2]int{valueOffset, nullsOffset}).Should(Equal([2]int{20, 40}))
		valueOffset, nullsOffset = GetDimensionStartOffsets(numDimsPerDimWidth, 2, 10)
		Ω([2]int{valueOffset, nullsOffset}).Should(Equal([2]int{30, 50}))

		numDimsPerDimWidth = DimCountsPerDimWidth{0, 0, 0, 0, 1}
		valueOffset, nullsOffset = GetDimensionStartOffsets(numDimsPerDimWidth, 0, 10)
		Ω([2]int{valueOffset, nullsOffset}).Should(Equal([2]int{0, 10}))
		valueOffset, nullsOffset = GetDimensionStartOffsets(numDimsPerDimWidth, 1, 10)
		Ω([2]int{valueOffset, nullsOffset}).Should(Equal([2]int{10, 20}))
		valueOffset, nullsOffset = GetDimensionStartOffsets(numDimsPerDimWidth, 2, 10)
		Ω([2]int{valueOffset, nullsOffset}).Should(Equal([2]int{10, 30}))
	})

	ginkgo.It("ReadDimension should work", func() {
		dimensionData := []byte{
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0, 0, 0, 0, 0, // dim0
			0, 0, 0, 0, 0, 0, 0, 0,
			2, 0, 0, 0, 0, 0, 0, 0,
			0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0, // dim1
			0, 0, 0, 0,
			1, 0, 0, 0,
			0xFF, 0xFF, 0xFF, 0xFF, // dim2
			0, 0,
			2, 0,
			2, 2, // dim3
			0, 2, 3, // dim4
			0, 1, 1, // dim0
			0, 1, 1, // dim1
			0, 1, 1, // dim2
			0, 1, 1, // dim3
			0, 1, 1, // dim4
		}

		enumReverseDict := []string{"a", "b", "c"}
		dimValueVector := unsafe.Pointer(&dimensionData[0])
		dimNullVector := unsafe.Pointer(&dimensionData[3*(16+8+4+2+1)])

		Ω(ReadDimension(memAccess(dimValueVector, 0),
			memAccess(dimNullVector, 0), 0, memCom.UUID, nil, TimeDimensionMeta{}, nil)).Should(BeNil())
		Ω(*ReadDimension(memAccess(dimValueVector, 0),
			memAccess(dimNullVector, 0), 1, memCom.UUID, nil, TimeDimensionMeta{}, nil)).Should(Equal("01000000-0000-0000-0000-000000000000"))
		Ω(*ReadDimension(memAccess(dimValueVector, 0),
			memAccess(dimNullVector, 0), 2, memCom.UUID, nil, TimeDimensionMeta{}, nil)).Should(Equal("ffffffff-ffff-ffff-0000-000000000000"))

		Ω(ReadDimension(memAccess(dimValueVector, 0+3*16),
			memAccess(dimNullVector, 3), 0, memCom.Int64, nil, TimeDimensionMeta{}, nil)).Should(BeNil())
		Ω(*ReadDimension(memAccess(dimValueVector, 0+3*16),
			memAccess(dimNullVector, 3), 1, memCom.Int64, nil, TimeDimensionMeta{}, nil)).Should(Equal("2"))
		Ω(*ReadDimension(memAccess(dimValueVector, 0+3*16),
			memAccess(dimNullVector, 3), 2, memCom.Int64, nil, TimeDimensionMeta{}, nil)).Should(Equal("4294967295"))

		Ω(ReadDimension(memAccess(dimValueVector, 48+3*8),
			memAccess(dimNullVector, 6), 0, memCom.Uint32, nil, TimeDimensionMeta{}, nil)).Should(BeNil())
		Ω(*ReadDimension(memAccess(dimValueVector, 48+3*8),
			memAccess(dimNullVector, 6), 1, memCom.Uint32, nil, TimeDimensionMeta{}, nil)).Should(Equal("1"))
		Ω(*ReadDimension(memAccess(dimValueVector, 48+3*8),
			memAccess(dimNullVector, 6), 2, memCom.Uint32, nil, TimeDimensionMeta{}, nil)).Should(Equal("4294967295"))

		Ω(ReadDimension(memAccess(dimValueVector, 72+3*4),
			memAccess(dimNullVector, 9), 0, memCom.Int16, nil, TimeDimensionMeta{}, nil)).Should(BeNil())
		Ω(*ReadDimension(memAccess(dimValueVector, 72+3*4),
			memAccess(dimNullVector, 9), 1, memCom.Int16, nil, TimeDimensionMeta{}, nil)).Should(Equal("2"))
		Ω(*ReadDimension(memAccess(dimValueVector, 72+3*4),
			memAccess(dimNullVector, 9), 2, memCom.Int16, nil, TimeDimensionMeta{}, nil)).Should(Equal("514"))

		Ω(ReadDimension(memAccess(dimValueVector, 84+3*2),
			memAccess(dimNullVector, 12), 0, memCom.Uint8, enumReverseDict, TimeDimensionMeta{}, nil)).Should(BeNil())
		Ω(*ReadDimension(memAccess(dimValueVector, 84+3*2),
			memAccess(dimNullVector, 12), 1, memCom.Uint8, enumReverseDict, TimeDimensionMeta{}, nil)).Should(Equal("c"))
		Ω(*ReadDimension(memAccess(dimValueVector, 84+3*2),
			memAccess(dimNullVector, 12), 2, memCom.Uint8, enumReverseDict, TimeDimensionMeta{}, nil)).Should(Equal("3"))
	})

	ginkgo.It("Timezone offset should work", func() {
		dimensionData := []byte{
			0, 0, 0, 0,
			1, 0, 0, 0,
			0xFF, 0xFF, 0xFF, 0xFF, // dim0
			0, 0,
			2, 0,
			2, 2, // dim1
			0, 2, 3, // dim2
			0, 1, 1, // dim0
			0, 1, 1, // dim1
			0, 1, 1, // dim2
		}
		dimValueVector := unsafe.Pointer(&dimensionData[0])
		dimNullVector := unsafe.Pointer(&dimensionData[21])

		Ω(*ReadDimension(memAccess(dimValueVector, 0),
			memAccess(dimNullVector, 0), 1, memCom.Uint32, nil, TimeDimensionMeta{TimeBucketizer: "minute", TimeUnit: "", IsTimezoneTable: false}, nil)).Should(Equal("1970-01-01 00:00"))
		Ω(*ReadDimension(memAccess(dimValueVector, 0),
			memAccess(dimNullVector, 0), 1, memCom.Uint32, nil, TimeDimensionMeta{TimeBucketizer: "second", TimeUnit: "", IsTimezoneTable: false}, nil)).Should(Equal("1"))
		sfLoc, _ := time.LoadLocation("America/Los_Angeles")

		Ω(*ReadDimension(memAccess(dimValueVector, 0),
			memAccess(dimNullVector, 0), 1, memCom.Uint32, nil, TimeDimensionMeta{TimeBucketizer: "minute", TimeUnit: "", IsTimezoneTable: false, TimeZone: sfLoc}, nil)).Should(Equal("1970-01-01 00:00"))
		Ω(*ReadDimension(memAccess(dimValueVector, 0),
			memAccess(dimNullVector, 0), 1, memCom.Uint32, nil, TimeDimensionMeta{TimeBucketizer: "minute", TimeUnit: "second", IsTimezoneTable: false, TimeZone: sfLoc}, nil)).Should(Equal("1"))

		_, offsetInSeconds := time.Now().In(sfLoc).Zone()
		Ω(*ReadDimension(memAccess(dimValueVector, 0),
			memAccess(dimNullVector, 0), 1, memCom.Uint32, nil, TimeDimensionMeta{TimeBucketizer: "minute", TimeUnit: "", IsTimezoneTable: false, TimeZone: sfLoc}, nil)).Should(Equal("1970-01-01 00:00"))
		Ω(*ReadDimension(memAccess(dimValueVector, 0),
			memAccess(dimNullVector, 0), 1, memCom.Uint32, nil, TimeDimensionMeta{TimeBucketizer: "minute", TimeUnit: "second", IsTimezoneTable: false, TimeZone: sfLoc, FromOffset: offsetInSeconds, ToOffset: offsetInSeconds}, nil)).Should(Equal("25201"))
	})

	ginkgo.It("ReadDimension from cache should work", func() {
		dimensionData := []byte{
			0, 0, 0, 0,
			1, 0, 0, 0,
			0xFF, 0xFF, 0xFF, 0xFF, // dim0
			0, 0,
			2, 0,
			2, 2, // dim1
			0, 2, 3, // dim2
			0, 1, 1, // dim0
			0, 1, 1, // dim1
			0, 1, 1, // dim2
		}
		dimValueVector := unsafe.Pointer(&dimensionData[0])
		dimNullVector := unsafe.Pointer(&dimensionData[21])

		meta := TimeDimensionMeta{TimeBucketizer: "minute", TimeUnit: "", IsTimezoneTable: false}
		cache := map[TimeDimensionMeta]map[int64]string{
			meta: {
				1: "foo",
			},
		}
		Ω(*ReadDimension(memAccess(dimValueVector, 0),
			memAccess(dimNullVector, 0), 1, memCom.Uint32, nil, TimeDimensionMeta{TimeBucketizer: "minute", TimeUnit: "", IsTimezoneTable: false}, cache)).Should(Equal("foo"))
		Ω(cache[meta][1]).Should(Equal("foo"))
	})
})
