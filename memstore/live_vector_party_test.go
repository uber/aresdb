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

package memstore

import (
	"bytes"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber/aresdb/memstore/common"
)

var _ = ginkgo.Describe("live vector party", func() {

	var mockMemStore *memStoreImpl
	var hostMemoryManager common.HostMemoryManager

	ginkgo.BeforeEach(func() {
		mockMemStore = getFactory().NewMockMemStore()
		hostMemoryManager = NewHostMemoryManager(mockMemStore, 1<<32)
	})

	ginkgo.It("SetGoDataValue should work", func() {
		vp1 := NewLiveVectorParty(10, common.GeoShape, common.NullDataValue, hostMemoryManager)
		vp1.Allocate(false)

		shape1 := &common.GeoShapeGo{
			Polygons: [][]common.GeoPointGo{
				{
					{
						90.0,
						180.0,
					},
				},
			},
		}
		vp1.SetGoValue(0, shape1, true)
		Ω(vp1.GetBytes()).Should(Equal(int64(8)))
		Ω(vp1.GetLength()).Should(Equal(10))
		dv := vp1.GetDataValue(0)
		Ω(dv.Valid).Should(BeTrue())

		vp1.SetGoValue(0, nil, false)
		dv = vp1.GetDataValue(0)
		Ω(dv.Valid).Should(BeFalse())
		Ω(dv.DataType).Should(Equal(common.GeoShape))

		Ω(vp1.GetBytes()).Should(Equal(int64(0)))
		Ω(vp1.GetLength()).Should(Equal(10))

		Ω(func() {
			vp1.SetBool(0, true, true)
		}).Should(Panic())

		Ω(func() {
			vp1.SetValue(0, nil, true)
		}).Should(Panic())

		Ω(func() {
			vp1.GetValue(0)
		}).Should(Panic())
	})

	ginkgo.It("SetDataValue should work", func() {
		vp1 := NewLiveVectorParty(10, common.GeoShape, common.NullDataValue, hostMemoryManager)
		vp1.Allocate(false)

		shape1 := &common.GeoShapeGo{
			Polygons: [][]common.GeoPointGo{
				{
					{
						90.0,
						180.0,
					},
				},
			},
		}
		vp1.SetDataValue(0, common.DataValue{
			Valid: true,
			GoVal: shape1,
		}, IgnoreCount)

		Ω(vp1.GetBytes()).Should(Equal(int64(8)))
		Ω(vp1.GetLength()).Should(Equal(10))
		dv := vp1.GetDataValue(0)
		Ω(dv.Valid).Should(BeTrue())

		vp1.SetDataValue(0, common.NullDataValue, IgnoreCount)
		dv = vp1.GetDataValue(0)
		Ω(dv.Valid).Should(BeFalse())
		Ω(dv.DataType).Should(Equal(common.GeoShape))

		Ω(vp1.GetBytes()).Should(Equal(int64(0)))
		Ω(vp1.GetLength()).Should(Equal(10))
	})

	ginkgo.It("Write and Read of goLiveVectorParty should work", func() {
		vpSerializer := &vectorPartyArchiveSerializer{
			vectorPartyBaseSerializer{
				hostMemoryManager: hostMemoryManager,
			},
		}

		vp1 := NewLiveVectorParty(10, common.GeoShape, common.NullDataValue, hostMemoryManager)
		vp1.Allocate(false)

		shape1 := &common.GeoShapeGo{
			Polygons: [][]common.GeoPointGo{
				{
					{
						90.0,
						180.0,
					},
				},
			},
		}

		vp1.SetDataValue(0, common.DataValue{
			Valid: true,
			GoVal: shape1,
		}, IgnoreCount)

		Ω(vp1.GetBytes()).Should(Equal(int64(8)))
		Ω(vp1.GetLength()).Should(Equal(10))

		buffer := bytes.Buffer{}
		err := vp1.Write(&buffer)
		Ω(err).Should(BeNil())

		vp2 := NewLiveVectorParty(10, common.GeoShape, common.NullDataValue, hostMemoryManager)
		err = vp2.Read(&buffer, vpSerializer)
		Ω(err).Should(BeNil())
		Ω(VectorPartyEquals(vp1, vp2)).Should(BeTrue())

		Ω(vp2.GetBytes()).Should(Equal(int64(8)))
		Ω(vp2.GetLength()).Should(Equal(10))
	})

	ginkgo.It("Slice goLiveVetorParty should work", func() {
		vp1 := NewLiveVectorParty(10, common.GeoShape, common.NullDataValue, hostMemoryManager)
		vp1.Allocate(false)

		shape1 := &common.GeoShapeGo{
			Polygons: [][]common.GeoPointGo{
				{
					{
						90.0,
						180.0,
					},
				},
			},
		}

		vp1.SetDataValue(0, common.DataValue{
			Valid: true,
			GoVal: shape1,
		}, IgnoreCount)

		Ω(vp1.Slice(0, 100)).Should(Equal(common.SlicedVector{
			Values: []interface{}{
				"Polygon((180.0000+90.0000))",
				nil, nil, nil, nil, nil, nil, nil, nil, nil,
			},
			Counts: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		}))
	})

	ginkgo.It("goLiveVectorParty.Equals should correctly compare lengths", func() {
		vp1 := NewLiveVectorParty(2, common.GeoShape, common.NullDataValue, hostMemoryManager)
		vp1.Allocate(false)
		vp2 := NewLiveVectorParty(1, common.GeoShape, common.NullDataValue, hostMemoryManager)
		vp1.Allocate(false)
		Ω(vp1.Equals(vp2)).Should(Equal(false))
	})
})
