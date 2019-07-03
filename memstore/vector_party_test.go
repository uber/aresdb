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
	"unsafe"

	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber/aresdb/memstore/common"
	"sync"
)

var _ = ginkgo.Describe("VectorParty", func() {

	ginkgo.It("GetHostVectorPartySlice should work", func() {
		locker := &sync.RWMutex{}
		sourceVP, err := testFactory.ReadArchiveVectorParty("sortedVP4", locker)
		Ω(err).Should(BeNil())
		vp := sourceVP.(*archiveVectorParty)
		hostVPSlice := vp.GetHostVectorPartySlice(0, vp.length)
		Ω(hostVPSlice).Should(Equal(common.HostVectorPartySlice{
			Values:          unsafe.Pointer(vp.values.buffer),
			Nulls:           unsafe.Pointer(vp.nulls.buffer),
			Counts:          unsafe.Pointer(vp.counts.buffer),
			Length:          5,
			ValueType:       vp.dataType,
			DefaultValue:    vp.defaultValue,
			ValueStartIndex: 0,
			NullStartIndex:  0,
			CountStartIndex: 0,
			ValueBytes:      64,
			NullBytes:       64,
			CountBytes:      64,
		}))
	})

	ginkgo.It("Slice should work", func() {
		locker := &sync.RWMutex{}
		vp, err := testFactory.ReadArchiveVectorParty("sortedVP4", locker)
		Ω(err).Should(BeNil())
		vectorData := vp.Slice(0, 3)
		Ω(vectorData.Values[0]).Should(BeEquivalentTo(1))
		Ω(vectorData.Values[1]).Should(BeEquivalentTo(2))
		Ω(vectorData.Values[2]).Should(BeEquivalentTo(2))
		Ω(vectorData.Counts).Should(BeEquivalentTo([]int{1, 2, 3}))

		vectorData = vp.Slice(5, 5)
		Ω(vectorData.Values).Should(Equal([]interface{}{}))
		Ω(vectorData.Counts).Should(BeEquivalentTo([]int{}))

		vp, err = testFactory.ReadArchiveVectorParty("sortedVP5", locker)
		Ω(err).Should(BeNil())
		vectorData = vp.Slice(0, 6)
		Ω(vectorData.Values).Should(ConsistOf(BeNil()))
		Ω(vectorData.Counts).Should(BeEquivalentTo([]int{5}))

		vectorData = vp.Slice(5, 5)
		Ω(vectorData.Values).Should(Equal([]interface{}{}))
		Ω(vectorData.Counts).Should(BeEquivalentTo([]int{}))

		vectorData = vp.Slice(1, 6)
		Ω(vectorData.Values).Should(ConsistOf(BeNil()))
		Ω(vectorData.Counts).Should(BeEquivalentTo([]int{5}))
	})

	ginkgo.It("SliceByValue", func() {
		locker := &sync.RWMutex{}
		vp1, err := testFactory.ReadArchiveVectorParty("sortedVP6", locker)
		Ω(err).Should(BeNil())
		value := 1
		startRow, endRow, startIndex, endIndex := vp1.SliceByValue(
			0, 5, unsafe.Pointer(&value))
		Ω(startRow).Should(Equal(0))
		Ω(endRow).Should(Equal(2))
		Ω(startIndex).Should(Equal(0))
		Ω(endIndex).Should(Equal(1))

		value = 2
		startRow, endRow, startIndex, endIndex = vp1.SliceByValue(
			0, 5, unsafe.Pointer(&value))
		Ω(startRow).Should(Equal(2))
		Ω(endRow).Should(Equal(5))
		Ω(startIndex).Should(Equal(1))
		Ω(endIndex).Should(Equal(2))

		value = 1
		startRow, endRow, startIndex, endIndex = vp1.SliceByValue(
			5, 20, unsafe.Pointer(&value))
		Ω(startRow).Should(Equal(5))
		Ω(endRow).Should(Equal(10))
		Ω(startIndex).Should(Equal(2))
		Ω(endIndex).Should(Equal(3))

		value = 2
		startRow, endRow, startIndex, endIndex = vp1.SliceByValue(
			5, 20, unsafe.Pointer(&value))
		Ω(startRow).Should(Equal(10))
		Ω(endRow).Should(Equal(20))
		Ω(startIndex).Should(Equal(3))
		Ω(endIndex).Should(Equal(4))

		startIndex, endIndex = vp1.SliceIndex(0, 5)
		Ω(startIndex).Should(Equal(0))
		Ω(endIndex).Should(Equal(2))

		vp2, err := testFactory.ReadArchiveVectorParty("sortedVP0", locker)
		value = 10
		startRow, endRow, startIndex, endIndex = vp2.SliceByValue(
			0, 5, unsafe.Pointer(&value))
		Ω(startRow).Should(Equal(1))
		Ω(endRow).Should(Equal(2))
		Ω(startIndex).Should(Equal(1))
		Ω(endIndex).Should(Equal(2))

		vp5, err := testFactory.ReadArchiveVectorParty("sortedVP5", locker)

		value = 1
		startRow, endRow, startIndex, endIndex = vp5.SliceByValue(
			0, 5, unsafe.Pointer(&value))
		Ω(startRow).Should(Equal(5))
		Ω(endRow).Should(Equal(5))
		Ω(startIndex).Should(Equal(5))
		Ω(endIndex).Should(Equal(5))

		defaultValue := common.DataValue{
			IsBool:   true,
			BoolVal:  true,
			DataType: common.Bool,
			Valid:    true,
		}

		vp5.(*archiveVectorParty).defaultValue = defaultValue
		startRow, endRow, startIndex, endIndex = vp5.SliceByValue(
			0, 5, unsafe.Pointer(&value))
		Ω(startRow).Should(Equal(0))
		Ω(endRow).Should(Equal(5))
		Ω(startIndex).Should(Equal(0))
		Ω(endIndex).Should(Equal(5))

	})

	ginkgo.It("SliceIndex", func() {
		vp, err := testFactory.ReadArchiveVectorParty("sortedVP6", nil)
		Ω(err).Should(BeNil())
		startIndex, endIndex := vp.SliceIndex(3, 15)
		Ω(startIndex).Should(Equal(1))
		Ω(endIndex).Should(Equal(4))

		startIndex, endIndex = vp.SliceIndex(21, 21)
		Ω(startIndex).Should(Equal(4))
		Ω(endIndex).Should(Equal(4))
	})

	ginkgo.It("GetDataValueByRow should work", func() {
		vp, err := testFactory.ReadArchiveVectorParty("sortedVP6", nil)
		Ω(err).Should(BeNil())
		Ω(*(*uint16)(vp.GetDataValueByRow(0).OtherVal)).Should(BeEquivalentTo(1))
		Ω(*(*uint16)(vp.GetDataValueByRow(4).OtherVal)).Should(BeEquivalentTo(2))
		Ω(*(*uint16)(vp.GetDataValueByRow(10).OtherVal)).Should(BeEquivalentTo(2))
		Ω(*(*uint16)(vp.GetDataValueByRow(19).OtherVal)).Should(BeEquivalentTo(2))
	})

	ginkgo.It("fillWithDefaultValue should work", func() {

		var defVal uint32 = 100

		defaultValue := common.DataValue{
			OtherVal: unsafe.Pointer(&defVal),
			Valid:    true,
			DataType: common.Uint32,
			CmpFunc:  common.GetCompareFunc(common.Uint32),
		}

		vp := cVectorParty{
			values: NewVector(common.Uint32, 100),
			nulls:  NewVector(common.Bool, 100),
			baseVectorParty: baseVectorParty{
				dataType:     common.Uint32,
				defaultValue: defaultValue,
			},
			columnMode: common.HasNullVector,
		}

		vp.fillWithDefaultValue()

		for i := 0; i < 100; i++ {
			Ω(vp.GetDataValueByRow(i).Compare(defaultValue)).Should(Equal(0))
		}

		Ω(func() {
			vp := &cVectorParty{
				values: NewVector(common.Uint32, 100),
				baseVectorParty: baseVectorParty{
					dataType:     common.Uint32,
					defaultValue: defaultValue,
				},
			}
			vp.fillWithDefaultValue()
		}).Should(Panic())

		Ω(func() {
			vp := cVectorParty{
				baseVectorParty: baseVectorParty{
					dataType:     common.Uint32,
					defaultValue: defaultValue,
				},
				nulls: NewVector(common.Bool, 100),
			}
			vp.fillWithDefaultValue()
		}).Should(Panic())

		vp.defaultValue = common.NullDataValue

		// Won't set again since the default value is NullDataValue.
		vp.fillWithDefaultValue()
		for i := 0; i < 100; i++ {
			Ω(vp.GetDataValueByRow(i).Compare(defaultValue)).Should(Equal(0))
		}

		defaultValue = common.DataValue{
			BoolVal:  true,
			IsBool:   true,
			Valid:    true,
			DataType: common.Bool,
		}

		vp = cVectorParty{
			values: NewVector(common.Bool, 100),
			nulls:  NewVector(common.Bool, 100),
			baseVectorParty: baseVectorParty{
				dataType:     common.Bool,
				defaultValue: defaultValue,
			},
		}

		vp.fillWithDefaultValue()
		for i := 0; i < 100; i++ {
			Ω(vp.GetDataValueByRow(i).Compare(defaultValue)).Should(Equal(0))
		}

	})

	ginkgo.It("Prune should work", func() {

		var defVal uint32 = 100

		defaultValue := common.DataValue{
			OtherVal: unsafe.Pointer(&defVal),
			Valid:    true,
			DataType: common.Uint32,
			CmpFunc:  common.GetCompareFunc(common.Uint32),
		}

		vp := archiveVectorParty{
			cVectorParty: cVectorParty{
				baseVectorParty: baseVectorParty{
					dataType:     common.Uint32,
					defaultValue: defaultValue,
				},
				values: NewVector(common.Uint32, 100),
				nulls:  NewVector(common.Bool, 100),
			},
		}

		vp.fillWithDefaultValue()

		for i := 0; i < 100; i++ {
			Ω(vp.GetDataValueByRow(i).Compare(defaultValue)).Should(Equal(0))
		}

		vp.Prune()
		Ω(vp.GetMode()).Should(Equal(common.AllValuesDefault))
		Ω(vp.values).Should(BeNil())
		Ω(vp.nulls).Should(BeNil())
		Ω(vp.counts).Should(BeNil())

		// But it still should to equal to defaultValue
		for i := 0; i < 100; i++ {
			Ω(vp.GetDataValueByRow(i).Compare(defaultValue)).Should(Equal(0))
		}

		vp = archiveVectorParty{
			cVectorParty: cVectorParty{
				values: NewVector(common.Uint32, 100),
				nulls:  NewVector(common.Bool, 100),
				baseVectorParty: baseVectorParty{
					dataType:     common.Uint32,
					defaultValue: common.NullDataValue,
				},
			},
		}

		vp.Prune()
		Ω(vp.GetMode()).Should(Equal(common.AllValuesDefault))
		Ω(vp.values).Should(BeNil())
		Ω(vp.nulls).Should(BeNil())
		Ω(vp.counts).Should(BeNil())
		for i := 0; i < 100; i++ {
			Ω(vp.GetDataValueByRow(i).Compare(common.NullDataValue)).Should(Equal(0))
		}

		vp = archiveVectorParty{
			cVectorParty: cVectorParty{
				baseVectorParty: baseVectorParty{
					dataType:     common.Uint32,
					defaultValue: defaultValue,
				},
				values: NewVector(common.Uint32, 100),
				nulls:  NewVector(common.Bool, 100),
			},
		}

		vp.fillWithDefaultValue()

		var zeroVal uint32 = 0
		zeroValue := common.DataValue{
			OtherVal: unsafe.Pointer(&zeroVal),
			Valid:    true,
			DataType: common.Uint32,
			CmpFunc:  common.GetCompareFunc(common.Uint32),
		}

		vp.SetDataValue(0, zeroValue, IncrementCount)

		vp.Prune()
		Ω(vp.GetMode()).Should(Equal(common.AllValuesPresent))
		Ω(vp.values).ShouldNot(BeNil())
		Ω(vp.nulls).Should(BeNil())
		Ω(vp.counts).Should(BeNil())

		Ω(vp.GetDataValueByRow(0).Compare(zeroValue)).Should(Equal(0))
		for i := 1; i < 100; i++ {
			Ω(vp.GetDataValueByRow(i).Compare(defaultValue)).Should(Equal(0))
		}
	})
})
