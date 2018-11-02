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
	"io/ioutil"
	"unsafe"
)

var _ = ginkgo.Describe("hll", func() {
	hllData := [DenseDataLength + 28]byte{}
	hllData[12] = 1
	hllData[13] = 1

	ginkgo.It("CalculateSizes should work", func() {
		hllData := HLLData{
			DimIndexes:                     make([]int, 7),
			DataTypes:                      make([]memCom.DataType, 7),
			PaddedRawDimValuesVectorLength: 100,
			ResultSize:                     10,
			PaddedHLLVectorLength:          DenseDataLength + 32,
		}
		headerSize, totalSize := hllData.CalculateSizes()
		Ω(headerSize).Should(BeEquivalentTo(56))
		Ω(totalSize).Should(BeEquivalentTo(16596))

		hllData.EnumDicts = map[int][]string{
			1: {"a", "b", "c", "d"}, // 4 + 8 + 4 + 8 = 24
			2: {},                   // 8
		}

		headerSize, totalSize = hllData.CalculateSizes()
		Ω(headerSize).Should(BeEquivalentTo(88))
		Ω(totalSize).Should(BeEquivalentTo(16628))
	})

	ginkgo.It("CalculateEnumCasesBytes should work", func() {
		Ω(CalculateEnumCasesBytes([]string{"ss", "a", "b"})).Should(BeEquivalentTo(16))
		Ω(CalculateEnumCasesBytes([]string{"ss"})).Should(BeEquivalentTo(8))
		Ω(CalculateEnumCasesBytes([]string{})).Should(BeEquivalentTo(0))
	})

	ginkgo.It("readHLL should work", func() {
		counts := []uint16{3, DenseDataLength, 4, DenseDataLength, 5}
		hllVector := [2*DenseDataLength + 48]byte{}

		hllVector[12] = 1
		hllVector[13] = 1

		var currentOffset int64
		var hllData HLL
		// Sparse
		hllData = readHLL(unsafe.Pointer(&hllVector[0]), counts[0], &currentOffset)
		Ω(currentOffset).Should(BeEquivalentTo(12))
		Ω(hllData.SparseData).ShouldNot(BeNil())
		Ω(hllData.DenseData).Should(BeNil())
		Ω(hllData.NonZeroRegisters).Should(BeEquivalentTo(3))

		// Dense
		hllData = readHLL(unsafe.Pointer(&hllVector[0]), counts[1], &currentOffset)
		Ω(currentOffset).Should(BeEquivalentTo(12 + DenseDataLength))
		Ω(hllData.SparseData).Should(BeNil())
		Ω(hllData.DenseData).ShouldNot(BeNil())
		Ω(hllData.NonZeroRegisters).Should(BeEquivalentTo(2))

		// Sparse
		hllData = readHLL(unsafe.Pointer(&hllVector[0]), counts[2], &currentOffset)
		Ω(currentOffset).Should(BeEquivalentTo(28 + DenseDataLength))
		Ω(hllData.SparseData).ShouldNot(BeNil())
		Ω(hllData.DenseData).Should(BeNil())
		Ω(hllData.NonZeroRegisters).Should(BeEquivalentTo(4))

		// Dense
		hllData = readHLL(unsafe.Pointer(&hllVector[0]), counts[3], &currentOffset)
		Ω(currentOffset).Should(BeEquivalentTo(28 + 2*DenseDataLength))
		Ω(hllData.SparseData).Should(BeNil())
		Ω(hllData.DenseData).ShouldNot(BeNil())
		Ω(hllData.NonZeroRegisters).Should(BeEquivalentTo(0))

		// Sparse
		hllData = readHLL(unsafe.Pointer(&hllVector[0]), counts[4], &currentOffset)
		Ω(currentOffset).Should(BeEquivalentTo(48 + 2*DenseDataLength))
		Ω(hllData.SparseData).ShouldNot(BeNil())
		Ω(hllData.DenseData).Should(BeNil())
		Ω(hllData.NonZeroRegisters).Should(BeEquivalentTo(5))
	})

	ginkgo.It("NewTimeSeriesHLLResult should work", func() {
		data, err := ioutil.ReadFile("../../testing/data/query/hll")
		Ω(err).Should(BeNil())
		res, err := NewTimeSeriesHLLResult(data, OldHLLDataHeader)
		Ω(err).Should(BeNil())

		expected := AQLTimeSeriesResult{
			"NULL": map[string]interface{}{
				"NULL": map[string]interface{}{
					"NULL": HLL{NonZeroRegisters: 3,
						SparseData: []HLLRegister{{Index: 1, Rho: 255}, {Index: 2, Rho: 254}, {Index: 3, Rho: 253}},
					},
				}},
			"1": map[string]interface{}{
				"c": map[string]interface{}{
					"2": HLL{NonZeroRegisters: 2, DenseData: hllData[12 : 12+DenseDataLength]},
				},
			},
			"4294967295": map[string]interface{}{
				"d": map[string]interface{}{
					"514": HLL{NonZeroRegisters: 4, SparseData: []HLLRegister{{Index: 255, Rho: 1}, {Index: 254, Rho: 2}, {Index: 253, Rho: 3}, {Index: 252, Rho: 4}}},
				},
			}}
		Ω(res).Should(Equal(expected))

		data, err = ioutil.ReadFile("../../testing/data/query/hll_new")
		Ω(err).Should(BeNil())
		res, err = NewTimeSeriesHLLResult(data, HLLDataHeader)
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal(expected))
	})

	ginkgo.It("ParseHLLQueryResults should work", func() {
		data, err := ioutil.ReadFile("../../testing/data/query/hll_query_results")
		Ω(err).Should(BeNil())
		results, errs, err := ParseHLLQueryResults(data)
		Ω(errs).Should(HaveLen(2))
		Ω(results).Should(HaveLen(2))
		Ω(errs[1].Error()).Should(Equal("test"))
		Ω(results[0]).Should(Equal(AQLTimeSeriesResult{
			"NULL": map[string]interface{}{
				"NULL": map[string]interface{}{
					"NULL": HLL{NonZeroRegisters: 3,
						SparseData: []HLLRegister{{Index: 1, Rho: 255}, {Index: 2, Rho: 254}, {Index: 3, Rho: 253}},
					},
				}},
			"1": map[string]interface{}{
				"c": map[string]interface{}{
					"2": HLL{NonZeroRegisters: 2, DenseData: hllData[12 : 12+DenseDataLength]},
				},
			},
			"4294967295": map[string]interface{}{
				"d": map[string]interface{}{
					"514": HLL{NonZeroRegisters: 4, SparseData: []HLLRegister{{Index: 255, Rho: 1}, {Index: 254, Rho: 2}, {Index: 253, Rho: 3}, {Index: 252, Rho: 4}}},
				},
			}}))
	})

	ginkgo.It("Computes hll correctly", func() {
		h := HLL{
			SparseData: []HLLRegister{
				{
					100,
					1,
				}, {
					200, 2,
				},
			},
			NonZeroRegisters: 2,
		}
		Ω(h.Compute()).Should(Equal(2.0))
	})

	ginkgo.It("Parse empty hll result", func() {
		data, err := ioutil.ReadFile("../../testing/data/query/hll_empty_results")
		Ω(err).Should(BeNil())
		results, errs, err := ParseHLLQueryResults(data)
		Ω(results).Should(Equal([]AQLTimeSeriesResult{{}}))
		Ω(errs).Should(Equal([]error{nil}))
		Ω(err).Should(BeNil())
	})

	ginkgo.It("encodes and decodes", func() {
		h1 := HLL{
			SparseData: []HLLRegister{
				{
					Index: 100,
					Rho:   1,
				},
				{
					Index: 200,
					Rho:   2,
				},
			},
			NonZeroRegisters: 2,
		}

		var h2 HLL
		h2.Decode(h1.Encode())
		Ω(h2).Should(Equal(h1))

		hllDenseData := make([]byte, 1<<hllP)
		hllDenseData[100] = 1
		hllDenseData[200] = 2
		h1 = HLL{
			DenseData:        hllDenseData,
			NonZeroRegisters: 2,
		}
		h2 = HLL{}
		h2.Decode(h1.Encode())
		Ω(h2).Should(Equal(h1))
	})

	ginkgo.It("stores data in sparse or dense format", func() {
		var h HLL
		h.Set(100, 1)
		h.Set(200, 2)
		Ω(h).Should(Equal(HLL{
			SparseData: []HLLRegister{
				{Index: 100, Rho: 1},
				{Index: 200, Rho: 2},
			},
			DenseData:        nil,
			NonZeroRegisters: 2,
		}))

		for i := 201; i < 4300; i++ {
			h.Set(uint16(i), 3)
		}
		Ω(h.SparseData).Should(BeNil())
		Ω(len(h.DenseData)).Should(Equal(0x4000))
		Ω(h.DenseData[100]).Should(Equal(byte(1)))
		Ω(h.DenseData[200]).Should(Equal(byte(2)))
		Ω(h.DenseData[201]).Should(Equal(byte(3)))
		Ω(h.DenseData[4299]).Should(Equal(byte(3)))
		Ω(h.DenseData[4300]).Should(Equal(byte(0)))
		Ω(h.NonZeroRegisters).Should(Equal(uint16(4101)))
	})
})
