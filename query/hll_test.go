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

package query

import (
	"github.com/onsi/ginkgo"
	memCom "github.com/uber/aresdb/memstore/common"
	queryCom "github.com/uber/aresdb/query/common"
	"unsafe"

	"errors"
	. "github.com/onsi/gomega"
	"time"
)

var _ = ginkgo.Describe("hll", func() {
	dimensionData := []byte{
		0, 0, 0, 0,
		1, 0, 0, 0,
		0xFF, 0xFF, 0xFF, 0xFF, // dim0
		0, 0, 0, 0,
		0, 0, 0, 0,
		0, 0, 0, 0, // res cap.
		0, 0,
		2, 0,
		2, 2, // dim1
		0, 0,
		0, 0,
		0, 0, // res cap
		0, 2, 3, // dim2
		0, 0, 0, // res cap
		0, 1, 1, // dim0
		0, 0, 0, // res cap
		0, 1, 1, // dim1
		0, 0, 0, // res cap
		0, 1, 1, // dim2
		0, 0, 0, // res cap
	}

	measureData := [3]uint32{}
	counts := []uint16{3, queryCom.DenseDataLength, 4}
	hllData := [queryCom.DenseDataLength + 28]byte{}
	*(*uint32)(unsafe.Pointer(&hllData[0])) = 0X00FF0001
	*(*uint32)(unsafe.Pointer(&hllData[4])) = 0X00FE0002
	*(*uint32)(unsafe.Pointer(&hllData[8])) = 0X00FD0003

	hllData[12] = 1
	hllData[13] = 1

	*(*uint32)(unsafe.Pointer(&hllData[12+queryCom.DenseDataLength])) = 0X000100FF
	*(*uint32)(unsafe.Pointer(&hllData[16+queryCom.DenseDataLength])) = 0X000200FE
	*(*uint32)(unsafe.Pointer(&hllData[20+queryCom.DenseDataLength])) = 0X000300FD
	*(*uint32)(unsafe.Pointer(&hllData[24+queryCom.DenseDataLength])) = 0X000400FC

	qc := AQLQueryContext{
		OOPK: OOPKContext{
			currentBatch: oopkBatchContext{
				resultCapacity:   6,
				dimensionVectorD: [2]devicePointer{{pointer: unsafe.Pointer(&dimensionData[0])}, nullDevicePointer},
				measureVectorD:   [2]devicePointer{{pointer: unsafe.Pointer(&measureData[0])}, nullDevicePointer},
			},
			hllVectorD:           devicePointer{pointer: unsafe.Pointer(&hllData[0])},
			hllVectorSize:        queryCom.DenseDataLength + 28,
			hllDimRegIDCountD:    devicePointer{pointer: unsafe.Pointer(&counts[0])},
			ResultSize:           3,
			NumDimsPerDimWidth:   queryCom.DimCountsPerDimWidth{0, 0, 1, 1, 1},
			DimensionVectorIndex: []int{0, 2, 1},
		},
	}

	ginkgo.It("SerializeHLL should work", func() {
		dataTypes := []memCom.DataType{memCom.Uint32, memCom.Uint8, memCom.Int16}
		enumReverseDict := map[int][]string{1: {"a", "b", "c"}}
		data, err := qc.SerializeHLL(dataTypes, enumReverseDict, nil)
		Ω(err).Should(BeNil())
		Ω(len(data)).Should(Equal(104 + queryCom.DenseDataLength + 32))
		Ω(data[64:94]).Should(Equal([]byte{
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
		}))
		Ω(data[96:102]).Should(Equal((*(*[6]byte)(unsafe.Pointer(&counts[0])))[:]))
		Ω(data[104 : 104+queryCom.DenseDataLength+28]).Should(Equal(hllData[:]))
	})

	ginkgo.It("Should work with timezone", func() {
		timeDimensionData := []byte{
			0, 0, 0, 0,
			1, 0, 0, 0,
			2, 0, 0, 0, // dim0
			0, 0, 0, 0,
			0, 0, 0, 0,
			0, 0, 0, 0, // res cap.
			0, 0,
			2, 0,
			2, 2, // dim1
			0, 0,
			0, 0,
			0, 0, // res cap
			0, 2, 3, // dim2
			0, 0, 0, // res cap
			0, 1, 1, // dim0
			0, 0, 0, // res cap
			0, 1, 1, // dim1
			0, 0, 0, // res cap
			0, 1, 1, // dim2
			0, 0, 0, // res cap
		}

		measureData := [3]uint32{}
		counts := []uint16{3, queryCom.DenseDataLength, 4}
		hllData := [queryCom.DenseDataLength + 28]byte{}
		*(*uint32)(unsafe.Pointer(&hllData[0])) = 0X00FF0001
		*(*uint32)(unsafe.Pointer(&hllData[4])) = 0X00FE0002
		*(*uint32)(unsafe.Pointer(&hllData[8])) = 0X00FD0003

		hllData[12] = 1
		hllData[13] = 1

		*(*uint32)(unsafe.Pointer(&hllData[12+queryCom.DenseDataLength])) = 0X000100FF
		*(*uint32)(unsafe.Pointer(&hllData[16+queryCom.DenseDataLength])) = 0X000200FE
		*(*uint32)(unsafe.Pointer(&hllData[20+queryCom.DenseDataLength])) = 0X000300FD
		*(*uint32)(unsafe.Pointer(&hllData[24+queryCom.DenseDataLength])) = 0X000400FC

		loc, _ := time.LoadLocation("Africa/Algiers")
		qc := AQLQueryContext{
			OOPK: OOPKContext{
				currentBatch: oopkBatchContext{
					resultCapacity:   6,
					dimensionVectorD: [2]devicePointer{{pointer: unsafe.Pointer(&timeDimensionData[0])}, nullDevicePointer},
					measureVectorD:   [2]devicePointer{{pointer: unsafe.Pointer(&measureData[0])}, nullDevicePointer},
				},
				hllVectorD:           devicePointer{pointer: unsafe.Pointer(&hllData[0])},
				hllVectorSize:        queryCom.DenseDataLength + 28,
				hllDimRegIDCountD:    devicePointer{pointer: unsafe.Pointer(&counts[0])},
				ResultSize:           3,
				NumDimsPerDimWidth:   queryCom.DimCountsPerDimWidth{0, 0, 1, 1, 1},
				DimensionVectorIndex: []int{0, 2, 1},
			},
			fixedTimezone: loc,
			fromTime:      &alignedTime{Time: time.Now().In(loc), Unit: "week"},
			toTime:        &alignedTime{Time: time.Now().In(loc), Unit: "week"},
		}

		dataTypes := []memCom.DataType{memCom.Uint32, memCom.Uint8, memCom.Int16}
		enumReverseDict := map[int][]string{1: {"a", "b", "c", "d"}}
		data, err := qc.SerializeHLL(dataTypes, enumReverseDict, []int{0})
		Ω(err).Should(BeNil())
		Ω(len(data)).Should(Equal(104 + queryCom.DenseDataLength + 32))
		Ω(data[64:94]).Should(Equal([]byte{
			0, 0, 0, 0,
			0, 0, 0, 0,
			0, 0, 0, 0, // dim0
			0, 0,
			2, 0,
			2, 2, // dim1
			0, 2, 3, // dim2
			0, 1, 1, // dim0
			0, 1, 1, // dim1
			0, 1, 1, // dim2
		}))
		Ω(data[96:102]).Should(Equal((*(*[6]byte)(unsafe.Pointer(&counts[0])))[:]))
		Ω(data[104 : 104+queryCom.DenseDataLength+28]).Should(Equal(hllData[:]))

		// Then we deserialize it.
		res, err := queryCom.NewTimeSeriesHLLResult(data, queryCom.HLLDataHeader, false)
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal(queryCom.AQLQueryResult{
			"NULL": map[string]interface{}{
				"NULL": map[string]interface{}{
					"NULL": queryCom.HLL{NonZeroRegisters: 3,
						SparseData: []queryCom.HLLRegister{{Index: 1, Rho: 255}, {Index: 2, Rho: 254}, {Index: 3, Rho: 253}},
					},
				}},
			"0": map[string]interface{}{
				"c": map[string]interface{}{
					"2": queryCom.HLL{NonZeroRegisters: 2, DenseData: hllData[12 : 12+queryCom.DenseDataLength]},
				},
				"d": map[string]interface{}{
					"514": queryCom.HLL{NonZeroRegisters: 4, SparseData: []queryCom.HLLRegister{{Index: 255, Rho: 1}, {Index: 254, Rho: 2}, {Index: 253, Rho: 3}, {Index: 252, Rho: 4}}},
				},
			},
		}))
	})

	ginkgo.It("Serialize and then deserialize should work", func() {
		dataTypes := []memCom.DataType{memCom.Uint32, memCom.Uint8, memCom.Int16}
		enumReverseDict := map[int][]string{1: {"a", "b", "c", "d"}}
		data, err := qc.SerializeHLL(dataTypes, enumReverseDict, nil)
		Ω(err).Should(BeNil())
		//ioutil.WriteFile("../testing/data/query/hll", data, 0644)
		res, err := queryCom.NewTimeSeriesHLLResult(data, queryCom.HLLDataHeader, false)
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal(queryCom.AQLQueryResult{
			"NULL": map[string]interface{}{
				"NULL": map[string]interface{}{
					"NULL": queryCom.HLL{NonZeroRegisters: 3,
						SparseData: []queryCom.HLLRegister{{Index: 1, Rho: 255}, {Index: 2, Rho: 254}, {Index: 3, Rho: 253}},
					},
				}},
			"1": map[string]interface{}{
				"c": map[string]interface{}{
					"2": queryCom.HLL{NonZeroRegisters: 2, DenseData: hllData[12 : 12+queryCom.DenseDataLength]},
				},
			},
			"4294967295": map[string]interface{}{
				"d": map[string]interface{}{
					"514": queryCom.HLL{NonZeroRegisters: 4, SparseData: []queryCom.HLLRegister{{Index: 255, Rho: 1}, {Index: 254, Rho: 2}, {Index: 253, Rho: 3}, {Index: 252, Rho: 4}}},
				},
			}}))
	})

	ginkgo.It("write HLLQueryResults should work", func() {
		dataTypes := []memCom.DataType{memCom.Uint32, memCom.Uint8, memCom.Int16}
		enumReverseDict := map[int][]string{1: {"a", "b", "c", "d"}}

		//queryE := NewHLLQueryResults()
		//queryE.WriteResult([]byte{})
		//err := ioutil.WriteFile("../testing/data/query/hll_empty_results", queryE.GetBytes(), 0644)

		queryResults := NewHLLQueryResults()
		data, err := qc.SerializeHLL(dataTypes, enumReverseDict, nil)
		Ω(err).Should(BeNil())

		queryResults.WriteResult(data)

		results, errs, err := queryCom.ParseHLLQueryResults(queryResults.GetBytes(), false)
		Ω(err).Should(BeNil())
		Ω(errs).Should(HaveLen(1))
		Ω(errs[0]).Should(BeNil())
		Ω(results).Should(HaveLen(1))
		Ω(results[0]).Should(Equal(queryCom.AQLQueryResult{
			"NULL": map[string]interface{}{
				"NULL": map[string]interface{}{
					"NULL": queryCom.HLL{NonZeroRegisters: 3,
						SparseData: []queryCom.HLLRegister{{Index: 1, Rho: 255}, {Index: 2, Rho: 254}, {Index: 3, Rho: 253}},
					},
				}},
			"1": map[string]interface{}{
				"c": map[string]interface{}{
					"2": queryCom.HLL{NonZeroRegisters: 2, DenseData: hllData[12 : 12+queryCom.DenseDataLength]},
				},
			},
			"4294967295": map[string]interface{}{
				"d": map[string]interface{}{
					"514": queryCom.HLL{NonZeroRegisters: 4, SparseData: []queryCom.HLLRegister{{Index: 255, Rho: 1}, {Index: 254, Rho: 2}, {Index: 253, Rho: 3}, {Index: 252, Rho: 4}}},
				},
			}}))

		queryResults.WriteError(errors.New("test"))
		bs := queryResults.GetBytes()
		//ioutil.WriteFile("../testing/data/query/hll_query_results", bs, 0644)

		results, errs, err = queryCom.ParseHLLQueryResults(bs, false)
		Ω(err).Should(BeNil())
		Ω(errs).Should(HaveLen(2))
		Ω(results).Should(HaveLen(2))
		Ω(errs[1].Error()).Should(Equal("test"))
	})
})
