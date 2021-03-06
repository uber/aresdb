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

package list

import (
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber/aresdb/memstore/common"
	"unsafe"
)

var _ = ginkgo.Describe("test factory", func() {
	ginkgo.It("test read list vector party", func() {
		// Path not exist should raise error.
		vp, err := GetFactory().ReadLiveVectorParty("list/fancy_non_existent_name")
		Ω(err).ShouldNot(BeNil())
		Ω(err.Error()).Should(HavePrefix("open ../../testing/data/vps/list/fancy_non_existent_name: " +
			"no such file or directory"))
		Ω(vp).Should(BeNil())

		// Length not match
		vp, err = GetFactory().ReadLiveVectorParty("list/length_not_match")
		Ω(err).ShouldNot(BeNil())
		Ω(err.Error()).Should(HavePrefix("List values length 4 is not as expected 5"))
		Ω(vp).Should(BeNil())

		// Unknown data type
		vp, err = GetFactory().ReadLiveVectorParty("list/unknown_data_type")
		Ω(err).ShouldNot(BeNil())
		Ω(err.Error()).Should(HavePrefix("Unknown DataType when reading vector from file"))
		Ω(vp).Should(BeNil())

		// Successful read.
		vp, err = GetFactory().ReadLiveVectorParty("list/live_vp_uint32")
		Ω(err).Should(BeNil())
		Ω(vp).ShouldNot(BeNil())

		Ω(vp.GetLength()).Should(Equal(4))
		Ω(vp.GetDataType()).Should(Equal(common.ArrayUint32))
		Ω(vp.IsList()).Should(BeTrue())

		listVP := vp.AsList()
		// compare row 0.
		// 1
		val, valid := listVP.GetListValue(0)
		Ω(valid).Should(BeTrue())
		reader := common.NewArrayValueReader(common.Uint32, val)
		Ω(reader.GetLength()).Should(Equal(1))
		cmpFunc := common.GetCompareFunc(common.Uint32)
		var expectedVal uint32 = 1
		Ω(cmpFunc(reader.Get(0), unsafe.Pointer(&expectedVal))).Should(BeZero())
		Ω(reader.IsItemValid(0)).Should(BeTrue())

		// row 1
		// 1 null 3
		val, valid = listVP.GetListValue(1)
		Ω(valid).Should(BeTrue())
		reader = common.NewArrayValueReader(common.Uint32, val)
		Ω(reader.GetLength()).Should(Equal(3))
		expectedVal = 1
		Ω(cmpFunc(reader.Get(0), unsafe.Pointer(&expectedVal))).Should(BeZero())
		Ω(reader.IsItemValid(0)).Should(BeTrue())

		Ω(reader.IsItemValid(1)).Should(BeFalse())

		expectedVal = 3
		Ω(cmpFunc(reader.Get(2), unsafe.Pointer(&expectedVal))).Should(BeZero())
		Ω(reader.IsItemValid(2)).Should(BeTrue())

		// row 2
		val, valid = listVP.GetListValue(2)
		Ω(valid).Should(BeFalse())

		// row 3
		// null
		val, valid = listVP.GetListValue(3)
		Ω(valid).Should(BeFalse())
	})
})
