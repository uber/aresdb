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
		vp, err := GetFactory().ReadListVectorParty("fancy_non_existent_name")
		Ω(err).ShouldNot(BeNil())
		Ω(err.Error()).Should(HavePrefix("open ../../testing/data/vps/list/fancy_non_existent_name: " +
			"no such file or directory"))
		Ω(vp).Should(BeNil())

		// Length not match
		vp, err = GetFactory().ReadListVectorParty("length_not_match")
		Ω(err).ShouldNot(BeNil())
		Ω(err.Error()).Should(HavePrefix("List values length 4 is not as expected 5"))
		Ω(vp).Should(BeNil())

		// Unknown data type
		vp, err = GetFactory().ReadListVectorParty("unknown_data_type")
		Ω(err).ShouldNot(BeNil())
		Ω(err.Error()).Should(HavePrefix("Unknown DataType when reading vector from file"))
		Ω(vp).Should(BeNil())

		// Successful read.
		vp, err = GetFactory().ReadListVectorParty("live_vp_uint32")
		Ω(err).Should(BeNil())
		Ω(vp).ShouldNot(BeNil())

		Ω(vp.GetLength()).Should(Equal(4))
		Ω(vp.GetDataType()).Should(Equal(common.Uint32))
		Ω(vp.IsList()).Should(BeTrue())

		listVP := vp.AsList()
		// compare row 0.
		// 1
		Ω(listVP.GetElementLength(0)).Should(Equal(1))
		cmpFunc := common.GetCompareFunc(common.Uint32)
		var expectedVal uint32 = 1
		Ω(cmpFunc(listVP.ReadElementValue(0, 0), unsafe.Pointer(&expectedVal))).Should(BeZero())
		Ω(listVP.ReadElementValidity(0, 0)).Should(BeTrue())

		// row 1
		// 1 null 3
		Ω(listVP.GetElementLength(1)).Should(Equal(3))
		expectedVal = 1
		Ω(cmpFunc(listVP.ReadElementValue(1, 0), unsafe.Pointer(&expectedVal))).Should(BeZero())
		Ω(listVP.ReadElementValidity(1, 0)).Should(BeTrue())

		Ω(listVP.ReadElementValidity(1, 1)).Should(BeFalse())

		expectedVal = 3
		Ω(cmpFunc(listVP.ReadElementValue(1, 2), unsafe.Pointer(&expectedVal))).Should(BeZero())
		Ω(listVP.ReadElementValidity(1, 2)).Should(BeTrue())

		// row 2
		Ω(listVP.GetElementLength(2)).Should(Equal(0))

		// row 3
		// null
		Ω(listVP.GetElementLength(3)).Should(Equal(1))
		Ω(listVP.ReadElementValidity(3, 0)).Should(BeFalse())
	})
})
