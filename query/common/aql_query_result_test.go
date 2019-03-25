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
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = ginkgo.Describe("time series result", func() {
	ginkgo.It("SetHLL should work", func() {
		res := AQLQueryResult{}
		dim0 := "dim0"
		dim1 := "dim1"
		res.SetHLL([]*string{&dim0, &dim1, nil}, HLL{NonZeroRegisters: 1})
		Ω(res).Should(Equal(AQLQueryResult{
			"dim0": map[string]interface{}{
				"dim1": map[string]interface{}{
					"NULL": HLL{DenseData: nil, NonZeroRegisters: 1},
				},
			},
		}))
	})

	ginkgo.It("Set should work", func() {
		res := AQLQueryResult{}
		dim0 := "dim0"
		dim1 := "dim1"
		v := 0.01
		res.Set([]*string{&dim0, &dim1, nil}, &v)
		Ω(res).Should(Equal(AQLQueryResult{
			"dim0": map[string]interface{}{
				"dim1": map[string]interface{}{
					"NULL": 0.01,
				},
			},
		}))
	})

	ginkgo.It("Append should work", func() {
		res := AQLQueryResult{}
		str := "1"
		res.Append([]*string{&str})
		Ω(res).Should(Equal(AQLQueryResult{
			"matrixData": [][]interface{}{
				{"1"},
			},
		}))
	})

	ginkgo.It("SetHeaders should work", func() {
		res := AQLQueryResult{}
		res.SetHeaders([]string{"field1", "field2"})
		Ω(res).Should(Equal(AQLQueryResult{
			"headers": []string{"field1", "field2"},
		}))
	})
})
