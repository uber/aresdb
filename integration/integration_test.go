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

package integration

import (
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	ex "github.com/uber/aresdb/examples/utils"
)

const (
	testHost = "localhost"
	testPort = 9374
)

var _ = ginkgo.Describe("Integration tests", func() {

	ginkgo.Context("Array related queries", func() {

		ginkgo.It("Array Length tests", func() {
			queryPath := "./test-data/queries/array_query_length.aql"
			expected := `{
	"results": [
		{
			"2019-06-08": {
				"0": 516,
				"1": 515,
				"2": 504,
				"3": 519,
				"NULL": 507
			},
			"2019-06-09": {
				"0": 76,
				"1": 77,
				"2": 88,
				"3": 72,
				"NULL": 85
			}
		}
	]
}`
			res := ex.MakeQuery(testHost, testPort, "array_length", ".aql", queryPath)
			Ω(res).Should(Equal(expected))
		})

		ginkgo.It("Array Contains tests", func() {
			queryPath := "./test-data/queries/array_query_contains.aql"
			expected := `{
	"results": [
		{
			"2019-06-08": 997,
			"2019-06-09": 157
		}
	]
}`
			res := ex.MakeQuery(testHost, testPort, "array_contains", ".aql", queryPath)
			Ω(res).Should(Equal(expected))
		})

		ginkgo.It("Array ElementAt tests", func() {
			queryPath := "./test-data/queries/array_query_elementat.aql"
			expected := `{
	"results": [
		{
			"2019-06-08": 1508,
			"2019-06-09": 248
		}
	]
}`
			res := ex.MakeQuery(testHost, testPort, "array_element_at", ".aql", queryPath)
			Ω(res).Should(Equal(expected))
		})
	})
})
