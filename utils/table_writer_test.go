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

package utils

import (
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type testRow struct {
	name  string
	count int
	value float64
}

type testDataSource []testRow

func (ds testDataSource) NumRows() int {
	return len(ds)
}

func (ds testDataSource) GetValue(row, col int) interface{} {
	rowValue := ds[row]
	if col == 0 {
		return rowValue.name
	} else if col == 1 {
		return rowValue.count
	}
	return rowValue.value
}

func (ds testDataSource) ColumnHeaders() []string {
	return []string{"name", "count", "value"}
}

var _ = ginkgo.Describe("table writer", func() {
	ginkgo.It("getFormatModifier should work", func() {
		Ω(getFormatModifier("ss")).Should(Equal("s"))
		Ω(getFormatModifier(1.1)).Should(Equal(".2f"))
		Ω(getFormatModifier(struct{}{})).Should(Equal("v"))
	})

	ginkgo.It("expandColumnWidth should work", func() {
		columnWidth := make([]int, 3)
		expandColumnWidth(columnWidth, "string", 0)
		expandColumnWidth(columnWidth, 123.456, 1)
		expandColumnWidth(columnWidth, struct{}{}, 2)
		Ω(columnWidth).Should(Equal([]int{6, 6, 2}))
	})

	ginkgo.It("sprintfStrings should work", func() {
		Ω(sprintfStrings("%s|%s|%s", []string{"1", "2", "3"})).
			Should(Equal("1|2|3"))
	})

	ginkgo.It("WriteTable should work", func() {
		Ω(WriteTable(testDataSource{})).Should(Equal(
			"|name|count|value|\n",
		))

		ds := testDataSource{
			testRow{
				name:  "a",
				value: 1,
			},
			testRow{
				name:  "b",
				value: 2,
			},
		}
		Ω(WriteTable(ds)).Should(Equal(
			"|name|count|value|\n" +
				"|   a|    0| 1.00|\n" +
				"|   b|    0| 2.00|\n",
		))

		ds = testDataSource{
			testRow{
				name:  "jason",
				value: 123.456,
			},
			testRow{
				name:  "thomas",
				value: 3456.789,
			},
		}
		Ω(WriteTable(ds)).Should(Equal(
			"|  name|count|  value|\n" +
				"| jason|    0| 123.46|\n" +
				"|thomas|    0|3456.79|\n",
		))
	})
})
