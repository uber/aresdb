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

var _ = ginkgo.Describe("Slices Test", func() {
	ginkgo.It("Test IndexOfStr", func() {
		Ω(IndexOfStr(nil, "a")).Should(BeEquivalentTo(-1))
		Ω(IndexOfStr([]string{}, "a")).Should(BeEquivalentTo(-1))
		Ω(IndexOfStr([]string{"a", "b", "c"}, "a")).Should(BeEquivalentTo(0))
		Ω(IndexOfStr([]string{"a", "b", "c"}, "c")).Should(BeEquivalentTo(2))
		Ω(IndexOfStr([]string{"a", "b", "c"}, "d")).Should(BeEquivalentTo(-1))
	})
	ginkgo.It("Test IndexOfInt", func() {
		Ω(IndexOfInt(nil, 0)).Should(BeEquivalentTo(-1))
		Ω(IndexOfInt([]int{}, 0)).Should(BeEquivalentTo(-1))
		Ω(IndexOfInt([]int{0, 1, 2}, 0)).Should(BeEquivalentTo(0))
		Ω(IndexOfInt([]int{0, 1, 2}, 2)).Should(BeEquivalentTo(2))
		Ω(IndexOfInt([]int{0, 1, 2}, 3)).Should(BeEquivalentTo(-1))
	})
})
