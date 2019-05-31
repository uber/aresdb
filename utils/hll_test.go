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

var _ = ginkgo.Describe("hyperloglog utils", func() {
	ginkgo.It("ComputeHLLValue should work", func() {
		tests := [][]interface{}{
			{uint64(0), uint32(3276800)},
			{uint64(0xffffffffffffffff), uint32(16383)},
			{uint64(0xf0f0f0f0f0f0f0f0), uint32(12528)},
			{uint64(0x0f0f0f0f0f0f0f0f), uint32(134927)},
			{uint64(8849112093580131862), uint32(15894)},
			{uint64(720734999560851427), uint32(266211)},
			{uint64(506097522914230528 ^ 1084818905618843912), uint32(329736)},
		}

		for _, test := range tests {
			input := test[0].(uint64)
			expected := test[1].(uint32)
			out := ComputeHLLValue(input)
			Ω(out).Should(Equal(expected))
		}
	})
})
