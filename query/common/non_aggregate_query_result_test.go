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

var _ = ginkgo.Describe("non aggregated query result", func() {
	ginkgo.It("Append should work", func() {
		res := AQLNonAggregatedQueryResult{}
		res.Append(0, 1)
		Ω(res).Should(Equal(AQLNonAggregatedQueryResult{
			"matrixData": [][]interface{}{
				{1},
			},
		}))
	})

	ginkgo.It("Append should fail", func() {
		res := AQLNonAggregatedQueryResult{}
		err := res.Append(1, 1)
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("SetHeaders should work", func() {
		res := AQLNonAggregatedQueryResult{}
		res.SetHeaders([]string{"field1", "field2"})
		Ω(res).Should(Equal(AQLNonAggregatedQueryResult{
			"headers": []string{"field1", "field2"},
		}))
	})
})
