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
	"encoding/json"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"strconv"
)

var _ = ginkgo.Describe("AQL context", func() {
	ginkgo.It("column usage marshal json should work", func() {
		usage := columnUsage(15)
		b, err := json.Marshal(usage)
		Ω(err).Should(BeNil())
		Ω(strconv.Unquote(string(b))).Should(Equal("allBatches+liveBatches+firstArchiveBatch+lastArchiveBatch"))
	})
})
