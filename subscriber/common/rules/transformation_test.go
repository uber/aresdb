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

package rules

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Transformation", func() {
	tranformationCfg := TransformationConfig{
		Type:    "SmallEnum",
		Source:  "status",
		Default: "ACTIVE",
		Context: map[string]string{},
	}
	It("transform", func() {
		from := "START"
		to, err := tranformationCfg.Transform(from)
		Ω(err).Should(BeNil())
		Ω(to).Should(Equal("START"))
	})
	It("transform with default", func() {
		to, err := tranformationCfg.Transform(nil)
		Ω(err).Should(BeNil())
		Ω(to).Should(BeNil())
	})
})
