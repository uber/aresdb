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

var _ = ginkgo.Describe("UUID", func() {
	ginkgo.It("NormalizeUUIDString should work", func() {
		normalizedStr, err := NormalizeUUIDString("0xSHAHDHDHA")
		Ω(err).ShouldNot(BeNil())

		normalizedStr, err = NormalizeUUIDString("0x00000192F23D460DBE60400C32EA0667")
		Ω(err).Should(BeNil())
		Ω(normalizedStr).Should(Equal("00000192F23D460DBE60400C32EA0667"))

		normalizedStr, err = NormalizeUUIDString("34c501bab6e011e896f8529269fb1459")
		Ω(err).Should(BeNil())
		Ω(normalizedStr).Should(Equal("34C501BAB6E011E896F8529269FB1459"))

		normalizedStr, err = NormalizeUUIDString("34c501ba-b6e0-11e8-96f8-529269fb1459")
		Ω(err).Should(BeNil())
		Ω(normalizedStr).Should(Equal("34C501BAB6E011E896F8529269FB1459"))

		normalizedStr, err = NormalizeUUIDString("0x34c501ba-b6e0-11e8-96f8-529269fb1459")
		Ω(err).Should(BeNil())
		Ω(normalizedStr).Should(Equal("34C501BAB6E011E896F8529269FB1459"))
	})
})
