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
	"errors"
	"fmt"

	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Error Test", func() {
	ginkgo.It("Should recover from null pointer error", func() {
		err := RecoverWrap(func() error {
			var ps *string
			fmt.Println(*ps)
			return nil
		})
		Ω(err).ShouldNot(BeNil())
	})
	ginkgo.It("Should return the same error as inner func returns", func() {
		expectedErr := errors.New("test error")
		err := RecoverWrap(func() error {
			return expectedErr
		})
		Ω(err).ShouldNot(BeNil())
		Ω(err).Should(Equal(expectedErr))
	})
	ginkgo.It("No error should be returned", func() {
		err := RecoverWrap(func() error {
			return nil
		})
		Ω(err).Should(BeNil())
	})
})
