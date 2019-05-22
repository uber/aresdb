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
	"unsafe"

	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = ginkgo.Describe("primary key", func() {
	ginkgo.It("GetPrimaryKeyBytes should work", func() {
		var v1 uint16 = 0xA0B0
		var v2 uint32 = 0xC0D0E0F0
		dataValues := []DataValue{
			{
				Valid:    true,
				IsBool:   true,
				DataType: Bool,
				BoolVal:  true,
			},
			{
				Valid:    true,
				DataType: Uint16,
				OtherVal: unsafe.Pointer(&v1),
			},
			{
				Valid:    true,
				DataType: Uint32,
				OtherVal: unsafe.Pointer(&v2),
			},
		}

		key, err := GetPrimaryKeyBytes(dataValues, 7)
		Ω(err).Should(BeNil())
		Ω(key).Should(BeEquivalentTo([]byte{1, 0xB0, 0XA0, 0xF0, 0xE0, 0xD0, 0xC0}))
	})
})
