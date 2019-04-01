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

package message

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("json decoder tests", func() {
	It("json decoder must pass", func() {
		jd := &JSONDecoder{}
		msg := &stringMessage{
			msg: `{"project": "ares-subscriber"}`,
		}
		m, err := jd.DecodeMsg(msg)
		Ω(err).Should(BeNil())
		Ω(m).ShouldNot(BeNil())
		Ω(m.DecodedMessage[MsgPrefix].(map[string]interface{})["project"]).Should(Equal("ares-subscriber"))

		msg = &stringMessage{
			msg: `{"ts":1.468449680235607e+09}`,
		}
		m, err = jd.DecodeMsg(msg)
		Ω(err).Should(BeNil())
		Ω(m).ShouldNot(BeNil())
		Ω(m.MsgMetaDataTS).Should(Equal(time.Unix(0, int64(1468449680000)*int64(time.Millisecond))))

		msg = &stringMessage{
			msg: `{"_updated":"2016-07-12T22:5478009+00:00"}`,
		}
		m, err = jd.DecodeMsg(msg)
		Ω(err).Should(BeNil())
		Ω(m).ShouldNot(BeNil())
	})

	It("json decoder will fail", func() {
		jd := &JSONDecoder{}
		msg := &stringMessage{
			msg: "project",
		}
		m, err := jd.DecodeMsg(msg)
		Ω(err).ShouldNot(BeNil())
		Ω(m).Should(BeNil())
	})
})
