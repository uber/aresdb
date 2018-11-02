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
	"time"

	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Time", func() {
	ginkgo.It("Should use the mocked time", func() {
		now := time.Unix(1498608694, 0)
		SetClockImplementation(func() time.Time {
			return now
		})
		Ω(Now()).Should(Equal(now))

		now = time.Unix(1498608604, 0)
		SetCurrentTime(now)
		Ω(Now()).Should(Equal(now))
	})

	ginkgo.It("FormatTimeStampToUTC should work", func() {
		Ω(FormatTimeStampToUTC(1498608694)).Should(Equal("2017-06-28T00:11:34Z"))
		Ω(FormatTimeStampToUTC(0)).Should(Equal("1970-01-01T00:00:00Z"))
	})

	ginkgo.It("CrossDST and CalculateDSTSwitchTs should work", func() {
		loc, _ := time.LoadLocation("Pacific/Auckland")
		Ω(CrossDST(1534680000, 1538439300, loc)).Should(BeTrue())
		Ω(CrossDST(1538439300, 1538439300, loc)).Should(BeFalse())
		Ω(CrossDST(1538439200, 1538439300, loc)).Should(BeFalse())

		// includes DST switch point
		switchTs, err := CalculateDSTSwitchTs(1534680000, 1538439300, loc)
		Ω(err).Should(BeNil())
		Ω(switchTs).Should(Equal(int64(1538229600)))

		// doesn't include
		switchTs, err = CalculateDSTSwitchTs(1538209600, 1538219600, loc)
		Ω(err).Should(BeNil())
		Ω(switchTs).Should(Equal(int64(0)))

		// size 0 range
		switchTs, err = CalculateDSTSwitchTs(1509872400, 1509872400, loc)
		Ω(err).Should(BeNil())
		Ω(switchTs).Should(Equal(int64(0)))

		loc, _ = time.LoadLocation("America/Los_Angeles")
		// includes DST switch point with a small range [-1, 1]
		switchTs, err = CalculateDSTSwitchTs(1509872399, 1509872401, loc)
		Ω(err).Should(BeNil())
		Ω(switchTs).Should(Equal(int64(1509872400)))

		switchTs, err = CalculateDSTSwitchTs(1509772400, 1509882400, loc)
		Ω(err).Should(BeNil())
		Ω(switchTs).Should(Equal(int64(1509872400)))
	})
})
