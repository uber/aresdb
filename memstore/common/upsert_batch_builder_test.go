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
	"github.com/uber/aresdb/utils"
	"time"
)

var _ = ginkgo.Describe("upsert batch", func() {

	ginkgo.AfterEach(func() {
		utils.ResetClockImplementation()
	})

	ginkgo.It("works for empty batch", func() {
		builder := NewUpsertBatchBuilder()
		buffer, err := builder.ToByteArray()
		Ω(err).Should(BeNil())
		Ω(buffer).Should(Equal([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}))

		// write with new version
		utils.SetCurrentTime(time.Unix(10, 0))
		bufferNew, err := builder.ToByteArrayNew()
		Ω(err).Should(BeNil())
		Ω(bufferNew).Should(Equal([]byte{1, 0, 237, 254, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0}))
	})

	ginkgo.It("works for empty row", func() {
		builder := NewUpsertBatchBuilder()
		builder.AddColumn(123, Uint8)
		buffer, err := builder.ToByteArray()
		Ω(err).Should(BeNil())
		Ω(buffer).Should(Equal([]byte{0, 0, 0, 0, 1, 0, 0, 0, 23, 0, 0, 0, 23, 0, 0, 0, 8, 0, 2, 0, 123, 0, 0, 0}))
	})

	ginkgo.It("works for empty column", func() {
		builder := NewUpsertBatchBuilder()
		builder.AddRow()
		buffer, err := builder.ToByteArray()
		Ω(err).Should(BeNil())
		Ω(buffer).Should(Equal([]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}))
	})

	ginkgo.It("works for one row, one col, no value", func() {
		builder := NewUpsertBatchBuilder()
		builder.AddRow()
		err := builder.AddColumn(123, Uint8)
		Ω(err).Should(BeNil())
		buffer, err := builder.ToByteArray()
		Ω(err).Should(BeNil())
		Ω(buffer).Should(Equal([]byte{1, 0, 0, 0, 1, 0, 0, 0, 23, 0, 0, 0, 23, 0, 0, 0, 8, 0, 2, 0, 123, 0, 0, 0}))
	})

	ginkgo.It("works for one row, one col, one value", func() {
		builder := NewUpsertBatchBuilder()
		builder.AddRow()
		err := builder.AddColumn(123, Uint8)
		builder.SetValue(0, 0, uint8(135))
		buffer, err := builder.ToByteArray()
		Ω(err).Should(BeNil())
		Ω(buffer).Should(Equal([]byte{1, 0, 0, 0, 1, 0, 0, 0, 23, 0, 0, 0, 25, 0, 0, 0, 8, 0, 2, 0, 123, 0, 1, 0, 135, 0, 0, 0, 0, 0, 0, 0}))

	})

	ginkgo.It("reset row works", func() {
		builder := NewUpsertBatchBuilder()
		builder.AddRow()
		err := builder.AddColumn(123, Uint8)
		builder.SetValue(0, 0, uint8(135))
		builder.ResetRows()
		buffer, err := builder.ToByteArray()
		Ω(err).Should(BeNil())
		Ω(buffer).Should(Equal([]byte{0, 0, 0, 0, 1, 0, 0, 0, 23, 0, 0, 0, 23, 0, 0, 0, 8, 0, 2, 0, 123, 0, 0, 0}))
	})

	ginkgo.It("raises error when setting wrong value type", func() {
		builder := NewUpsertBatchBuilder()
		builder.AddRow()
		builder.AddColumn(123, Uint8)
		err := builder.SetValue(0, 0, "a")
		Ω(err).ShouldNot(BeNil())
		buffer, err := builder.ToByteArray()
		Ω(buffer).Should(Equal([]byte{1, 0, 0, 0, 1, 0, 0, 0, 23, 0, 0, 0, 23, 0, 0, 0, 8, 0, 2, 0, 123, 0, 0, 0}))
	})

	ginkgo.It("last value wins", func() {
		builder := NewUpsertBatchBuilder()
		builder.AddRow()
		builder.AddRow()
		builder.AddColumn(123, Bool)
		err := builder.SetValue(0, 0, false)
		Ω(err).Should(BeNil())

		buffer, err := builder.ToByteArray()
		Ω(err).Should(BeNil())
		Ω(buffer).Should(Equal([]byte{2, 0, 0, 0, 1, 0, 0, 0, 23, 0, 0, 0, 25, 0, 0, 0, 1, 0, 0, 0,
			123, 0, 2, 1, 0, 0, 0, 0, 0, 0, 0, 0}))

		buffer, err = builder.ToByteArray()
		Ω(err).Should(BeNil())
		Ω(buffer).Should(Equal([]byte{2, 0, 0, 0, 1, 0, 0, 0, 23, 0, 0, 0, 25, 0, 0, 0, 1, 0, 0, 0,
			123, 0, 2, 1, 0, 0, 0, 0, 0, 0, 0, 0}))

		err = builder.SetValue(0, 0, nil)
		Ω(err).Should(BeNil())
		Ω(buffer).Should(Equal([]byte{2, 0, 0, 0, 1, 0, 0, 0,
			23, 0, 0, 0, 25, 0, 0, 0, 1, 0, 0, 0, 123, 0, 2, 1, 0, 0, 0, 0, 0, 0, 0, 0}))

		buffer, err = builder.ToByteArray()
		Ω(err).Should(BeNil())
		Ω(buffer).Should(Equal([]byte{2, 0, 0, 0, 1, 0, 0, 0, 23, 0, 0, 0, 23, 0, 0, 0, 1, 0, 0, 0, 123, 0, 0, 0}))
	})

	ginkgo.It("works for bool type", func() {
		builder := NewUpsertBatchBuilder()

		// All null bools.
		err := builder.AddColumn(123, Bool)
		Ω(err).Should(BeNil())

		// Half valid bools.
		err = builder.AddColumn(456, Bool)
		builder.AddRow()
		Ω(err).Should(BeNil())
		err = builder.SetValue(0, 1, true)

		// All valid bools.
		err = builder.AddColumn(789, Bool)
		builder.AddRow()
		Ω(err).Should(BeNil())
		err = builder.SetValue(0, 2, true)
		Ω(err).Should(BeNil())
		err = builder.SetValue(1, 2, false)
		Ω(err).Should(BeNil())

		buffer, err := builder.ToByteArray()
		Ω(err).Should(BeNil())
		Ω(buffer).Should(Equal([]byte{2, 0, 0, 0, 3, 0, 0, 0, 45, 0, 0, 0, 45, 0, 0, 0, 49, 0, 0, 0, 57, 0, 0, 0, 1, 0,
			0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 123, 0, 200, 1, 21, 3, 0, 2, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
			1, 0, 0, 0, 0, 0, 0, 0}))
	})

	ginkgo.It("UpdateOverWrite add column ex fail", func() {
		builder := NewUpsertBatchBuilder()
		err := builder.AddColumn(1, Uint8)
		Ω(err).Should(BeNil())
		err = builder.AddColumnWithUpdateMode(2, Uint8, UpdateWithAddition)
		Ω(err).Should(BeNil())
		err = builder.AddColumnWithUpdateMode(2, Uint8, MaxColumnUpdateMode)
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("UpdateWithAddition test", func() {
		oldValue, _ := ValueFromString("null", Bool)
		newValue, _ := ValueFromString("1", Bool)
		finalVal, needUpdate, err := UpdateWithAdditionFunc(&oldValue, &newValue)
		Ω(err).ShouldNot(BeNil())

		oldValue, _ = ValueFromString("null", SmallEnum)
		newValue, _ = ValueFromString("1", SmallEnum)
		finalVal, needUpdate, err = UpdateWithAdditionFunc(&oldValue, &newValue)
		Ω(err).ShouldNot(BeNil())

		oldValue, _ = ValueFromString("null", Int8)
		newValue, _ = ValueFromString("1", Int8)
		finalVal, needUpdate, err = UpdateWithAdditionFunc(&oldValue, &newValue)
		Ω(err).Should(BeNil())
		Ω(needUpdate).Should(BeTrue())
		Ω(finalVal).Should(Equal(&newValue))

		oldValue, _ = ValueFromString("1", Int8)
		newValue, _ = ValueFromString("null", Int8)
		finalVal, needUpdate, err = UpdateWithAdditionFunc(&oldValue, &newValue)
		Ω(err).Should(BeNil())
		Ω(needUpdate).ShouldNot(BeTrue())
		Ω(finalVal).Should(Equal(&oldValue))

		oldValue, _ = ValueFromString("1", Int8)
		newValue, _ = ValueFromString("1", Int8)
		finalVal, needUpdate, err = UpdateWithAdditionFunc(&oldValue, &newValue)
		Ω(err).Should(BeNil())
		Ω(needUpdate).Should(BeTrue())
		v := *(*int8)(finalVal.OtherVal)
		Ω(v).Should(Equal(int8(2)))

		oldValue, _ = ValueFromString("1", Uint8)
		newValue, _ = ValueFromString("1", Uint8)
		finalVal, needUpdate, err = UpdateWithAdditionFunc(&oldValue, &newValue)
		Ω(err).Should(BeNil())
		Ω(needUpdate).Should(BeTrue())
		vuint8 := *(*uint8)(finalVal.OtherVal)
		Ω(vuint8).Should(Equal(uint8(2)))

		oldValue, _ = ValueFromString("1", Int16)
		newValue, _ = ValueFromString("1", Int16)
		finalVal, needUpdate, err = UpdateWithAdditionFunc(&oldValue, &newValue)
		Ω(err).Should(BeNil())
		Ω(needUpdate).Should(BeTrue())
		vint16 := *(*int16)(finalVal.OtherVal)
		Ω(vint16).Should(Equal(int16(2)))

		oldValue, _ = ValueFromString("1", Uint16)
		newValue, _ = ValueFromString("1", Uint16)
		finalVal, needUpdate, err = UpdateWithAdditionFunc(&oldValue, &newValue)
		Ω(err).Should(BeNil())
		Ω(needUpdate).Should(BeTrue())
		vuint16 := *(*uint16)(finalVal.OtherVal)
		Ω(vuint16).Should(Equal(uint16(2)))

		oldValue, _ = ValueFromString("1", Int32)
		newValue, _ = ValueFromString("1", Int32)
		finalVal, needUpdate, err = UpdateWithAdditionFunc(&oldValue, &newValue)
		Ω(err).Should(BeNil())
		Ω(needUpdate).Should(BeTrue())
		vint32 := *(*int32)(finalVal.OtherVal)
		Ω(vint32).Should(Equal(int32(2)))

		oldValue, _ = ValueFromString("1", Uint32)
		newValue, _ = ValueFromString("1", Uint32)
		finalVal, needUpdate, err = UpdateWithAdditionFunc(&oldValue, &newValue)
		Ω(err).Should(BeNil())
		Ω(needUpdate).Should(BeTrue())
		vuint32 := *(*uint32)(finalVal.OtherVal)
		Ω(vuint32).Should(Equal(uint32(2)))

		oldValue, _ = ValueFromString("1", Int64)
		newValue, _ = ValueFromString("1", Int64)
		finalVal, needUpdate, err = UpdateWithAdditionFunc(&oldValue, &newValue)
		Ω(err).Should(BeNil())
		Ω(needUpdate).Should(BeTrue())
		vint64 := *(*int64)(finalVal.OtherVal)
		Ω(vint64).Should(Equal(int64(2)))

		oldValue, _ = ValueFromString("1", Float32)
		newValue, _ = ValueFromString("1", Float32)
		finalVal, needUpdate, err = UpdateWithAdditionFunc(&oldValue, &newValue)
		Ω(err).Should(BeNil())
		Ω(needUpdate).Should(BeTrue())
		vfloat32 := *(*float32)(finalVal.OtherVal)
		Ω(vfloat32).Should(Equal(float32(2)))
	})

	ginkgo.It("UpdateWithMin test", func() {
		oldValue, _ := ValueFromString("null", Bool)
		newValue, _ := ValueFromString("1", Bool)
		finalVal, needUpdate, err := UpdateWithMinFunc(&oldValue, &newValue)
		Ω(err).ShouldNot(BeNil())

		oldValue, _ = ValueFromString("null", SmallEnum)
		newValue, _ = ValueFromString("1", SmallEnum)
		finalVal, needUpdate, err = UpdateWithMinFunc(&oldValue, &newValue)
		Ω(err).ShouldNot(BeNil())

		oldValue, _ = ValueFromString("null", Int8)
		newValue, _ = ValueFromString("1", Int8)
		finalVal, needUpdate, err = UpdateWithMinFunc(&oldValue, &newValue)
		Ω(err).Should(BeNil())
		Ω(needUpdate).Should(BeTrue())
		Ω(finalVal).Should(Equal(&newValue))

		oldValue, _ = ValueFromString("1", Int8)
		newValue, _ = ValueFromString("null", Int8)
		finalVal, needUpdate, err = UpdateWithMinFunc(&oldValue, &newValue)
		Ω(err).Should(BeNil())
		Ω(needUpdate).ShouldNot(BeTrue())
		Ω(finalVal).Should(Equal(&oldValue))

		oldValue, _ = ValueFromString("1", Int8)
		newValue, _ = ValueFromString("2", Int8)
		finalVal, needUpdate, err = UpdateWithMinFunc(&oldValue, &newValue)
		Ω(err).Should(BeNil())
		Ω(needUpdate).ShouldNot(BeTrue())
		Ω(finalVal).Should(Equal(&oldValue))

		oldValue, _ = ValueFromString("2", Int8)
		newValue, _ = ValueFromString("1", Int8)
		finalVal, needUpdate, err = UpdateWithMinFunc(&oldValue, &newValue)
		Ω(err).Should(BeNil())
		Ω(needUpdate).Should(BeTrue())
		Ω(finalVal).Should(Equal(&newValue))

		oldValue, _ = ValueFromString("3", Uint8)
		newValue, _ = ValueFromString("2", Uint8)
		finalVal, needUpdate, err = UpdateWithMinFunc(&oldValue, &newValue)
		Ω(err).Should(BeNil())
		Ω(needUpdate).Should(BeTrue())
		Ω(finalVal).Should(Equal(&newValue))

		oldValue, _ = ValueFromString("3", Int16)
		newValue, _ = ValueFromString("2", Int16)
		finalVal, needUpdate, err = UpdateWithMinFunc(&oldValue, &newValue)
		Ω(err).Should(BeNil())
		Ω(needUpdate).Should(BeTrue())
		Ω(finalVal).Should(Equal(&newValue))

		oldValue, _ = ValueFromString("3", Uint16)
		newValue, _ = ValueFromString("2", Uint16)
		finalVal, needUpdate, err = UpdateWithMinFunc(&oldValue, &newValue)
		Ω(err).Should(BeNil())
		Ω(needUpdate).Should(BeTrue())
		Ω(finalVal).Should(Equal(&newValue))

		oldValue, _ = ValueFromString("3", Int32)
		newValue, _ = ValueFromString("2", Int32)
		finalVal, needUpdate, err = UpdateWithMinFunc(&oldValue, &newValue)
		Ω(err).Should(BeNil())
		Ω(needUpdate).Should(BeTrue())
		Ω(finalVal).Should(Equal(&newValue))

		oldValue, _ = ValueFromString("3", Uint32)
		newValue, _ = ValueFromString("2", Uint32)
		finalVal, needUpdate, err = UpdateWithMinFunc(&oldValue, &newValue)
		Ω(err).Should(BeNil())
		Ω(needUpdate).Should(BeTrue())
		Ω(finalVal).Should(Equal(&newValue))

		oldValue, _ = ValueFromString("3", Int64)
		newValue, _ = ValueFromString("2", Int64)
		finalVal, needUpdate, err = UpdateWithMinFunc(&oldValue, &newValue)
		Ω(err).Should(BeNil())
		Ω(needUpdate).Should(BeTrue())
		Ω(finalVal).Should(Equal(&newValue))

		oldValue, _ = ValueFromString("3", Float32)
		newValue, _ = ValueFromString("2", Float32)
		finalVal, needUpdate, err = UpdateWithMinFunc(&oldValue, &newValue)
		Ω(err).Should(BeNil())
		Ω(needUpdate).Should(BeTrue())
		Ω(finalVal).Should(Equal(&newValue))
	})

	ginkgo.It("UpdateWithMax test", func() {
		oldValue, _ := ValueFromString("null", Bool)
		newValue, _ := ValueFromString("1", Bool)
		finalVal, needUpdate, err := UpdateWithMaxFunc(&oldValue, &newValue)
		Ω(err).ShouldNot(BeNil())

		oldValue, _ = ValueFromString("null", SmallEnum)
		newValue, _ = ValueFromString("1", SmallEnum)
		finalVal, needUpdate, err = UpdateWithMaxFunc(&oldValue, &newValue)
		Ω(err).ShouldNot(BeNil())

		oldValue, _ = ValueFromString("null", Int8)
		newValue, _ = ValueFromString("1", Int8)
		finalVal, needUpdate, err = UpdateWithMaxFunc(&oldValue, &newValue)
		Ω(err).Should(BeNil())
		Ω(needUpdate).Should(BeTrue())
		Ω(finalVal).Should(Equal(&newValue))

		oldValue, _ = ValueFromString("1", Int8)
		newValue, _ = ValueFromString("null", Int8)
		finalVal, needUpdate, err = UpdateWithMaxFunc(&oldValue, &newValue)
		Ω(err).Should(BeNil())
		Ω(needUpdate).ShouldNot(BeTrue())
		Ω(finalVal).Should(Equal(&oldValue))

		oldValue, _ = ValueFromString("2", Int8)
		newValue, _ = ValueFromString("1", Int8)
		finalVal, needUpdate, err = UpdateWithMaxFunc(&oldValue, &newValue)
		Ω(err).Should(BeNil())
		Ω(needUpdate).ShouldNot(BeTrue())
		Ω(finalVal).Should(Equal(&oldValue))

		oldValue, _ = ValueFromString("1", Int8)
		newValue, _ = ValueFromString("2", Int8)
		finalVal, needUpdate, err = UpdateWithMaxFunc(&oldValue, &newValue)
		Ω(err).Should(BeNil())
		Ω(needUpdate).Should(BeTrue())
		Ω(finalVal).Should(Equal(&newValue))

		oldValue, _ = ValueFromString("1", Uint8)
		newValue, _ = ValueFromString("2", Uint8)
		finalVal, needUpdate, err = UpdateWithMaxFunc(&oldValue, &newValue)
		Ω(err).Should(BeNil())
		Ω(needUpdate).Should(BeTrue())
		Ω(finalVal).Should(Equal(&newValue))

		oldValue, _ = ValueFromString("1", Int16)
		newValue, _ = ValueFromString("2", Int16)
		finalVal, needUpdate, err = UpdateWithMaxFunc(&oldValue, &newValue)
		Ω(err).Should(BeNil())
		Ω(needUpdate).Should(BeTrue())
		Ω(finalVal).Should(Equal(&newValue))

		oldValue, _ = ValueFromString("1", Uint16)
		newValue, _ = ValueFromString("2", Uint16)
		finalVal, needUpdate, err = UpdateWithMaxFunc(&oldValue, &newValue)
		Ω(err).Should(BeNil())
		Ω(needUpdate).Should(BeTrue())
		Ω(finalVal).Should(Equal(&newValue))

		oldValue, _ = ValueFromString("1", Int32)
		newValue, _ = ValueFromString("2", Int32)
		finalVal, needUpdate, err = UpdateWithMaxFunc(&oldValue, &newValue)
		Ω(err).Should(BeNil())
		Ω(needUpdate).Should(BeTrue())
		Ω(finalVal).Should(Equal(&newValue))

		oldValue, _ = ValueFromString("1", Uint32)
		newValue, _ = ValueFromString("2", Uint32)
		finalVal, needUpdate, err = UpdateWithMaxFunc(&oldValue, &newValue)
		Ω(err).Should(BeNil())
		Ω(needUpdate).Should(BeTrue())
		Ω(finalVal).Should(Equal(&newValue))

		oldValue, _ = ValueFromString("1", Int64)
		newValue, _ = ValueFromString("2", Int64)
		finalVal, needUpdate, err = UpdateWithMaxFunc(&oldValue, &newValue)
		Ω(err).Should(BeNil())
		Ω(needUpdate).Should(BeTrue())
		Ω(finalVal).Should(Equal(&newValue))

		oldValue, _ = ValueFromString("1", Float32)
		newValue, _ = ValueFromString("2", Float32)
		finalVal, needUpdate, err = UpdateWithMaxFunc(&oldValue, &newValue)
		Ω(err).Should(BeNil())
		Ω(needUpdate).Should(BeTrue())
		Ω(finalVal).Should(Equal(&newValue))
	})
})
