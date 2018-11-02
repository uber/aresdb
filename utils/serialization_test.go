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

var _ = ginkgo.Describe("serialization", func() {

	ginkgo.It("works for alignment", func() {
		Ω(AlignOffset(0, 0)).Should(Equal(0))
		Ω(AlignOffset(0, 1)).Should(Equal(0))
		Ω(AlignOffset(0, 8)).Should(Equal(0))

		Ω(AlignOffset(1, 0)).Should(Equal(1))
		Ω(AlignOffset(1, 1)).Should(Equal(1))
		Ω(AlignOffset(1, 8)).Should(Equal(8))

		Ω(AlignOffset(7, 8)).Should(Equal(8))
		Ω(AlignOffset(8, 8)).Should(Equal(8))
		Ω(AlignOffset(9, 8)).Should(Equal(16))

		Ω(AlignOffset(15, 8)).Should(Equal(16))
		Ω(AlignOffset(16, 8)).Should(Equal(16))
		Ω(AlignOffset(17, 8)).Should(Equal(24))

		Ω(AlignOffset(16, 17)).Should(Equal(17))
		Ω(AlignOffset(17, 17)).Should(Equal(17))
		Ω(AlignOffset(25, 17)).Should(Equal(34))

		// Aligntment is idempotent.
		Ω(AlignOffset(AlignOffset(AlignOffset(15, 8), 8), 8)).Should(Equal(16))
	})

	ginkgo.It("works for bool", func() {
		buffer := make([]byte, 2)
		writer := NewBufferWriter(buffer)
		for i := 0; i < 16; i++ {
			value := i%3 == 0
			Ω(writer.AppendBool(value)).Should(BeNil())
		}
		Ω(writer.AppendBool(true)).ShouldNot(BeNil())

		// Output should be 1001001,001001001
		Ω(buffer[1]).Should(Equal(byte(0x92)))
		Ω(buffer[0]).Should(Equal(byte(0x49)))
	})

	ginkgo.It("works for int8", func() {
		buffer := make([]byte, 2)
		writer := NewBufferWriter(buffer)
		Ω(writer.AppendInt8(23)).Should(BeNil())
		Ω(writer.AppendInt8(-45)).Should(BeNil())
		Ω(writer.AppendInt8(67)).ShouldNot(BeNil())

		reader := NewBufferReader(buffer)
		result, err := reader.ReadInt8(0)
		Ω(result).Should(Equal(int8(23)))
		Ω(err).Should(BeNil())
		result, err = reader.ReadInt8(1)
		Ω(result).Should(Equal(int8(-45)))
		Ω(err).Should(BeNil())
		result, err = reader.ReadInt8(3)
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("works for uint8", func() {
		buffer := make([]byte, 2)
		writer := NewBufferWriter(buffer)
		Ω(writer.AppendUint8(23)).Should(BeNil())
		Ω(writer.AppendUint8(45)).Should(BeNil())
		Ω(writer.AppendUint8(67)).ShouldNot(BeNil())

		reader := NewBufferReader(buffer)
		result, err := reader.ReadUint8(0)
		Ω(result).Should(Equal(uint8(23)))
		Ω(err).Should(BeNil())
		result, err = reader.ReadUint8(1)
		Ω(result).Should(Equal(uint8(45)))
		Ω(err).Should(BeNil())
		result, err = reader.ReadUint8(2)
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("works for int16", func() {
		buffer := make([]byte, 3)
		writer := NewBufferWriter(buffer)
		Ω(writer.AppendInt16(-23456)).Should(BeNil())
		Ω(writer.AppendInt16(45)).ShouldNot(BeNil())

		reader := NewBufferReader(buffer)
		result, err := reader.ReadInt16(0)
		Ω(result).Should(Equal(int16(-23456)))
		Ω(err).Should(BeNil())
		result, err = reader.ReadInt16(2)
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("works for uint16", func() {
		buffer := make([]byte, 3)
		writer := NewBufferWriter(buffer)
		Ω(writer.AppendUint16(23456)).Should(BeNil())
		Ω(writer.AppendUint16(45)).ShouldNot(BeNil())

		reader := NewBufferReader(buffer)
		result, err := reader.ReadUint16(0)
		Ω(result).Should(Equal(uint16(23456)))
		Ω(err).Should(BeNil())
		result, err = reader.ReadUint16(2)
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("works for int32", func() {
		buffer := make([]byte, 4)
		writer := NewBufferWriter(buffer)
		Ω(writer.AppendInt32(-23456789)).Should(BeNil())
		Ω(writer.AppendInt32(45)).ShouldNot(BeNil())

		reader := NewBufferReader(buffer)
		result, err := reader.ReadInt32(0)
		Ω(result).Should(Equal(int32(-23456789)))
		Ω(err).Should(BeNil())
		result, err = reader.ReadInt32(4)
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("works for uint32", func() {
		buffer := make([]byte, 4)
		writer := NewBufferWriter(buffer)
		Ω(writer.AppendUint32(23456789)).Should(BeNil())
		Ω(writer.AppendUint32(45)).ShouldNot(BeNil())

		reader := NewBufferReader(buffer)
		result, err := reader.ReadUint32(0)
		Ω(result).Should(Equal(uint32(23456789)))
		Ω(err).Should(BeNil())
		result, err = reader.ReadUint32(4)
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("works for uint64", func() {
		buffer := make([]byte, 15)
		writer := NewBufferWriter(buffer)
		Ω(writer.AppendUint64(20174294967296)).Should(BeNil())
		Ω(writer.AppendUint64(45)).ShouldNot(BeNil())

		reader := NewBufferReader(buffer)
		result, err := reader.ReadUint64(0)
		Ω(result).Should(Equal(uint64(20174294967296)))
		Ω(err).Should(BeNil())
		result, err = reader.ReadUint64(8)
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("works for float32", func() {
		buffer := make([]byte, 7)
		writer := NewBufferWriter(buffer)
		Ω(writer.AppendFloat32(-1.23456)).Should(BeNil())
		Ω(writer.AppendFloat32(1.57)).ShouldNot(BeNil())

		reader := NewBufferReader(buffer)
		result, err := reader.ReadFloat32(0)
		Ω(result).Should(Equal(float32(-1.23456)))
		Ω(err).Should(BeNil())
		result, err = reader.ReadFloat32(4)
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("works for byte slice", func() {
		buffer := make([]byte, 3)
		writer := NewBufferWriter(buffer)
		Ω(writer.Append([]byte{1})).Should(BeNil())
		Ω(writer.Append([]byte{2, 3})).Should(BeNil())

		reader := NewBufferReader(buffer)
		result, err := reader.ReadUint8(0)
		Ω(result).Should(Equal(uint8(1)))
		Ω(err).Should(BeNil())
		result, err = reader.ReadUint8(1)
		Ω(result).Should(Equal(uint8(2)))
		Ω(err).Should(BeNil())
		result, err = reader.ReadUint8(2)
		Ω(result).Should(Equal(uint8(3)))
		Ω(err).Should(BeNil())
	})

	ginkgo.It("works for mixed type append", func() {
		buffer := make([]byte, 9)
		writer := NewBufferWriter(buffer)
		// Byte 1.
		Ω(writer.AppendBool(false)).Should(BeNil())
		Ω(writer.AppendBool(true)).Should(BeNil())
		// Byte 1, 2.
		Ω(writer.AppendInt16(-356)).Should(BeNil())
		// Align to byte 4.
		writer.AlignBytes(4)
		// Byte 4, 5, 6, 7
		Ω(writer.AppendFloat32(-3.14159)).Should(BeNil())
		// Byte 8
		Ω(writer.AppendBool(true)).Should(BeNil())
		Ω(writer.AppendBool(true)).Should(BeNil())

		reader := NewBufferReader(buffer)
		resultBool, err := reader.ReadUint8(0)
		Ω(err).Should(BeNil())
		Ω(resultBool).Should(Equal(uint8(0x02))) // 00000010

		resultInt16, err := reader.ReadInt16(1)
		Ω(err).Should(BeNil())
		Ω(resultInt16).Should(Equal(int16(-356)))

		resultFloat32, err := reader.ReadFloat32(4)
		Ω(err).Should(BeNil())
		Ω(resultFloat32).Should(Equal(float32(-3.14159)))

		resultBool, err = reader.ReadUint8(8)
		Ω(err).Should(BeNil())
		Ω(resultBool).Should(Equal(uint8(0x03))) // 00000011
	})

	ginkgo.It("works for random write", func() {
		buffer := make([]byte, 8)
		writer := NewBufferWriter(buffer)
		Ω(writer.WriteInt8(-1, 0)).Should(BeNil())
		Ω(writer.WriteUint8(234, 1)).Should(BeNil())
		Ω(writer.WriteInt16(-567, 0)).Should(BeNil())
		Ω(writer.WriteUint16(8910, 1)).Should(BeNil())
		Ω(writer.WriteInt32(-1112131415, 0)).Should(BeNil())
		Ω(writer.WriteUint32(1617181920, 1)).Should(BeNil())
		Ω(writer.WriteUint64(20174294967296, 0)).Should(BeNil())
		Ω(writer.WriteFloat32(-12.313412, 1)).Should(BeNil())
	})

	ginkgo.It("skips to correct offset", func() {
		buffer := make([]byte, 8)
		writer := NewBufferWriter(buffer)
		writer.SkipBytes(5)
		Ω(writer.GetOffset()).Should(Equal(5))
		writer.SkipBits(3)
		Ω(writer.GetOffset()).Should(Equal(5))
		Ω(writer.bitOffset).Should(Equal(3))
		writer.SkipBits(4)
		Ω(writer.GetOffset()).Should(Equal(5))
		Ω(writer.bitOffset).Should(Equal(7))
		writer.SkipBits(13)
		Ω(writer.GetOffset()).Should(Equal(7))
		Ω(writer.bitOffset).Should(Equal(4))
	})
})
