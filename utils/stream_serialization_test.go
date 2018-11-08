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
	"bytes"

	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"io/ioutil"
	"os"
		"io"
)

var _ = ginkgo.Describe("stream serialization", func() {

	ginkgo.It("works for int8", func() {
		bs := make([]byte, 0, 2)
		buf := bytes.NewBuffer(bs)
		writer := NewStreamDataWriter(buf)
		Ω(writer.WriteInt8(-1)).Should(BeNil())
		Ω(writer.WriteInt8(1)).Should(BeNil())
		reader := NewStreamDataReader(bytes.NewReader(buf.Bytes()))
		var v int8
		var err error
		v, err = reader.ReadInt8()
		Ω(err).Should(BeNil())
		Ω(v).Should(BeEquivalentTo(-1))

		v, err = reader.ReadInt8()
		Ω(err).Should(BeNil())
		Ω(v).Should(BeEquivalentTo(1))
	})

	ginkgo.It("works for skipBytes", func() {
		bs := make([]byte, 0, 2)
		buf := bytes.NewBuffer(bs)
		writer := NewStreamDataWriter(buf)
		writer.SkipBytes(2)
		reader := NewStreamDataReader(bytes.NewReader(buf.Bytes()))
		var v int8
		var err error
		v, err = reader.ReadInt8()
		Ω(err).Should(BeNil())
		Ω(v).Should(BeEquivalentTo(0))

		v, err = reader.ReadInt8()
		Ω(err).Should(BeNil())
		Ω(v).Should(BeEquivalentTo(0))
	})

	ginkgo.It("works for uint8", func() {
		bs := make([]byte, 0, 2)
		buf := bytes.NewBuffer(bs)
		writer := NewStreamDataWriter(buf)
		Ω(writer.WriteUint8(1)).Should(BeNil())
		Ω(writer.WriteUint8(2)).Should(BeNil())

		reader := NewStreamDataReader(bytes.NewReader(buf.Bytes()))
		var v uint8
		var err error
		v, err = reader.ReadUint8()
		Ω(err).Should(BeNil())
		Ω(v).Should(BeEquivalentTo(1))

		v, err = reader.ReadUint8()
		Ω(err).Should(BeNil())
		Ω(v).Should(BeEquivalentTo(2))
	})

	ginkgo.It("works for int16", func() {
		bs := make([]byte, 0, 2*2)
		buf := bytes.NewBuffer(bs)
		writer := NewStreamDataWriter(buf)
		Ω(writer.WriteInt16(-130)).Should(BeNil())
		Ω(writer.WriteInt16(130)).Should(BeNil())

		reader := NewStreamDataReader(bytes.NewReader(buf.Bytes()))
		var v int16
		var err error
		v, err = reader.ReadInt16()
		Ω(err).Should(BeNil())
		Ω(v).Should(BeEquivalentTo(-130))

		v, err = reader.ReadInt16()
		Ω(err).Should(BeNil())
		Ω(v).Should(BeEquivalentTo(130))
	})

	ginkgo.It("works for uint16", func() {
		bs := make([]byte, 0, 2*2)
		buf := bytes.NewBuffer(bs)
		writer := NewStreamDataWriter(buf)
		Ω(writer.WriteUint16(130)).Should(BeNil())
		Ω(writer.WriteUint16(131)).Should(BeNil())

		reader := NewStreamDataReader(bytes.NewReader(buf.Bytes()))
		var v uint16
		var err error
		v, err = reader.ReadUint16()
		Ω(err).Should(BeNil())
		Ω(v).Should(BeEquivalentTo(130))

		v, err = reader.ReadUint16()
		Ω(err).Should(BeNil())
		Ω(v).Should(BeEquivalentTo(131))
	})

	ginkgo.It("works for int32", func() {
		bs := make([]byte, 0, 4*2)
		buf := bytes.NewBuffer(bs)
		writer := NewStreamDataWriter(buf)
		Ω(writer.WriteInt32(-23456789)).Should(BeNil())
		Ω(writer.WriteInt32(23456789)).Should(BeNil())

		reader := NewStreamDataReader(bytes.NewReader(buf.Bytes()))
		var v int32
		var err error
		v, err = reader.ReadInt32()
		Ω(err).Should(BeNil())
		Ω(v).Should(BeEquivalentTo(-23456789))

		v, err = reader.ReadInt32()
		Ω(err).Should(BeNil())
		Ω(v).Should(BeEquivalentTo(23456789))
	})

	ginkgo.It("works for uint32", func() {
		bs := make([]byte, 0, 4*2)
		buf := bytes.NewBuffer(bs)
		writer := NewStreamDataWriter(buf)
		Ω(writer.WriteUint32(23456790)).Should(BeNil())
		Ω(writer.WriteUint32(23456791)).Should(BeNil())

		reader := NewStreamDataReader(bytes.NewReader(buf.Bytes()))
		var v uint32
		var err error
		v, err = reader.ReadUint32()
		Ω(err).Should(BeNil())
		Ω(v).Should(BeEquivalentTo(23456790))

		v, err = reader.ReadUint32()
		Ω(err).Should(BeNil())
		Ω(v).Should(BeEquivalentTo(23456791))
	})

	ginkgo.It("works for uint64", func() {
		bs := make([]byte, 0, 8*2)
		buf := bytes.NewBuffer(bs)
		writer := NewStreamDataWriter(buf)
		Ω(writer.WriteUint64(20174294967296)).Should(BeNil())
		Ω(writer.WriteUint64(20174294967297)).Should(BeNil())

		reader := NewStreamDataReader(bytes.NewReader(buf.Bytes()))
		var v uint64
		var err error
		v, err = reader.ReadUint64()
		Ω(err).Should(BeNil())
		Ω(v).Should(BeEquivalentTo(20174294967296))

		v, err = reader.ReadUint64()
		Ω(err).Should(BeNil())
		Ω(v).Should(BeEquivalentTo(20174294967297))
	})

	ginkgo.It("works for float32", func() {
		bs := make([]byte, 0, 4*2)
		buf := bytes.NewBuffer(bs)
		writer := NewStreamDataWriter(buf)
		Ω(writer.WriteFloat32(-0.1)).Should(BeNil())
		Ω(writer.WriteFloat32(0.1)).Should(BeNil())

		reader := NewStreamDataReader(bytes.NewReader(buf.Bytes()))
		var v float32
		var err error
		v, err = reader.ReadFloat32()
		Ω(err).Should(BeNil())
		Ω(v).Should(BeEquivalentTo(float32(-0.1)))

		v, err = reader.ReadFloat32()
		Ω(err).Should(BeNil())
		Ω(v).Should(BeEquivalentTo(float32(0.1)))
	})

	ginkgo.It("works for eof", func() {
		bs := make([]byte, 0)
		buf := bytes.NewBuffer(bs)
		reader := NewStreamDataReader(bytes.NewReader(buf.Bytes()))
		var err error
		_, err = reader.ReadFloat32()
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("works for big bytes slice (>1GB)", func() {
		sourceBytes := make([]byte, 10)
		buf := bytes.NewBuffer(sourceBytes)
		reader := NewStreamDataReader(bytes.NewReader(buf.Bytes()))
		var err error
		dstBytes := make([]byte, 1<<31)
		err = reader.Read(dstBytes)
		Ω(err).ShouldNot(BeNil())

		tmpfile, err := ioutil.TempFile("",
			"utils_stream_serialization_test")
		Ω(err).Should(BeNil())
		defer os.Remove(tmpfile.Name())

		sourceBytes = make([]byte, 1<<31)
		sourceBytes[1<<30] = 1
		sourceBytes[(1<<30)+1] = 2

		_, err = tmpfile.Write(sourceBytes)
		Ω(tmpfile.Close()).Should(BeNil())
		Ω(err).Should(BeNil())
		tmpfileForRead, err := os.Open(tmpfile.Name())
		Ω(err).Should(BeNil())
		defer tmpfileForRead.Close()
		reader = NewStreamDataReader(tmpfileForRead)
		Ω(reader.Read(dstBytes)).Should(BeNil())
		Ω(bytes.Equal(dstBytes, sourceBytes)).Should(BeTrue())
		Ω(reader.Read(dstBytes)).Should(Equal(io.EOF))
	})

	ginkgo.It("works for insufficient length", func() {
		bs := make([]byte, 2)
		buf := bytes.NewBuffer(bs)
		reader := NewStreamDataReader(bytes.NewReader(buf.Bytes()))
		var err error
		_, err = reader.ReadFloat32()
		Ω(err).ShouldNot(BeNil())
	})

	ginkgo.It("works for skip bytes", func() {
		bs := make([]byte, 0, 4*2)
		buf := bytes.NewBuffer(bs)
		writer := NewStreamDataWriter(buf)
		Ω(writer.SkipBytes(4)).Should(BeNil())
		Ω(writer.SkipBytes(4)).Should(BeNil())

		reader := NewStreamDataReader(bytes.NewReader(buf.Bytes()))
		Ω(reader.SkipBytes(4)).Should(BeNil())
		Ω(reader.SkipBytes(4)).Should(BeNil())

		// EOF.
		_, err := reader.ReadFloat32()
		Ω(err).ShouldNot(BeNil())
	})
})
